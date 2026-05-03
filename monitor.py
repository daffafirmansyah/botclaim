"""
Qolvex auto-withdraw — watch-loop mode (multi-account).

Polls the site's dev / hot wallet (GXZB...dne11) on Solana mainnet. When
the balance goes up by more than TOPUP_THRESHOLD_LAMPORTS (i.e. the admin
topped it up so payouts can flow), this script iterates over every
account in config.json and fires ONE withdraw per eligible account.

Eligibility per account:
  * not inside its observed ~24h daily cooldown, and
  * at least PER_ACCOUNT_SPACING_SEC has passed since that account's
    previous attempt (per-cookie rate limit — ~3 req / 60 s like claimyshare).

State (last hot balance + per-account last success / attempt times) is
persisted to state.json so restarts don't re-fire attempts.

Run:
    python monitor.py

Ctrl+C stops cleanly.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from core import (
    DAILY_COOLDOWN_SEC,
    EXIT_COOLDOWN,
    EXIT_OK,
    HOT_WALLET,
    attempt_withdraw,
    bootstrap_last_success_iso,
    get_account_state,
    get_balance_lamports,
    iso_to_unix,
    load_accounts,
    load_state,
    make_logger,
    save_state,
    utc_now_iso,
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

POLL_INTERVAL_SEC = 10
# 0.1 SOL — only fire on real admin refills, not dust / tx-fee noise.
# Tune after observing typical topup sizes on qolvex's hot wallet.
TOPUP_THRESHOLD_LAMPORTS = 100_000_000

# Per-account spacing between attempts from the SAME account across
# different topup events. core.py's 429 retry logic absorbs any burst
# that exceeds the site's per-cookie rate limit.
PER_ACCOUNT_SPACING_SEC = 5

# Parallel firing: fire all eligible accounts simultaneously when a top-up
# is detected, instead of sequential with INTER_ACCOUNT_SPACING_SEC between
# them. Trade-off: makes the burst pattern from one IP more visible to WAF.
PARALLEL_FIRE = True
MAX_PARALLEL_WORKERS = 10
# Stagger the parallel dispatch so account #N waits N * PARALLEL_STAGGER_MS
# before its first request fires. 500ms = moderate: first 10 accts fire in
# ~5s (still good snipe window), rest trickle across ~50s for 100 accts.
# Previous 2ms burst (all 32 workers in <100ms) triggered qolvex's
# 'max 3 accounts per wallet' 403 during 2026-05-03 parallel run.
# Raise to 1000-2000 if 403 wallet-limit still appears under load.
PARALLEL_STAGGER_MS = 500

# Sequential fallback (only used if PARALLEL_FIRE = False):
INTER_ACCOUNT_SPACING_SEC = 5

# Stop firing if hot wallet drops below this — in parallel mode this is
# checked once before kicking off the batch; in sequential mode it's
# checked between accounts.
HOT_WALLET_FLOOR_LAMPORTS = 200_000  # ~0.0002 SOL

# Log a short "alive" line every N seconds even when nothing interesting
# is happening. Set to 0 to disable.
HEARTBEAT_INTERVAL_SEC = 300  # 5 minutes

_stop = False


def _handle_sigint(signum, frame):  # noqa: ARG001
    global _stop
    _stop = True
    print("\n[monitor] stop requested, finishing current iteration...", flush=True)


def _human_duration(sec: float) -> str:
    sec = max(0, int(sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _seconds_until_cooldown_ends(last_success_iso: Optional[str], now: float) -> float:
    if not last_success_iso:
        return 0.0
    remaining = (iso_to_unix(last_success_iso) + DAILY_COOLDOWN_SEC) - now
    return max(0.0, remaining)


def _eligible_accounts(accounts: list[dict], state: dict, now: float) -> list[dict]:
    eligible: list[dict] = []
    for acc in accounts:
        entry = get_account_state(state, acc["name"])
        if _seconds_until_cooldown_ends(entry["last_success_at"], now) > 0:
            continue
        if now - float(entry["last_attempt_ts"]) < PER_ACCOUNT_SPACING_SEC:
            continue
        eligible.append(acc)
    return eligible


def _bootstrap_accounts(accounts: list[dict], state: dict, log) -> None:
    """
    Fill last_success_at for each account from chain. Accounts that SHARE
    a wallet_address with any other account are SKIPPED (bootstrap can't
    distinguish them on-chain).
    """
    wallet_count: dict[str, int] = {}
    for acc in accounts:
        w = acc.get("wallet_address", "")
        wallet_count[w] = wallet_count.get(w, 0) + 1

    shared_wallets: dict[str, list[str]] = {}
    for acc in accounts:
        w = acc.get("wallet_address", "")
        if wallet_count.get(w, 0) > 1:
            shared_wallets.setdefault(w, []).append(acc["name"])

    for wallet, names in shared_wallets.items():
        preview = ", ".join(names[:3])
        if len(names) > 3:
            preview += f", ... +{len(names) - 3} more"
        log(
            f"[bootstrap-skip] wallet {wallet[:8]}...{wallet[-4:]} is shared "
            f"by {len(names)} account(s) [{preview}]; on-chain scan can't "
            "distinguish them, leaving cooldown untracked until first fire."
        )

        entries = [get_account_state(state, n) for n in names]
        never_fired = all(
            float(e.get("last_attempt_ts", 0.0) or 0.0) == 0.0 for e in entries
        )
        poisoned = [
            n for n, e in zip(names, entries)
            if e.get("last_success_at") is not None
        ]
        if never_fired and poisoned:
            log(
                f"[bootstrap-heal] clearing stale last_success_at on "
                f"{len(poisoned)} account(s) under {wallet[:8]}...{wallet[-4:]} "
                "(bootstrap artifact from a previous run)."
            )
            for entry in entries:
                entry["last_success_at"] = None

    for acc in accounts:
        entry = get_account_state(state, acc["name"])
        if entry["last_success_at"] is not None:
            continue
        if wallet_count.get(acc.get("wallet_address", ""), 0) > 1:
            continue
        log(f"[{acc['name']}] [bootstrap] scanning chain for last hot-wallet payout...")
        discovered = bootstrap_last_success_iso(acc["wallet_address"], log)
        if discovered:
            entry["last_success_at"] = discovered
    save_state(state)


def _log_startup_status(accounts: list[dict], state: dict, log) -> None:
    now = time.time()
    for acc in accounts:
        entry = get_account_state(state, acc["name"])
        lsa = entry["last_success_at"]
        if lsa:
            cd = _seconds_until_cooldown_ends(lsa, now)
            status = (
                f"ready ({_human_duration(-cd)} past cooldown)"
                if cd <= 0
                else f"cooldown ends in {_human_duration(cd)}"
            )
            log(f"[{acc['name']}] last success {lsa}, {status}")
        else:
            log(f"[{acc['name']}] no prior success; will attempt on first top-up.")


def _record_attempt_outcome(
    state: dict,
    acc: dict,
    exit_code: int,
    log,
) -> None:
    entry = get_account_state(state, acc["name"])
    if exit_code == EXIT_OK:
        entry["last_success_at"] = utc_now_iso()
        log(
            f"[{acc['name']}] [ok] success; next attempt earliest in "
            f"{_human_duration(DAILY_COOLDOWN_SEC)}."
        )
    elif exit_code == EXIT_COOLDOWN:
        entry["last_success_at"] = utc_now_iso()
        log(f"[{acc['name']}] [cooldown] server refused; assuming 24h from now.")
    else:
        log(f"[{acc['name']}] [error] failed (exit={exit_code}); keep cooldown unchanged.")


def _fire_one_threaded(
    acc: dict,
    state: dict,
    state_lock: threading.Lock,
    log,
    start_delay_sec: float = 0.0,
) -> tuple[dict, int]:
    if start_delay_sec > 0:
        time.sleep(start_delay_sec)
    with state_lock:
        entry = get_account_state(state, acc["name"])
        entry["last_attempt_ts"] = time.time()
    log(f"[{acc['name']}] [fire] starting withdraw.")
    exit_code, _parsed, _status = attempt_withdraw(acc, log, verify_onchain=False)
    with state_lock:
        _record_attempt_outcome(state, acc, exit_code, log)
    return acc, exit_code


def _process_topup_parallel(
    eligible: list[dict],
    state: dict,
    log,
) -> None:
    stagger = max(PARALLEL_STAGGER_MS, 0) / 1000.0
    total_dispatch = stagger * (len(eligible) - 1)
    log(
        f"[parallel] firing {len(eligible)} account(s) "
        f"(max workers={MAX_PARALLEL_WORKERS}, "
        f"stagger={PARALLEL_STAGGER_MS}ms => dispatch window {total_dispatch:.1f}s)."
    )
    state_lock = threading.Lock()
    workers = min(MAX_PARALLEL_WORKERS, len(eligible))

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="wd") as ex:
        futures = [
            ex.submit(_fire_one_threaded, acc, state, state_lock, log, i * stagger)
            for i, acc in enumerate(eligible)
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001
                log(f"[error] worker thread crashed: {e}")

    save_state(state)
    log(f"[parallel] all {len(eligible)} attempts complete.")


def _process_topup_sequential(
    eligible: list[dict],
    state: dict,
    log,
) -> None:
    for i, acc in enumerate(eligible):
        if _stop:
            break

        if i > 0:
            current_hot_now = get_balance_lamports(HOT_WALLET)
            if current_hot_now is not None and current_hot_now < HOT_WALLET_FLOOR_LAMPORTS:
                log(
                    f"[topup] hot wallet drained to "
                    f"{current_hot_now/1e9:.9f} SOL; aborting remaining "
                    f"{len(eligible) - i} account(s)."
                )
                break

        entry = get_account_state(state, acc["name"])
        log(f"[{acc['name']}] [fire] {i + 1}/{len(eligible)} starting withdraw.")
        entry["last_attempt_ts"] = time.time()
        exit_code, _parsed, _status = attempt_withdraw(acc, log, verify_onchain=False)
        _record_attempt_outcome(state, acc, exit_code, log)
        save_state(state)

        if i < len(eligible) - 1 and not _stop:
            _sleep_with_stop(INTER_ACCOUNT_SPACING_SEC)


def _process_topup(
    accounts: list[dict],
    state: dict,
    current_hot: int,
    prev_hot: int,
    log,
) -> None:
    now = time.time()
    eligible = _eligible_accounts(accounts, state, now)
    delta = current_hot - prev_hot
    log(
        f"[topup] hot wallet {prev_hot/1e9:.9f} -> {current_hot/1e9:.9f} SOL "
        f"(+{delta/1e9:.9f}); {len(eligible)} of {len(accounts)} account(s) eligible."
    )
    if not eligible:
        return

    pre_check = get_balance_lamports(HOT_WALLET)
    if pre_check is not None and pre_check < HOT_WALLET_FLOOR_LAMPORTS:
        log(
            f"[topup] hot wallet already drained to {pre_check/1e9:.9f} SOL "
            "before we could fire; aborting batch."
        )
        return

    if PARALLEL_FIRE:
        _process_topup_parallel(eligible, state, log)
    else:
        _process_topup_sequential(eligible, state, log)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Qolvex watch-loop auto-withdraw.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python monitor.py                      # normal run (no chain bootstrap)\n"
            "  python monitor.py --bootstrap          # seed last_success_at from on-chain history (unique-wallet setups only)\n"
            "  python monitor.py --reset-cooldowns    # clear all cooldowns on startup\n"
        ),
    )
    p.add_argument(
        "--reset-cooldowns",
        action="store_true",
        help=(
            "Clear last_success_at and last_attempt_ts for every account on "
            "startup, then skip the bootstrap chain scan."
        ),
    )
    p.add_argument(
        "--bootstrap",
        action="store_true",
        help=(
            "Run the on-chain scan to seed last_success_at for accounts "
            "with no prior history. DEFAULT IS OFF — multi-account setups "
            "sharing one destination wallet would get the same (wrong) "
            "cooldown. Pass this flag if you have unique wallets per account."
        ),
    )
    return p.parse_args()


def _reset_cooldowns(accounts: list[dict], state: dict, log) -> None:
    cleared = 0
    for acc in accounts:
        entry = get_account_state(state, acc["name"])
        had_success = entry.get("last_success_at") is not None
        entry["last_success_at"] = None
        entry["last_attempt_ts"] = 0.0
        if had_success:
            cleared += 1
    save_state(state)
    log(
        f"[reset] cleared cooldown for {cleared} of {len(accounts)} account(s). "
        "All accounts will be eligible on the next top-up."
    )


def main() -> int:
    args = _parse_args()
    accounts = load_accounts()
    log = make_logger("monitor.log")
    signal.signal(signal.SIGINT, _handle_sigint)

    log(
        f"monitor started | accounts={len(accounts)} "
        f"poll={POLL_INTERVAL_SEC}s topup>={TOPUP_THRESHOLD_LAMPORTS/1e9:.6f} SOL "
        f"reset_cooldowns={args.reset_cooldowns} bootstrap={args.bootstrap}"
    )
    log(f"watching hot wallet: {HOT_WALLET}")

    state = load_state()

    if args.reset_cooldowns:
        _reset_cooldowns(accounts, state, log)
    elif args.bootstrap:
        _bootstrap_accounts(accounts, state, log)
    else:
        log("[bootstrap] skipped (default — use --bootstrap to opt in).")

    _log_startup_status(accounts, state, log)

    last_balance = int(state.get("last_hot_balance_lamports", 0))
    last_logged_balance = last_balance
    last_heartbeat_ts = time.time()

    while not _stop:
        current = get_balance_lamports(HOT_WALLET)
        now = time.time()

        if current is None:
            log("[warn] RPC balance read failed; sleeping and retrying.")
            _sleep_with_stop(POLL_INTERVAL_SEC)
            continue

        if last_balance == 0:
            last_balance = current
            last_logged_balance = current
            state["last_hot_balance_lamports"] = current
            save_state(state)
            log(f"initial hot wallet balance: {current/1e9:.9f} SOL")

        delta = current - last_balance
        topup_detected = delta >= TOPUP_THRESHOLD_LAMPORTS

        if topup_detected:
            eligible_count = len(_eligible_accounts(accounts, state, now))
            if eligible_count > 0:
                _process_topup(accounts, state, current, last_balance, log)
            else:
                log(
                    f"[topup-skip] {last_balance/1e9:.9f} -> {current/1e9:.9f} SOL "
                    f"(+{delta/1e9:.9f}); all accounts in cooldown."
                )
            last_balance = current
            last_logged_balance = current

        else:
            if abs(current - last_logged_balance) >= TOPUP_THRESHOLD_LAMPORTS:
                log(
                    f"balance {last_logged_balance/1e9:.9f} -> "
                    f"{current/1e9:.9f} SOL (no topup)."
                )
                last_logged_balance = current
            last_balance = current

        state["last_hot_balance_lamports"] = last_balance
        save_state(state)

        if (
            HEARTBEAT_INTERVAL_SEC > 0
            and now - last_heartbeat_ts >= HEARTBEAT_INTERVAL_SEC
        ):
            eligible_now = _eligible_accounts(accounts, state, now)
            log(
                f"[heartbeat] alive | hot_wallet={current/1e9:.9f} SOL | "
                f"eligible={len(eligible_now)}/{len(accounts)} | "
                f"next poll in {POLL_INTERVAL_SEC}s"
            )
            last_heartbeat_ts = now

        _sleep_with_stop(POLL_INTERVAL_SEC)

    log("[monitor] stopped.")
    return 0


def _sleep_with_stop(seconds: int) -> None:
    end = time.time() + seconds
    while time.time() < end and not _stop:
        time.sleep(min(1.0, end - time.time()))


if __name__ == "__main__":
    sys.exit(main())
