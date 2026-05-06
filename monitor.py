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

from core import (
    BALANCE_CACHE_MAX_AGE_SEC,
    EXIT_COOLDOWN,
    EXIT_OK,
    HOT_WALLET,
    _quick_balance,
    attempt_withdraw,
    get_account_state,
    get_balance_lamports,
    invalidate_balance_cache,
    load_accounts,
    load_balance_cache,
    load_state,
    make_logger,
    priority_sort_accounts,
    save_state,
    update_balance_cache,
    utc_now_iso,
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

POLL_INTERVAL_SEC = 1
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
# One worker per account so EVERY account dispatches inside the stagger
# window regardless of how long the earlier ones spend retrying on 429/5xx.
# Previously capped at 50 — accounts 51-100 had to wait for a worker slot,
# and if the first 50 got stuck retrying for 30+s the back half never fired
# in time before the hot wallet drained. 100 worker threads is fine: each
# is mostly blocked on HTTP I/O (~20MB total memory, no CPU contention).
MAX_PARALLEL_WORKERS = 100
# Stagger the parallel dispatch so account #N waits N * PARALLEL_STAGGER_MS
# before its first request fires. 2ms = pure burst: 100 accts dispatched in
# ~200ms. Risk: WAF / 'max 3 accounts per wallet' 403 (seen 2026-05-03),
# 429 per-IP rate-limit storm. Mitigated by core.py infinite retry on
# 429/5xx — eventual success but log noise will be heavy.
PARALLEL_STAGGER_MS = 2

# Sequential fallback (only used if PARALLEL_FIRE = False):
INTER_ACCOUNT_SPACING_SEC = 5

# Background balance refresh thread — keeps balance_cache.json fresh without
# needing an external cron. Each cycle re-fetches every account whose cache
# entry is older than BALANCE_REFRESH_FRESH_AGE_SEC, so balance increases
# from completed tasks/earns are picked up automatically.
BALANCE_REFRESH_ENABLED = True
BALANCE_REFRESH_INTERVAL_SEC = 120        # sweep every 2 minutes
BALANCE_REFRESH_WORKERS = 2               # polite — avoid per-IP 429 storms
BALANCE_REFRESH_FETCH_TIMEOUT_SEC = 5     # per-request timeout in the sweep
# Anything older than this is considered stale and gets re-fetched.
# Setting it equal to the interval = every account refreshed every cycle
# (subject to per-IP rate-limit; failed fetches don't update fetched_at,
# so they stay "stale" and get retried automatically next cycle).
BALANCE_REFRESH_FRESH_AGE_SEC = 120

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


def _eligible_accounts(accounts: list[dict], state: dict, now: float) -> list[dict]:
    """Eligible = not fired in the last PER_ACCOUNT_SPACING_SEC seconds.

    Qolvex has no per-account 24h cooldown (claim is per-topup-event), so the
    only gate is a short debounce to prevent double-firing the same account
    if monitor.py detects two near-simultaneous topup deltas.
    """
    eligible: list[dict] = []
    for acc in accounts:
        entry = get_account_state(state, acc["name"])
        if now - float(entry["last_attempt_ts"]) < PER_ACCOUNT_SPACING_SEC:
            continue
        eligible.append(acc)
    return eligible


def _log_startup_status(accounts: list[dict], state: dict, log) -> None:
    """One-line summary on startup; no per-account cooldown to report."""
    prior = sum(
        1
        for a in accounts
        if get_account_state(state, a["name"])["last_success_at"]
    )
    log(
        f"startup: {len(accounts)} account(s) ready "
        f"({prior} have prior success, {len(accounts) - prior} first-time)."
    )


def _record_attempt_outcome(
    state: dict,
    acc: dict,
    exit_code: int,
    log,
) -> None:
    entry = get_account_state(state, acc["name"])
    if exit_code == EXIT_OK:
        entry["last_success_at"] = utc_now_iso()
        # Account just claimed its claimable balance; cache is now stale.
        # Drop it so the next priority_sort live-fetches the real value
        # (probably 0 until the next admin topup refills the per-account
        # claimable amount).
        invalidate_balance_cache(acc["name"])
        log(f"[{acc['name']}] [ok] success.")
    elif exit_code == EXIT_COOLDOWN:
        # Qolvex isn't expected to return cooldown; defensive fallback.
        entry["last_success_at"] = utc_now_iso()
        log(f"[{acc['name']}] [cooldown] server refused (unexpected on qolvex).")
    else:
        log(f"[{acc['name']}] [error] failed (exit={exit_code}).")


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

    # Priority + balance-desc sort so hafidz fires first and the next stagger
    # slots go to the highest-balance accounts. Adds ~3s latency at most.
    eligible = priority_sort_accounts(eligible, log)

    if PARALLEL_FIRE:
        _process_topup_parallel(eligible, state, log)
    else:
        _process_topup_sequential(eligible, state, log)


def _balance_refresh_loop(accounts: list[dict], log) -> None:
    """Daemon loop: periodically refresh stale entries in balance_cache.json.

    Wakes every BALANCE_REFRESH_INTERVAL_SEC, picks the subset of accounts
    whose cache entry is missing or older than BALANCE_REFRESH_FRESH_AGE_SEC,
    and live-fetches them with a small worker pool. Successful values are
    written back via update_balance_cache (which already merges atomically).

    Runs alongside the main poll loop — does NOT block fire timing. Exits
    cleanly when the global _stop flag flips.
    """
    log(
        f"[refresh] background thread started | interval="
        f"{BALANCE_REFRESH_INTERVAL_SEC}s, workers={BALANCE_REFRESH_WORKERS}, "
        f"fresh_threshold={BALANCE_REFRESH_FRESH_AGE_SEC}s"
    )
    # Initial small delay so we don't fight the first priority_sort fetch.
    _sleep_with_stop(15)

    while not _stop:
        try:
            cache = load_balance_cache()
            now = time.time()
            stale = [
                a for a in accounts
                if (
                    a["name"] not in cache
                    or now - float(cache[a["name"]].get("fetched_at", 0))
                    >= BALANCE_REFRESH_FRESH_AGE_SEC
                )
            ]
            if not stale:
                _sleep_with_stop(BALANCE_REFRESH_INTERVAL_SEC)
                continue

            results: dict[str, float] = {}
            workers = min(BALANCE_REFRESH_WORKERS, len(stale))
            with ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="refresh"
            ) as pool:
                futs = {
                    pool.submit(
                        _quick_balance, a, BALANCE_REFRESH_FETCH_TIMEOUT_SEC
                    ): a["name"]
                    for a in stale
                }
                for fut in as_completed(futs):
                    name = futs[fut]
                    try:
                        results[name] = fut.result()
                    except Exception:  # noqa: BLE001
                        results[name] = -1.0
                    if _stop:
                        break

            ok = sum(1 for v in results.values() if v >= 0)
            update_balance_cache(results)
            log(
                f"[refresh] swept {len(stale)} stale/missing | "
                f"updated={ok}, failed={len(results) - ok}"
            )
        except Exception as e:  # noqa: BLE001
            log(f"[refresh] cycle error (continuing): {e}")

        _sleep_with_stop(BALANCE_REFRESH_INTERVAL_SEC)

    log("[refresh] background thread stopped.")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Qolvex watch-loop auto-withdraw.")
    return p.parse_args()


def main() -> int:
    _parse_args()
    accounts = load_accounts()
    log = make_logger("monitor.log")
    signal.signal(signal.SIGINT, _handle_sigint)

    log(
        f"monitor started | accounts={len(accounts)} "
        f"poll={POLL_INTERVAL_SEC}s topup>={TOPUP_THRESHOLD_LAMPORTS/1e9:.6f} SOL"
    )
    log(f"watching hot wallet: {HOT_WALLET}")

    state = load_state()
    _log_startup_status(accounts, state, log)

    if BALANCE_REFRESH_ENABLED:
        refresh_thread = threading.Thread(
            target=_balance_refresh_loop,
            args=(accounts, log),
            name="balance-refresh",
            daemon=True,
        )
        refresh_thread.start()

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
