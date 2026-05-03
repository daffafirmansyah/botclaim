"""
Qolvex auto-withdraw — one-shot mode.

Iterates over every account in config.json and sends ONE POST per account.
By default fires them in parallel via a thread pool to maximize the chance
of catching the current hot-wallet refill before it drains.

For the "attempt whenever hot wallet is topped up" behavior, use
monitor.py instead.

Exit code reflects the best outcome across accounts:
  0 if at least one account succeeded,
  2 if all attempts were cooldown (nothing to do right now),
  3 otherwise (see withdraw.log for details).
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from core import (
    EXIT_API_ERROR,
    EXIT_COOLDOWN,
    EXIT_NETWORK,
    EXIT_OK,
    attempt_withdraw,
    load_accounts,
    make_logger,
)

# Parallel firing: send all account POSTs concurrently. Set False to fall
# back to a sequential loop with INTER_ACCOUNT_SPACING_SEC between requests.
PARALLEL_FIRE = True
MAX_PARALLEL_WORKERS = 5
# Stagger the parallel dispatch so account #N waits N * PARALLEL_STAGGER_MS
# before its first request fires. 1000ms spacing chosen to dodge qolvex's
# 'max 3 accounts per wallet' 403 seen during burst runs (2026-05-03):
# at 5 workers + 1s stagger, never more than ~3 accts fire per 1-sec window.
# For 100 accts: dispatch window ~100s, max 5 in-flight.
# Raise to 2000/5000 if 403 wallet-limit appears, or drop to 0 for pure burst.
PARALLEL_STAGGER_MS = 1000

# Sequential fallback only.
INTER_ACCOUNT_SPACING_SEC = 5


def _fire_one(acc: dict, log, start_delay_sec: float = 0.0) -> int:
    if start_delay_sec > 0:
        time.sleep(start_delay_sec)
    log(f"[{acc['name']}] [fire] starting withdraw.")
    exit_code, _parsed, _status = attempt_withdraw(acc, log, verify_onchain=False)
    return exit_code


def _run_parallel(accounts: list[dict], log) -> list[int]:
    stagger = max(PARALLEL_STAGGER_MS, 0) / 1000.0
    total_dispatch = stagger * (len(accounts) - 1)
    log(
        f"[parallel] firing {len(accounts)} account(s) "
        f"(max workers={MAX_PARALLEL_WORKERS}, "
        f"stagger={PARALLEL_STAGGER_MS}ms => dispatch window {total_dispatch:.1f}s)."
    )
    workers = min(MAX_PARALLEL_WORKERS, len(accounts))
    results: list[int] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="wd") as ex:
        futures = [
            ex.submit(_fire_one, acc, log, i * stagger)
            for i, acc in enumerate(accounts)
        ]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:  # noqa: BLE001
                log(f"[error] worker thread crashed: {e}")
                results.append(EXIT_API_ERROR)
    return results


def _run_sequential(accounts: list[dict], log) -> list[int]:
    results: list[int] = []
    for i, acc in enumerate(accounts):
        log(
            f"[{acc['name']}] attempt {i + 1}/{len(accounts)} "
            f"| wallet={acc['wallet_address']} amount_sol={acc['amount_sol']}"
        )
        results.append(_fire_one(acc, log))
        if i < len(accounts) - 1:
            time.sleep(INTER_ACCOUNT_SPACING_SEC)
    return results


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Qolvex auto-withdraw, one-shot mode.",
    )
    p.add_argument(
        "--name",
        help="run only the account with this name (default: all accounts).",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    accounts = load_accounts()
    log = make_logger("withdraw.log")

    if args.name:
        match = [a for a in accounts if a.get("name") == args.name]
        if not match:
            print(f"[error] account {args.name!r} not found in config.json.",
                  file=sys.stderr)
            return EXIT_API_ERROR
        accounts = match

    log(
        f"one-shot start | accounts={[a['name'] for a in accounts]} "
        f"mode={'parallel' if PARALLEL_FIRE and len(accounts) > 1 else 'sequential'}"
    )

    if PARALLEL_FIRE and len(accounts) > 1:
        results = _run_parallel(accounts, log)
    else:
        results = _run_sequential(accounts, log)

    ok = sum(1 for c in results if c == EXIT_OK)
    cd = sum(1 for c in results if c == EXIT_COOLDOWN)
    err = sum(1 for c in results if c not in (EXIT_OK, EXIT_COOLDOWN))
    log(f"summary | ok={ok} cooldown={cd} error={err} total={len(results)}")

    if ok > 0:
        return EXIT_OK
    if cd == len(results) and len(results) > 0:
        return EXIT_COOLDOWN
    if any(c == EXIT_NETWORK for c in results):
        return EXIT_NETWORK
    return EXIT_API_ERROR


if __name__ == "__main__":
    sys.exit(main())
