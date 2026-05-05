"""
Quick status dump for all accounts: name, claimable balance, wallet, last
successful withdraw.

Read-only — does NOT fire any withdraw. Just pings /api/stats/dashboard
for each account in parallel.

Usage:
    python check_status.py                # all 100 accounts
    python check_status.py --name folktor # single account
    python check_status.py --group        # group output by wallet
    python check_status.py --min 0.001    # hide accounts below this balance

Flags:
    --workers N   concurrent dashboard fetches (default 5, be gentle)
    --no-state    don't read state.json (show '-' in Last Success column)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import core


def fmt_sol(x: float | None) -> str:
    if x is None:
        return "     ERR     "
    return f"{x:>13.9f}"


def fetch_one(acc: dict) -> tuple[str, float | None]:
    """Return (name, balance_or_None). Silently swallows all log noise."""
    silent_log = lambda *a, **kw: None  # noqa: E731
    try:
        bal = core.fetch_claimable_balance(acc, silent_log)
    except Exception:
        bal = None
    return acc["name"], bal


def main() -> int:
    ap = argparse.ArgumentParser(description="Check balance + wallet for all accounts.")
    ap.add_argument("--name", help="only check a single account by name")
    ap.add_argument(
        "--group", action="store_true",
        help="group output by wallet address instead of flat list",
    )
    ap.add_argument(
        "--min", type=float, default=0.0,
        help="hide accounts whose claimable balance is below this (default 0)",
    )
    ap.add_argument(
        "--workers", type=int, default=5,
        help="parallel dashboard fetches (default 5)",
    )
    ap.add_argument(
        "--no-state", action="store_true",
        help="don't read state.json",
    )
    args = ap.parse_args()

    # ---- Load config + state -------------------------------------------------
    try:
        accounts = core.load_accounts()
    except SystemExit:
        raise
    except Exception as e:
        print(f"[abort] could not load config.json: {e}")
        return 1

    if args.name:
        accounts = [a for a in accounts if a["name"] == args.name]
        if not accounts:
            print(f"[abort] no account named {args.name!r} in config.json")
            return 1

    state = {}
    if not args.no_state:
        try:
            state = core.load_state()
        except Exception:
            state = {}

    # ---- Fetch balances in parallel ------------------------------------------
    print(f"fetching balances for {len(accounts)} account(s) "
          f"(workers={args.workers}) ...", file=sys.stderr)
    balances: dict[str, float | None] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(fetch_one, a): a for a in accounts}
        for fut in as_completed(futs):
            name, bal = fut.result()
            balances[name] = bal

    # ---- Render --------------------------------------------------------------
    rows = []
    for a in accounts:
        bal = balances.get(a["name"])
        if bal is not None and bal < args.min:
            continue
        last = state.get(a["name"], {}).get("last_success_iso", "-")
        rows.append({
            "name": a["name"],
            "balance": bal,
            "wallet": a["wallet_address"],
            "last_success": last,
        })

    if args.group:
        _render_grouped(rows)
    else:
        _render_flat(rows)

    _render_summary(rows)
    return 0


def _render_flat(rows: list[dict]) -> None:
    header = f"{'NAME':<22} {'BALANCE (SOL)':>15}  {'WALLET':<46}  LAST SUCCESS"
    print(header)
    print("-" * len(header))
    for r in sorted(rows, key=lambda r: (-(r["balance"] or -1), r["name"])):
        print(
            f"{r['name']:<22} {fmt_sol(r['balance']):>15}  "
            f"{r['wallet']:<46}  {r['last_success']}"
        )


def _render_grouped(rows: list[dict]) -> None:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[r["wallet"]].append(r)

    for i, (wallet, group) in enumerate(groups.items(), 1):
        # wallet header with totals
        total = sum(r["balance"] for r in group if r["balance"] is not None)
        n_ok = sum(1 for r in group if r["last_success"] != "-")
        err_count = sum(1 for r in group if r["balance"] is None)
        err_note = f"  [{err_count} err]" if err_count else ""
        print(
            f"\nwallet#{i:02d} {wallet}  "
            f"[{len(group)}/3 accts, {n_ok} locked, total={total:.9f} SOL]{err_note}"
        )
        for r in group:
            print(
                f"    {r['name']:<22} {fmt_sol(r['balance'])}  "
                f"last: {r['last_success']}"
            )


def _render_summary(rows: list[dict]) -> None:
    ok = [r for r in rows if r["balance"] is not None]
    err = [r for r in rows if r["balance"] is None]
    total_bal = sum(r["balance"] for r in ok)
    eligible = [r for r in ok if r["balance"] >= core.MIN_WITHDRAW_SOL]
    eligible_sum = sum(r["balance"] for r in eligible)

    print("\n" + "=" * 78)
    print(f"summary: {len(rows)} accounts shown")
    print(f"  ok fetch     : {len(ok)}")
    print(f"  error fetch  : {len(err)}  (likely expired cookie or 429)")
    print(f"  total balance: {total_bal:.9f} SOL")
    print(
        f"  withdrawable : {len(eligible)} acct(s) "
        f">= {core.MIN_WITHDRAW_SOL} SOL = {eligible_sum:.9f} SOL"
    )
    if err:
        print(f"\n  error accounts: {[r['name'] for r in err]}")


if __name__ == "__main__":
    sys.exit(main())
