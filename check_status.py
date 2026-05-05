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
    --workers N       concurrent dashboard fetches (default 5, be gentle)
    --retry N         extra rounds to re-fetch accounts that hit 429 (default 0)
    --retry-wait SEC  seconds to sleep between retry rounds (default 60)
    --no-state        don't read state.json (show '-' in Last Success column)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import core


def fmt_sol(x: float | None) -> str:
    if x is None:
        return "     ERR     "
    return f"{x:>13.9f}"


_CANDIDATE_FIELDS = (
    "currentBalance",
    "balanceSolTask",
    "balanceSol",
    "balance",
    "claimable",
    "claimableSol",
    "rewardSol",
    "pendingSol",
)


def _classify(status: int) -> str:
    """Map HTTP status to short human reason."""
    return {
        401: "cookie invalid/expired",
        403: "forbidden (account flagged?)",
        404: "endpoint 404",
        429: "rate-limited (429)",
        500: "qolvex 500",
        502: "qolvex 502 bad gateway",
        503: "qolvex 503 outage",
        504: "qolvex 504 timeout",
    }.get(status, f"http {status}")


def fetch_one(acc: dict) -> dict:
    """Return full diagnostic: name, balance, status, reason.

    Bypasses core.fetch_claimable_balance so we can surface the real cause
    of failures (401 vs 429 vs timeout etc.) instead of a silent None.
    """
    name = acc["name"]
    try:
        headers = core.build_headers(acc["cookie"], referer_path="/dashboard")
    except Exception as e:
        return {"name": name, "balance": None, "status": 0, "reason": f"bad-cookie-field: {e}"}

    try:
        resp = requests.get(core.USER_API_URL, headers=headers, timeout=15)
    except requests.Timeout:
        return {"name": name, "balance": None, "status": 0, "reason": "timeout (15s)"}
    except requests.RequestException as e:
        return {"name": name, "balance": None, "status": 0, "reason": f"net: {e.__class__.__name__}"}

    status = resp.status_code
    if status != 200:
        return {"name": name, "balance": None, "status": status, "reason": _classify(status)}

    try:
        parsed = resp.json()
    except Exception:
        return {"name": name, "balance": None, "status": status, "reason": "non-JSON body"}

    if not isinstance(parsed, dict):
        return {"name": name, "balance": None, "status": status, "reason": "non-object body"}

    # Search top-level then 1-level-nested (mirror core.fetch_claimable_balance).
    for field in _CANDIDATE_FIELDS:
        val = parsed.get(field)
        if val is None:
            for v in parsed.values():
                if isinstance(v, dict) and field in v:
                    val = v[field]
                    break
        if isinstance(val, (int, float)):
            return {"name": name, "balance": float(val), "status": status, "reason": "ok"}

    return {"name": name, "balance": None, "status": status, "reason": "no balance field in response"}


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
        "--retry", type=int, default=0,
        help="extra rounds to re-fetch accounts that hit 429 (default 0)",
    )
    ap.add_argument(
        "--retry-wait", type=int, default=60,
        help="seconds to sleep between retry rounds (default 60)",
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
    diags: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(fetch_one, a): a for a in accounts}
        for fut in as_completed(futs):
            r = fut.result()
            diags[r["name"]] = r

    # ---- Retry rounds for 429-only (rate-limited) ---------------------------
    by_name = {a["name"]: a for a in accounts}
    for round_n in range(1, args.retry + 1):
        rl_names = [n for n, d in diags.items() if d["status"] == 429]
        if not rl_names:
            break
        print(
            f"[retry {round_n}/{args.retry}] {len(rl_names)} accounts hit 429; "
            f"sleeping {args.retry_wait}s then re-fetching serially ...",
            file=sys.stderr,
        )
        time.sleep(args.retry_wait)
        # Use 1 worker for retries — max politeness.
        with ThreadPoolExecutor(max_workers=1) as pool:
            futs = {pool.submit(fetch_one, by_name[n]): n for n in rl_names}
            for fut in as_completed(futs):
                r = fut.result()
                diags[r["name"]] = r
        still = sum(1 for n in rl_names if diags[n]["status"] == 429)
        print(
            f"[retry {round_n}/{args.retry}] done: recovered "
            f"{len(rl_names) - still}/{len(rl_names)}",
            file=sys.stderr,
        )

    # ---- Persist balances to cache for priority_sort_accounts ----------------
    fresh = {n: d["balance"] for n, d in diags.items() if d["balance"] is not None}
    if fresh:
        try:
            core.update_balance_cache(fresh)
            print(
                f"[cache] wrote {len(fresh)} balance(s) to balance_cache.json",
                file=sys.stderr,
            )
        except Exception as e:  # noqa: BLE001
            print(f"[cache] write failed: {e}", file=sys.stderr)

    # ---- Render --------------------------------------------------------------
    rows = []
    for a in accounts:
        d = diags.get(a["name"], {"balance": None, "status": 0, "reason": "no-result"})
        bal = d["balance"]
        if bal is not None and bal < args.min:
            continue
        last = state.get(a["name"], {}).get("last_success_iso", "-")
        rows.append({
            "name": a["name"],
            "balance": bal,
            "status": d["status"],
            "reason": d["reason"],
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
        suffix = ""
        if r["balance"] is None:
            suffix = f"  ({r['reason']})"
        print(
            f"{r['name']:<22} {fmt_sol(r['balance']):>15}  "
            f"{r['wallet']:<46}  {r['last_success']}{suffix}"
        )


def _render_grouped(rows: list[dict]) -> None:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[r["wallet"]].append(r)

    for i, (wallet, group) in enumerate(groups.items(), 1):
        total = sum(r["balance"] for r in group if r["balance"] is not None)
        n_ok = sum(1 for r in group if r["last_success"] != "-")
        err_count = sum(1 for r in group if r["balance"] is None)
        err_note = f"  [{err_count} err]" if err_count else ""
        print(
            f"\nwallet#{i:02d} {wallet}  "
            f"[{len(group)}/3 accts, {n_ok} locked, total={total:.9f} SOL]{err_note}"
        )
        for r in group:
            suffix = f"  ({r['reason']})" if r["balance"] is None else ""
            print(
                f"    {r['name']:<22} {fmt_sol(r['balance'])}  "
                f"last: {r['last_success']}{suffix}"
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
    print(f"  error fetch  : {len(err)}")
    print(f"  total balance: {total_bal:.9f} SOL")
    print(
        f"  withdrawable : {len(eligible)} acct(s) "
        f">= {core.MIN_WITHDRAW_SOL} SOL = {eligible_sum:.9f} SOL"
    )
    if err:
        reasons: Counter[str] = Counter(r["reason"] for r in err)
        print("\n  error breakdown:")
        for reason, n in reasons.most_common():
            names = [r["name"] for r in err if r["reason"] == reason]
            print(f"    {n:>3} x {reason}")
            print(f"        {names}")


if __name__ == "__main__":
    sys.exit(main())
