"""
One-shot audit: verify wallet_address in config.json for all accounts.

Checks:
  1. Every account has a non-empty wallet_address field.
  2. Each address is valid Solana base58 (32-44 chars, no invalid chars).
  3. Cross-check against the canonical NEW_WALLETS list in _apply_new_wallets.py
     (34 wallets that were last distributed).
  4. Distribution: how many accounts per unique wallet, flag any wallet
     with > 3 accounts (violates the 3-per-wallet constraint).
  5. List orphan wallets (in config.json but not in canonical NEW_WALLETS).

Read-only. Prints a full report.

Usage:
    python verify_wallets.py
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

import core

BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _load_canonical_wallets() -> list[str]:
    """Extract NEW_WALLETS list from _apply_new_wallets.py as source of truth."""
    p = Path("_apply_new_wallets.py")
    if not p.exists():
        return []
    src = p.read_text(encoding="utf-8")
    m = re.search(r"NEW_WALLETS.*?=.*?\[(.*?)\]", src, re.DOTALL)
    if not m:
        return []
    return re.findall(r'"([1-9A-HJ-NP-Za-km-z]{32,44})"', m.group(1))


def main() -> int:
    # core.load_accounts() sanitizes embedded CR/LF/TAB in cookie strings
    # that would otherwise break raw json.loads(). Mirror what monitor.py
    # and withdraw.py actually see at runtime.
    try:
        accounts = core.load_accounts()
    except (OSError, ValueError, RuntimeError) as e:
        print(f"[error] core.load_accounts() failed: {e}", file=sys.stderr)
        return 1
    if not accounts:
        print("[error] no accounts in config.json", file=sys.stderr)
        return 1

    canonical = _load_canonical_wallets()
    canonical_set = set(canonical)

    print(f"=== AUDIT: {len(accounts)} accounts ===")
    print(f"canonical wallets in _apply_new_wallets.py: {len(canonical)}")
    print()

    # 1. Missing or invalid format
    missing = []
    invalid = []
    for acc in accounts:
        name = acc.get("name", "?")
        w = acc.get("wallet_address", "")
        if not w:
            missing.append(name)
        elif not BASE58_RE.match(w):
            invalid.append((name, w))

    if missing:
        print(f"MISSING wallet_address ({len(missing)}):")
        for n in missing:
            print(f"  - {n}")
        print()
    if invalid:
        print(f"INVALID base58 format ({len(invalid)}):")
        for n, w in invalid:
            print(f"  - {n}: {w!r}")
        print()

    # 2. Distribution per unique wallet
    counter = Counter(acc.get("wallet_address", "") for acc in accounts if acc.get("wallet_address"))
    unique_wallets = len(counter)
    print(f"unique wallets in config.json: {unique_wallets}")
    print()

    # 3. Flag violations (> 3 accounts per wallet)
    over_limit = [(w, c) for w, c in counter.items() if c > 3]
    if over_limit:
        print(f"VIOLATIONS (> 3 accounts per wallet): {len(over_limit)}")
        for w, c in sorted(over_limit, key=lambda x: -x[1]):
            acc_names = [a["name"] for a in accounts if a.get("wallet_address") == w]
            print(f"  {c}x: {w}")
            print(f"      accounts: {', '.join(acc_names)}")
        print()
    else:
        print("OK: no wallet has > 3 accounts (constraint satisfied)")
        print()

    # 4. Distribution summary
    dist = Counter(counter.values())
    print("distribution (accounts per wallet):")
    for k in sorted(dist.keys()):
        print(f"  {k} account(s): {dist[k]} wallet(s)")
    print()

    # 5. Orphan wallets (in config.json but not in canonical NEW_WALLETS)
    if canonical:
        config_wallets = set(counter.keys())
        orphans = config_wallets - canonical_set
        unused_canonical = canonical_set - config_wallets
        if orphans:
            print(f"ORPHAN wallets (in config but NOT in _apply_new_wallets.py): {len(orphans)}")
            for w in sorted(orphans):
                acc_names = [a["name"] for a in accounts if a.get("wallet_address") == w]
                print(f"  - {w}  (used by: {', '.join(acc_names)})")
            print()
        if unused_canonical:
            print(f"UNUSED canonical wallets (in _apply but NOT in config): {len(unused_canonical)}")
            for w in sorted(unused_canonical):
                print(f"  - {w}")
            print()

    # 6. Top wallets (most accounts assigned)
    print("top 10 most-assigned wallets:")
    for w, c in counter.most_common(10):
        acc_names = [a["name"] for a in accounts if a.get("wallet_address") == w]
        print(f"  {c}x: {w}  <- {', '.join(acc_names)}")
    print()

    # Final status
    errors = len(missing) + len(invalid) + len(over_limit)
    if errors == 0:
        print("=== RESULT: ALL GOOD ===")
        return 0
    else:
        print(f"=== RESULT: {errors} ISSUE(S) FOUND ===")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
