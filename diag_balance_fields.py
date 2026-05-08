"""
Diagnostic: dump ALL numeric fields from /api/stats/dashboard response
for one account, so we can see which field is the real claimable balance.

Usage:
    python diag_balance_fields.py --name folktor

This is read-only - just a GET, no withdraw.
"""

from __future__ import annotations

import argparse
import json
import sys

import requests

import core
from core import USER_API_URL, build_headers, get_proxies, load_accounts


def _flatten(obj, prefix=""):
    """Yield (path, value) for every leaf in a nested dict/list."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            yield from _flatten(v, p)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{prefix}[{i}]"
            yield from _flatten(v, p)
    else:
        yield prefix, obj


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--name", required=True, help="account name from config.json")
    args = p.parse_args()

    accs = load_accounts()
    acc = next((a for a in accs if a.get("name") == args.name), None)
    if not acc:
        print(f"[error] account {args.name!r} not found.", file=sys.stderr)
        return 1

    headers = build_headers(acc["cookie"], referer_path="/dashboard")
    resp = requests.get(USER_API_URL, headers=headers, timeout=15, proxies=get_proxies())
    print(f"status: {resp.status_code}")
    if resp.status_code != 200:
        print(f"body: {resp.text[:500]}")
        return 1

    try:
        data = resp.json()
    except ValueError:
        print("non-JSON body")
        print(resp.text[:500])
        return 1

    print("\n=== ALL NUMERIC FIELDS ===")
    print(f"{'path':<40}{'value':<20}")
    print("-" * 60)
    for path, val in _flatten(data):
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            print(f"{path:<40}{val!r}")

    print("\n=== FULL RAW JSON ===")
    print(json.dumps(data, indent=2)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
