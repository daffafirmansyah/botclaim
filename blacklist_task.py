"""
Blacklist (or unblacklist) a task ID across some/all accounts so tasks.py
skips it without burning POST attempts on a broken-backend task.

Usage examples:
    # Blacklist task 45 for ALL accounts in config.json:
    python blacklist_task.py --task 45

    # Blacklist task 45 for one account only:
    python blacklist_task.py --task 45 --name folktor

    # Remove the blacklist (re-enable retry):
    python blacklist_task.py --task 45 --unblacklist

    # Show current blacklist for a task:
    python blacklist_task.py --task 45 --show

This just edits done_tasks.json (the same cache tasks.py uses to skip
already-completed work). Anything in that file is skipped silently before
any POST is made, so it doubles as a manual blacklist mechanism.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import core
from tasks import DONE_TASKS_PATH, load_done_tasks


BLACKLIST_TS = "blacklist:manual"  # marker so we know it wasn't a real completion


def _save(data: dict) -> None:
    DONE_TASKS_PATH.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _accounts_in_scope(name: str | None) -> list[str]:
    accounts = core.load_accounts()
    if name:
        match = [a["name"] for a in accounts if a.get("name") == name]
        if not match:
            print(f"[error] account {name!r} not found in config.json.", file=sys.stderr)
            sys.exit(1)
        return match
    return [a["name"] for a in accounts]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--task", required=True, help="task id to blacklist/unblacklist.")
    p.add_argument("--name", help="limit to one account (default: all accounts).")
    p.add_argument(
        "--unblacklist",
        action="store_true",
        help="remove the blacklist entry instead of adding it.",
    )
    p.add_argument(
        "--show",
        action="store_true",
        help="just print which accounts currently have this task blacklisted/cached.",
    )
    args = p.parse_args()
    task_id = str(args.task)

    data = load_done_tasks()
    accounts = _accounts_in_scope(args.name)

    if args.show:
        marked = [a for a in accounts if task_id in (data.get(a) or {})]
        print(f"task {task_id} currently in done_tasks.json for {len(marked)} account(s):")
        for a in marked:
            ts = (data.get(a) or {}).get(task_id, "?")
            print(f"  - {a}: {ts}")
        return 0

    changed = 0
    if args.unblacklist:
        for a in accounts:
            bucket = data.get(a) or {}
            if task_id in bucket:
                del bucket[task_id]
                changed += 1
        _save(data)
        print(f"[unblacklist] removed task {task_id} from {changed} account(s).")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") + " " + BLACKLIST_TS
    for a in accounts:
        bucket = data.setdefault(a, {})
        if task_id not in bucket:
            bucket[task_id] = ts
            changed += 1
    _save(data)
    print(f"[blacklist] added task {task_id} to {changed} new account(s).")
    print(f"  -> tasks.py will now skip task {task_id} for these accounts.")
    print(f"  -> remove with: python blacklist_task.py --task {task_id} --unblacklist")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
