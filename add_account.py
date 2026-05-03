"""
Helper to add accounts to config.json without hand-editing JSON.

Two modes:

  Interactive (default):
      python add_account.py
      -> prompts for name, cookie, wallet, amount per account, loops.

  Bulk import from TSV:
      python add_account.py --bulk accounts.tsv
      -> reads a tab-separated file and imports every row.

  List existing accounts (without leaking the cookie value):
      python add_account.py --list

  Remove account(s) by name or by index from --list:
      python add_account.py --remove acc1
      python add_account.py --remove acc1,acc2,acc5
      python add_account.py --remove 3,7        # by --list index
      python add_account.py --remove all        # wipe every account
      python add_account.py --remove all --yes  # wipe without confirmation

Bulk file format (header row required, any column order):

    name<TAB>cookie<TAB>wallet_address<TAB>amount_sol
    acc1<TAB>session=...; other=...<TAB>GXZB...<TAB>auto
    acc2<TAB>session=...; other=...<TAB>GXZB...<TAB>0.0033999998

Notes:
  * qolvex.xyz uses COOKIE-ONLY auth (no Bearer JWT), so there is no
    bearer_token field here — this differs from claimyshare-withdraw.
  * Existing config.json (legacy single-account or multi-account) is
    preserved and migrated; new entries are appended.
  * Duplicate names are rejected — pick unique names per account.
  * Wallet and amount default to the last-added values to speed up bulk
    interactive entry where most accounts share the same wallet.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Optional

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

REQUIRED_FIELDS = ("name", "cookie", "wallet_address", "amount_sol")


# ---------------------------------------------------------------------------
# config.json read / write
# ---------------------------------------------------------------------------

def load_config_data() -> dict:
    """Return the raw config dict, normalized to {'accounts': [...]}."""
    if not CONFIG_PATH.exists():
        return {"accounts": []}

    raw = CONFIG_PATH.read_text(encoding="utf-8-sig")
    # Auto-repair cookie values that were pasted from DevTools with an
    # embedded newline — those are raw control chars which json.loads()
    # rejects. Shared helper with core.py.
    from core import sanitize_json_text
    raw = sanitize_json_text(raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        sys.exit(f"[error] existing config.json is invalid JSON: {e}")

    if "accounts" in data:
        if not isinstance(data["accounts"], list):
            sys.exit("[error] existing config.json 'accounts' is not a list.")
        return data

    if any(k in data for k in ("cookie", "wallet_address", "amount_sol")):
        legacy = dict(data)
        legacy["name"] = legacy.get("name") or "default"
        # Drop any stray bearer_token field (not used by qolvex).
        legacy.pop("bearer_token", None)
        return {"accounts": [legacy]}

    return {"accounts": []}


def save_config_data(data: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def existing_names(data: dict) -> set[str]:
    return {a.get("name") for a in data.get("accounts", []) if a.get("name")}


def parse_amount(raw: str):
    """Return float for a positive number, string "auto" sentinel, or ValueError."""
    raw = raw.strip()
    if not raw:
        return "auto"
    if raw.lower() == "auto":
        return "auto"
    v = float(raw)
    if v <= 0:
        raise ValueError("amount must be > 0 (or \"auto\")")
    return v


def validate_entry(entry: dict, taken_names: set[str]) -> Optional[str]:
    for f in REQUIRED_FIELDS:
        v = entry.get(f)
        if v is None or (isinstance(v, str) and not v.strip()):
            return f"missing field {f!r}"
    if entry["name"] in taken_names:
        return f"duplicate name {entry['name']!r}"
    amt = entry["amount_sol"]
    if isinstance(amt, str):
        if amt.strip().lower() != "auto":
            return f"invalid amount_sol: {amt!r} (use a number or \"auto\")"
    elif not isinstance(amt, (int, float)) or amt <= 0:
        return f"invalid amount_sol: {amt!r}"
    return None


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------

def _prompt(label: str, default: str | None = None, required: bool = True) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        raw = input(f"  {label}{suffix}: ").strip()
        if not raw and default is not None:
            return default
        if raw or not required:
            return raw
        print("    (required)")


def _suggest_next_name(taken: set[str]) -> str:
    i = 1
    while f"acc{i}" in taken:
        i += 1
    return f"acc{i}"


def interactive_loop(data: dict) -> int:
    print(f"\nqolvex-withdraw add-account (interactive)")
    print(f"Existing accounts: {len(data['accounts'])}")
    print("Press Ctrl+C any time to stop. config.json is saved after every add.\n")

    last_wallet: str | None = None
    last_amount: str | None = None
    if data["accounts"]:
        last = data["accounts"][-1]
        last_wallet = last.get("wallet_address")
        amt = last.get("amount_sol")
        last_amount = str(amt) if amt is not None else None

    added = 0
    try:
        while True:
            taken = existing_names(data)
            print(f"--- new account #{len(data['accounts']) + 1} ---")
            name = _prompt("name", default=_suggest_next_name(taken))
            if name in taken:
                print(f"  ! name {name!r} already in use, try another.")
                continue
            cookie = _prompt("cookie (paste full value from DevTools > Network > request headers)")
            wallet = _prompt("wallet_address (Solana)", default=last_wallet)
            amount_str = _prompt(
                "amount_sol ('auto' = withdraw full claimable balance)",
                default=last_amount or "auto",
            )

            try:
                amount = parse_amount(amount_str)
            except ValueError as e:
                print(f"  ! invalid amount: {e}; try again.")
                continue

            entry = {
                "name": name,
                "cookie": cookie,
                "wallet_address": wallet,
                "amount_sol": amount,
            }
            err = validate_entry(entry, taken)
            if err:
                print(f"  ! rejected: {err}; try again.")
                continue

            data["accounts"].append(entry)
            save_config_data(data)
            added += 1
            last_wallet = wallet
            last_amount = amount_str
            print(f"  + saved. total accounts: {len(data['accounts'])}\n")

            cont = input("Add another? [Y/n]: ").strip().lower()
            if cont in ("n", "no"):
                break
    except (KeyboardInterrupt, EOFError):
        print("\n[interrupted]")

    print(f"\nDone. {added} new account(s) added. Total: {len(data['accounts'])}.")
    return 0


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------

def _shorten_wallet(w: str) -> str:
    return f"{w[:8]}...{w[-4:]}" if len(w) > 16 else w


def _cookie_fingerprint(c: str) -> str:
    """Show last 6 chars + total length so you can tell cookies apart
    when rotating, without printing the actual cookie value."""
    if not c:
        return "(empty)"
    return f"...{c[-6:]} ({len(c)}c)"


def list_accounts(data: dict) -> int:
    accounts = data.get("accounts", [])
    if not accounts:
        print("No accounts in config.json yet.")
        print(f"  config path: {CONFIG_PATH}")
        return 0

    print(f"\nTotal accounts: {len(accounts)}  (config: {CONFIG_PATH})\n")
    header = f"  {'#':>3}  {'name':<12}  {'wallet':<22}  {'amount':<13}  {'cookie':<22}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for i, a in enumerate(accounts, 1):
        name = a.get("name", "?")
        wallet = _shorten_wallet(a.get("wallet_address", ""))
        amount = a.get("amount_sol", "?")
        cookie = _cookie_fingerprint(a.get("cookie", ""))
        print(
            f"  {i:>3}  {name:<12}  {wallet:<22}  {amount!s:<13}  {cookie:<22}"
        )
    print()

    missing_cookie = sum(1 for a in accounts if not a.get("cookie"))
    if missing_cookie:
        print(f"  WARN: {missing_cookie} account(s) missing cookie.")
    return 0


# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------

def remove_accounts(data: dict, spec: str, skip_confirm: bool) -> int:
    accounts = data.get("accounts", [])
    if not accounts:
        print("No accounts in config.json to remove.")
        return 1

    targets = [t.strip() for t in spec.split(",") if t.strip()]
    if not targets:
        print("[error] --remove value is empty.")
        return 1

    to_remove: set[str] = set()
    not_found: list[str] = []
    name_set = {a.get("name") for a in accounts}

    if any(t.lower() == "all" for t in targets):
        to_remove = {a.get("name") for a in accounts if a.get("name")}
    else:
        for t in targets:
            if t.isdigit():
                idx = int(t)
                if 1 <= idx <= len(accounts):
                    to_remove.add(accounts[idx - 1]["name"])
                else:
                    not_found.append(f"index {t} (valid: 1..{len(accounts)})")
            elif t in name_set:
                to_remove.add(t)
            else:
                not_found.append(f"name {t!r}")

    if not_found:
        print(f"[error] not found: {', '.join(not_found)}")
        return 1

    print(f"\nWill remove {len(to_remove)} account(s):")
    for n in sorted(to_remove):
        print(f"  - {n}")

    if not skip_confirm:
        try:
            ans = input("\nConfirm? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = ""
        if ans not in ("y", "yes"):
            print("Aborted. config.json unchanged.")
            return 1

    new_accounts = [a for a in accounts if a.get("name") not in to_remove]
    data["accounts"] = new_accounts
    save_config_data(data)
    print(f"\nRemoved {len(accounts) - len(new_accounts)} account(s). "
          f"Total now: {len(new_accounts)}.")
    return 0


# ---------------------------------------------------------------------------
# Bulk import
# ---------------------------------------------------------------------------

def _merge_broken_rows(lines: list[str], delimiter: str, expected_cols: int) -> tuple[list[str], int]:
    """Re-assemble physical lines into logical rows when a value (usually a
    cookie pasted from a browser) contained a literal newline that caused
    one logical row to be split across multiple physical lines.
    """
    expected_delims = expected_cols - 1
    result: list[str] = []
    absorbed = 0

    i = 0
    while i < len(lines):
        current = lines[i]

        if not current.strip():
            i += 1
            continue

        if current.count(delimiter) >= expected_delims:
            result.append(current)
            i += 1
            continue

        merged = current
        j = i + 1
        while j < len(lines) and merged.count(delimiter) < expected_delims:
            nxt = lines[j]
            if not nxt.strip():
                j += 1
                continue
            merged = merged + nxt
            j += 1

        result.append(merged)
        absorbed += max(0, (j - i) - 1)
        i = j if j > i else i + 1

    return result, absorbed


def bulk_import(data: dict, tsv_path: Path) -> int:
    if not tsv_path.exists():
        sys.exit(f"[error] bulk file not found: {tsv_path}")

    text = tsv_path.read_text(encoding="utf-8-sig")
    if "\t" in text:
        delimiter = "\t"
    elif ";" in text:
        delimiter = ";"
    else:
        delimiter = ","

    raw_lines = text.splitlines()
    if raw_lines:
        header_line = raw_lines[0]
        data_lines, absorbed = _merge_broken_rows(
            raw_lines[1:], delimiter, len(REQUIRED_FIELDS)
        )
        if absorbed > 0:
            print(
                f"  [info] auto-merged {absorbed} extra physical line(s) back "
                f"into their logical rows (newline embedded in a value)."
            )
        merged_lines = [header_line] + data_lines
    else:
        merged_lines = raw_lines

    reader = csv.DictReader(merged_lines, delimiter=delimiter)
    if reader.fieldnames is None:
        sys.exit("[error] bulk file appears empty or has no header row.")
    header_set = {h.strip() for h in reader.fieldnames}
    missing = [f for f in REQUIRED_FIELDS if f not in header_set]
    if missing:
        sys.exit(
            f"[error] bulk file missing required header(s): {', '.join(missing)}.\n"
            f"  Header found: {', '.join(reader.fieldnames)}"
        )

    added = 0
    skipped = 0
    for row_num, row in enumerate(reader, start=2):
        taken = existing_names(data)

        missing_cols = [f for f in REQUIRED_FIELDS if row.get(f) is None]
        if missing_cols:
            print(
                f"  ! row {row_num}: missing column(s) {missing_cols}; "
                f"this usually means a TAB got lost or a value contained a "
                f"newline. Skipped."
            )
            skipped += 1
            continue

        try:
            entry = {
                "name": row["name"].strip(),
                "cookie": row["cookie"].strip(),
                "wallet_address": row["wallet_address"].strip(),
                "amount_sol": parse_amount(row["amount_sol"]),
            }
        except (KeyError, ValueError, AttributeError, TypeError) as e:
            print(f"  ! row {row_num}: {e}; skipped.")
            skipped += 1
            continue

        err = validate_entry(entry, taken)
        if err:
            print(f"  ! row {row_num} ({entry.get('name', '?')}): {err}; skipped.")
            skipped += 1
            continue

        data["accounts"].append(entry)
        added += 1
        if added % 10 == 0:
            save_config_data(data)
            print(f"  ... {added} added so far (saved).")

    save_config_data(data)
    print(f"\nDone. {added} added, {skipped} skipped. Total accounts: {len(data['accounts'])}.")
    return 0 if added > 0 else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Add account(s) to config.json (interactive or bulk TSV)."
    )
    ap.add_argument(
        "--bulk",
        type=Path,
        help="Path to a TSV/CSV file with header row (name, cookie, wallet_address, amount_sol).",
    )
    ap.add_argument(
        "--list",
        action="store_true",
        help="List existing accounts without printing the cookie value.",
    )
    ap.add_argument(
        "--remove",
        metavar="NAMES_OR_INDICES",
        help="Remove account(s) by name or 1-based index from --list. Comma-separated.",
    )
    ap.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt for --remove.",
    )
    ns = ap.parse_args()

    data = load_config_data()
    if ns.list:
        return list_accounts(data)
    if ns.remove:
        return remove_accounts(data, ns.remove, ns.yes)
    if ns.bulk:
        return bulk_import(data, ns.bulk)
    return interactive_loop(data)


if __name__ == "__main__":
    sys.exit(main())
