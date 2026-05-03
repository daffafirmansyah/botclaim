"""
x_auto.py — Phase 2 + Phase 3: follow/like on X, then immediately claim the
reward on qolvex.

Pipeline:
  1. tasks.py (Phase 1) writes pending_x.json with rows like:
       folktor:
         { action: "follow", target: "@golieth_io", task_id: 67, ... }
         { action: "like",   target: "2049772417996705815", task_id: 62, ... }
  2. x_auto.py reads pending_x.json + x_accounts.tsv (cookies per name).
  3. For every pending row whose account has filled cookies:
       - follow -> POST /1.1/friendships/create.json screen_name=<handle>
       - like   -> POST /1.1/favorites/create.json    id=<tweet_id>
       - random 30-90s delay between actions per account (anti-bot)
  4. After each successful X action, wait --claim-delay seconds for X to
     propagate, then POST /api/tasks/{id}/complete on qolvex to credit the
     reward. Retries on "still verifying" / "not following yet" responses.
  5. Successful entries are removed from pending_x.json. Failed entries
     (X action failed OR claim kept failing) are kept so the next run can
     retry.

Use --skip-claim to run Phase 2 only: do the X action but do not call
qolvex afterwards. You'd then have to run tasks.py separately.

Examples:
  python x_auto.py                           # full pipeline (Phase 2 + 3)
  python x_auto.py --claim-delay 25          # longer wait for X propagation
  python x_auto.py --name folktor            # one account, full pipeline
  python x_auto.py --skip-claim              # X action only, don't claim
  python x_auto.py --dry-run                 # preview, don't POST anything

Outputs:
  x_auto.log          human-readable log
  pending_x.json      rewritten without completed entries (or removed if empty)

Exit code:
  0 if at least one action succeeded, 3 otherwise.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from core import (
    EXIT_API_ERROR,
    EXIT_OK,
    SCRIPT_DIR,
    load_accounts,
    make_logger,
)
# Reuse the same classifier and complete-task HTTP wrapper tasks.py uses so
# Phase 3 stays in lockstep if the API shape changes.
from tasks import _classify_response, complete_task

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

PENDING_X_PATH = SCRIPT_DIR / "pending_x.json"
X_ACCOUNTS_PATH = SCRIPT_DIR / "x_accounts.tsv"

# ---------------------------------------------------------------------------
# X endpoints (legacy v1.1 — simpler than GraphQL, stable URL)
# ---------------------------------------------------------------------------

FOLLOW_URL = "https://x.com/i/api/1.1/friendships/create.json"
LIKE_URL = "https://x.com/i/api/1.1/favorites/create.json"

# Public bearer from x.com web bundle. Same value every browser uses.
X_WEB_BEARER = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Delay between actions for the SAME account. Random within [min, max].
# X is sensitive to burst follow/like patterns from a single session.
ACTION_DELAY_MIN_SEC = 30
ACTION_DELAY_MAX_SEC = 90

# Delay between accounts (smaller — different sessions, less suspicious).
# Bumped 5-12s -> 8-18s 2026-05-04 to match overall throttle-down pass.
ACCOUNT_DELAY_MIN_SEC = 8
ACCOUNT_DELAY_MAX_SEC = 18

HTTP_TIMEOUT_SEC = 20

# --- Auto-claim (Phase 3) tunables ---
# After every successful X action we wait this long to let X propagate the
# follow/like to its backend, then POST to qolvex's /api/tasks/{id}/complete.
# Too short -> server will still say "not following this account yet" and we
# have to retry on next run.
AUTO_CLAIM_DELAY_SEC_DEFAULT = 15
# If the first claim attempt says "still verifying" / "try again later",
# wait this much longer and retry, up to AUTO_CLAIM_MAX_RETRIES times.
AUTO_CLAIM_RETRY_BACKOFF_SEC = 15
AUTO_CLAIM_MAX_RETRIES = 2

# --- "Already done" fast-path ---
# When X says we already follow/like the target (code 160/158/139), the X
# state has been stable for who-knows-how-long; there's no propagation to
# wait for and no reason to throttle this account as heavily as a fresh
# action. These shorter delays kick in ONLY when an action's outcome is
# "already".
ALREADY_ACTION_DELAY_SEC = 2   # sleep between actions after an "already" hit
ALREADY_CLAIM_DELAY_SEC = 3    # claim-delay override for "already" actions

# X error codes that mean "the action is already done" — treat as success.
ALREADY_FOLLOWING_CODES = {160, 158}   # 160 = already requested, 158 = already following
ALREADY_LIKED_CODES = {139}            # 139 = already favorited

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_placeholder(value: str | None) -> bool:
    """True if the cookie field is empty or still contains REPLACE_WITH_*."""
    if not value:
        return True
    return "REPLACE_WITH" in value


def _x_headers(ct0: str, auth_token: str, referer: str) -> dict:
    """Browser-like headers for an authenticated x.com action POST."""
    return {
        "authorization": f"Bearer {X_WEB_BEARER}",
        "x-csrf-token": ct0,
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "user-agent": USER_AGENT,
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://x.com",
        "referer": referer,
        "cookie": f"auth_token={auth_token}; ct0={ct0}",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }


def _parse_x_error_codes(body) -> list[int]:
    """Pull X error codes out of a JSON response body. Empty list if none."""
    if not isinstance(body, dict):
        return []
    errs = body.get("errors")
    if not isinstance(errs, list):
        return []
    out: list[int] = []
    for e in errs:
        if isinstance(e, dict) and isinstance(e.get("code"), int):
            out.append(e["code"])
    return out


def _post_action(
    url: str, body_form: dict, headers: dict
) -> tuple[int, dict | None]:
    """POST a form-encoded action to X. Returns (status, parsed_body)."""
    try:
        resp = requests.post(
            url,
            data=body_form,
            headers=headers,
            timeout=HTTP_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return 0, {"error": f"network: {e}"}
    try:
        parsed = resp.json()
    except ValueError:
        parsed = {"raw": resp.text[:300]}
    return resp.status_code, parsed


def perform_follow(
    auth_token: str, ct0: str, target_handle: str
) -> tuple[str, dict]:
    """
    Follow `target_handle` (with or without leading @).

    Returns (outcome, debug):
      outcome ∈ {"ok", "already", "invalid-target", "auth", "rate-limit",
                 "error"}
    """
    handle = target_handle.lstrip("@").strip()
    if not handle:
        return "invalid-target", {"reason": "empty handle"}

    headers = _x_headers(ct0, auth_token, referer=f"https://x.com/{handle}")
    status, body = _post_action(FOLLOW_URL, {"screen_name": handle}, headers)

    if 200 <= status < 300 and isinstance(body, dict) and body.get("id_str"):
        return "ok", {"id_str": body["id_str"], "screen_name": body.get("screen_name")}

    codes = _parse_x_error_codes(body)
    if any(c in ALREADY_FOLLOWING_CODES for c in codes):
        return "already", {"codes": codes}
    if 401 <= status <= 403 or 32 in codes or 89 in codes:
        return "auth", {"http": status, "codes": codes, "body": body}
    if status == 429 or 88 in codes:
        return "rate-limit", {"http": status, "codes": codes}
    if 50 in codes or 63 in codes:  # user not found / suspended
        return "invalid-target", {"codes": codes}
    return "error", {"http": status, "body": body}


def perform_like(auth_token: str, ct0: str, tweet_id: str) -> tuple[str, dict]:
    """
    Like tweet `tweet_id` (numeric string).

    Returns (outcome, debug) with the same outcome vocabulary as
    perform_follow().
    """
    tid = (tweet_id or "").strip()
    if not tid.isdigit():
        return "invalid-target", {"reason": f"non-numeric tweet id {tid!r}"}

    headers = _x_headers(
        ct0, auth_token, referer=f"https://x.com/i/web/status/{tid}"
    )
    status, body = _post_action(LIKE_URL, {"id": tid}, headers)

    if 200 <= status < 300 and isinstance(body, dict) and body.get("id_str"):
        return "ok", {"id_str": body["id_str"]}

    codes = _parse_x_error_codes(body)
    if any(c in ALREADY_LIKED_CODES for c in codes):
        return "already", {"codes": codes}
    if 401 <= status <= 403 or 32 in codes or 89 in codes:
        return "auth", {"http": status, "codes": codes, "body": body}
    if status == 429 or 88 in codes:
        return "rate-limit", {"http": status, "codes": codes}
    if 144 in codes:  # tweet not found
        return "invalid-target", {"codes": codes}
    return "error", {"http": status, "body": body}


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def load_qolvex_creds() -> dict[str, dict]:
    """
    Return {name: {"cookie": ...}} for accounts in config.json that have a
    filled cookie. Used by auto-claim to call /api/tasks/{id}/complete after
    an X action succeeds.

    Qolvex is cookie-only auth (unlike claimyshare which also needed a
    bearer JWT), so this just surfaces the cookie field.

    Never raises: returns an empty dict if config.json is missing or
    malformed (BaseException catches core.load_accounts()'s sys.exit too).
    """
    out: dict[str, dict] = {}
    try:
        accounts = load_accounts()
    except BaseException:
        return out
    for acc in accounts:
        name = acc.get("name")
        cookie = acc.get("cookie") or ""
        if name and cookie:
            out[name] = {"cookie": cookie}
    return out


def try_auto_claim(
    name: str,
    task_id: int,
    cookie: str,
    initial_delay_sec: int,
    log,
) -> str:
    """
    After a successful X action, wait for X to propagate, then call
    qolvex's /api/tasks/{id}/complete. Retries with backoff on "throttled"
    (still-verifying) responses.

    Returns the final claim outcome string:
      "ok"           -> reward credited
      "already-done" -> server says already claimed
      "need-follow"  -> server still doesn't see our follow (keep action pending)
      "need-like"    -> server still doesn't see our like  (keep action pending)
      "throttled"    -> gave up after max retries
      "error"        -> HTTP / network / unknown response
      "skip"         -> missing task_id, nothing to claim
    """
    if not task_id:
        log(f"[{name}] [auto-claim] no task_id on action, skipping claim.")
        return "skip"

    delay = max(1, int(initial_delay_sec))
    log(f"[{name}] [auto-claim] waiting {delay}s for X propagation, then claim task {task_id}...")
    time.sleep(delay)

    for attempt in range(1, AUTO_CLAIM_MAX_RETRIES + 2):  # +1 initial + retries
        # tasks.complete_task takes (cookie, task_id, log=, label=) — qolvex
        # is cookie-only. Note: its own internal 429 retry is orthogonal to
        # ours; we keep the outer retry on need-follow/need-like propagation.
        status, body = complete_task(
            cookie, task_id, log=log, label=f"[{name}] [auto-claim]"
        )
        outcome = _classify_response(status, body)

        if outcome == "ok":
            msg = ""
            if isinstance(body, dict):
                msg = str(body.get("message") or "")
            log(f"[{name}] [auto-claim] [ok] task {task_id} claimed ({msg or 'reward credited'}).")
            return "ok"
        if outcome == "already-done":
            log(f"[{name}] [auto-claim] [already-done] task {task_id}.")
            return "already-done"
        if outcome in ("need-follow", "need-like"):
            if attempt <= AUTO_CLAIM_MAX_RETRIES:
                backoff = AUTO_CLAIM_RETRY_BACKOFF_SEC
                log(
                    f"[{name}] [auto-claim] server says '{outcome}' on task "
                    f"{task_id}; waiting {backoff}s and retrying "
                    f"(attempt {attempt}/{AUTO_CLAIM_MAX_RETRIES})..."
                )
                time.sleep(backoff)
                continue
            log(f"[{name}] [auto-claim] gave up on task {task_id} after "
                f"{attempt} attempt(s) — server still reports '{outcome}'. "
                "Keeping action pending for next run.")
            return outcome
        if outcome == "throttled":
            if attempt <= AUTO_CLAIM_MAX_RETRIES:
                backoff = AUTO_CLAIM_RETRY_BACKOFF_SEC
                log(f"[{name}] [auto-claim] throttled; waiting {backoff}s "
                    f"(attempt {attempt}/{AUTO_CLAIM_MAX_RETRIES})...")
                time.sleep(backoff)
                continue
            log(f"[{name}] [auto-claim] throttled, giving up on task {task_id}.")
            return "throttled"
        # error / unknown
        log(f"[{name}] [auto-claim] [error] task {task_id}: status={status} body={body}")
        return "error"

    return "error"


def load_x_accounts() -> dict[str, dict]:
    """Return {name: {"auth_token": ..., "ct0": ...}} for filled rows."""
    if not X_ACCOUNTS_PATH.exists():
        sys.exit(
            f"[error] {X_ACCOUNTS_PATH} not found.\n"
            "Create it with header row:  name\\tauth_token\\tct0\n"
            "and one row per account. Copy auth_token and ct0 from x.com "
            "cookies (F12 > Application > Cookies > https://x.com)."
        )
    out: dict[str, dict] = {}
    with X_ACCOUNTS_PATH.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            name = (row.get("name") or "").strip()
            if not name:
                continue
            auth_token = (row.get("auth_token") or "").strip()
            ct0 = (row.get("ct0") or "").strip()
            if _is_placeholder(auth_token) or _is_placeholder(ct0):
                continue
            out[name] = {"auth_token": auth_token, "ct0": ct0}
    return out


def load_pending() -> dict:
    """Return the parsed pending_x.json or exit if missing/empty."""
    if not PENDING_X_PATH.exists():
        sys.exit(
            f"[error] {PENDING_X_PATH} not found. Run `python tasks.py` "
            "first to generate the pending list."
        )
    try:
        data = json.loads(PENDING_X_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.exit(f"[error] {PENDING_X_PATH} not valid JSON: {e}")
    accounts = data.get("accounts") or {}
    if not accounts:
        sys.exit(
            f"[info] {PENDING_X_PATH} is empty — nothing to do. "
            "Run `python tasks.py` again or add cookies for new accounts."
        )
    return data


def save_pending(remaining: dict[str, list], log) -> None:
    """Rewrite pending_x.json with only unfinished entries (or delete)."""
    if not remaining:
        if PENDING_X_PATH.exists():
            try:
                PENDING_X_PATH.unlink()
                log(f"[pending-x] removed {PENDING_X_PATH.name} (all done).")
            except OSError as e:
                log(f"[pending-x] could not remove file: {e}")
        return

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "accounts": remaining,
    }
    try:
        PENDING_X_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        total = sum(len(v) for v in remaining.values())
        log(
            f"[pending-x] {total} action(s) still pending across "
            f"{len(remaining)} account(s)."
        )
    except OSError as e:
        log(f"[pending-x] FAILED to rewrite: {e}")


# ---------------------------------------------------------------------------
# Per-account worker
# ---------------------------------------------------------------------------


def process_account(
    name: str,
    actions: list[dict],
    creds: dict,
    log,
    dry_run: bool,
    auto_claim: bool = False,
    claim_creds: dict | None = None,
    claim_delay_sec: int = AUTO_CLAIM_DELAY_SEC_DEFAULT,
) -> tuple[list[dict], dict]:
    """
    Walk one account's pending actions, executing each one.

    When auto_claim is True AND claim_creds has cookie, after each
    successful X action we additionally POST /api/tasks/{id}/complete on
    qolvex to claim the reward. Actions whose claim still says
    "need-follow"/"need-like" are kept in the pending list so the next
    run can retry.

    Returns (still_pending_for_this_account, stats).
    """
    auth_token = creds["auth_token"]
    ct0 = creds["ct0"]

    still_pending: list[dict] = []
    stats = {"ok": 0, "already": 0, "invalid": 0, "auth": 0,
             "rate_limit": 0, "error": 0,
             "claim_ok": 0, "claim_fail": 0, "claim_skip": 0}
    last_outcome: str | None = None

    for i, action in enumerate(actions):
        kind = action.get("action")
        target = action.get("target")
        task_id = action.get("task_id")

        if i > 0 and not dry_run:
            # Fast path: if the previous action was an already-done no-op,
            # X didn't actually change state, so we don't need to throttle
            # this account as heavily.
            if last_outcome == "already":
                sleep_s: float = ALREADY_ACTION_DELAY_SEC
                log(f"[{name}] prev action was already-done; using short "
                    f"{sleep_s:.0f}s delay before next action.")
            else:
                sleep_s = random.uniform(ACTION_DELAY_MIN_SEC, ACTION_DELAY_MAX_SEC)
                log(f"[{name}] sleeping {sleep_s:.1f}s before next action...")
            time.sleep(sleep_s)

        if dry_run:
            log(f"[{name}] [dry-run] would {kind} {target} (task {task_id})")
            still_pending.append(action)
            continue

        if kind == "follow":
            outcome, debug = perform_follow(auth_token, ct0, target)
        elif kind == "like":
            outcome, debug = perform_like(auth_token, ct0, target)
        else:
            outcome, debug = "invalid", {"reason": f"unknown action {kind!r}"}

        x_succeeded = outcome in ("ok", "already")

        if outcome == "ok":
            stats["ok"] += 1
            log(f"[{name}] [ok] {kind} {target} (task {task_id}) {debug}")
        elif outcome == "already":
            stats["already"] += 1
            log(f"[{name}] [already] {kind} {target} (task {task_id}) — "
                f"counting as success.")
        elif outcome == "invalid-target":
            stats["invalid"] += 1
            log(f"[{name}] [invalid-target] {kind} {target} {debug} — dropping.")
        elif outcome == "auth":
            stats["auth"] += 1
            log(f"[{name}] [auth-fail] {kind} {target} {debug} — cookies "
                "expired? Keeping action for next run.")
            still_pending.append(action)
            for leftover in actions[i + 1:]:
                still_pending.append(leftover)
            break
        elif outcome == "rate-limit":
            stats["rate_limit"] += 1
            log(f"[{name}] [rate-limit] {kind} {target} {debug} — backing off "
                "from this account.")
            still_pending.append(action)
            for leftover in actions[i + 1:]:
                still_pending.append(leftover)
            break
        else:
            stats["error"] += 1
            log(f"[{name}] [error] {kind} {target} {debug} — keeping for retry.")
            still_pending.append(action)

        # --- Optional: immediately claim the reward on qolvex ---
        if x_succeeded and auto_claim and not dry_run:
            if not claim_creds or not claim_creds.get("cookie"):
                log(f"[{name}] [auto-claim] no qolvex cookie for this "
                    "account; skipping claim.")
                stats["claim_skip"] += 1
            else:
                effective_claim_delay = (
                    ALREADY_CLAIM_DELAY_SEC
                    if outcome == "already"
                    else claim_delay_sec
                )
                claim_outcome = try_auto_claim(
                    name=name,
                    task_id=task_id,
                    cookie=claim_creds["cookie"],
                    initial_delay_sec=effective_claim_delay,
                    log=log,
                )
                if claim_outcome in ("ok", "already-done"):
                    stats["claim_ok"] += 1
                elif claim_outcome == "skip":
                    stats["claim_skip"] += 1
                else:
                    # need-follow/need-like/throttled/error: X action was
                    # real but claim didn't stick. Put action back in
                    # pending so the next run can retry the claim (no need
                    # to redo the X side — it's already done).
                    stats["claim_fail"] += 1
                    if action not in still_pending:
                        still_pending.append(action)

        last_outcome = outcome

    return still_pending, stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Follow/like pending X tasks and claim the reward "
                    "on qolvex in one shot.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python x_auto.py                              # full pipeline (default)\n"
            "  python x_auto.py --claim-delay 25             # longer X propagation wait\n"
            "  python x_auto.py --name folktor               # one account only\n"
            "  python x_auto.py --skip-claim                 # do X action, don't claim\n"
            "  python x_auto.py --dry-run                    # preview only\n"
        ),
    )
    p.add_argument(
        "--name",
        help="run only this account (default: every account in pending_x.json).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="preview actions without POSTing to X.",
    )
    p.add_argument(
        "--skip-claim",
        action="store_true",
        help=(
            "do the X follow/like but do NOT call /api/tasks/{id}/complete "
            "afterwards. You'll need to run tasks.py separately to actually "
            "credit the reward."
        ),
    )
    p.add_argument(
        "--claim-delay",
        type=int,
        default=AUTO_CLAIM_DELAY_SEC_DEFAULT,
        help=(
            f"seconds to wait between X action and claim (default "
            f"{AUTO_CLAIM_DELAY_SEC_DEFAULT}). Too short = server still "
            "says 'not following yet' and we have to retry."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    log = make_logger("x_auto.log")

    auto_claim = not args.skip_claim

    pending = load_pending()
    accounts_pending: dict[str, list] = pending.get("accounts") or {}

    x_creds = load_x_accounts()
    claim_creds_all = load_qolvex_creds() if auto_claim else {}

    if auto_claim and not claim_creds_all:
        log("[warn] claim step requires cookie in config.json, but no "
            "accounts have it filled. Claims will be skipped. "
            "Run `python tasks.py` manually afterwards to credit rewards.")

    if args.name:
        if args.name not in accounts_pending:
            print(
                f"[error] {args.name!r} has no pending actions in pending_x.json.",
                file=sys.stderr,
            )
            return EXIT_API_ERROR
        accounts_pending = {args.name: accounts_pending[args.name]}

    log(
        f"x_auto start | accounts_pending={list(accounts_pending.keys())} "
        f"x_accounts_filled={len(x_creds)} "
        f"auto_claim={auto_claim} claim_delay={args.claim_delay}s "
        f"dry_run={args.dry_run}"
    )

    grand_stats = {"ok": 0, "already": 0, "invalid": 0, "auth": 0,
                   "rate_limit": 0, "error": 0,
                   "claim_ok": 0, "claim_fail": 0, "claim_skip": 0}
    remaining: dict[str, list] = {}

    account_names = list(accounts_pending.keys())
    for idx, name in enumerate(account_names):
        actions = accounts_pending[name]
        log(f"=== account {idx + 1}/{len(account_names)}: {name} "
            f"({len(actions)} action(s)) ===")

        creds = x_creds.get(name)
        if not creds:
            log(f"[{name}] [skip-no-cookies] no entry in x_accounts.tsv (or "
                f"still placeholder). Keeping {len(actions)} action(s) for next run.")
            remaining[name] = list(actions)
            continue

        if idx > 0 and not args.dry_run:
            sleep_s = random.uniform(ACCOUNT_DELAY_MIN_SEC, ACCOUNT_DELAY_MAX_SEC)
            log(f"[{name}] sleeping {sleep_s:.1f}s before account starts...")
            time.sleep(sleep_s)

        still_pending, stats = process_account(
            name,
            actions,
            creds,
            log,
            args.dry_run,
            auto_claim=auto_claim,
            claim_creds=claim_creds_all.get(name),
            claim_delay_sec=args.claim_delay,
        )

        for k, v in stats.items():
            grand_stats[k] += v

        if still_pending:
            remaining[name] = still_pending

    log("=== summary ===")
    log(
        f"  X: ok={grand_stats['ok']} already={grand_stats['already']} "
        f"invalid-target={grand_stats['invalid']} auth-fail={grand_stats['auth']} "
        f"rate-limit={grand_stats['rate_limit']} error={grand_stats['error']}"
    )
    if auto_claim:
        log(
            f"  claim: ok={grand_stats['claim_ok']} "
            f"fail={grand_stats['claim_fail']} "
            f"skip={grand_stats['claim_skip']}"
        )

    if not args.dry_run:
        save_pending(remaining, log)
    else:
        log("[dry-run] not modifying pending_x.json.")

    succeeded = grand_stats["ok"] + grand_stats["already"]
    return EXIT_OK if succeeded > 0 else EXIT_API_ERROR


if __name__ == "__main__":
    sys.exit(main())
