"""
Qolvex auto social-tasks — one-shot mode.

For every account in config.json:
  1. GET  /api/tasks                          -> list of available tasks
  2. Filter to follow/like task types          -> skip everything else
  3. POST /api/tasks/{taskId}/complete         -> per-task, sequential
  4. Classify the response by message content
  5. Tasks that need a real X action are written to pending_x.json so a
     Phase-2 script can pick them up later.

IMPORTANT: this script does NOT do the actual follow/like on X. If a task
requires a real follow you haven't done yet, qolvex will reject the claim —
bot will skip it and write the target to pending_x.json.

NOTE: qolvex's complete-task endpoint takes the task id in the URL path
(POST /api/tasks/41/complete), NOT in the body — unlike claimyshare.
Request body is small (~21 bytes observed) but the exact shape is
TO-VERIFY. See TASK_COMPLETE_BODY below.

Examples:
  python tasks.py                    # all accounts, sequential
  python tasks.py --parallel         # all accounts, parallel + stagger
  python tasks.py --name acc1        # only one account
  python tasks.py --dry-run          # list eligible tasks, don't POST
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from core import (
    EXIT_API_ERROR,
    EXIT_OK,
    SCRIPT_DIR,
    TASKS_LIST_URL,
    build_headers,
    load_accounts,
    make_logger,
    tasks_complete_url,
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

# Backend verifies follow/like against X's API after we POST. Fire too fast
# and it returns "still verifying" / rate-limits. 8s is the safe baseline
# carried over from claimyshare — tune after observing qolvex behavior.
TASK_INTER_DELAY_SEC = 8

# Only attempt tasks whose title starts with one of these (case-insensitive).
# Anything else (Register, Share, Visit, etc.) is skipped silently.
TASK_TITLE_PREFIXES = ("follow", "like", "retweet", "repost")

# Per-request HTTP timeout.
HTTP_TIMEOUT_SEC = 20

# Parallel mode: fire multiple accounts at once. Each account still walks
# its own task list sequentially with TASK_INTER_DELAY_SEC between tasks.
MAX_PARALLEL_WORKERS = 8
PARALLEL_STAGGER_MS = 500

# 429 retry policy — applies to both GET /api/tasks and POST /api/tasks/{id}/
# complete. More patient than core.py's snipe-mode because tasks don't race
# the hot wallet; we'd rather wait than burn the cookie's rate-limit budget
# on instant retries. Retry-After header (if the server sends one) is
# respected when longer than TASK_429_WAIT_SEC.
TASK_429_MAX_RETRIES = 3
TASK_429_WAIT_SEC = 10
TASK_429_WAIT_MAX_SEC = 60

# Output file for tasks that need a real follow/like on X.
PENDING_X_PATH = SCRIPT_DIR / "pending_x.json"

# ----- Task-complete request body -----
# TO-VERIFY: qolvex expects a small (~21 byte) JSON body on
# POST /api/tasks/{id}/complete. Paste the actual payload from
# DevTools -> Network -> (task complete request) -> Request Payload
# and replace this constant accordingly. Common guesses (all close to 21 bytes):
#     {}                          (empty body, 2 bytes)
#     {"verification":true}       (21 bytes — plausible)
#     {"verified":true}           (17 bytes)
#     {"action":"verify"}         (20 bytes)
# If the real body is empty, set to {} here.
TASK_COMPLETE_BODY: dict = {}

# Response classification — keywords matched (case-insensitive) against the
# response 'message' / 'error' field. Tune after seeing real qolvex replies.
_OK_KEYWORDS = (
    "task completed",
    "completed successfully",
    "reward",
    "success",
)
_NEED_FOLLOW_KEYWORDS = (
    "not following",
    "please follow",
    "follow the account",
    "follow and try",
)
_NEED_LIKE_KEYWORDS = (
    "haven't liked",
    "have not liked",
    "please like",
    "like the post",
    "like and try",
)
_ALREADY_DONE_KEYWORDS = (
    "already completed",
    "already claimed",
    "already done",
)
_THROTTLED_KEYWORDS = (
    "still verifying",
    "still syncing",
    "try again later",
    "try again in a few",
    "too many requests",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tasks_list_headers(cookie: str) -> dict:
    """Referer for GET /api/tasks is the /tasks page."""
    return build_headers(cookie, referer_path="/tasks")


def _task_complete_headers(cookie: str, task_id: int | str) -> dict:
    """Referer for POST /api/tasks/{id}/complete is the per-task page."""
    return build_headers(cookie, referer_path=f"/tasks/{task_id}")


def _is_eligible(task: dict) -> bool:
    """True if the task is a follow/like type and not already completed."""
    # Title is the primary signal; some APIs also expose `type` or `category`.
    title = str(task.get("title") or task.get("name") or "").strip().lower()
    ttype = str(task.get("type") or task.get("category") or "").strip().lower()

    title_match = title.startswith(TASK_TITLE_PREFIXES)
    type_match = any(t in ttype for t in TASK_TITLE_PREFIXES)
    if not (title_match or type_match):
        return False

    if task.get("completed") is True:
        return False
    if task.get("done") is True:
        return False
    if task.get("isCompleted") is True:
        return False
    return True


def _classify_response(status: int, body: dict | None) -> str:
    """
    Coarse outcome classification.

    Returns one of:
      "ok"           -> reward claimed
      "need-follow"  -> backend says we haven't followed on X yet
      "need-like"    -> backend says we haven't liked the tweet on X yet
      "already-done" -> task was already completed earlier
      "throttled"    -> verification still in progress / soft rate limit
      "error"        -> anything else, including network failures
    """
    msg = ""
    if isinstance(body, dict):
        msg = str(
            body.get("message")
            or body.get("error")
            or body.get("raw")
            or ""
        ).lower()

    if 200 <= status < 300 and any(k in msg for k in _OK_KEYWORDS):
        return "ok"
    # Some successful responses have NO message at all, just data fields.
    if 200 <= status < 300 and isinstance(body, dict) and (
        body.get("rewardSol") or body.get("reward") or body.get("success") is True
    ):
        return "ok"
    if any(k in msg for k in _NEED_FOLLOW_KEYWORDS):
        return "need-follow"
    if any(k in msg for k in _NEED_LIKE_KEYWORDS):
        return "need-like"
    if any(k in msg for k in _ALREADY_DONE_KEYWORDS):
        return "already-done"
    if status == 429 or any(k in msg for k in _THROTTLED_KEYWORDS):
        return "throttled"
    return "error"


def _infer_action(task: dict) -> str:
    """Return 'follow' or 'like' based on task title/type. Default 'follow'."""
    t = f"{task.get('title', '')} {task.get('type', '')}".lower()
    if "like" in t:
        return "like"
    if "retweet" in t or "repost" in t:
        return "retweet"
    return "follow"


def _build_pending_entry(task: dict, outcome: str) -> dict | None:
    """Build a pending_x.json entry for a task needing a real X action."""
    if outcome not in ("need-follow", "need-like"):
        return None

    action = "follow" if outcome == "need-follow" else "like"

    # Prefer structured fields if the API exposes them.
    target = (
        task.get("verificationTarget")
        or task.get("target")
        or task.get("handle")
    )
    url = task.get("url") or task.get("link") or ""

    if not target and url:
        if action == "follow":
            tail = url.rstrip("/").split("/")[-1]
            target = f"@{tail}" if tail else None
        else:
            parts = url.split("/status/")
            if len(parts) == 2:
                target = parts[1].split("?")[0].split("/")[0] or None

    if not target:
        return None

    return {
        "task_id": task.get("id"),
        "title": task.get("title") or task.get("name"),
        "action": action,
        "target": target,
        "url": url or None,
        "reward_sol": task.get("rewardSol") or task.get("reward"),
        "verification_type": task.get("verificationType") or task.get("type"),
    }


def _fmt_reward(parsed: dict | None) -> str:
    """Compact one-line reward summary from the complete-task response."""
    if not isinstance(parsed, dict):
        return ""
    sol = parsed.get("rewardSol") or parsed.get("reward") or 0
    pts = parsed.get("points") or parsed.get("rewardPoints") or 0
    parts = []
    if sol:
        parts.append(f"+{sol} SOL")
    if pts:
        parts.append(f"+{pts} pts")
    return " ".join(parts) if parts else "(no reward fields)"


def _compute_429_wait(resp: requests.Response) -> float:
    """Wait time for a 429 retry: max(server Retry-After, TASK_429_WAIT_SEC),
    capped at TASK_429_WAIT_MAX_SEC, plus sub-second jitter."""
    raw = resp.headers.get("retry-after", "")
    try:
        server_wait = int(raw) if raw else 0
    except ValueError:
        server_wait = 0
    wait = max(server_wait, TASK_429_WAIT_SEC)
    wait = min(wait, TASK_429_WAIT_MAX_SEC)
    return wait + random.uniform(0, 1)


def fetch_tasks(cookie: str, log=None, label: str = "") -> list[dict]:
    """GET /api/tasks for one account, with transparent 429 retry.
    Raises on network error or on non-2xx that is not 429 (or on 429 after
    all retries are exhausted)."""
    resp = None
    for attempt in range(TASK_429_MAX_RETRIES + 1):
        resp = requests.get(
            TASKS_LIST_URL,
            headers=_tasks_list_headers(cookie),
            timeout=HTTP_TIMEOUT_SEC,
        )
        if resp.status_code != 429 or attempt == TASK_429_MAX_RETRIES:
            break
        wait = _compute_429_wait(resp)
        if log:
            log(
                f"{label} [retry] 429 on /api/tasks; sleep {wait:.1f}s "
                f"then retry {attempt + 2}/{TASK_429_MAX_RETRIES + 1}."
            )
        time.sleep(wait)

    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("tasks", "data", "result", "items"):
            v = data.get(key)
            if isinstance(v, list):
                return v
    raise ValueError(f"unexpected /api/tasks response shape: {type(data).__name__}")


def complete_task(
    cookie: str, task_id: int | str, log=None, label: str = ""
) -> tuple[int, dict | None]:
    """POST /api/tasks/{id}/complete with TASK_COMPLETE_BODY, with 429 retry."""
    resp = None
    for attempt in range(TASK_429_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                tasks_complete_url(task_id),
                headers=_task_complete_headers(cookie, task_id),
                json=TASK_COMPLETE_BODY,
                timeout=HTTP_TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            return 0, {"error": f"network: {e}"}
        if resp.status_code != 429 or attempt == TASK_429_MAX_RETRIES:
            break
        wait = _compute_429_wait(resp)
        if log:
            log(
                f"{label} [retry] 429 on task {task_id} complete; "
                f"sleep {wait:.1f}s then retry {attempt + 2}/{TASK_429_MAX_RETRIES + 1}."
            )
        time.sleep(wait)

    try:
        parsed = resp.json()
    except ValueError:
        # qolvex returns text/plain "Too many requests" on 429; keep the raw
        # string so the classifier can still see it.
        parsed = {"raw": resp.text[:300]}
    return resp.status_code, parsed


# ---------------------------------------------------------------------------
# Per-account worker
# ---------------------------------------------------------------------------


def process_account(acc: dict, log, dry_run: bool) -> dict:
    """Walk one account's task list and POST complete on each eligible task."""
    name = acc.get("name", "?")
    cookie = acc["cookie"]

    empty_result = {
        "name": name,
        "ok": 0,
        "need_follow": 0,
        "need_like": 0,
        "already_done": 0,
        "throttled": 0,
        "error": 0,
        "skipped_other": 0,
        "reward_sol": 0.0,
        "pending_x": [],
    }

    log(f"[{name}] fetching tasks list...")
    try:
        tasks = fetch_tasks(cookie, log=log, label=f"[{name}]")
    except Exception as e:  # noqa: BLE001
        log(f"[{name}] [error] failed to fetch tasks: {e}")
        empty_result["error"] = 1
        return empty_result

    eligible = [t for t in tasks if _is_eligible(t)]
    other = len(tasks) - len(eligible)
    log(
        f"[{name}] {len(tasks)} task(s) total | "
        f"{len(eligible)} eligible (follow/like/retweet) | "
        f"{other} skipped (other types or already done)."
    )

    result = dict(empty_result)
    result["skipped_other"] = other

    if not eligible:
        return result

    for i, task in enumerate(eligible):
        tid = task.get("id")
        title = task.get("title") or task.get("name") or "?"
        expected = task.get("rewardSol") or task.get("reward") or "?"

        if i > 0 and not dry_run:
            log(f"[{name}] sleeping {TASK_INTER_DELAY_SEC}s before next task...")
            time.sleep(TASK_INTER_DELAY_SEC)

        if dry_run:
            log(
                f"[{name}] [dry-run] would POST /api/tasks/{tid}/complete "
                f"('{title}', expected +{expected} SOL, "
                f"verify={task.get('verificationType') or task.get('type')})."
            )
            continue

        log(f"[{name}] posting /api/tasks/{tid}/complete ('{title}')...")
        status, body = complete_task(cookie, tid, log=log, label=f"[{name}]")
        outcome = _classify_response(status, body)

        if outcome == "ok":
            result["ok"] += 1
            try:
                r = (body or {}).get("rewardSol") or (body or {}).get("reward") or 0
                result["reward_sol"] += float(r)
            except (TypeError, ValueError):
                pass
            log(f"[{name}] [ok] task {tid} '{title}' -> {_fmt_reward(body)}")

        elif outcome == "need-follow":
            result["need_follow"] += 1
            entry = _build_pending_entry(task, outcome)
            if entry:
                result["pending_x"].append(entry)
            target = (entry or {}).get("target", task.get("verificationTarget", "?"))
            log(f"[{name}] [need-follow] task {tid} '{title}' -> follow {target} on X")

        elif outcome == "need-like":
            result["need_like"] += 1
            entry = _build_pending_entry(task, outcome)
            if entry:
                result["pending_x"].append(entry)
            target = (entry or {}).get("target", task.get("verificationTarget", "?"))
            log(f"[{name}] [need-like] task {tid} '{title}' -> like tweet {target} on X")

        elif outcome == "already-done":
            result["already_done"] += 1
            log(f"[{name}] [already-done] task {tid} '{title}' (server says claimed before)")

        elif outcome == "throttled":
            result["throttled"] += 1
            log(
                f"[{name}] [throttled] task {tid} '{title}' "
                f"status={status} body={body} — re-run later."
            )

        else:
            result["error"] += 1
            log(
                f"[{name}] [error] task {tid} '{title}' "
                f"status={status} body={body}"
            )

    return result


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------


def _run_sequential(accounts: list[dict], log, dry_run: bool) -> list[dict]:
    results: list[dict] = []
    for i, acc in enumerate(accounts):
        log(f"=== account {i + 1}/{len(accounts)}: {acc.get('name', '?')} ===")
        results.append(process_account(acc, log, dry_run))
    return results


def _run_parallel(accounts: list[dict], log, dry_run: bool) -> list[dict]:
    stagger = max(PARALLEL_STAGGER_MS, 0) / 1000.0
    total_dispatch = stagger * (len(accounts) - 1)
    log(
        f"[parallel] firing {len(accounts)} account(s) "
        f"(max workers={MAX_PARALLEL_WORKERS}, "
        f"stagger={PARALLEL_STAGGER_MS}ms => dispatch window {total_dispatch:.1f}s)."
    )
    workers = min(MAX_PARALLEL_WORKERS, len(accounts))
    results: list[dict] = []

    def _delayed(acc: dict, delay: float) -> dict:
        if delay > 0:
            time.sleep(delay)
        return process_account(acc, log, dry_run)

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="task") as ex:
        futures = [
            ex.submit(_delayed, acc, i * stagger)
            for i, acc in enumerate(accounts)
        ]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:  # noqa: BLE001
                log(f"[error] worker thread crashed: {e}")
                results.append({
                    "name": "?", "ok": 0, "fail": 0, "skipped": 0, "reward_sol": 0.0,
                })
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Auto-complete qolvex follow/like tasks for all accounts.",
    )
    p.add_argument(
        "--parallel",
        action="store_true",
        help="run accounts in parallel (default: sequential).",
    )
    p.add_argument(
        "--name",
        help="run only the account with this name (default: all accounts).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch + filter tasks but do NOT POST complete.",
    )
    return p.parse_args()


def _write_pending_x(results: list[dict], log) -> None:
    payload_accounts: dict[str, list] = {}
    for r in results:
        pending = r.get("pending_x") or []
        if pending:
            payload_accounts[r["name"]] = pending

    if not payload_accounts:
        if PENDING_X_PATH.exists():
            try:
                PENDING_X_PATH.unlink()
                log(f"[pending-x] removed stale {PENDING_X_PATH.name} (nothing pending).")
            except OSError as e:
                log(f"[pending-x] could not remove stale {PENDING_X_PATH.name}: {e}")
        return

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "accounts": payload_accounts,
    }
    try:
        PENDING_X_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        total = sum(len(v) for v in payload_accounts.values())
        log(
            f"[pending-x] wrote {total} pending action(s) across "
            f"{len(payload_accounts)} account(s) to {PENDING_X_PATH.name}."
        )
    except OSError as e:
        log(f"[pending-x] FAILED to write {PENDING_X_PATH.name}: {e}")


def main() -> int:
    args = _parse_args()
    accounts = load_accounts()

    if args.name:
        match = [a for a in accounts if a.get("name") == args.name]
        if not match:
            print(f"[error] account {args.name!r} not found in config.json.",
                  file=sys.stderr)
            return EXIT_API_ERROR
        accounts = match

    log = make_logger("tasks.log")
    log(
        f"tasks one-shot start | accounts={[a['name'] for a in accounts]} "
        f"mode={'parallel' if args.parallel else 'sequential'} "
        f"dry_run={args.dry_run}"
    )

    if args.parallel and len(accounts) > 1:
        results = _run_parallel(accounts, log, args.dry_run)
    else:
        results = _run_sequential(accounts, log, args.dry_run)

    log("=== summary ===")
    for r in results:
        log(
            f"  {r['name']}: ok={r['ok']} "
            f"need-follow={r['need_follow']} need-like={r['need_like']} "
            f"already-done={r['already_done']} throttled={r['throttled']} "
            f"error={r['error']} other-skipped={r['skipped_other']} "
            f"reward=+{r['reward_sol']:.6f} SOL"
        )

    total_ok = sum(r["ok"] for r in results)
    total_need_follow = sum(r["need_follow"] for r in results)
    total_need_like = sum(r["need_like"] for r in results)
    total_already = sum(r["already_done"] for r in results)
    total_throttled = sum(r["throttled"] for r in results)
    total_error = sum(r["error"] for r in results)
    total_reward = sum(r["reward_sol"] for r in results)

    log(
        f"TOTAL across {len(results)} account(s): "
        f"ok={total_ok} need-follow={total_need_follow} "
        f"need-like={total_need_like} already-done={total_already} "
        f"throttled={total_throttled} error={total_error} "
        f"reward=+{total_reward:.6f} SOL"
    )

    if (total_need_follow + total_need_like) > 0:
        log("=== pending X actions (need real follow/like) ===")
        for r in results:
            for p in r.get("pending_x") or []:
                log(f"  {r['name']}: {p['action']} {p['target']} "
                    f"(task {p['task_id']}, +{p.get('reward_sol', 0)} SOL)")

    if not args.dry_run:
        _write_pending_x(results, log)

    return EXIT_OK if total_ok > 0 else EXIT_API_ERROR


if __name__ == "__main__":
    sys.exit(main())
