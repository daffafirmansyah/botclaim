"""
Shared helpers for the qolvex-withdraw scripts.

Keeps all HTTP, RPC and state-persistence logic in one place so
`withdraw.py` (one-shot) and `monitor.py` (watch loop) stay thin.

Auth model: qolvex.xyz uses COOKIE-ONLY auth (no Bearer JWT). Every
request just needs the full `cookie:` header value pulled from a
logged-in browser session.
"""

from __future__ import annotations

import json
import math
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Endpoint base. Change here if they ever move to a versioned URL.
BASE_URL = "https://qolvex.xyz"

# User / balance endpoint.
#   Observed GET from /dashboard page. Response includes balance fields.
#   TODO(verify): confirm the exact balance field name in the JSON body.
#                 Currently assumes `balanceSolTask`; fall back to
#                 `balanceSol` / `balance` if the primary is missing.
USER_API_URL = f"{BASE_URL}/api/stats/dashboard"

# Withdraw endpoint. Body shape (observed content-length 84 bytes matches):
#     {"amountSol": <float>, "walletAddress": "<44-char-solana-addr>"}
API_URL = f"{BASE_URL}/api/wallet/withdraw"

# Task endpoints.
#   LIST:     GET  /api/tasks
#   COMPLETE: POST /api/tasks/{taskId}/complete   (task id is in the URL path)
TASKS_LIST_URL = f"{BASE_URL}/api/tasks"
def tasks_complete_url(task_id: int | str) -> str:
    return f"{BASE_URL}/api/tasks/{task_id}/complete"

# Solana RPC endpoints, tried in order. First success wins; last-known-good
# is remembered and preferred afterwards.
#
# Helius is the primary because monitor.py polls every 1s (60 req/min) and
# fires bursts during topups; public RPCs rate-limit hard under that load.
# The API key is read from $HELIUS_API_KEY first (rotate without commit),
# falling back to the hardcoded key for VPS convenience.
HELIUS_API_KEY = os.environ.get(
    "HELIUS_API_KEY",
    "4861e563-4c9c-40d9-8e36-aedd41b2a3e3",
)
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

SOLANA_RPCS = [
    HELIUS_RPC,
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
]
SOLANA_RPC = SOLANA_RPCS[0]

# Developer / hot wallet — the one that funds user withdrawals on qolvex.
# monitor.py watches this address; when the balance jumps by
# >= TOPUP_THRESHOLD_LAMPORTS we assume the admin refilled and fire.
HOT_WALLET = "GXZBHbZiFoutudEXJM9HfKpBgyncAtukieGhxPQdne11"

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
STATE_PATH = SCRIPT_DIR / "state.json"
# Balance cache: written by check_status.py, read by priority_sort_accounts.
# Decouples balance discovery from fire timing so per-IP rate-limit doesn't
# blind the sort.
BALANCE_CACHE_PATH = SCRIPT_DIR / "balance_cache.json"
# Cache entries older than this trigger a live-fetch refresh attempt. If that
# refresh fails (per-IP rate-limit), priority_sort_accounts falls back to the
# stale cache value rather than -1, so high-balance accounts keep their slot
# even when the refresh round gets blocked.
# 2 hours is a sweet spot: balances change slowly between topups, and most
# users run fill-cache or cron more often than this anyway.
BALANCE_CACHE_MAX_AGE_SEC = 2 * 3600

# Optional proxy for ALL qolvex traffic (not Solana RPC). One line in
# proxy.txt, e.g. "socks5h://USER:PASS@gw.dataimpulse.com:823" for rotating
# DataImpulse residential proxies. With proxy active, per-IP rate-limit
# (429) is rotated away every request, letting the parallel fire and
# background refresh thread run aggressively without being blocked.
PROXY_PATH = SCRIPT_DIR / "proxy.txt"


def _load_proxy_url() -> str | None:
    """Read the first non-comment line of proxy.txt as the proxy URL.

    Uses utf-8-sig so a BOM (PowerShell `Out-File -Encoding utf8`, Notepad,
    etc.) is auto-stripped instead of contaminating the URL string.
    """
    if not PROXY_PATH.exists():
        return None
    try:
        raw = PROXY_PATH.read_text(encoding="utf-8-sig")
    except OSError:
        return None
    for line in raw.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            return line
    return None


# Loaded once at import; restart the process to pick up proxy.txt changes.
PROXY_URL: str | None = _load_proxy_url()


def get_proxies() -> dict | None:
    """Return a {'http', 'https'} dict for requests, or None if no proxy."""
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}

# Exit codes (used by withdraw.py; monitor.py uses them internally).
EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_COOLDOWN = 2
EXIT_API_ERROR = 3
EXIT_NETWORK = 4

# Protocol / business constants. These mirror the claimyshare defaults;
# tune after observing real qolvex behavior under load.
RATE_LIMIT_WINDOW_SEC = 60
RATE_LIMIT_MAX_REQS = 3
# NOTE: qolvex has no per-account 24h cooldown like claimyshare — claims
# are per-topup-event. Cooldown logic was removed from monitor.py; the
# EXIT_COOLDOWN exit code stays as a defensive fallback if the server
# ever does return a 'too many withdrawals' response.

# Auto-withdraw mode: skip withdraws for accounts whose claimable balance
# is below this. Prevents burning rate-limit budget on dust or on accounts
# already drained in the current cycle. Adjust after checking what the
# smallest payout typically looks like on qolvex.
MIN_WITHDRAW_SOL = 0.0005

# Account that always fires first regardless of balance. Set to '' to
# disable priority pinning. Used by priority_sort_accounts() below, called
# from monitor.py and withdraw.py just before parallel dispatch.
PRIORITY_ACCOUNT_NAME = "hafidz"

# Safety buffer subtracted from claimable balance in 'auto' mode. Qolvex
# occasionally rejects exact-balance withdraws as INSUFFICIENT BALANCE due
# to a tiny drift between the /api/stats/dashboard reading and the moment
# the withdraw POST is processed (rewards may have been re-computed,
# rounding rules at server differ, etc.). Leaving 0.000001 SOL on the table
# is cheap insurance against the whole call failing. Set to 0 to disable.
AUTO_WITHDRAW_BUFFER_SOL = 0.000001

# ----- Retry policy for transient failures -----
# AGGRESSIVE / SNIPE mode: retry 429 and 5xx as fast as possible.
# 24h daily cooldown (server-enforced lock) is NEVER retried — retrying
# just burns rate-limit budget that could snipe other eligible accounts.
MAX_RETRIES_RATE_LIMIT = math.inf      # infinite retries on 429
MAX_RETRIES_SERVER_ERROR = math.inf    # infinite retries on 5xx
RETRY_429_FALLBACK_SEC = 2             # used when server omits Retry-After
RETRY_429_MAX_WAIT_SEC = 2             # cap on actual wait — overrides server
RETRY_429_COOLDOWN_THRESHOLD_SEC = 3600  # Retry-After > 1h => treat as lock
SERVER_ERROR_BACKOFF_SEC = (2, 2, 2, 2)  # flat 2s wait, no escalation
RETRY_JITTER_SEC = 1                     # small jitter to desync workers

# 429 retry for GET /api/stats/dashboard (balance fetch, not time-critical).
# More patient than the withdraw POST retry — we'd rather wait for the
# per-cookie rate-limit window to reset than burn all 3 budget slots in 6s.
BALANCE_FETCH_429_MAX_RETRIES = 3
BALANCE_FETCH_429_WAIT_SEC = 10
BALANCE_FETCH_429_WAIT_MAX_SEC = 60

# Network/SSL errors via SOCKS5 proxy: usually transient (bad exit IP). Retry
# bounded with backoff so DataImpulse rotates between attempts. Withdraw is
# critical (don't lose money to flaky proxy IP), so its retry budget is
# generous; balance fetch is informational and gets fewer.
WITHDRAW_NETWORK_MAX_RETRIES = 8       # 9 total attempts; covers most rotation cycles
WITHDRAW_NETWORK_WAIT_SEC = 2          # base wait — fast, proxy rotates quickly
BALANCE_NETWORK_MAX_RETRIES = 3
BALANCE_NETWORK_WAIT_SEC = 2

# Qolvex returns 429 with Content-Type: text/plain and body "Too many requests"
# (17 bytes). These substrings let us detect the daily-cooldown message that
# sometimes comes back as 200-OK in a JSON body. Tune if qolvex phrases it
# differently (check withdraw.log / monitor.log for actual messages).
COOLDOWN_MESSAGE_KEYWORDS = (
    "too many withdrawal",
    "already withdrawn",
    "daily limit",
    "try again tomorrow",
    "try again in 24",
)

# Site-wide withdraw outage detection. Distinct from 24h per-account
# cooldown: this is a global, temporary pause (observed qolvex response:
# 'Withdrawals are currently unavailable due to technical issues. Please
# try again in 5–30 minutes. Your funds are safe.'). When we see this,
# 2-second retries are actively harmful — they burn rate-limit budget and
# spam the server during its own outage. Pause long and patient instead.
OUTAGE_MESSAGE_KEYWORDS = (
    "currently unavailable",
    "technical issue",      # matches "technical issues" too
    "temporarily unavailable",
    "under maintenance",
    "withdrawals are paused",
    "withdrawals are disabled",
)
# Patient outage wait. Jittered across this range so 65 parallel accounts
# in monitor.py don't all retry at the same second and re-trigger 429.
OUTAGE_WAIT_MIN_SEC = 300   # 5 min — matches server's "5-30 minutes" advice
OUTAGE_WAIT_MAX_SEC = 600   # 10 min upper end of jitter
OUTAGE_MAX_RETRIES = 3      # give up after ~30 min total

Logger = Callable[[str], None]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_logger(log_filename: str) -> Logger:
    """
    Return a thread-safe log(msg) function that prints + appends to
    SCRIPT_DIR/log_filename. Safe to call from multiple worker threads
    (parallel withdraw firing).
    """
    log_path = SCRIPT_DIR / log_filename
    lock = threading.RLock()

    def log(line: str) -> None:
        stamped = f"[{utc_now_iso()}] {line}"
        with lock:
            print(stamped, flush=True)
            try:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(stamped + "\n")
            except OSError as e:
                print(f"[warn] could not write log file {log_path}: {e}", file=sys.stderr)

    return log


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# amount_sol is intentionally NOT here: it's optional (default "auto" =>
# fetch claimable balance at withdraw time). Validated separately.
#
# NOTE: qolvex uses cookie-only auth, so bearer_token is NOT required
# (unlike claimyshare). Keeping the field optional for forward-compat in
# case they add a Bearer header later — but it's ignored today.
REQUIRED_ACCOUNT_FIELDS = ("cookie", "wallet_address")


def _normalize_amount_sol(raw, acc_name: str):
    """
    Accepts:
      - missing / None / "" / "auto" (any case) -> returns "auto" sentinel
      - positive int or float -> returns float
    Anything else aborts via sys.exit(EXIT_CONFIG).
    """
    if raw is None:
        return "auto"
    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in ("", "auto"):
            return "auto"
        try:
            v = float(s)
        except ValueError:
            print(
                f"[error] account {acc_name!r}: amount_sol={raw!r} is not "
                f"a number or \"auto\".",
                file=sys.stderr,
            )
            sys.exit(EXIT_CONFIG)
        if v <= 0:
            print(
                f"[error] account {acc_name!r}: amount_sol must be > 0 "
                f"(got {v}).",
                file=sys.stderr,
            )
            sys.exit(EXIT_CONFIG)
        return v
    if isinstance(raw, (int, float)):
        if raw <= 0:
            return "auto"
        return float(raw)
    print(
        f"[error] account {acc_name!r}: amount_sol has unsupported type "
        f"{type(raw).__name__}.",
        file=sys.stderr,
    )
    sys.exit(EXIT_CONFIG)


# Matches any JSON string value for a sensitive auth key, capturing the key,
# the value contents, and the closing quote so we can scrub embedded control
# chars from the value. DOTALL is set by caller; [^"]* keeps us from reading
# past the closing quote even across multiple physical lines.
_SENSITIVE_KEY_VALUE_RE = re.compile(
    r'("(?:cookie|bearer_token)"\s*:\s*")([^"]*)(")',
    re.DOTALL,
)


def sanitize_json_text(text: str) -> str:
    """
    Strip literal CR / LF / TAB from inside cookie (and bearer_token) string
    values. Browsers sometimes copy cookies with wrapped/embedded newlines
    from DevTools; those break json.loads() because raw control chars are
    not allowed inside JSON string literals.

    This is a no-op if all values are already clean, so it's safe to call
    unconditionally before every parse.
    """
    def _clean(m: re.Match) -> str:
        return m.group(1) + re.sub(r"[\r\n\t]+", "", m.group(2)) + m.group(3)

    return _SENSITIVE_KEY_VALUE_RE.sub(_clean, text)


def _read_config_file() -> dict:
    if not CONFIG_PATH.exists():
        print(
            f"[error] config.json not found at {CONFIG_PATH}.\n"
            "Copy config.example.json -> config.json and fill it in.",
            file=sys.stderr,
        )
        sys.exit(EXIT_CONFIG)

    # utf-8-sig tolerates a BOM if Notepad / PowerShell created the file.
    raw = CONFIG_PATH.read_text(encoding="utf-8-sig")
    # Auto-repair the most common config.json mistake: a cookie pasted from
    # DevTools with an embedded newline. Without this, json.loads() throws
    # "Invalid control character at: ..." and the user has to hand-edit.
    raw = sanitize_json_text(raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[error] config.json is not valid JSON: {e}", file=sys.stderr)
        sys.exit(EXIT_CONFIG)


def load_accounts() -> list[dict]:
    """
    Return a list of account dicts.

    Supports two schemas in config.json:

    1) Multi-account (preferred):
         {"accounts": [ {"name": "acc1", "cookie": ..., ...}, ... ]}

    2) Legacy single-account:
         {"cookie": ..., "wallet_address": ..., "amount_sol": ...}
       This is wrapped as [{"name": "default", ...}].

    Each account MUST have all of REQUIRED_ACCOUNT_FIELDS. Names must be
    unique (used as keys in state.json).
    """
    data = _read_config_file()

    # Legacy single-account.
    if "accounts" not in data:
        missing = [k for k in REQUIRED_ACCOUNT_FIELDS if not data.get(k)]
        if missing:
            print(
                f"[error] config.json missing fields: {', '.join(missing)}",
                file=sys.stderr,
            )
            sys.exit(EXIT_CONFIG)
        acc = dict(data)
        acc["name"] = acc.get("name") or "default"
        acc["amount_sol"] = _normalize_amount_sol(acc.get("amount_sol"), acc["name"])
        return [acc]

    # Multi-account.
    raw_accounts = data.get("accounts")
    if not isinstance(raw_accounts, list) or not raw_accounts:
        print(
            "[error] config.json 'accounts' must be a non-empty list.",
            file=sys.stderr,
        )
        sys.exit(EXIT_CONFIG)

    def _is_blank(v) -> bool:
        return v is None or (isinstance(v, str) and not v.strip())

    seen_names: set[str] = set()
    normalized: list[dict] = []
    skipped_blank: list[str] = []
    for idx, acc in enumerate(raw_accounts):
        if not isinstance(acc, dict):
            print(f"[error] accounts[{idx}] is not an object.", file=sys.stderr)
            sys.exit(EXIT_CONFIG)
        name = acc.get("name") or f"acc{idx + 1}"
        if name in seen_names:
            print(
                f"[error] duplicate account name {name!r} in config.json.",
                file=sys.stderr,
            )
            sys.exit(EXIT_CONFIG)
        seen_names.add(name)

        # Gradual-fill friendly: if a required field is blank (empty string
        # or null), silently skip the account instead of aborting. Lets the
        # user test with a subset of filled accounts while the rest of
        # config.json is still being populated.
        blank = [k for k in REQUIRED_ACCOUNT_FIELDS if _is_blank(acc.get(k))]
        if blank:
            skipped_blank.append(name)
            continue

        cleaned = dict(acc)
        cleaned["name"] = name
        cleaned["amount_sol"] = _normalize_amount_sol(acc.get("amount_sol"), name)
        normalized.append(cleaned)

    if skipped_blank:
        preview = ", ".join(skipped_blank[:5])
        if len(skipped_blank) > 5:
            preview += f", ... +{len(skipped_blank) - 5} more"
        print(
            f"[info] skipped {len(skipped_blank)} account(s) with blank "
            f"cookie/wallet ({preview}). Fill them in config.json to enable.",
            file=sys.stderr,
        )

    if not normalized:
        print(
            "[error] no usable accounts in config.json (all have blank "
            "cookie or wallet_address). Fill in at least one account.",
            file=sys.stderr,
        )
        sys.exit(EXIT_CONFIG)

    return normalized


def load_config() -> dict:
    """Backward-compat wrapper: returns the first account. Deprecated."""
    return load_accounts()[0]


# ---------------------------------------------------------------------------
# Solana RPC helpers
# ---------------------------------------------------------------------------

_last_good_rpc_idx = 0


def _rpc(method: str, params: list, timeout: int = 10) -> dict | None:
    """
    Call a Solana JSON-RPC method with automatic fallover across SOLANA_RPCS.
    Returns the parsed response on first success, or None if every endpoint
    fails / times out.
    """
    global _last_good_rpc_idx
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    n = len(SOLANA_RPCS)
    if n == 0:
        return None
    order = [(_last_good_rpc_idx + i) % n for i in range(n)]
    for idx in order:
        url = SOLANA_RPCS[idx]
        try:
            r = requests.post(url, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data and "result" not in data:
                continue
            _last_good_rpc_idx = idx
            return data
        except Exception:
            continue
    return None


def get_balance_lamports(address: str) -> int | None:
    data = _rpc("getBalance", [address])
    if not data or "result" not in data:
        return None
    try:
        return int(data["result"]["value"])
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
#
# Schema (multi-account):
#   {
#     "last_hot_balance_lamports": <int>,
#     "accounts": {
#       "<account_name>": {
#         "last_success_at": "<ISO UTC>" | null,
#         "last_attempt_ts": <unix seconds>
#       },
#       ...
#     }
#   }

DEFAULT_ACCOUNT_STATE: dict = {
    "last_success_at": None,
    "last_attempt_ts": 0,
}

DEFAULT_STATE: dict = {
    "last_hot_balance_lamports": 0,
    "accounts": {},
}


def _migrate_legacy_state(data: dict) -> dict:
    """Detect single-account state and migrate under the 'default' key."""
    if "accounts" in data:
        return data
    migrated = {
        "last_hot_balance_lamports": int(data.get("last_hot_balance_lamports", 0)),
        "accounts": {
            "default": {
                "last_success_at": data.get("last_success_at"),
                "last_attempt_ts": float(data.get("last_attempt_ts", 0)),
            }
        },
    }
    return migrated


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"last_hot_balance_lamports": 0, "accounts": {}}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        data = _migrate_legacy_state(data)
        if "last_hot_balance_lamports" not in data:
            data["last_hot_balance_lamports"] = 0
        if "accounts" not in data or not isinstance(data["accounts"], dict):
            data["accounts"] = {}
        return data
    except (OSError, json.JSONDecodeError):
        return {"last_hot_balance_lamports": 0, "accounts": {}}


def get_account_state(state: dict, name: str) -> dict:
    """Return (and lazily create) the per-account state entry."""
    accounts = state.setdefault("accounts", {})
    entry = accounts.get(name)
    if entry is None:
        entry = dict(DEFAULT_ACCOUNT_STATE)
        accounts[name] = entry
    else:
        for k, v in DEFAULT_ACCOUNT_STATE.items():
            entry.setdefault(k, v)
    return entry


def save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(
            json.dumps(state, indent=2, sort_keys=True), encoding="utf-8"
        )
    except OSError as e:
        print(f"[warn] could not write state.json: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def build_headers(cookie: str, referer_path: str = "/wallet") -> dict:
    """
    Build headers for a qolvex.xyz API call. Cookie-only auth (no Bearer).

    referer_path: the browser-facing page these headers would come from.
      - "/wallet"     for /api/wallet/withdraw
      - "/dashboard"  for /api/stats/dashboard
      - "/tasks"      for GET /api/tasks
      - "/tasks/<id>" for POST /api/tasks/<id>/complete
    """
    # Normalize: accept "/wallet" or "wallet" or full URL.
    if referer_path.startswith("http"):
        referer = referer_path
    else:
        referer = f"{BASE_URL}{referer_path if referer_path.startswith('/') else '/' + referer_path}"

    return {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "cookie": cookie,
        "origin": BASE_URL,
        "referer": referer,
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
        "sec-ch-ua": '"Chromium";v="147", "Not.A/Brand";v="8"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }


def is_cooldown_message(parsed: dict | None) -> bool:
    """True if response body looks like a server-enforced daily cooldown."""
    if not isinstance(parsed, dict):
        return False
    msg = str(parsed.get("message") or parsed.get("error") or "").lower()
    return any(k in msg for k in COOLDOWN_MESSAGE_KEYWORDS)


def is_outage_message(parsed: dict | None) -> bool:
    """True if response body looks like a site-wide withdraw pause
    (e.g. qolvex's 'currently unavailable due to technical issues').
    Tells us to back off for minutes, not seconds.
    """
    if not isinstance(parsed, dict):
        return False
    msg = str(parsed.get("message") or parsed.get("error") or "").lower()
    return any(k in msg for k in OUTAGE_MESSAGE_KEYWORDS)


def _extract_retry_minutes(parsed: dict | None) -> int | None:
    """Parse 'try again in N minutes' / 'N-M minutes' (also en-dash/em-dash)
    from the response body. Returns the LOWER bound so we don't overshoot
    if the server says 5-30 minutes.
    """
    if not isinstance(parsed, dict):
        return None
    msg = str(parsed.get("message") or parsed.get("error") or "")
    if not msg:
        return None
    m = re.search(r"(\d+)\s*(?:[-–—]\s*(\d+))?\s*minute", msg, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _try_parse_json(resp: requests.Response) -> dict | None:
    """Return parsed JSON body, or None for non-JSON (e.g. text/plain 429)."""
    try:
        parsed = resp.json()
    except ValueError:
        return None
    if isinstance(parsed, dict):
        return parsed
    # Some error payloads are plain strings or arrays; wrap into a dict so the
    # rest of the pipeline can still read .get("message") safely.
    return {"raw": parsed}


def _quick_balance(acc: dict, timeout: float = 3.0) -> float:
    """One-shot balance fetch, NO retries. Returns -1.0 on any failure.

    Used only by priority_sort_accounts() pre-fire — we want fast sort,
    not perfect data. Accounts that 429/timeout sort to the end and fire
    last; they still fire and the infinite-retry policy in attempt_withdraw
    will eventually succeed for them.
    """
    try:
        headers = build_headers(acc["cookie"], referer_path="/dashboard")
        resp = requests.get(
            USER_API_URL, headers=headers, timeout=timeout, proxies=get_proxies()
        )
    except Exception:  # noqa: BLE001
        return -1.0
    if resp.status_code != 200:
        return -1.0
    try:
        parsed = resp.json()
    except Exception:  # noqa: BLE001
        return -1.0
    if not isinstance(parsed, dict):
        return -1.0
    for field in (
        "currentBalance", "balanceSolTask", "balanceSol",
        "balance", "claimable", "claimableSol",
    ):
        v = parsed.get(field)
        if isinstance(v, (int, float)):
            return float(v)
        for sub in parsed.values():
            if isinstance(sub, dict) and field in sub:
                vv = sub[field]
                if isinstance(vv, (int, float)):
                    return float(vv)
    return -1.0


def load_balance_cache() -> dict[str, dict]:
    """Read {name -> {value, fetched_at}} from balance_cache.json.

    Returns empty dict if file missing or malformed.
    """
    if not BALANCE_CACHE_PATH.exists():
        return {}
    try:
        raw = BALANCE_CACHE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, ValueError):
        return {}
    cache = data.get("balances", {})
    return cache if isinstance(cache, dict) else {}


def update_balance_cache(new_values: dict[str, float | None]) -> None:
    """Merge fresh {name -> balance} readings into balance_cache.json.

    None / missing values are skipped (we don't want to clobber a known
    balance with a None from a transient 429). Each entry is timestamped
    independently so partial updates work cleanly.
    """
    if not new_values:
        return
    existing = load_balance_cache()
    now = time.time()
    for name, value in new_values.items():
        if value is None or (isinstance(value, float) and value < 0):
            continue
        existing[name] = {"value": float(value), "fetched_at": now}
    payload = {"balances": existing}
    tmp = BALANCE_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(BALANCE_CACHE_PATH)


def invalidate_balance_cache(name: str) -> bool:
    """Remove a single account's cache entry.

    Used after a successful withdraw so the next priority_sort triggers a
    fresh live-fetch instead of trusting the now-stale (likely zero) value.
    Returns True if an entry was removed, False if it wasn't present.
    """
    existing = load_balance_cache()
    if name not in existing:
        return False
    del existing[name]
    payload = {"balances": existing}
    try:
        tmp = BALANCE_CACHE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(BALANCE_CACHE_PATH)
    except OSError:
        return False
    return True


def priority_sort_accounts(
    accounts: list[dict],
    log: Logger,
    priority_name: str = PRIORITY_ACCOUNT_NAME,
    fetch_timeout: float = 3.0,
    max_workers: int = 50,
    cache_max_age_sec: float = BALANCE_CACHE_MAX_AGE_SEC,
) -> list[dict]:
    """Order accounts: ``priority_name`` first, then by claimable balance desc.

    Strategy:
      1. Read balance_cache.json (populated by check_status.py).
      2. For accounts whose cache entry is fresh (< cache_max_age_sec), use it.
      3. For accounts missing or stale, do a parallel one-shot live fetch.
      4. Sort: priority first, then known balance desc, then unknowns last
         (config order).

    Live fetch is bounded by ``fetch_timeout`` per request, so total added
    latency stays low even when most accounts need it. Run check_status.py
    --retry 2 periodically to keep the cache warm and skip the live fetch
    almost entirely.
    """
    priority = [a for a in accounts if a.get("name") == priority_name]
    others = [a for a in accounts if a.get("name") != priority_name]

    if not others:
        return priority

    cache = load_balance_cache()
    now = time.time()
    balances: dict[str, float] = {}
    needs_live: list[dict] = []
    stale_fallback: dict[str, float] = {}  # last-known good values for stale entries

    for a in others:
        name = a["name"]
        entry = cache.get(name)
        if entry and now - float(entry.get("fetched_at", 0)) < cache_max_age_sec:
            balances[name] = float(entry.get("value", -1.0))
        else:
            needs_live.append(a)
            # Remember any stale value so we can fall back if live fetch fails.
            if entry and "value" in entry:
                val = float(entry["value"])
                if val >= 0:
                    stale_fallback[name] = val

    cache_hits = len(others) - len(needs_live)
    t0 = time.time()
    live_fetched = 0
    fallback_used = 0
    if needs_live:
        workers = min(max_workers, len(needs_live))
        live_results: dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="prio") as pool:
            futs = {
                pool.submit(_quick_balance, a, fetch_timeout): a["name"]
                for a in needs_live
            }
            for fut in as_completed(futs):
                name = futs[fut]
                try:
                    val = fut.result()
                except Exception:  # noqa: BLE001
                    val = -1.0
                live_results[name] = val
                # Apply stale fallback if live fetch failed but we have an old reading.
                if val < 0 and name in stale_fallback:
                    balances[name] = stale_fallback[name]
                    fallback_used += 1
                else:
                    balances[name] = val
        live_fetched = sum(1 for v in live_results.values() if v >= 0)
        # Persist only the successful live values; stale fallbacks already in cache.
        update_balance_cache(live_results)

    others.sort(key=lambda a: -balances.get(a["name"], -1.0))

    elapsed = time.time() - t0
    known = sum(1 for v in balances.values() if v >= 0)
    unknown = len(others) - known
    top_n = min(5, len(others))
    top_preview = ", ".join(
        f"{a['name']}={balances.get(a['name'], -1):.6f}"
        for a in others[:top_n]
    )
    head = f"{priority_name} (priority) + " if priority else ""
    fallback_note = f", stale_fallback={fallback_used}" if fallback_used else ""
    log(
        f"[priority] sorted in {elapsed:.1f}s | order: {head}"
        f"cache_hits={cache_hits}/{len(others)}, "
        f"live_ok={live_fetched}/{len(needs_live)}"
        + fallback_note
        + (f", {unknown} unknown -> last" if unknown else "")
        + f" | top {top_n}: {top_preview}"
    )
    return priority + others


def fetch_claimable_balance(cfg: dict, log: Logger) -> float | None:
    """
    GET /api/stats/dashboard with this account's cookie and return the
    claimable SOL balance as a float.

    TODO(verify): confirm the exact field name. Currently probes the common
    ones in order: balanceSolTask, balanceSol, balance, claimable, rewardSol.
    Once the real field is known, simplify to just that one.

    Returns None on any failure (network, non-200, non-JSON, missing field).
    """
    name = cfg.get("name", "?")
    headers = build_headers(cfg["cookie"], referer_path="/dashboard")

    resp = None
    network_retries_left = BALANCE_NETWORK_MAX_RETRIES
    for attempt in range(BALANCE_FETCH_429_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                USER_API_URL, headers=headers, timeout=15, proxies=get_proxies()
            )
        except requests.RequestException as e:
            # SOCKS5 hiccups (SSL WRONG_VERSION_NUMBER, conn reset, etc.) — try
            # again once or twice while the proxy rotates to a new IP.
            if network_retries_left > 0:
                wait = BALANCE_NETWORK_WAIT_SEC + random.uniform(0, RETRY_JITTER_SEC)
                network_retries_left -= 1
                log(
                    f"[{name}] [balance] network error ({type(e).__name__}); "
                    f"sleep {wait:.1f}s and retry "
                    f"({network_retries_left} network retries left)."
                )
                time.sleep(wait)
                continue
            log(f"[{name}] [balance] network error: {e}")
            return None
        if resp.status_code != 429 or attempt == BALANCE_FETCH_429_MAX_RETRIES:
            break
        # 429: respect Retry-After if present, else fall back to constant.
        raw = resp.headers.get("retry-after", "")
        try:
            server_wait = int(raw) if raw else 0
        except ValueError:
            server_wait = 0
        wait = max(server_wait, BALANCE_FETCH_429_WAIT_SEC)
        wait = min(wait, BALANCE_FETCH_429_WAIT_MAX_SEC)
        wait += random.uniform(0, RETRY_JITTER_SEC)
        log(
            f"[{name}] [balance] 429 on /api/stats/dashboard; sleep "
            f"{wait:.1f}s then retry {attempt + 2}/{BALANCE_FETCH_429_MAX_RETRIES + 1}."
        )
        time.sleep(wait)

    if resp.status_code != 200:
        log(f"[{name}] [balance] unexpected status {resp.status_code}.")
        return None

    parsed = _try_parse_json(resp)
    if not isinstance(parsed, dict):
        log(f"[{name}] [balance] non-JSON or non-object response.")
        return None

    # Probe known/guessed field names in priority order. First numeric wins.
    # `currentBalance` is the confirmed qolvex.xyz field (observed in real
    # /api/stats/dashboard response). Others are kept as fallbacks in case
    # the schema changes or other sites reuse this code.
    # Observed qolvex response also exposes `totalEarned`, `tasksCompleted`,
    # `totalReferrals`, `referralEarnings`, `availableTasks`,
    # `tournamentPrizeSol`, `tournamentRank` — but only `currentBalance` is
    # the spendable/withdrawable figure.
    candidate_fields = (
        "currentBalance",
        "balanceSolTask",
        "balanceSol",
        "balance",
        "claimable",
        "claimableSol",
        "rewardSol",
        "pendingSol",
    )
    for field in candidate_fields:
        val = parsed.get(field)
        if val is None:
            # Also search 1-level-nested structures (e.g. parsed["stats"]["balance"]).
            for v in parsed.values():
                if isinstance(v, dict) and field in v:
                    val = v[field]
                    break
        if val is None:
            continue
        try:
            fv = float(val)
        except (TypeError, ValueError):
            continue
        return fv

    log(
        f"[{name}] [balance] no known balance field found in /api/stats/dashboard "
        f"response. Top-level keys: {list(parsed.keys())}"
    )
    return None


# ---------------------------------------------------------------------------
# Core withdraw attempt
# ---------------------------------------------------------------------------

def attempt_withdraw(
    cfg: dict,
    log: Logger,
    verify_onchain: bool = True,
) -> tuple[int, dict | None, int]:
    """
    Send exactly one POST to /api/wallet/withdraw for a single account.

    Args:
        cfg: account dict with cookie, wallet_address. amount_sol optional:
               - "auto" (or missing) -> fetch balance, withdraw whatever is claimable.
               - positive number -> withdraw exactly that.
        log: logger callable.
        verify_onchain: if True (default), sleep 30s after a 2xx success and
             confirm the balance delta on-chain. Set False when iterating
             multiple accounts on a single top-up event to stay snappy.

    Returns (exit_code, parsed_body_or_none, http_status_or_0).
    """
    wallet = cfg["wallet_address"]
    name = cfg.get("name", "?")

    raw_amount = cfg.get("amount_sol", "auto")
    if isinstance(raw_amount, str) and raw_amount.strip().lower() == "auto":
        balance = fetch_claimable_balance(cfg, log)
        if balance is None:
            log(f"[{name}] [error] could not fetch claimable balance; skipping.")
            return EXIT_API_ERROR, None, 0
        if balance < MIN_WITHDRAW_SOL:
            log(
                f"[{name}] [skip] claimable balance {balance:.9f} SOL "
                f"below threshold {MIN_WITHDRAW_SOL} SOL; nothing to withdraw."
            )
            return EXIT_COOLDOWN, None, 0
        # Leave a small buffer so we don't trigger qolvex's INSUFFICIENT
        # BALANCE error on edge-case rounding mismatches.
        amount = balance - AUTO_WITHDRAW_BUFFER_SOL
        if amount < MIN_WITHDRAW_SOL:
            log(
                f"[{name}] [skip] balance {balance:.9f} SOL minus "
                f"buffer {AUTO_WITHDRAW_BUFFER_SOL} SOL would be "
                f"below threshold {MIN_WITHDRAW_SOL} SOL; skipping."
            )
            return EXIT_COOLDOWN, None, 0
        log(
            f"[{name}] [auto] claimable={balance:.9f} SOL, "
            f"withdrawing {amount:.9f} SOL (buffer {AUTO_WITHDRAW_BUFFER_SOL})."
        )
    else:
        amount = float(raw_amount)

    pre: int | None = None
    if verify_onchain:
        pre = get_balance_lamports(wallet)
        if pre is not None:
            log(f"[{name}] pre-balance on-chain: {pre / 1e9:.9f} SOL")

    headers = build_headers(cfg["cookie"], referer_path="/wallet")
    # Body shape CONFIRMED from live browser DevTools Payload capture:
    #   POST /api/wallet/withdraw  {"solanaAddress": "<44-char-b58>",
    #                               "amountSol": <float SOL>}
    # (Not "address"/"amount" — the 400 "Valid Solana address and amount
    # required" message is a generic validator response, not a hint at
    # the real field names. Earlier guesses were wrong.)
    body = {"solanaAddress": wallet, "amountSol": amount}
    # Keep the outgoing-body log so future shape regressions are easy to
    # spot. Wallet redacted to last 4 chars.
    log(
        f"[{name}] POST {API_URL} body={{'solanaAddress': "
        f"'...{wallet[-4:]}', 'amountSol': {amount}}}"
    )

    rate_limit_retries_left = MAX_RETRIES_RATE_LIMIT
    server_error_retries_left = MAX_RETRIES_SERVER_ERROR
    outage_retries_left = OUTAGE_MAX_RETRIES
    network_retries_left = WITHDRAW_NETWORK_MAX_RETRIES
    attempt_num = 0

    while True:
        attempt_num += 1

        try:
            resp = requests.post(
                API_URL, headers=headers, json=body, timeout=30, proxies=get_proxies()
            )
        except requests.RequestException as e:
            # Almost always a transient SOCKS5 / SSL hiccup from a flaky
            # DataImpulse exit IP. Wait briefly for the proxy to rotate and
            # try again; only give up after WITHDRAW_NETWORK_MAX_RETRIES.
            if network_retries_left > 0:
                wait = WITHDRAW_NETWORK_WAIT_SEC + random.uniform(0, RETRY_JITTER_SEC)
                network_retries_left -= 1
                log(
                    f"[{name}] [retry] network error ({type(e).__name__}: {e}); "
                    f"sleeping {wait:.1f}s for proxy rotation "
                    f"(attempt {attempt_num + 1}, "
                    f"{network_retries_left} network retries left)."
                )
                time.sleep(wait)
                continue
            log(f"[{name}] [error] network error during POST (retries exhausted): {e}")
            return EXIT_NETWORK, None, 0

        status = resp.status_code
        parsed = _try_parse_json(resp)
        body_repr = parsed if parsed is not None else resp.text[:200]

        log(f"[{name}] response status={status} body={body_repr!r}")

        # ----- 429: rate-limit, retry fast -----
        if status == 429:
            retry_after_raw = resp.headers.get("retry-after", "")
            try:
                retry_after = int(retry_after_raw) if retry_after_raw else RETRY_429_FALLBACK_SEC
            except ValueError:
                retry_after = RETRY_429_FALLBACK_SEC

            if retry_after > RETRY_429_COOLDOWN_THRESHOLD_SEC:
                log(
                    f"[{name}] [cooldown] 429 retry-after={retry_after}s "
                    f"exceeds {RETRY_429_COOLDOWN_THRESHOLD_SEC}s; "
                    f"treating as cooldown."
                )
                return EXIT_COOLDOWN, parsed, status

            effective_wait = min(retry_after, RETRY_429_MAX_WAIT_SEC)

            if rate_limit_retries_left > 0:
                wait = effective_wait + random.uniform(0, RETRY_JITTER_SEC)
                rate_limit_retries_left -= 1
                left_str = (
                    "unlimited"
                    if rate_limit_retries_left == math.inf
                    else f"{int(rate_limit_retries_left)}"
                )
                log(
                    f"[{name}] [retry] 429 rate-limit; sleeping {wait:.1f}s "
                    f"then retry (attempt {attempt_num + 1}, "
                    f"{left_str} retries left)."
                )
                time.sleep(wait)
                continue

            log(f"[{name}] [cooldown] 429 retries exhausted; giving up.")
            return EXIT_COOLDOWN, parsed, status

        # ----- 200-ish + cooldown message: 24h daily cooldown, NO retry -----
        if is_cooldown_message(parsed):
            log(f"[{name}] [cooldown] daily cooldown message detected; not retrying.")
            return EXIT_COOLDOWN, parsed, status

        # NOTE: outage-detection branch (is_outage_message) was removed per
        # user preference for pure snipe mode — when qolvex is in a global
        # withdraw outage, we'd rather keep hammering at 2s so we catch the
        # instant it recovers than politely wait 5-30 min and miss the
        # topup window. The is_outage_message() / _extract_retry_minutes()
        # helpers are retained in the module for quick re-enable if/when
        # the trade-off changes.

        # ----- 5xx server unavailable: retry fast (2s, infinite) -----
        if 500 <= status < 600:
            if server_error_retries_left > 0:
                idx = min(attempt_num - 1, len(SERVER_ERROR_BACKOFF_SEC) - 1)
                base = SERVER_ERROR_BACKOFF_SEC[idx]
                wait = base + random.uniform(0, RETRY_JITTER_SEC)
                server_error_retries_left -= 1
                left_str = (
                    "unlimited"
                    if server_error_retries_left == math.inf
                    else f"{int(server_error_retries_left)}"
                )
                log(
                    f"[{name}] [retry] {status} server error; sleeping "
                    f"{wait:.1f}s then retry (attempt {attempt_num + 1}, "
                    f"{left_str} retries left)."
                )
                time.sleep(wait)
                continue

            log(f"[{name}] [error] {status} server error; retries exhausted.")
            return EXIT_API_ERROR, parsed, status

        # ----- 2xx success path -----
        if 200 <= status < 300:
            # Success is either:
            #   - parsed is a dict with success=True (or no explicit flag)
            #   - parsed is a dict missing explicit success (assume OK on 2xx)
            #   - parsed is None but status==200 (plain-text OK response)
            if parsed is None or not isinstance(parsed, dict) or parsed.get("success", True):
                if verify_onchain:
                    log(f"[{name}] [ok] API success. verifying on-chain in 30s...")
                    time.sleep(30)
                    post = get_balance_lamports(wallet)
                    if post is not None and pre is not None:
                        delta = (post - pre) / 1e9
                        log(
                            f"[{name}] post-balance: {post / 1e9:.9f} SOL | "
                            f"delta: {delta:+.9f} SOL"
                        )
                        if delta > 0:
                            log(f"[{name}] [ok] on-chain delta confirms withdraw landed.")
                        else:
                            log(
                                f"[{name}] [warn] API success but on-chain delta "
                                "is zero. Withdraw may still be queued."
                            )
                else:
                    log(f"[{name}] [ok] API success (on-chain verify skipped).")
                return EXIT_OK, parsed, status

        log(f"[{name}] [error] unexpected response (treated as failure).")
        return EXIT_API_ERROR, parsed, status
