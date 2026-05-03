# qolvex-withdraw

Minimal auto-withdraw + auto-tasks for one or more of your own accounts
on `qolvex.xyz`. Fires one POST per account per eligible window, with
on-chain confirmation, persistent state, and clean cooldown handling.

Modeled after the `claimyshare-withdraw` project (same architecture,
different site). **Key differences** vs claimyshare:

| | claimyshare | qolvex |
| --- | --- | --- |
| Auth | Bearer JWT + cookie | Cookie only |
| Task complete | `POST /api/tasks/complete` with `{taskId}` in body | `POST /api/tasks/{taskId}/complete` (path param) |
| Dashboard | `/api/user` | `/api/stats/dashboard` |
| Withdraw | `/api/withdraw` | `/api/wallet/withdraw` |
| Hot wallet | `8MrX...` | `GXZBHbZiFoutudEXJM9HfKpBgyncAtukieGhxPQdne11` |

## Modes

| Mode | Entry point | What it does |
| --- | --- | --- |
| **One-shot withdraw** | `python withdraw.py` | Fires one POST per account in `config.json` in parallel, exits. |
| **Monitor withdraw** | `python monitor.py` | Watches the dev wallet on-chain. The moment it's topped up, fires one withdraw in parallel for every eligible account. Keeps running. |
| **One-shot tasks** | `python tasks.py` | Fetches each account's task list, POSTs complete for every follow/like task, writes pending X actions to `pending_x.json`. |

Pick **one** withdraw mode at a time — running both wastes rate-limit
budget.

## Setup

Requires Python 3.10+.

```powershell
cd C:\Users\daffa\CascadeProjects\qolvex-withdraw
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create your config:

```powershell
copy config.example.json config.json
```

`config.json` supports multiple accounts:

```json
{
  "accounts": [
    { "name": "acc1", "cookie": "...", "wallet_address": "...", "amount_sol": "auto" },
    { "name": "acc2", "cookie": "...", "wallet_address": "...", "amount_sol": "auto" }
  ]
}
```

Pull each field from your browser:

| Field | Where to get it |
| --- | --- |
| `name` | Any unique label per account (e.g. `acc1`). Used as a key in `state.json` and in log lines. |
| `cookie` | DevTools > Network > any authenticated `/api/...` request > Request Headers > `cookie`. Copy the **entire** value. That's the only auth qolvex uses. |
| `wallet_address` | Your Solana wallet where withdrawals should land. |
| `amount_sol` | Either a number, or `"auto"` (recommended — fetch current claimable balance and withdraw that). |

`config.json` is gitignored — don't commit it.

### Adding accounts (helper)

```powershell
python add_account.py              # interactive
python add_account.py --list       # list without leaking cookie
python add_account.py --remove acc1
python add_account.py --bulk accounts.tsv
```

Bulk file format (header row required, tab-separated):

```
name<TAB>cookie<TAB>wallet_address<TAB>amount_sol
acc1<TAB>session=...<TAB>GXZB...<TAB>auto
acc2<TAB>session=...<TAB>GXZB...<TAB>0.0033999998
```

## Run: one-shot withdraw

```powershell
python withdraw.py
```

Fires every account in `config.json` in parallel via a thread pool
(max 20 concurrent). Set `PARALLEL_FIRE = False` at the top of
`withdraw.py` to fall back to a sequential 5 s-spaced loop.

## Run: monitor mode

```powershell
python monitor.py
```

What happens on start:

1. Load per-account state from `state.json`.
2. Poll the hot wallet (`GXZB...dne11`) every 10 seconds.
3. When balance jumps by >= `TOPUP_THRESHOLD_LAMPORTS` (0.1 SOL default),
   fire one POST per eligible account in parallel (up to 32 workers).
4. After each success, that account is locked out for ~23 h 55 m.
5. State is persisted per-account so Ctrl+C -> restart is safe.

Tunables (top of `monitor.py`):

| Name | Default | Meaning |
| --- | --- | --- |
| `POLL_INTERVAL_SEC` | 10 | How often to hit Solana RPC for the hot wallet balance. |
| `TOPUP_THRESHOLD_LAMPORTS` | 100,000,000 (0.1 SOL) | Ignore dust / tx-fee noise; only react to real refills. |
| `PARALLEL_FIRE` | True | Fire eligible accounts concurrently. |
| `MAX_PARALLEL_WORKERS` | 32 | Cap on concurrent in-flight POSTs. |
| `PARALLEL_STAGGER_MS` | 2 | Per-account dispatch offset. |
| `HOT_WALLET_FLOOR_LAMPORTS` | 200,000 | Pre-flight floor: abort batch if hot wallet already below this. |
| `HEARTBEAT_INTERVAL_SEC` | 300 | Log an "alive" line every N seconds. Set 0 to disable. |

## Run: auto-tasks

```powershell
python tasks.py                   # all accounts, sequential
python tasks.py --parallel        # all accounts, parallel
python tasks.py --name acc1       # just one
python tasks.py --dry-run         # list eligible tasks, don't POST
```

For each account, fetches `/api/tasks`, filters to follow/like tasks,
and POSTs `/api/tasks/{id}/complete` one by one with an 8 s delay
between each. Tasks that fail because you haven't actually followed /
liked on X yet are written to `pending_x.json` (not auto-performed —
follow those manually and re-run).

## Fields that need verification against live qolvex traffic

This project was scaffolded from one round of network captures. The
following constants may need adjustment once real request/response
samples are observed. **Search for `TO-VERIFY` in the source** to find
each one:

1. **`TASK_COMPLETE_BODY` in `tasks.py`** — the ~21-byte JSON body qolvex
   expects on `POST /api/tasks/{id}/complete`. Currently `{}`. Replace
   with the real shape (check DevTools > Network > complete request >
   Request Payload).

2. **Balance field name in `core.py`** — `fetch_claimable_balance()`
   probes a list of common field names (`balanceSolTask`, `balanceSol`,
   `balance`, `claimable`, etc.) and returns the first numeric hit. Once
   you see the real `/api/stats/dashboard` response, simplify to just
   the correct field.

3. **`COOLDOWN_MESSAGE_KEYWORDS` in `core.py`** — substrings that mark a
   24h daily-cooldown response. Based on claimyshare's phrasing; tune
   if qolvex uses different wording.

4. **`_OK_KEYWORDS` / `_NEED_FOLLOW_KEYWORDS` / etc. in `tasks.py`** —
   substrings that classify task-complete responses. Tune after
   observing a few real replies (success + need-follow + already-done).

## Files

| File | Purpose |
| --- | --- |
| `core.py` | Shared helpers: HTTP POST, Solana RPC, state persistence, account loading. Cookie-only auth. |
| `withdraw.py` | One-shot entry point. Fires every account once (parallel by default). |
| `monitor.py` | Watch-loop entry point. Polls hot wallet, fires eligible accounts on top-up. |
| `tasks.py` | One-shot auto follow/like task claimer. |
| `add_account.py` | Interactive / bulk helper to append accounts to `config.json`. |
| `config.example.json` | Template. Copy to `config.json` and fill. |
| `config.json` | Your real credentials, multi-account (gitignored). |
| `state.json` | Per-account persisted state (gitignored). |
| `withdraw.log` / `monitor.log` / `tasks.log` | Append-only audit logs. |
| `pending_x.json` | Tasks that need real follow/like on X before they'll claim. |
| `requirements.txt` | Python deps. Just `requests`. |

## Safety notes

- **Do not deposit SOL / USDC to qolvex.** Any "unlock withdrawal by
  paying fee" prompt is a scam pattern.
- **Multi-account is risky.** Reward sites commonly detect sybil
  patterns and forfeit all related balances at once. Prefer different
  `wallet_address` per account.
- **Cross-check every payout on-chain** via
  `https://solscan.io/account/<your_wallet>`. If the API reports success
  but nothing lands on-chain for 10+ minutes, treat the success as
  suspect.
- **Cookie lifetime** — sessions typically expire in days/weeks. When
  `withdraw.py` starts returning 401/403 for every account, log in
  again and paste the fresh `cookie` value into `config.json`.
