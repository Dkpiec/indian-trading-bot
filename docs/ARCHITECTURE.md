# Indian Trading Bot — Architecture

Version 4.0.0. A multi-tenant Indian options trading platform: a FastAPI
control plane that arms strategies for many users, executes them through
broker adapters (Dhan / Upstox), and exposes an interactive Telegram
command surface.

## Service topology (do NOT mix these up)

| Service | Coolify app | Source dir | Owns |
|---|---|---|---|
| Hermes Multi-broker Trading engine | (V0 engine) | `/opt/data/trading-engine` | Execution loop, strategy ticks, REST |
| Indian-Crypto-Bot | `indian-crypto-bot` | `/opt/data/Indian_Crypto_Bot` | Crypto futures (its own codebase) |
| **Indian-trading-bot** | `indian-trading-bot` | `/opt/data/indian-trading-bot` | This app |

**Token ownership:** the interactive Telegram bot token is now owned by
**this app** (`indian-trading-bot`). The V0 engine's interactive poller is
gated off (`V0_TELEGRAM_POLLING=0`), so only ITB polls the token and
answers `/commands`. The V0 engine keeps its own execution loop + REST.
If you hand the interactive token back to the engine, set
`V0_TELEGRAM_POLLING=1` on V0 and `TRADING_BOT_POLLING=0` on ITB.

## Layer map

```
core/
  main.py        FastAPI app factory + lifespan + supervisor loop
  config.py      Settings (env-driven, 12-factor)
  security.py    MasterCredentialRouter, JWT, insecure-default guard

layers/
  brokers/       dhan_client.py, upstox_client.py, upstox_ws_v3.py, base.py
  state/         models.py (SQLAlchemy), fallback_db.py
  strategies/    engine.py (execution engine), balance_gate.py
  telegram/      bot_manager.py (manual poll + dispatch + user map),
                 commands.py (command handlers, MENU_COMMANDS)

catalog/         5 strategy plugins (ORB / SL-Hunt, NIFTY/BANKNIFTY/SENSEX)
rpc/             FastAPI routers: auth, strategies, catalog, admin,
                 telegram_api, positions  (mounted at /api/v1/*)
alembic/         DB migrations
tests/           unit tests
```

## Data flow

1. **Startup** (`core/main.py` lifespan): init DB (async engine + Alembic
   migrations), seed the strategy catalog, init broker clients from env /
   disk credentials, start the Upstox v3 WS LTP feed, start the Telegram
   bot manager, start the `ExecutionEngine` + supervisor loop.
2. **Execution**: `ExecutionEngine.tick()` (1 Hz) — query armed
   subscriptions, fetch LTP (Upstox WS, Dhan fallback), evaluate strategy
   plugins, place Dhan orders, update `StrategyInstance` /
   `TradeOrderLedger`.
3. **Commands**: Telegram poller (`bot_manager._manual_polling_loop`) →
   `commands.py` handlers → mutate `app.state.execution_engine` /
   `bot_manager` / broker clients in-memory, persist user mappings via
   `RedisStore` / DB.
4. **REST**: `rpc/*` routers expose the same state for programmatic use.

## Config

All secrets come from env / `coolify.env` — never hardcoded:
- `TRADING_TELEGRAM_BOT_TOKEN`, `TRADING_TELEGRAM_CHAT_ID` (interactive token)
- `TRADING_BOT_POLLING` (`1` = this app polls the token; `0` = REST-only)
- `UPSTOX_ACCESS_TOKEN`, `UPSTOX_ANALYTICS_TOKEN`
- `DHAN_ACCESS_TOKEN`, `DHAN_CLIENT_ID`
- `DATABASE_URL`, `REDIS_URL`, `JWT_SECRET`

## Automated workflow

- **CI (GitHub Actions):** `.github/workflows/ci.yml` — ruff (lint) +
  pytest + pyright (type-check) on every push/PR. **Deploy only when
  green.**
- **Pre-commit:** `.pre-commit-config.yaml` — ruff + test gate before
  committing.
- **Deployment:** Coolify auto-deploy on green CI; health-check gated.
