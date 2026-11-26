# Indian Trading Bot

Async-first algorithmic trading platform for Indian equity and derivatives markets (NSE/BSE). Phase 1 delivers the project scaffold, strategy catalog, broker integration stubs, and database migration infrastructure.

## Architecture

- **core/** — Application entrypoint, configuration, and security primitives
- **catalog/** — Strategy implementations (ORB invalidation, SL hunt reversal, momentum straddle)
- **layers/** — Layered architecture: brokers, state persistence, strategy orchestration, Telegram notifications
- **rpc/** — FastAPI routers for admin, auth, strategy catalog, and Telegram API
- **alembic/** — Async PostgreSQL migrations via asyncpg

## Tech Stack

- Python 3.12+, FastAPI, Uvicorn
- SQLAlchemy 2.0 (async), asyncpg, Alembic
- Redis (hiredis) for caching/pub-sub
- python-telegram-bot for notifications
- Broker SDKs: Upstox (local wheelhouse), DhanHQ

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env  # Create and fill in secrets

# Run migrations
alembic upgrade head

# Start the server
uvicorn core.main:app --reload
```

## Database Migrations

Alembic is configured for async PostgreSQL via asyncpg. The migration environment imports models from `layers.state.models` automatically.

```bash
# Create a new migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head

# Rollback one step
alembic downgrade -1
```

## Project Structure

```
indian-trading-bot/
├── alembic/              # Migration scripts and env.py
├── catalog/              # Strategy implementations
├── core/                 # Config, main app, security
├── layers/
│   ├── brokers/          # Broker integration stubs
│   ├── state/            # ORM models and persistence
│   ├── strategies/       # Strategy orchestration
│   └── telegram/         # Notification layer
├── rpc/                  # API routers
├── tests/                # Test suite
├── alembic.ini           # Alembic configuration
├── docker-compose.yml    # Local dev services
├── Dockerfile            # Production container
└── requirements.txt      # Pinned dependencies
```

## License

Private — all rights reserved.# Trigger rebuild Thu Nov 26 06:19:28 UTC 2026
