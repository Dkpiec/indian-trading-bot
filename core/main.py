"""FastAPI application factory — Indian Trading Bot control plane."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings
from core.security import MasterCredentialRouter

log = structlog.get_logger(__name__)


def _run_alembic_upgrade(cfg) -> None:
    """Run alembic upgrade in a sync context (called from thread pool)."""
    from alembic import command
    command.upgrade(cfg, "head")


# ---------------------------------------------------------------------------
# Database + Redis setup
# ---------------------------------------------------------------------------

engine = create_async_engine(
    settings.database_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)

async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

_redis_pool = None


async def get_redis():
    """Lazy Redis connection pool."""
    global _redis_pool
    if _redis_pool is None:
        import redis.asyncio as aioredis
        _redis_pool = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    return _redis_pool


# ---------------------------------------------------------------------------
# Credential router (singleton)
# ---------------------------------------------------------------------------

cred_router = MasterCredentialRouter(async_session_factory)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown hooks."""
    log.info("Indian Trading Bot starting", version="4.0.0")

    # Run Alembic migrations on startup (async-safe: no asyncio.run())
    try:
        from sqlalchemy import text
        async with engine.begin() as conn:
            # Check if tables already exist (idempotent first-run check)
            result = await conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'users'"
            ))
            table_exists = result.scalar() == 1

            if not table_exists:
                # Run the initial migration directly (avoids alembic CLI's asyncio.run)
                from alembic.config import Config
                import asyncio as _asyncio
                alembic_cfg = Config("alembic.ini")
                alembic_cfg.set_main_option("sqlalchemy.url", settings.database_url)

                # alembic command.upgrade is sync; run in a thread to avoid
                # blocking the event loop and avoid nested asyncio.run()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    loop = _asyncio.get_event_loop()
                    await loop.run_in_executor(
                        pool,
                        lambda: _run_alembic_upgrade(alembic_cfg)
                    )
                log.info("Database migrations applied")
            else:
                log.info("Tables already exist, skipping migration")
    except Exception as e:
        log.warning("Migration skipped or failed", error=str(e))

    # ------------------------------------------------------------------
    # Broker clients (real credentials from env / coolify.env)
    # ------------------------------------------------------------------
    from layers.brokers.upstox_client import UpstoxClient
    from layers.brokers.dhan_client import DhanClient

    upstox_client = None
    dhan_client = None

    if settings.upstox_access_token:
        upstox_client = UpstoxClient(access_token=settings.upstox_access_token)
        log.info("Upstox REST client initialized")
    else:
        log.warning("UPSTOX_ACCESS_TOKEN not set — Upstox REST disabled")

    if settings.dhan_access_token and settings.dhan_client_id:
        dhan_client = DhanClient(
            client_id=settings.dhan_client_id,
            access_token=settings.dhan_access_token,
        )
        log.info("Dhan REST client initialized")
    else:
        log.warning("DHAN credentials incomplete — Dhan REST disabled")

    app.state.upstox_client = upstox_client
    app.state.dhan_client = dhan_client

    # ------------------------------------------------------------------
    # Upstox v3 WebSocket streamer (async wrapper)
    # ------------------------------------------------------------------
    from layers.brokers.upstox_ws_v3 import UpstoxWSV3

    upstox_ws = None
    if settings.upstox_access_token:
        upstox_ws = UpstoxWSV3(access_token=settings.upstox_access_token)
        # Don't start streaming yet — subscribe when strategies arm
        log.info("Upstox v3 WS adapter ready (not yet connected)")
    app.state.upstox_ws = upstox_ws

    # ------------------------------------------------------------------
    # Telegram bot (user's trading bot token)
    # ------------------------------------------------------------------
    from layers.telegram.bot_manager import BotManager

    redis = await get_redis()
    bot_manager = BotManager(session_factory=async_session_factory, redis=redis)
    app.state.bot_manager = bot_manager

    tg_token = settings.trading_telegram_bot_token
    tg_chat = settings.telegram_chat_id
    if tg_token and tg_chat:
        try:
            await bot_manager.start_bot(
                user_id="system",
                token=tg_token,
                chat_id=tg_chat,
            )
            log.info("Telegram trading bot started", chat_id=tg_chat)
        except Exception as e:
            log.error("Telegram bot start failed", error=str(e))
    else:
        log.warning("TRADING_TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set — bot disabled")

    # ------------------------------------------------------------------
    # Strategy supervisor loop (background task)
    # ------------------------------------------------------------------
    app.state.supervisor_task = asyncio.create_task(_supervisor_loop())
    log.info("Strategy supervisor started")

    yield

    # Shutdown
    app.state.supervisor_task.cancel()
    try:
        await app.state.supervisor_task
    except asyncio.CancelledError:
        pass

    # Stop Telegram bot
    if bot_manager:
        try:
            await bot_manager.stop_all()
        except Exception as e:
            log.warning("Bot manager shutdown error", error=str(e))

    # Stop Upstox WS
    if upstox_ws:
        try:
            await upstox_ws.stop()
        except Exception as e:
            log.warning("Upstox WS shutdown error", error=str(e))

    # Close broker HTTP clients
    if upstox_client:
        await upstox_client.close()
    if dhan_client:
        await dhan_client.close()

    if _redis_pool:
        await _redis_pool.close()
    await engine.dispose()
    log.info("Indian Trading Bot stopped")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="Indian Trading Bot",
        description="Multi-tenant Indian options trading platform with shared balance pool",
        version="4.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health endpoint
    @app.get("/health")
    async def health():
        return {
            "status": "ok",
            "version": "4.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Register routers
    from rpc.auth import router as auth_router
    from rpc.strategies import router as strategies_router
    from rpc.catalog import router as catalog_router
    from rpc.admin import router as admin_router
    from rpc.telegram_api import router as telegram_router

    app.include_router(auth_router, prefix="/api/v1/auth", tags=["auth"])
    app.include_router(strategies_router, prefix="/api/v1/strategies", tags=["strategies"])
    app.include_router(catalog_router, prefix="/api/v1/catalog", tags=["catalog"])
    app.include_router(admin_router, prefix="/api/v1/admin", tags=["admin"])
    app.include_router(telegram_router, prefix="/api/v1/telegram", tags=["telegram"])

    return app


app = create_app()


# ---------------------------------------------------------------------------
# Strategy Supervisor Loop (runs all armed user strategies concurrently)
# ---------------------------------------------------------------------------

async def _supervisor_loop():
    """Main loop: evaluates all armed strategy instances every tick.

    Each user's armed strategies run as independent asyncio tasks within
    a per-user TaskGroup. Balance gate (20% floor) is checked before each
    entry evaluation.
    """
    log.info("Supervisor loop initialized")
    while True:
        try:
            await asyncio.sleep(1.0)  # 1s tick rate
            # TODO: Phase 3 implementation — iterate armed instances,
            # check balance floor, evaluate entries/exits
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Supervisor loop error", error=str(e))
            await asyncio.sleep(5.0)