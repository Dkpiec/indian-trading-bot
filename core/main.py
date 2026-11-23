"""FastAPI application factory — Indian Trading Bot control plane."""
from __future__ import annotations

import asyncio
import time
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

engine = None
async_session_factory = None
_db_available = False
_db_init_attempted = False


async def _init_database_async() -> None:
    """Initialize the async database engine. Must be called from async context.

    If the target database doesn't exist yet, connects to the 'postgres'
    maintenance database and creates it first.
    """
    global engine, async_session_factory, _db_available, _db_init_attempted
    if _db_init_attempted:
        return

    _db_init_attempted = True
    try:
        db_url = settings.database_url
        db_name = db_url.rstrip('/').split('/')[-1]

        from sqlalchemy import text

        # Test connection to target DB; if it doesn't exist, create it
        test_engine = create_async_engine(db_url, pool_size=1, echo=False)
        try:
            async with test_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            log.info("Database connection OK", db=db_name)
        except Exception:
            # Database doesn't exist — create it via 'postgres' DB
            admin_url = db_url.replace(f"/{db_name}", "/postgres")
            admin_engine = create_async_engine(admin_url, pool_size=1, echo=False)
            try:
                async with admin_engine.connect() as admin_conn:
                    await admin_conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                    await admin_conn.commit()
                    log.info("Created database", db=db_name)
            finally:
                await admin_engine.dispose()
        finally:
            await test_engine.dispose()

        # Now create the main engine (DB should exist)
        engine = create_async_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,
        )
        async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        _db_available = True
        log.info("Database engine initialized", url=db_url.split("@")[-1])
    except Exception as e:
        log.warning("Database engine init failed — running in degraded mode", error=str(e))
        _db_available = False

# Database init is deferred to lifespan (async-safe)
# _db_available is set to True after _init_database_async() succeeds in lifespan

_redis_pool = None


async def get_redis():
    """Lazy Redis connection pool. Returns None if Redis is unavailable."""
    global _redis_pool
    if _redis_pool is None:
        try:
            import redis.asyncio as aioredis
            _redis_pool = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
        except Exception as e:
            log.warning("Redis pool init failed — running without Redis", error=str(e))
            _redis_pool = False  # sentinel: tried and failed
    return _redis_pool if _redis_pool else None


# ---------------------------------------------------------------------------
# Credential router (singleton) — initialized in lifespan after DB is ready
# ---------------------------------------------------------------------------

cred_router = None
# Will be set in lifespan() after _init_database_async() succeeds


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown hooks."""
    log.info("Indian Trading Bot starting", version="4.0.0")

    # Initialize database (async-safe)
    global cred_router
    await _init_database_async()

    # Initialize credential router now that DB is available
    if _db_available and async_session_factory is not None:
        cred_router = MasterCredentialRouter(async_session_factory)
        log.info("Credential router initialized")
    else:
        log.warning("Credential router disabled — database unavailable")

    # Run Alembic migrations on startup (async-safe: no asyncio.run())
    global engine
    if _db_available and engine is not None:
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
    else:
        log.info("Database unavailable — skipping migrations (degraded mode)")

    # ------------------------------------------------------------------
    # Seed strategy catalog if empty (only when DB is available)
    # ------------------------------------------------------------------
    if _db_available:
        try:
            from sqlalchemy import text as _text
            async with async_session_factory() as session:
                # Clear any stale sample/old strategies that don't match live roster
                await session.execute(_text("""
                    DELETE FROM strategy_catalog WHERE id IN (
                        'nifty-orb-v1', 'banknifty-momentum-v1', 'nifty-mean-revert-v1',
                        'dhan_v6e_nifty', 'dhan_v6e_nifty_pe', 'nifty_slhunt_reversal',
                        'orb_banknifty_fyers', 'orb_banknifty_fyers_pe', 'orb_sensex_dhan',
                        'orb_sensex_dhan_pe', 'sensex_slhunt_ce', 'sensex_slhunt_pe'
                    )
                """))
                await session.commit()

                result = await session.execute(_text("SELECT COUNT(*) FROM strategy_catalog"))
                count = result.scalar() or 0
                if count == 0:
                    await session.execute(_text("""
                        INSERT INTO strategy_catalog (id, display_name, description, plugin_file, underlying, segment, strategy_type, default_params, is_active, version, created_at, updated_at)
                        VALUES
                        ('NIFTY101', 'NIFTY ORB v6e', 'NIFTY ORB v6e CE+PE legs', 'dhan_v6e_nifty.py', 'NIFTY', 'NSE_FNO', 'orb', '{"legs":["CE","PE"]}', true, '6e', NOW(), NOW()),
                        ('NIFTY102', 'NIFTY SL-Hunt v4.1b', 'NIFTY SL-Hunt v4.1b CE leg', 'nifty_slhunt_reversal.py', 'NIFTY', 'NSE_FNO', 'slhunt', '{"legs":["CE"]}', true, '4.1b', NOW(), NOW()),
                        ('BANKNIFTY101', 'BankNifty ORB v2', 'BankNifty ORB Invalidation v2 CE+PE monthly', 'orb_banknifty_fyers.py', 'BANKNIFTY', 'NSE_FNO', 'orb', '{"legs":["CE","PE"]}', true, '2', NOW(), NOW()),
                        ('SENSEX101', 'SENSEX ORB Invalidation', 'SENSEX ORB Invalidation CE+PE legs', 'orb_sensex_dhan.py', 'SENSEX', 'NSE_FNO', 'orb', '{"legs":["CE","PE"]}', true, '5', NOW(), NOW()),
                        ('SENSEX102', 'SENSEX SL-Hunt V4.1g', 'SENSEX SL-Hunt V4.1g CE legs', 'sensex_slhunt_ce.py', 'SENSEX', 'NSE_FNO', 'slhunt', '{"legs":["CE"]}', true, '4.1g', NOW(), NOW())
                        ON CONFLICT (id) DO NOTHING
                    """))
                    await session.commit()
                    log.info("Seeded 5 merged strategies (CE+PE legs) from Hermes multi-broker engine")
                else:
                    log.info(f"Strategy catalog already has {count} entries, skipping seed")
        except Exception as e:
            log.warning("Strategy catalog seed skipped", error=str(e))
    else:
        log.info("Database unavailable — skipping strategy catalog seed (degraded mode)")

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
    ws_token = settings.upstox_analytics_token or settings.upstox_access_token
    if ws_token:
        upstox_ws = UpstoxWSV3(
            access_token=settings.upstox_access_token,
            analytics_token=settings.upstox_analytics_token,
        )
        # Don't start streaming yet — subscribe when strategies arm
        log.info("Upstox v3 WS adapter ready (not yet connected)", token_type="analytics" if settings.upstox_analytics_token else "access")
    else:
        log.warning("No Upstox token available — v3 WS disabled")
    app.state.upstox_ws = upstox_ws

    # ------------------------------------------------------------------
    # Telegram bot (user's trading bot token)
    # ------------------------------------------------------------------
    from layers.telegram.bot_manager import BotManager

    redis = await get_redis()
    bot_manager = BotManager(
        session_factory=async_session_factory,
        redis=redis,
        dhan_client=dhan_client,
    )
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
    # Strategy execution engine + supervisor loop
    # ------------------------------------------------------------------
    from layers.strategies.engine import ExecutionEngine

    engine = ExecutionEngine(
        session_factory=async_session_factory,
        dhan_client=dhan_client,
        upstox_ws=upstox_ws,
        balance_floor_pct=settings.balance_floor_pct,
    )
    app.state.execution_engine = engine
    await engine.start()

    app.state.supervisor_task = asyncio.create_task(_supervisor_loop(engine))
    log.info("Strategy supervisor started", engine_source=engine._feed.source)

    yield

    # Shutdown
    app.state.supervisor_task.cancel()
    try:
        await app.state.supervisor_task
    except asyncio.CancelledError:
        pass

    # Stop execution engine
    if hasattr(app.state, "execution_engine"):
        try:
            await app.state.execution_engine.stop()
        except Exception as e:
            log.warning("Execution engine shutdown error", error=str(e))

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

    if _redis_pool and _redis_pool is not False:
        await _redis_pool.close()
    if engine:
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
        db_status = "unavailable"
        if _db_available and engine:
            try:
                from sqlalchemy import text
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                db_status = "connected"
            except Exception:
                db_status = "unavailable"
        
        redis_status = "unavailable"
        if _redis_pool and _redis_pool is not False:
            try:
                await _redis_pool.ping()
                redis_status = "connected"
            except Exception:
                redis_status = "unavailable"
        
        return {
            "status": "ok",
            "version": "4.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": db_status,
            "redis": redis_status,
        }
    
    # Status endpoint (was missing)
    @app.get("/api/v1/status")
    async def get_status():
        """Get bot status: database, redis, broker clients, active strategies."""
        from core.main import _db_available as db_avail, _redis_pool as redis_p
        status = {
            "database": "connected" if db_avail else "unavailable",
            "redis": "connected" if redis_p and redis_p is not False else "unavailable",
            "dhan_client": bool(app.state.dhan_client) if hasattr(app.state, "dhan_client") else False,
            "upstox_client": bool(app.state.upstox_client) if hasattr(app.state, "upstox_client") else False,
            "active_strategies": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # Count active strategy instances if DB available
        if db_avail:
            try:
                from sqlalchemy import select, func
                from layers.state.models import StrategyInstance
                async with async_session_factory() as session:
                    result = await session.execute(
                        select(func.count()).where(StrategyInstance.status == "RUNNING")
                    )
                    status["active_strategies"] = result.scalar() or 0
            except Exception:
                pass
        return status
    
    # Balance endpoint (was missing)
    @app.get("/api/v1/balance")
    async def get_balance():
        """Get balance from active broker client."""
        if not hasattr(app.state, "dhan_client") or not app.state.dhan_client:
            return {
                "status": "unavailable",
                "message": "No broker client initialized. Check DHAN_ACCESS_TOKEN env var.",
            }
        try:
            balance = await app.state.dhan_client.get_balance()
            return {
                "status": "ok",
                "total_equity": balance.total_equity,
                "free_balance": balance.free_balance,
                "used_margin": balance.used_margin,
                "timestamp": balance.timestamp,
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
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

async def _supervisor_loop(engine=None):
    """Main loop: delegates to the execution engine every second.

    The engine handles:
      - querying armed subscriptions
      - fetching market data (Upstox WS / Dhan fallback)
      - calling evaluate_tick() on strategy plugins
      - placing Dhan orders
      - updating StrategyInstance / TradeOrderLedger rows
    """
    if engine is None:
        # Fallback: no engine (DB unavailable at startup)
        log.info("Supervisor loop running without engine (degraded)")
        while True:
            await asyncio.sleep(30.0)
            return

    log.info("Supervisor loop started", feed=engine._feed.source)
    consecutive_errors = 0
    last_error_log = 0.0
    while True:
        try:
            await asyncio.sleep(1.0)
            await engine.tick()
            consecutive_errors = 0  # Reset on success
        except asyncio.CancelledError:
            raise
        except Exception as e:
            consecutive_errors += 1
            now = time.time()
            # Log first error immediately, then throttle to every 60s
            if consecutive_errors == 1 or (now - last_error_log) > 60:
                log.error(
                    "Supervisor loop error",
                    error=str(e),
                    consecutive=consecutive_errors,
                )
                last_error_log = now
            await asyncio.sleep(min(5.0 * consecutive_errors, 60.0))