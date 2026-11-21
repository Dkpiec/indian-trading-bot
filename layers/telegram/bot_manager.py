"""Telegram Bot Manager — one Application per unique bot token, not per user.

Memory-efficient lifecycle:
- Multiple users sharing the same bot_token share ONE Application instance (~2MB each).
- stop_bot() checks remaining user count via DB before stopping — never kills a bot
  still used by others. Uses redis.delete() + DB count instead of redis.keys() (O(N) bug).
- chat_id → user_id mapping stored in Redis for fast routing in command handlers.
"""
from __future__ import annotations

import asyncio
from functools import partial
from typing import TYPE_CHECKING, Optional

import structlog
from telegram import BotCommand
from telegram.ext import Application, ApplicationBuilder, CommandHandler

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

log = structlog.get_logger(__name__)

# Menu commands shown in Telegram's command menu
MENU_COMMANDS = [
    BotCommand("catalog", "Browse available strategies"),
    BotCommand("subscribe", "Subscribe to a strategy"),
    BotCommand("unsubscribe", "Unsubscribe from a strategy"),
    BotCommand("arm", "Arm a subscribed strategy"),
    BotCommand("disarm", "Disarm an active strategy"),
    BotCommand("status", "Strategy status & balance overview"),
    BotCommand("balance", "Equity / free / used / floor breakdown"),
    BotCommand("trades", "Open positions"),
    BotCommand("profit", "Unrealized P&L summary"),
    BotCommand("forceexit", "Close all open positions immediately"),
    BotCommand("help", "Show this help message"),
]


class BotManager:
    """Manages python-telegram-bot Application instances keyed by bot_token.

    Shared-token deduplication: if N users connect the same bot token, only
    one Application is created. Each Application costs ~2MB memory.

    Lifecycle:
        start_bot(user_id, token, chat_id) → create or reuse Application
        stop_bot(user_id, token)           → remove user mapping; stop App only
                                              when no other users remain
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis: Redis,
        dhan_client=None,
    ) -> None:
        self._session_factory = session_factory
        self._redis = redis
        self._app_state_dhan_client = dhan_client
        # token → Application instance
        self._apps: dict[str, Application] = {}
        # token → set of user_ids currently using this token
        self._token_users: dict[str, set[str]] = {}
        # chat_id → user_id (in-memory cache; Redis is source of truth)
        self._chat_user_map: dict[str, str] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start_bot(self, user_id: str, token: str, chat_id: str) -> None:
        """Start or reuse a bot Application for the given token.

        - If no Application exists for this token, build one, register all
          command handlers, set the menu, initialize, start polling.
        - Map chat_id → user_id in both Redis and in-memory cache so that
          incoming updates can be routed to the correct user.
        """
        async with self._lock:
            if token not in self._apps:
                app = await self._build_application(token)
                self._apps[token] = app
                self._token_users[token] = set()
                log.info("Bot application started", token_prefix=token[:10])

            self._token_users[token].add(user_id)
            self._chat_user_map[str(chat_id)] = user_id

            # Persist mapping in Redis for cross-process lookups
            await self._redis.set(f"tg_user:{chat_id}", user_id)

        log.info(
            "Bot user registered",
            user_id=user_id,
            chat_id=chat_id,
            token_prefix=token[:10],
            total_users_on_token=len(self._token_users.get(token, set())),
        )

    async def stop_bot(self, user_id: str, token: str) -> None:
        """Remove a user from a bot. Stop the Application only if no users remain.

        BUG FIX (from plan audit): previous implementation used redis.keys()
        which is O(N) and blocks the event loop. Fixed approach:
        1. Delete this user's chat mapping from Redis (O(1)).
        2. Remove from in-memory tracking.
        3. Query DB for remaining users with this exact encrypted token.
        4. Only stop the Application when count == 0.
        """
        async with self._lock:
            # Remove chat mappings for this user
            chat_ids_to_remove = [
                cid for cid, uid in self._chat_user_map.items() if uid == user_id
            ]
            for cid in chat_ids_to_remove:
                await self._redis.delete(f"tg_user:{cid}")
                del self._chat_user_map[cid]

            # Remove from token tracking
            if token in self._token_users:
                self._token_users[token].discard(user_id)

            remaining_in_memory = len(self._token_users.get(token, set()))

            # Also check DB for any users we might not know about in-memory
            # (e.g., after a restart where in-memory state was lost)
            remaining_in_db = await self._count_users_with_token(token)
            remaining = max(remaining_in_memory, remaining_in_db)

            if remaining > 0:
                log.info(
                    "Bot kept alive for remaining users",
                    token_prefix=token[:10],
                    remaining=remaining,
                )
                return

            # No users left — tear down the Application
            app = self._apps.pop(token, None)
            self._token_users.pop(token, None)

            if app is not None:
                try:
                    await app.updater.stop()
                except Exception as e:
                    log.warning("Updater stop error (non-fatal)", error=str(e))
                try:
                    await app.stop()
                except Exception as e:
                    log.warning("App stop error (non-fatal)", error=str(e))
                try:
                    await app.shutdown()
                except Exception as e:
                    log.warning("App shutdown error (non-fatal)", error=str(e))

                log.info("Bot application fully stopped", token_prefix=token[:10])

    async def resolve_user_id(self, chat_id: str) -> Optional[str]:
        """Look up the user_id for a given chat_id.

        Checks in-memory cache first, then falls back to Redis.
        Returns None if no mapping exists (unauthorized chat).
        """
        user_id = self._chat_user_map.get(str(chat_id))
        if user_id:
            return user_id

        # Fallback to Redis (survives restarts)
        user_id = await self._redis.get(f"tg_user:{chat_id}")
        if user_id:
            self._chat_user_map[str(chat_id)] = user_id
        return user_id

    async def send_message(self, chat_id: str, text: str) -> bool:
        """Send a message to a chat using the appropriate bot Application.

        Returns True on success, False on failure.
        """
        user_id = await self.resolve_user_id(chat_id)
        if not user_id:
            log.warning("Cannot send message: no user mapping", chat_id=chat_id)
            return False

        # Find which Application serves this chat
        for token, app in self._apps.items():
            if user_id in self._token_users.get(token, set()):
                try:
                    await app.bot.send_message(chat_id=chat_id, text=text)
                    return True
                except Exception as e:
                    log.error("Failed to send Telegram message", error=str(e), chat_id=chat_id)
                    return False

        log.warning("No active bot for user", user_id=user_id, chat_id=chat_id)
        return False

    async def stop_all(self) -> None:
        """Gracefully stop all running bot Applications (shutdown hook)."""
        tokens = list(self._apps.keys())
        for token in tokens:
            app = self._apps.pop(token, None)
            self._token_users.pop(token, None)
            if app is not None:
                try:
                    await app.updater.stop()
                    await app.stop()
                    await app.shutdown()
                except Exception as e:
                    log.warning("Error during stop_all", error=str(e), token_prefix=token[:10])

        self._chat_user_map.clear()
        log.info("All bot applications stopped")

    @property
    def active_bot_count(self) -> int:
        """Number of currently running Application instances."""
        return len(self._apps)

    @property
    def active_user_count(self) -> int:
        """Total number of users across all active bots."""
        return sum(len(users) for users in self._token_users.values())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _build_application(self, token: str) -> Application:
        """Build, configure, and start a new Application for the given token.

        PTB v21+ requires: build() → initialize() → add_handler() → start() → start_polling().
        Handlers added before initialize() are silently ignored by the dispatcher.
        """
        from layers.telegram.commands import register_handlers
        from telegram import Update
        from telegram.ext import ContextTypes

        app = ApplicationBuilder().token(token).build()

        # Initialize FIRST — makes dispatcher and app.bot available
        await app.initialize()

        # Global error handler — logs ANY unhandled exception during update processing
        async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
            log.error(
                "Telegram update processing error",
                error=str(context.error),
                update_type=type(update).__name__,
                chat_id=getattr(getattr(update, 'effective_chat', None), 'id', '?'),
            )

        app.add_error_handler(_error_handler)

        # Register handlers AFTER initialize (PTB v21+ requirement)
        register_handlers(app, self)
        log.info("Handlers registered after initialize", token_prefix=token[:10])

        try:
            await app.bot.set_my_commands(MENU_COMMANDS)
        except Exception as e:
            log.warning("Failed to set bot commands menu", error=str(e))

        await app.start()
        await app.updater.start_polling(drop_pending_updates=False)

        log.info("Bot polling started successfully", token_prefix=token[:10])
        return app

    async def _count_users_with_token(self, token: str) -> int:
        """Count how many users in the DB have this exact bot token.

        Tokens are stored encrypted, so we store a SHA-256 hash alongside
        (users.telegram_token_hash) for O(1) lookup. If the hash column
        doesn't exist yet, returns 0 and relies on in-memory tracking.
        """
        import hashlib
        from sqlalchemy import select, func as sa_func

        token_hash = hashlib.sha256(token.encode()).hexdigest()

        try:
            from layers.state.models import User  # type: ignore[attr-defined]
        except (ImportError, AttributeError):
            log.debug("User model not available; skipping DB token count")
            return 0

        try:
            async with self._session_factory() as session:
                stmt = select(sa_func.count()).where(User.telegram_token_hash == token_hash)
                result = await session.execute(stmt)
                return result.scalar() or 0
        except Exception as e:
            # Column may not exist yet (migration not applied)
            log.warning("DB token hash count failed (column missing?)", error=str(e))
            return 0