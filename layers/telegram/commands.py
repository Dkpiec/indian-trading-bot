"""Telegram command handlers — wired to real DB + broker clients.

Every handler receives user_id via context.user_data['user_id'], resolved
by the routing wrapper from chat_id → user_id mapping in BotManager.
Handlers query ONLY that user's data; no cross-user leakage.
Broker clients and session_factory are accessed via bot_manager.
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import structlog
from sqlalchemy import select, func
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters as ptb_filters,
)

from layers.telegram.bot_manager import BotManager
from layers.state.models import (
    StrategyCatalog,
    UserSubscription,
    StrategyInstance,
    TradeOrderLedger,
    RiskLimit,
)

log = structlog.get_logger(__name__)


def register_handlers(app: Application, bot_manager: BotManager) -> None:
    """Register all command handlers on the given Application."""
    from telegram.ext import MessageHandler, filters

    # Store bot_manager in bot_data so handlers can access it via context.bot_data
    app.bot_data["bot_manager"] = bot_manager

    # Debug catch-all: logs EVERY update before command handlers see it
    async def _debug_catch_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = update.effective_chat.id if update.effective_chat else "?"
        text = update.message.text if update.message else "(non-text)"
        print(f"DEBUG_CATCH_ALL_PRINT: chat_id={chat_id} text={text[:80]} update_id={update.update_id}", flush=True)
        log.info("DEBUG_CATCH_ALL received update", chat_id=chat_id, text=text[:80], update_id=update.update_id)

    app.add_handler(MessageHandler(filters.ALL, _debug_catch_all), group=-1)

    commands = {
        "catalog": cmd_catalog,
        "subscribe": cmd_subscribe,
        "unsubscribe": cmd_unsubscribe,
        "arm": cmd_arm,
        "disarm": cmd_disarm,
        "status": cmd_status,
        "balance": cmd_balance,
        "trades": cmd_trades,
        "profit": cmd_profit,
        "forceexit": cmd_forceexit,
        "broker": cmd_broker,
        "refreshtoken": cmd_refresh_token_async,
        "tokenstatus": cmd_tokenstatus,
        "market": cmd_market,
        "settings": cmd_settings,
        "version": cmd_version,
        "ping": cmd_ping,
        "log": cmd_log,
        "alerts": cmd_alerts,
        "help": cmd_help,
    }
    for name, handler in commands.items():
        app.add_handler(CommandHandler(name, _wrap(handler, bot_manager)))

    # /credentials conversation handler (multi-step credential entry)
    cred_conv = ConversationHandler(
        entry_points=[CommandHandler("credentials", _wrap(_cred_start, bot_manager))],
        states={
            CRED_SELECT_BROKER: [CallbackQueryHandler(_cred_broker_selected, pattern=r"^cred_")],
            CRED_ENTER_FIELD: [MessageHandler(ptb_filters.TEXT & ~ptb_filters.COMMAND, _cred_receive_field)],
        },
        fallbacks=[CommandHandler("cancel", _cred_timeout)],
        conversation_timeout=300,
    )
    app.add_handler(cred_conv)

    # Inline keyboard callback handlers
    # NOTE: cred_ callbacks are handled INSIDE the ConversationHandler above;
    # do NOT match them here or the conversation handler will never see them.
    # _handle_broker_callback MUST be in a lower group than _callback_query_handler
    # so refresh: callbacks are processed before the catch-all strategy handler.
    app.add_handler(CallbackQueryHandler(_handle_broker_callback, pattern=r"^refresh:"), group=-1)
    app.add_handler(CallbackQueryHandler(_callback_query_handler))


async def _callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline keyboard button presses for strategy selection.

    NOTE: refresh: and cred_ callbacks are handled by _handle_broker_callback
    (group=-1) and the /credentials ConversationHandler respectively.
    This catch-all must skip them to avoid consuming the callback.
    """
    query = update.callback_query
    if not query or not query.data:
        return

    # Let dedicated handlers process their own callbacks
    if query.data.startswith("refresh:") or query.data.startswith("cred_"):
        return

    await query.answer()
    data = query.data  # format: "action:strategy_id" e.g. "subscribe:NIFTY101"

    parts = data.split(":", 1)
    if len(parts) != 2:
        await query.edit_message_text("❌ Invalid selection.")
        return

    action, strategy_id = parts[0], parts[1]

    # Resolve user_id from chat
    chat_id = str(query.message.chat.id)
    bot_mgr = context.bot_data.get("bot_manager")
    if not bot_mgr:
        await query.edit_message_text("❌ Bot manager not available.")
        return

    user_id = await bot_mgr.resolve_user_id(chat_id)
    if not user_id:
        from core.config import settings
        # Compare as integers to avoid string formatting mismatches
        try:
            configured_chat = int(str(settings.telegram_chat_id).strip('"').strip("'"))
            if int(chat_id) == configured_chat:
                user_id = "system"
        except (ValueError, TypeError):
            pass

    if not user_id:
        await query.edit_message_text("⚠️ Unauthorized. Chat ID not recognized.")
        return

    uid = _parse_user_id(user_id)
    if not uid:
        # Deterministic UUID for 'system' or any non-UUID user_id
        # Same Telegram chat_id always produces the same UUID
        import uuid as _uuid
        uid = _uuid.uuid5(_uuid.NAMESPACE_URL, f"trading-bot:user:{user_id}")
        log.warning("Callback query user_id not a valid UUID, using deterministic placeholder", chat_id=chat_id, raw_user_id=user_id, uid=str(uid))

        # Auto-register user in users table if not present (satisfies FK constraint)
        from layers.state.models import User
        import hashlib
        try:
            async with await _get_session(bot_mgr) as reg_session:
                existing_user = await reg_session.execute(
                    select(User).where(User.id == uid)
                )
                if not existing_user.scalar_one_or_none():
                    # User model requires email + password_hash (NOT NULL)
                    placeholder_email = f"system-{chat_id}@trading-bot.local"
                    placeholder_hash = hashlib.sha256(f"system:{chat_id}".encode()).hexdigest()
                    user_obj = User(
                        id=uid,
                        email=placeholder_email,
                        password_hash=placeholder_hash,
                        telegram_chat_id=chat_id,
                        is_active=True,
                    )
                    reg_session.add(user_obj)
                    await reg_session.commit()
                    log.info("Auto-registered user in users table", uid=str(uid), chat_id=chat_id)
        except Exception as reg_err:
            log.warning("User auto-registration failed (may already exist)", error=str(reg_err))

    # Broker selection does not need DB session — handle before entering session block
    if action == "broker":
        broker_key = strategy_id  # e.g. "dhan" or "upstox"
        if broker_key not in SUPPORTED_BROKERS:
            await query.edit_message_text(f"❌ Unknown broker: {broker_key}")
            return
        # Store selected broker in Redis for this user
        redis = bot_mgr._redis
        try:
            await redis.set(f"trading-bot:user:{uid}:active_broker", broker_key, ex=86400 * 30)
            broker_name = SUPPORTED_BROKERS[broker_key]["name"]
            await query.edit_message_text(
                f"✅ Active broker set to {broker_name}.\n\n"
                f"All trading commands will now use {broker_name}."
            )
            log.info("Broker selected via inline", user_id=user_id, broker=broker_key)
        except Exception as e:
            log.error("Failed to store broker selection", error=str(e), user_id=user_id)
            await query.edit_message_text(f"❌ Failed to save broker: {e}")
        return

    async with await _get_session(bot_mgr) as session:
        if action == "subscribe":
            # Check if already subscribed
            existing = await session.execute(
                select(UserSubscription).where(
                    UserSubscription.user_id == uid,
                    UserSubscription.catalog_id == strategy_id,
                )
            )
            if existing.scalar_one_or_none():
                await query.edit_message_text(f"ℹ️ Already subscribed to `{strategy_id}`.", parse_mode="Markdown")
                return
            sub = UserSubscription(user_id=uid, catalog_id=strategy_id, is_armed=False)
            session.add(sub)
            await session.commit()
            await query.edit_message_text(
                f"✅ Subscribed to `{strategy_id}`.\nUse /arm to activate.",
                parse_mode="Markdown",
            )
            log.info("Strategy subscribed via inline", user_id=user_id, strategy=strategy_id)

        elif action == "unsubscribe":
            result = await session.execute(
                select(UserSubscription).where(
                    UserSubscription.user_id == uid,
                    UserSubscription.catalog_id == strategy_id,
                )
            )
            sub = result.scalar_one_or_none()
            if not sub:
                await query.edit_message_text(f"❌ Not subscribed to `{strategy_id}`.", parse_mode="Markdown")
                return
            await session.delete(sub)
            await session.commit()
            await query.edit_message_text(f"🔕 Unsubscribed from `{strategy_id}`.", parse_mode="Markdown")
            log.info("Strategy unsubscribed via inline", user_id=user_id, strategy=strategy_id)

        elif action == "arm":
            result = await session.execute(
                select(UserSubscription).where(
                    UserSubscription.user_id == uid,
                    UserSubscription.catalog_id == strategy_id,
                )
            )
            sub = result.scalar_one_or_none()
            if not sub:
                await query.edit_message_text(f"❌ Not subscribed to `{strategy_id}`. Subscribe first.", parse_mode="Markdown")
                return
            if sub.is_armed:
                await query.edit_message_text(f"ℹ️ `{strategy_id}` is already armed.", parse_mode="Markdown")
                return
            sub.is_armed = True
            await session.commit()
            await query.edit_message_text(f"🟢 Armed `{strategy_id}`.", parse_mode="Markdown")
            log.info("Strategy armed via inline", user_id=user_id, strategy=strategy_id)

        elif action == "disarm":
            result = await session.execute(
                select(UserSubscription).where(
                    UserSubscription.user_id == uid,
                    UserSubscription.catalog_id == strategy_id,
                )
            )
            sub = result.scalar_one_or_none()
            if not sub:
                await query.edit_message_text(f"❌ Not subscribed to `{strategy_id}`.", parse_mode="Markdown")
                return
            if not sub.is_armed:
                await query.edit_message_text(f"ℹ️ `{strategy_id}` is already disarmed.", parse_mode="Markdown")
                return
            sub.is_armed = False
            await session.commit()
            await query.edit_message_text(f"🔴 Disarmed `{strategy_id}`.", parse_mode="Markdown")
            log.info("Strategy disarmed via inline", user_id=user_id, strategy=strategy_id)

        else:
            await query.edit_message_text(f"❌ Unknown action: {action}")


def _wrap(handler, bot_manager: BotManager):
    """Wrap a command handler to inject user_id and enforce authorization."""

    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        print(f"WRAP_CALLED update_type={type(update).__name__} has_chat={update.effective_chat is not None}", flush=True)
        if not update.effective_chat:
            log.debug("Update received with no effective_chat")
            return

        chat_id = str(update.effective_chat.id)
        text = update.message.text if update.message else "(no message)"
        log.info("Incoming Telegram update", chat_id=chat_id, text=text[:50])

        user_id = await bot_manager.resolve_user_id(chat_id)

        # Fallback: if chat_id matches the configured system chat, accept as system user
        if not user_id:
            from core.config import settings
            try:
                configured_chat = int(str(settings.telegram_chat_id).strip('"').strip("'"))
                if int(chat_id) == configured_chat:
                    user_id = "system"
                    log.info("Chat accepted via configured TELEGRAM_CHAT_ID fallback", chat_id=chat_id)
            except (ValueError, TypeError):
                pass

        if not user_id:
            log.warning(
                "Unauthorized Telegram command — no mapping found",
                chat_id=chat_id,
            )
            if update.message:
                await update.message.reply_text(
                    "⚠️ Unauthorized. Chat ID not recognized."
                )
            return

        # Use deterministic UUID matching callback handler's uuid5 logic
        # so /arm, /disarm, /unsubscribe find subscriptions created via inline buttons
        uid = _parse_user_id(user_id)
        if not uid:
            import uuid as _uuid
            uid = _uuid.uuid5(_uuid.NAMESPACE_URL, f"trading-bot:user:{user_id}")
            # Auto-register user in users table if not present (satisfies FK constraint)
            from layers.state.models import User
            import hashlib
            try:
                async with await _get_session(bot_manager) as reg_session:
                    existing_user = await reg_session.execute(
                        select(User).where(User.id == uid)
                    )
                    if not existing_user.scalar_one_or_none():
                        placeholder_email = f"system-{chat_id}@trading-bot.local"
                        placeholder_hash = hashlib.sha256(f"system:{chat_id}".encode()).hexdigest()
                        user_obj = User(
                            id=uid,
                            email=placeholder_email,
                            password_hash=placeholder_hash,
                            telegram_chat_id=chat_id,
                            is_active=True,
                        )
                        reg_session.add(user_obj)
                        await reg_session.commit()
                        log.info("Auto-registered user via _wrap", uid=str(uid), chat_id=chat_id)
            except Exception as reg_err:
                log.warning("User auto-registration in _wrap failed (non-fatal)", error=str(reg_err))
            # Store the UUID string so handlers can use it consistently
            # This MUST happen even if DB registration failed — ConversationHandler
            # and credential commands don't require a DB row to function.
            user_id = str(uid)

        # Inject user_id into context for the handler
        context.user_data["user_id"] = user_id  # type: ignore[index]

        try:
            result = await handler(update, context)
            return result  # Pass through return value (required by ConversationHandler)
        except Exception as e:
            log.error("Command handler error", command=handler.__name__, error=str(e), user_id=user_id)
            # Safely reply: update.message may be None for callback queries or edge cases
            if update.message:
                try:
                    await update.message.reply_text(f"❌ Error: {e}")
                except Exception as reply_err:
                    log.error("Failed to send error reply", error=str(reply_err))
            elif update.callback_query:
                try:
                    await update.callback_query.answer(f"❌ Error: {e}", show_alert=True)
                except Exception as reply_err:
                    log.error("Failed to answer callback with error", error=str(reply_err))

    return wrapped


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_session(bot_manager: BotManager):
    """Get an async DB session from the bot manager's session factory.
    
    Always returns a session factory context manager.
    If DB is unavailable, returns a no-op context manager.
    """
    if bot_manager._session_factory is None:
        # Return a no-op context manager that yields None
        import contextlib
        @contextlib.asynccontextmanager
        async def noop_session():
            yield None
        return noop_session()
    return bot_manager._session_factory()


def _parse_user_id(user_id: str) -> uuid.UUID | None:
    """Safely parse user_id string to UUID."""
    try:
        return uuid.UUID(user_id)
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _build_strategy_keyboard(action: str, strategies) -> InlineKeyboardMarkup:
    """Build an inline keyboard with one button per strategy for the given action."""
    buttons = []
    for s in strategies:
        label = f"{s.id} — {s.display_name}"
        callback_data = f"{action}:{s.id}"
        buttons.append([InlineKeyboardButton(label, callback_data=callback_data)])
    return InlineKeyboardMarkup(buttons)


async def cmd_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Browse available strategies from the DB catalog (with in-memory fallback)."""
    bot_mgr = context.bot_data["bot_manager"]
    session = await _get_session(bot_mgr)
    
    if session is None:
        # In-memory fallback
        from layers.state.fallback_db import get_catalog
        strategies_data = await get_catalog()
        if not strategies_data:
            await update.message.reply_text("📋 No active strategies in catalog.")
            return
        
        lines = ["📋 *Available Strategies:*", ""]
        for i, s in enumerate(strategies_data, 1):
            lines.append(f"{i}. *{s['id']}* — {s['display_name']}")
            lines.append(f"   {s['underlying']} | {s['strategy_type']} | v{s['version']}")
        lines.append("")
        lines.append("ℹ️ *Database unavailable* — showing in-memory catalog")
        lines.append("Use /subscribe, /arm, /disarm, /unsubscribe to manage.")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return
    
    # Normal DB path
    async with session as s:
        result = await s.execute(
            select(StrategyCatalog).where(StrategyCatalog.is_active == True).order_by(StrategyCatalog.id)  # noqa: E712
        )
        strategies = result.scalars().all()

    if not strategies:
        await update.message.reply_text("📋 No active strategies in catalog.")
        return

    lines = ["📋 *Available Strategies:*", ""]
    for i, s in enumerate(strategies, 1):
        lines.append(f"{i}. *{s.id}* — {s.display_name}")
        lines.append(f"   {s.underlying} | {s.strategy_type} | v{s.version}")
    lines.append("")
    lines.append("Use /subscribe, /arm, /disarm, /unsubscribe to manage.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show strategy selection menu to subscribe."""
    bot_mgr = context.bot_data["bot_manager"]
    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.is_active == True).order_by(StrategyCatalog.id)  # noqa: E712
        )
        strategies = result.scalars().all()

    if not strategies:
        await update.message.reply_text("📋 No active strategies available.")
        return

    keyboard = _build_strategy_keyboard("subscribe", strategies)
    await update.message.reply_text(
        "📋 *Select a strategy to subscribe:*",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show subscribed strategies selection menu to unsubscribe."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(UserSubscription).where(UserSubscription.user_id == uid)
        )
        subs = result.scalars().all()

    if not subs:
        await update.message.reply_text("ℹ️ You have no subscriptions.")
        return

    # Fetch catalog entries for display names
    cat_ids = [s.catalog_id for s in subs]
    async with await _get_session(bot_mgr) as session:
        cat_result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.id.in_(cat_ids))
        )
        catalog_map = {c.id: c for c in cat_result.scalars().all()}

    buttons = []
    for sub in subs:
        cat = catalog_map.get(sub.catalog_id)
        name = cat.display_name if cat else sub.catalog_id
        label = f"{sub.catalog_id} — {name}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"unsubscribe:{sub.catalog_id}")])

    await update.message.reply_text(
        "🔕 *Select a strategy to unsubscribe:*",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


async def cmd_arm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show subscribed strategies selection menu to arm."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.is_armed == False,  # noqa: E712
            )
        )
        subs = result.scalars().all()

    if not subs:
        await update.message.reply_text("ℹ️ No disarmed subscriptions to arm.")
        return

    cat_ids = [s.catalog_id for s in subs]
    async with await _get_session(bot_mgr) as session:
        cat_result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.id.in_(cat_ids))
        )
        catalog_map = {c.id: c for c in cat_result.scalars().all()}

    buttons = []
    for sub in subs:
        cat = catalog_map.get(sub.catalog_id)
        name = cat.display_name if cat else sub.catalog_id
        label = f"{sub.catalog_id} — {name}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"arm:{sub.catalog_id}")])

    await update.message.reply_text(
        "🟢 *Select a strategy to arm:*",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


async def cmd_disarm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show armed strategies selection menu to disarm."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.is_armed == True,  # noqa: E712
            )
        )
        subs = result.scalars().all()

    if not subs:
        await update.message.reply_text("ℹ️ No armed subscriptions to disarm.")
        return

    cat_ids = [s.catalog_id for s in subs]
    async with await _get_session(bot_mgr) as session:
        cat_result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.id.in_(cat_ids))
        )
        catalog_map = {c.id: c for c in cat_result.scalars().all()}

    buttons = []
    for sub in subs:
        cat = catalog_map.get(sub.catalog_id)
        name = cat.display_name if cat else sub.catalog_id
        label = f"{sub.catalog_id} — {name}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"disarm:{sub.catalog_id}")])

    await update.message.reply_text(
        "🔴 *Select a strategy to disarm:*",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown",
    )


# Legacy arg-based handlers kept as fallback (not used by inline menus)
async def _cmd_arm_legacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Arm a subscribed strategy for live execution. Usage: /arm <strategy_id>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)

    if not context.args:
        await update.message.reply_text("Usage: /arm <strategy_id>")
        return

    strategy_id = context.args[0]
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        # Find subscription
        result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.catalog_id == strategy_id,
            )
        )
        sub = result.scalar_one_or_none()
        if not sub:
            await update.message.reply_text(f"❌ Not subscribed to `{strategy_id}`. Use /subscribe first.")
            return

        if sub.is_armed:
            await update.message.reply_text(f"ℹ️ `{strategy_id}` is already armed.")
            return

        # Check balance floor before arming
        dhan_client = getattr(context.bot_data.get("app_state", {}), "dhan_client", None)
        if dhan_client:
            try:
                balance = await dhan_client.get_balance()
                risk_result = await session.execute(
                    select(RiskLimit).where(RiskLimit.user_id == uid)
                )
                risk = risk_result.scalar_one_or_none()
                floor_pct = risk.balance_floor_pct if risk else 0.20
                floor_amount = balance.total_equity * floor_pct
                if balance.free_balance < floor_amount:
                    await update.message.reply_text(
                        f"⚠️ Cannot arm: free balance ₹{balance.free_balance:,.0f} "
                        f"is below floor ₹{floor_amount:,.0f} ({floor_pct*100:.0f}% of equity)."
                    )
                    return
            except Exception as e:
                log.warning("Balance check failed during arm", error=str(e))
                # Proceed anyway — balance check is best-effort

        sub.is_armed = True
        await session.commit()

    await update.message.reply_text(
        f"🔫 `{strategy_id}` armed. Will execute on next valid signal.",
        parse_mode="Markdown",
    )
    log.info("Strategy armed", user_id=user_id, strategy=strategy_id)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show strategy status & balance overview for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            # In-memory fallback: show degraded status
            from layers.state.fallback_db import get_catalog
            catalog = await get_catalog()
            lines = [
                "📊 *Status* (degraded — database unavailable)",
                "",
                f"*Available Strategies:* {len(catalog)} in catalog",
                "No subscriptions or instances tracked.",
                "",
                "Use /catalog, /balance, /help for more info.",
            ]
            await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
            return
        
        # Get subscriptions with catalog info
        result = await session.execute(
            select(UserSubscription, StrategyCatalog)
            .join(StrategyCatalog, UserSubscription.catalog_id == StrategyCatalog.id)
            .where(UserSubscription.user_id == uid)
        )
        subs = result.all()

        # Get running instances
        inst_result = await session.execute(
            select(StrategyInstance).where(StrategyInstance.user_id == uid)
        )
        instances = inst_result.scalars().all()

    lines = [f"📊 *Status*", ""]

    if not subs:
        lines.append("No subscriptions. Use /catalog to browse.")
    else:
        lines.append("*Subscriptions:*")
        for sub, cat in subs:
            armed = "🔫 ARMED" if sub.is_armed else "💤 idle"
            lines.append(f"  • `{cat.id}` — {cat.display_name} [{armed}]")
        lines.append("")

    if instances:
        lines.append("*Running Instances:*")
        for inst in instances:
            pnl = inst.unrealized_pnl + inst.realized_pnl
            lines.append(f"  • `{inst.instance_id[:16]}…` {inst.status} P&L: ₹{pnl:,.2f}")
    else:
        lines.append("No running strategy instances.")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show equity / free / used / floor breakdown from Dhan broker."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    # Try Dhan client first (stored on app.state during lifespan)
    dhan_client = bot_mgr._app_state_dhan_client if hasattr(bot_mgr, "_app_state_dhan_client") else None

    if not dhan_client:
        if update.message:
            await update.message.reply_text(
                "💰 Balance\n\nDhan broker client not available.\nCheck service logs."
            )
        return

    try:
        balance = await dhan_client.get_balance()
    except RuntimeError as e:
        err_str = str(e)
        if "401" in err_str or "Invalid_Authentication" in err_str:
            log.error("Dhan auth token expired/invalid", user_id=user_id)
            if update.message:
                await update.message.reply_text(
                    "❌ Dhan auth failed (401 Invalid_Authentication)\n\n"
                    "The DHAN_ACCESS_TOKEN is expired or invalid.\n"
                    "Update DHAN_ACCESS_TOKEN in /opt/data/secrets/coolify.env and redeploy."
                )
        else:
            log.error("Balance fetch failed", error=err_str, user_id=user_id)
            if update.message:
                await update.message.reply_text(f"❌ Failed to fetch balance: {err_str}")
        return
    except Exception as e:
        log.error("Balance fetch failed", error=str(e), user_id=user_id)
        if update.message:
            await update.message.reply_text(f"❌ Failed to fetch balance: {e}")
        return

    # Get user's risk limit for floor calculation (with fallback)
    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            floor_pct = 0.20  # Default when DB unavailable
        else:
            risk_result = await session.execute(
                select(RiskLimit).where(RiskLimit.user_id == uid)
            )
            risk = risk_result.scalar_one_or_none()
            floor_pct = risk.balance_floor_pct if risk else 0.20

    floor_amount = balance.total_equity * floor_pct
    ts = datetime.fromtimestamp(balance.timestamp, tz=timezone.utc).strftime("%H:%M:%S UTC")

    text = (
        f"💰 Balance (as of {ts})\n\n"
        f"Total Equity: Rs.{balance.total_equity:,.2f}\n"
        f"Free Balance: Rs.{balance.free_balance:,.2f}\n"
        f"Used Margin: Rs.{balance.used_margin:,.2f}\n"
        f"Floor ({floor_pct*100:.0f}%): Rs.{floor_amount:,.2f}\n"
    )
    if balance.free_balance < floor_amount:
        text += "\n⚠️ Below floor — new entries blocked."

    if update.message:
        await update.message.reply_text(text)


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show open positions for this user from strategy_instances."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            await update.message.reply_text("📈 No open positions tracked (database unavailable).")
            return
        
        result = await session.execute(
            select(StrategyInstance).where(
                StrategyInstance.user_id == uid,
                StrategyInstance.net_qty != 0,
            )
        )
        positions = result.scalars().all()

    if not positions:
        await update.message.reply_text("📈 No open positions.")
        return

    lines = ["📈 *Open Positions:*", ""]
    for p in positions:
        direction = "LONG" if p.net_qty > 0 else "SHORT"
        lines.append(
            f"• `{p.instance_id[:16]}…` {p.catalog_id}\n"
            f"  Qty: {abs(p.net_qty):.0f} {direction} | Entry: ₹{p.entry_price or 0:,.2f}\n"
            f"  LTP: ₹{p.last_ltp or 0:,.2f} | uP&L: ₹{p.unrealized_pnl:,.2f}"
        )
        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_profit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show unrealized P&L summary from strategy_instances."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            await update.message.reply_text("💵 No P&L data tracked (database unavailable).")
            return
        
        result = await session.execute(
            select(
                func.sum(StrategyInstance.unrealized_pnl).label("total_unrealized"),
                func.sum(StrategyInstance.realized_pnl).label("total_realized"),
                func.count(StrategyInstance.instance_id).label("count"),
            ).where(StrategyInstance.user_id == uid)
        )
        row = result.one()

    total_unrealized = float(row.total_unrealized or 0)
    total_realized = float(row.total_realized or 0)
    count = int(row.count or 0)

    emoji_u = "🟢" if total_unrealized >= 0 else "🔴"
    emoji_r = "🟢" if total_realized >= 0 else "🔴"

    text = (
        f"💵 *P&L Summary* ({count} instances)\n\n"
        f"{emoji_u} Unrealized: ₹{total_unrealized:,.2f}\n"
        f"{emoji_r} Realized: ₹{total_realized:,.2f}\n"
        f"{'🟢' if (total_unrealized + total_realized) >= 0 else '🔴'} Total: ₹{total_unrealized + total_realized:,.2f}"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_forceexit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close ALL open positions immediately for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(StrategyInstance).where(
                StrategyInstance.user_id == uid,
                StrategyInstance.net_qty != 0,
            )
        )
        positions = result.scalars().all()

    if not positions:
        await update.message.reply_text("ℹ️ No open positions to close.")
        return

    dhan_client = bot_mgr._app_state_dhan_client if hasattr(bot_mgr, "_app_state_dhan_client") else None

    closed = 0
    errors = []
    for p in positions:
        side = "SELL" if p.net_qty > 0 else "BUY"
        qty = abs(int(p.net_qty))
        try:
            if dhan_client:
                order = await dhan_client.place_order(
                    symbol=p.catalog_id,
                    security_id=p.instance_id,
                    side=side,
                    quantity=qty,
                    order_type="MARKET",
                    product_type="INTRADAY",
                )
                if order.get("status") == "ACCEPTED":
                    closed += 1
                    log.info("Force exit order placed", instance=p.instance_id, order=order.get("broker_order_id"))
                else:
                    errors.append(f"`{p.instance_id[:12]}…`: {order.get('message', 'rejected')}")
            else:
                errors.append(f"`{p.instance_id[:12]}…`: no broker client")
        except Exception as e:
            errors.append(f"`{p.instance_id[:12]}…`: {e}")

    msg = f"⚠️ *Force Exit Complete*\n\nClosed: {closed}/{len(positions)}"
    if errors:
        msg += "\n\n*Errors:*\n" + "\n".join(errors)

    await update.message.reply_text(msg, parse_mode="Markdown")
    log.warning("Force exit executed", user_id=user_id, closed=closed, total=len(positions))


async def cmd_market(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show market status: open/closed, entry window, time to open/close."""
    from datetime import datetime as dt, timezone as tz, timedelta
    from core.config import settings

    ist = tz(timedelta(hours=5, minutes=30))
    now_ist = dt.now(ist)
    hhmm = now_ist.strftime("%H:%M")

    open_hhmm = settings.market_open  # "09:15"
    close_hhmm = settings.market_close  # "15:30"
    entry_start = settings.entry_window_start  # "09:45"
    entry_end = settings.entry_window_end  # "13:15"

    open_h, open_m = map(int, open_hhmm.split(":"))
    close_h, close_m = map(int, close_hhmm.split(":"))
    es_h, es_m = map(int, entry_start.split(":"))
    ee_h, ee_m = map(int, entry_end.split(":"))

    market_open = (open_h * 60 + open_m) <= (now_ist.hour * 60 + now_ist.minute) < (close_h * 60 + close_m)
    entry_open = (es_h * 60 + es_m) <= (now_ist.hour * 60 + now_ist.minute) < (ee_h * 60 + ee_m)

    # Calculate time to next event
    current_min = now_ist.hour * 60 + now_ist.minute
    open_min = open_h * 60 + open_m
    close_min = close_h * 60 + close_m
    es_min = es_h * 60 + es_m
    ee_min = ee_h * 60 + ee_m

    if market_open:
        mins_to_close = close_min - current_min
        status_icon = "🟢"
        status_text = "OPEN"
        next_event = f"{mins_to_close}m to close" if mins_to_close > 0 else "closing now"
    else:
        status_icon = "🔴"
        status_text = "CLOSED"
        if current_min < open_min:
            mins_to_open = open_min - current_min
            next_event = f"{mins_to_open}m to open"
        else:
            next_event = "reopens tomorrow 09:15"

    entry_icon = "🟢" if entry_open else "⚪"
    entry_text = "ENTRY WINDOW OPEN" if entry_open else "ENTRY WINDOW CLOSED"
    if not entry_open:
        if current_min < es_min:
            entry_detail = f"opens in {es_min - current_min}m"
        elif current_min >= ee_min:
            entry_detail = "closed for today"
        else:
            entry_detail = "closed"
    else:
        entry_detail = f"closes in {ee_min - current_min}m"

    lines = [
        f"📊 *Market Status* — {now_ist.strftime('%a %d %b %Y %H:%M IST')}",
        "",
        f"{status_icon} *{status_text}* ({open_hhmm}–{close_hhmm} IST) — {next_event}",
        f"{entry_icon} *{entry_text}* ({entry_start}–{entry_end}) — {entry_detail}",
        "",
        "Weekend: closed",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current bot settings: broker, risk limit, market hours."""
    user_id = context.user_data["user_id"]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]
    from core.config import settings

    dhan_client = bot_mgr._app_state_dhan_client if hasattr(bot_mgr, "_app_state_dhan_client") else None
    broker_status = "✅ DhanHQ connected" if dhan_client else "❌ DhanHQ not available"

    upstox_ok = bool(getattr(settings, "upstox_access_token", None))
    upstox_status = "✅ Upstox configured" if upstox_ok else "❌ Upstox not configured"

    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            floor_pct = 0.20
            risk_line = "💳 Floor: 20% (default, DB unavailable)"
        else:
            risk_result = await session.execute(
                select(RiskLimit).where(RiskLimit.user_id == uid)
            )
            risk = risk_result.scalar_one_or_none()
            if risk:
                floor_pct = risk.balance_floor_pct
                risk_line = f"💳 Floor: {floor_pct*100:.0f}% | Max position: ₹{(risk.max_position_value or 0):,.0f}"
            else:
                floor_pct = 0.20
                risk_line = "💳 Floor: 20% (default)"

    lines = [
        "⚙️ *Bot Settings*",
        "",
        f"{broker_status}",
        f"{'✅' if upstox_ok else '❌'} Upstox: {'configured' if upstox_ok else 'not configured'}",
        "",
        f"📈 Market hours: {settings.market_open}–{settings.market_close} IST",
        f"⏰ Entry window: {settings.entry_window_start}–{settings.entry_window_end}",
        "",
        risk_line,
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot version, build info, and uptime."""
    import os, time
    from core.config import settings

    version = "4.0.0"
    started_at = os.environ.get("BOT_START_TIME", "unknown")

    lines = [
        "🐍 *Indian Trading Bot*",
        "",
        f"Version: v{version}",
        f"Build: {os.environ.get('BUILD_COMMIT', 'unknown')}",
        f"Python: {time.strftime('%Y.%m.%d')}",
        "",
        "Stack: FastAPI + asyncpg + Redis + Alembic",
        "Broker: DhanHQ (REST v2)",
        "Market: NSE Options (NSE_FNO)",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Quick health check: respond with latency."""
    import time
    t0 = time.time()
    await update.message.reply_text(f"🏓 Pong! ({(time.time()-t0)*1000:.0f}ms)")


async def cmd_log(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show recent bot log entries."""
    bot_mgr = context.bot_data.get("bot_manager")
    if not bot_mgr:
        await update.message.reply_text("❌ Log not available.")
        return

    # Read from structured log file if available
    import os
    log_file = "/tmp/bot.log"
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()[-10:]
        text = "📋 *Recent Logs:*\n\n" + "\n".join(l.rstrip() for l in lines)
    else:
        text = "📋 No log file found. Check Coolify service logs."

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show active alert notifications and toggle them."""
    user_id = context.user_data["user_id"]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    session_ctx = await _get_session(bot_mgr)
    async with session_ctx as session:
        if session is None:
            await update.message.reply_text(
                "🔔 *Alerts* (DB unavailable)\n\n"
                "Default alerts enabled:\n"
                "• Position opened/closed\n"
                "• P&L threshold crossed\n"
                "• Token expiry warning"
            )
            return

    alert_lines = [
        "🔔 *Alert Notifications*",
        "",
        "• 🟢 Position opened — signal triggered",
        "• 🔴 Position closed — exit executed",
        "• 💰 P&L crossed threshold",
        "• ⚠️ Token expiry in <24h",
        "• 📉 Balance below floor",
        "",
        "To disable: /alerts off",
    ]
    await update.message.reply_text("\n".join(alert_lines), parse_mode="Markdown")


SUPPORTED_BROKERS = {
    "dhan": {"name": "DhanHQ", "env_prefix": "DHAN"},
    "upstox": {"name": "Upstox", "env_prefix": "UPSTOX"},
    "fyers": {"name": "FYERS", "env_prefix": "FYERS"},
}

# --- Credential entry conversation states ---
CRED_SELECT_BROKER, CRED_ENTER_FIELD, CRED_CONFIRM = range(3)

# Fields required per broker for credential storage
BROKER_CREDENTIAL_FIELDS = {
    "dhan": [
        ("client_id", "Dhan Client ID"),
        ("access_token", "Dhan Access Token"),
        ("totp_secret", "TOTP Secret (base32, from authenticator app setup)"),
    ],
    "fyers": [
        ("app_id", "FYERS App ID"),
        ("user_id", "FYERS User ID"),
        ("password", "FYERS Password"),
        ("totp_secret", "TOTP Secret (base32)"),
        ("pin", "MPIN / PIN"),
    ],
    "upstox": [
        ("api_key", "Upstox API Key"),
        ("api_secret", "Upstox API Secret"),
        ("redirect_uri", "Redirect URI"),
    ],
}


def _cred_file_path(broker: str) -> str:
    """Return the on-disk path for a broker's credential file."""
    return f"/opt/data/secrets/{broker}.json"


async def _save_credentials(broker: str, creds: dict) -> None:
    """Persist credentials to disk with restrictive permissions."""
    import json
    import os

    path = _cred_file_path(broker)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(creds, f, indent=2)
    os.chmod(path, 0o600)
    log.info("Broker credentials saved", broker=broker, path=path)


async def _load_credentials(broker: str) -> dict | None:
    """Load credentials from disk, returning None if missing or unreadable."""
    import json

    path = _cred_file_path(broker)
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("Failed to load broker credentials", broker=broker, error=str(e))
        return None


def _decode_jwt_exp(token: str) -> float | None:
    """Decode the 'exp' claim from a JWT without verification. Returns epoch seconds or None."""
    import base64
    import json

    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        # Add padding
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        exp = data.get("exp")
        return float(exp) if exp is not None else None
    except Exception:
        return None


# --- Conversation handler steps for /credentials ---

async def _cred_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Step 1: Show broker selection for credential entry."""
    if not update.message:
        return ConversationHandler.END

    buttons = []
    for key, info in SUPPORTED_BROKERS.items():
        has_creds = await _load_credentials(key) is not None
        marker = "📝" if has_creds else "➕"
        buttons.append([InlineKeyboardButton(f"{marker} {info['name']}", callback_data=f"cred_broker:{key}")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cred_cancel")])

    await update.message.reply_text(
        "🔑 Select broker to configure credentials:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return CRED_SELECT_BROKER


async def _cred_broker_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Callback: broker chosen, start field entry."""
    query = update.callback_query
    if not query or not query.data:
        return ConversationHandler.END

    await query.answer()

    ud = context.user_data
    if ud is None:
        return ConversationHandler.END

    if query.data == "cred_cancel":
        await query.edit_message_text("❌ Credential entry cancelled.")
        ud.pop("_cred_broker", None)
        ud.pop("_cred_fields", None)
        ud.pop("_cred_values", None)
        return ConversationHandler.END

    if not query.data.startswith("cred_broker:"):
        return CRED_SELECT_BROKER

    broker = query.data.split(":", 1)[1]
    fields = BROKER_CREDENTIAL_FIELDS.get(broker)
    if not fields:
        await query.edit_message_text(f"❌ Unknown broker: {broker}")
        return ConversationHandler.END

    ud["_cred_broker"] = broker
    ud["_cred_fields"] = list(fields)
    ud["_cred_values"] = {}
    ud["_cred_field_idx"] = 0

    field_name, field_label = fields[0]
    await query.edit_message_text(
        f"🏦 Configuring *{SUPPORTED_BROKERS[broker]['name']}*\n\n"
        f"Enter `{field_label}`:\n_(Your input will be deleted after reading)_",
        parse_mode="Markdown",
    )
    return CRED_ENTER_FIELD


async def _cred_receive_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive a credential value, delete the message, advance to next field or confirm."""
    if not update.message or not update.message.text:
        return CRED_ENTER_FIELD

    ud = context.user_data
    if ud is None:
        return ConversationHandler.END

    broker = ud.get("_cred_broker")
    fields = ud.get("_cred_fields", [])
    idx = ud.get("_cred_field_idx", 0)
    values = ud.get("_cred_values", {})

    if not broker or idx >= len(fields):
        await update.message.reply_text("❌ Session expired. Please run /credentials again.")
        return ConversationHandler.END

    # Delete user's message for privacy
    try:
        await update.message.delete()
    except Exception:
        pass  # Bot may not have delete permission

    field_name, field_label = fields[idx]
    values[field_name] = update.message.text.strip()
    ud["_cred_values"] = values

    idx += 1
    ud["_cred_field_idx"] = idx

    if idx < len(fields):
        next_name, next_label = fields[idx]
        await update.message.reply_text(
            f"✅ Saved.\n\nEnter `{next_label}`:\n_(Your input will be deleted after reading)_",
            parse_mode="Markdown",
        )
        return CRED_ENTER_FIELD

    # All fields collected — save
    try:
        await _save_credentials(broker, values)
        await update.message.reply_text(
            f"✅ Credentials for *{SUPPORTED_BROKERS[broker]['name']}* saved securely.\n\n"
            f"Use /refreshtoken to refresh the access token.",
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Failed to save credentials: {e}")

    # Clean up session state
    for k in ("_cred_broker", "_cred_fields", "_cred_values", "_cred_field_idx"):
        ud.pop(k, None)

    return ConversationHandler.END


async def _cred_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Conversation timed out."""
    if update and update.message:
        await update.message.reply_text("⏰ Credential entry timed out. Run /credentials again.")
    ud = context.user_data
    if ud is not None:
        for k in ("_cred_broker", "_cred_fields", "_cred_values", "_cred_field_idx"):
            ud.pop(k, None)
    return ConversationHandler.END


async def cmd_tokenstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show token expiry status for all configured brokers using JWT decode."""
    if not update.message:
        return

    import time

    lines = ["🔐 *Broker Token Status*\n"]
    now = time.time()

    for broker_key, info in SUPPORTED_BROKERS.items():
        creds = await _load_credentials(broker_key)
        if not creds:
            lines.append(f"*{info['name']}*: ⚪ No credentials configured")
            continue

        token = creds.get("access_token") or creds.get("token")
        if not token:
            lines.append(f"*{info['name']}*: 🟡 Credentials stored but no access token yet")
            continue

        exp = _decode_jwt_exp(token)
        if exp is None:
            lines.append(f"*{info['name']}*: 🟡 Token present but cannot decode expiry")
            continue

        remaining = exp - now
        if remaining <= 0:
            status = "🔴 EXPIRED"
            detail = f"{abs(remaining)/3600:.1f}h ago"
        elif remaining < 3600:
            status = "🟠 Expiring soon"
            detail = f"{remaining/60:.0f}m remaining"
        elif remaining < 7200:
            status = "🟡"
            detail = f"{remaining/3600:.1f}h remaining"
        else:
            status = "🟢 Valid"
            detail = f"{remaining/3600:.1f}h remaining"

        exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines.append(f"*{info['name']}*: {status} — {detail}\n  Expires: `{exp_dt}`")

    lines.append("\nUse /refreshtoken to renew an expired or expiring token.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_refresh_token_async(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Trigger token refresh for a selected broker via background subprocess."""
    if not update.message:
        return

    bot_mgr = context.bot_data.get("bot_manager")
    dhan_ok = bool(getattr(bot_mgr, "_app_state_dhan_client", None))

    from core.config import settings
    upstox_ok = bool(getattr(settings, "upstox_access_token", None))

    buttons = []
    for key, info in SUPPORTED_BROKERS.items():
        has_creds = await _load_credentials(key) is not None
        active = "✅" if (key == "dhan" and dhan_ok) or (key == "upstox" and upstox_ok) else ""
        cred_marker = "🔑" if has_creds else "⚠️"
        label = f"{cred_marker} {info['name']} {active}".strip()
        buttons.append([InlineKeyboardButton(label, callback_data=f"refresh:{key}")])

    await update.message.reply_text(
        "🔄 Select broker to refresh token:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _handle_broker_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline keyboard callbacks for refresh triggers only.
    
    NOTE: cred_* callbacks are handled by the /credentials ConversationHandler.
    Do NOT add cred_ matching here or the conversation handler will never see them.
    """
    query = update.callback_query
    if not query or not query.data:
        return

    if not query.data.startswith("refresh:"):
        return  # Not our callback; let other handlers process it

    await query.answer()
    data = query.data
    broker = data.split(":", 1)[1]
    if broker not in SUPPORTED_BROKERS:
        await query.edit_message_text(f"❌ Unknown broker: {broker}")
        return

    creds = await _load_credentials(broker)
    if not creds:
        await query.edit_message_text(
            f"❌ No credentials stored for *{SUPPORTED_BROKERS[broker]['name']}*.\n"
            f"Run /credentials first to save them.",
            parse_mode="Markdown",
        )
        return

    await query.edit_message_text(
        f"🔄 Refreshing *{SUPPORTED_BROKERS[broker]['name']}* token in background...\n"
        f"_(FYERS can take up to 5 minutes; Dhan ~30s)_",
        parse_mode="Markdown",
    )
    asyncio.create_task(_run_refresh_in_background(broker, creds, query))


async def _run_refresh_in_background(
    broker: str,
    creds: dict,
    query,
) -> None:
    """Run the refresh script in a background task and notify the chat on completion."""
    import subprocess

    script_map = {
        "dhan": ["/opt/data/.venv/bin/python", "/opt/data/scripts/dhan-renew.py", "--force"],
        "fyers": ["/opt/data/.venv/bin/python", "/opt/data/scripts/fyers_headless_auth.py", "--force"],
        "upstox": None,  # Upstox uses env-var based tokens, no TOTP refresh script
    }

    cmd = script_map.get(broker)
    if cmd is None:
        await query.edit_message_text(
            f"⚠️ {SUPPORTED_BROKERS[broker]['name']} does not support TOTP refresh.\n"
            f"Tokens are managed via environment variables."
        )
        return

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd="/opt/data",
        )
        # Poll for completion up to 300s
        for _ in range(60):
            await asyncio.sleep(5)
            if proc.poll() is not None:
                break
        else:
            proc.kill()
            await query.edit_message_text(
                f"❌ {SUPPORTED_BROKERS[broker]['name']} refresh timed out (5 min) and was killed."
            )
            return

        stdout = str(proc.stdout.read()) if proc.stdout is not None else ""
        stderr = str(proc.stderr.read()) if proc.stderr is not None else ""

        if proc.returncode == 0:
            combined = stdout + stderr
            summary_lines = [l for l in combined.split("\n") if l.strip()][-3:]
            summary = "\n".join(summary_lines) if summary_lines else "Refresh complete."
            await query.edit_message_text(
                f"✅ *{SUPPORTED_BROKERS[broker]['name']}* token refreshed successfully.\n\n"
                f"```\n{summary}\n```\n\n"
                f"Check /tokenstatus to confirm.",
                parse_mode="Markdown",
            )
        else:
            err_text = (stderr or stdout).strip()[-500:]
            await query.edit_message_text(
                f"❌ *{SUPPORTED_BROKERS[broker]['name']}* refresh failed:\n"
                f"```\n{err_text}\n```",
                parse_mode="Markdown",
            )

    except Exception as e:
        await query.edit_message_text(
            f"❌ Refresh error for {SUPPORTED_BROKERS[broker]['name']}: {e}"
        )


async def cmd_broker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show broker selection menu with current status."""
    if not update.message:
        return

    bot_mgr = context.bot_data.get("bot_manager")
    dhan_ok = bool(getattr(bot_mgr, "_app_state_dhan_client", None))

    # Check Upstox availability from env
    from core.config import settings
    upstox_ok = bool(getattr(settings, "upstox_access_token", None))

    buttons = []
    for key, info in SUPPORTED_BROKERS.items():
        has_creds = await _load_credentials(key) is not None
        active = "✅" if (key == "dhan" and dhan_ok) or (key == "upstox" and upstox_ok) or (key == "fyers" and has_creds) else "❌"
        label = f"{active} {info['name']}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"broker:{key}")])

    await update.message.reply_text(
        "🏦 Select active broker:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cmd_refresh_token(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Trigger Dhan token refresh via the renewal script and redeploy."""
    if not update.message:
        return

    await update.message.reply_text("🔄 Refreshing Dhan access token...")

    import subprocess
    try:
        result = subprocess.run(
            ["python3", "/opt/data/scripts/dhan-renew.py", "--force"],
            capture_output=True, text=True, timeout=60, cwd="/opt/data",
        )
        output = result.stdout.strip()
        if result.returncode != 0:
            err = result.stderr.strip() or output
            await update.message.reply_text(f"❌ Token refresh failed:\n{err[-500:]}")
            return

        # Extract key info from output
        lines = [l for l in output.split("\n") if l.strip()]
        summary = "\n".join(lines[-3:]) if lines else "Token refreshed."
        await update.message.reply_text(
            f"✅ Dhan token refreshed!\n\n{summary}\n\n"
            "⚠️ Redeploy the service to pick up the new token.\n"
            "Use Coolify dashboard or ask me to redeploy."
        )
        log.info("Dhan token refreshed via /refreshtoken command")
    except subprocess.TimeoutExpired:
        await update.message.reply_text("❌ Token refresh timed out (60s).")
    except Exception as e:
        await update.message.reply_text(f"❌ Token refresh error: {e}")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help message with all available commands."""
    help_text = (
        "🤖 Indian Trading Bot — Commands\n\n"
        "Strategy Management:\n"
        "/catalog — Browse available strategies\n"
        "/subscribe — Subscribe to a strategy\n"
        "/unsubscribe — Unsubscribe\n"
        "/arm — Activate for live trading\n"
        "/disarm — Deactivate\n\n"
        "Monitoring:\n"
        "/status — Overview & subscriptions\n"
        "/balance — Equity breakdown (live from Dhan)\n"
        "/trades — Open positions\n"
        "/profit — Unrealized P&L\n\n"
        "Broker:\n"
        "/broker — Select active broker\n"
        "/credentials — Enter/update broker API credentials\n"
        "/refreshtoken — Refresh broker access token\n"
        "/tokenstatus — Check token expiry\n\n"
        "Market & Info:\n"
        "/market — Market open/closed, entry window status\n"
        "/settings — Bot settings: broker, risk, hours\n"
        "/alerts — Notification types & toggles\n"
        "/version — Bot version & build info\n"
        "/log — Recent bot log entries\n\n"
        "Emergency:\n"
        "/forceexit — Close ALL positions now\n"
        "/ping — Quick health check\n\n"
        "/help — Show this message"
    )
    if update.message:
        await update.message.reply_text(help_text)