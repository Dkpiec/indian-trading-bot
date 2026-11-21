"""Telegram command handlers — wired to real DB + broker clients.

Every handler receives user_id via context.user_data['user_id'], resolved
by the routing wrapper from chat_id → user_id mapping in BotManager.
Handlers query ONLY that user's data; no cross-user leakage.
Broker clients and session_factory are accessed via bot_manager.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from sqlalchemy import select, func
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

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
        "help": cmd_help,
    }
    for name, handler in commands.items():
        app.add_handler(CommandHandler(name, _wrap(handler, bot_manager)))


def _wrap(handler, bot_manager: BotManager):
    """Wrap a command handler to inject user_id and enforce authorization."""

    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
            configured_chat = str(settings.telegram_chat_id).strip('"').strip("'")
            if chat_id == configured_chat:
                user_id = "system"
                log.info("Chat accepted via configured TELEGRAM_CHAT_ID fallback", chat_id=chat_id)
            else:
                log.warning(
                    "Unauthorized Telegram command — no mapping found",
                    chat_id=chat_id,
                    configured_chat=configured_chat,
                    redis_keys_checked=True,
                )
                if update.message:
                    await update.message.reply_text(
                        "⚠️ Unauthorized. Chat ID not recognized."
                    )
                return

        # Inject user_id into context for the handler
        context.user_data["user_id"] = user_id  # type: ignore[index]

        try:
            await handler(update, context)
        except Exception as e:
            log.error("Command handler error", command=handler.__name__, error=str(e), user_id=user_id)
            await update.message.reply_text(f"❌ Error: {e}")

    return wrapped


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_session(bot_manager: BotManager):
    """Get an async DB session from the bot manager's session factory."""
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


async def cmd_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Browse available strategies from the DB catalog."""
    async with await _get_session(context.bot_data["bot_manager"]) as session:
        result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.is_active == True).order_by(StrategyCatalog.display_name)  # noqa: E712
        )
        strategies = result.scalars().all()

    if not strategies:
        await update.message.reply_text("📋 No active strategies in catalog.")
        return

    lines = ["📋 *Available Strategies:*", ""]
    for i, s in enumerate(strategies, 1):
        armed_marker = ""
        lines.append(f"{i}. `{s.id}` — {s.display_name}")
        lines.append(f"   Type: {s.strategy_type} | Underlying: {s.underlying} | v{s.version}")
    lines.append("")
    lines.append("Use /subscribe <strategy_id> to subscribe.")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Subscribe to a strategy. Usage: /subscribe <strategy_id>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)

    if not context.args:
        await update.message.reply_text("Usage: /subscribe <strategy_id>")
        return

    strategy_id = context.args[0]
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        # Validate strategy exists
        cat = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.id == strategy_id, StrategyCatalog.is_active == True)  # noqa: E712
        )
        catalog_entry = cat.scalar_one_or_none()
        if not catalog_entry:
            await update.message.reply_text(f"❌ Strategy `{strategy_id}` not found or inactive.")
            return

        # Check if already subscribed
        existing = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.catalog_id == strategy_id,
            )
        )
        if existing.scalar_one_or_none():
            await update.message.reply_text(f"ℹ️ Already subscribed to `{strategy_id}`.")
            return

        # Create subscription
        sub = UserSubscription(user_id=uid, catalog_id=strategy_id, is_armed=False)
        session.add(sub)
        await session.commit()

    await update.message.reply_text(
        f"✅ Subscribed to `{strategy_id}`.\nUse /arm {strategy_id} to activate.",
        parse_mode="Markdown",
    )
    log.info("Strategy subscribed", user_id=user_id, strategy=strategy_id)


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unsubscribe from a strategy. Usage: /unsubscribe <strategy_id>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)

    if not context.args:
        await update.message.reply_text("Usage: /unsubscribe <strategy_id>")
        return

    strategy_id = context.args[0]
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.catalog_id == strategy_id,
            )
        )
        sub = result.scalar_one_or_none()
        if not sub:
            await update.message.reply_text(f"❌ Not subscribed to `{strategy_id}`.")
            return

        await session.delete(sub)
        await session.commit()

    await update.message.reply_text(
        f"🔕 Unsubscribed from `{strategy_id}`.",
        parse_mode="Markdown",
    )
    log.info("Strategy unsubscribed", user_id=user_id, strategy=strategy_id)


async def cmd_arm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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


async def cmd_disarm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Disarm an active strategy. Usage: /disarm <strategy_id>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)

    if not context.args:
        await update.message.reply_text("Usage: /disarm <strategy_id>")
        return

    strategy_id = context.args[0]
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
        result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == uid,
                UserSubscription.catalog_id == strategy_id,
            )
        )
        sub = result.scalar_one_or_none()
        if not sub:
            await update.message.reply_text(f"❌ Not subscribed to `{strategy_id}`.")
            return

        if not sub.is_armed:
            await update.message.reply_text(f"ℹ️ `{strategy_id}` is already disarmed.")
            return

        sub.is_armed = False
        await session.commit()

    await update.message.reply_text(
        f"🛑 `{strategy_id}` disarmed. No new trades will be placed.",
        parse_mode="Markdown",
    )
    log.info("Strategy disarmed", user_id=user_id, strategy=strategy_id)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show strategy status & balance overview for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]
    uid = _parse_user_id(user_id)
    bot_mgr = context.bot_data["bot_manager"]

    async with await _get_session(bot_mgr) as session:
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
        await update.message.reply_text(
            "💰 *Balance*\n\nDhan broker client not available.\nCheck service logs.",
            parse_mode="Markdown",
        )
        return

    try:
        balance = await dhan_client.get_balance()
    except Exception as e:
        log.error("Balance fetch failed", error=str(e), user_id=user_id)
        await update.message.reply_text(f"❌ Failed to fetch balance: {e}")
        return

    # Get user's risk limit for floor calculation
    async with await _get_session(bot_mgr) as session:
        risk_result = await session.execute(
            select(RiskLimit).where(RiskLimit.user_id == uid)
        )
        risk = risk_result.scalar_one_or_none()
        floor_pct = risk.balance_floor_pct if risk else 0.20

    floor_amount = balance.total_equity * floor_pct
    ts = datetime.fromtimestamp(balance.timestamp, tz=timezone.utc).strftime("%H:%M:%S UTC")

    text = (
        f"💰 *Balance* (as of {ts})\n\n"
        f"Total Equity: ₹{balance.total_equity:,.2f}\n"
        f"Free Balance: ₹{balance.free_balance:,.2f}\n"
        f"Used Margin: ₹{balance.used_margin:,.2f}\n"
        f"Floor ({floor_pct*100:.0f}%): ₹{floor_amount:,.2f}\n"
    )
    if balance.free_balance < floor_amount:
        text += "\n⚠️ *Below floor — new entries blocked.*"

    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show open positions for this user from strategy_instances."""
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

    async with await _get_session(bot_mgr) as session:
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


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help message with all available commands."""
    help_text = (
        "🤖 *Indian Trading Bot — Commands*\n\n"
        "*Strategy Management:*\n"
        "/catalog — Browse available strategies\n"
        "/subscribe <id> — Subscribe to a strategy\n"
        "/unsubscribe <id> — Unsubscribe\n"
        "/arm <id> — Activate for live trading\n"
        "/disarm <id> — Deactivate\n\n"
        "*Monitoring:*\n"
        "/status — Overview & subscriptions\n"
        "/balance — Equity breakdown (live from Dhan)\n"
        "/trades — Open positions\n"
        "/profit — Unrealized P&L\n\n"
        "*Emergency:*\n"
        "/forceexit — Close ALL positions now\n\n"
        "/help — Show this message"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")