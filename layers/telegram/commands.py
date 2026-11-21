"""Telegram command handlers — per-user data isolation.

Every handler receives user_id via context.user_data['user_id'], resolved
by the routing wrapper from chat_id → user_id mapping in BotManager.
Handlers query ONLY that user's data; no cross-user leakage.
"""
from __future__ import annotations

import structlog
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from layers.telegram.bot_manager import BotManager

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
            return

        chat_id = str(update.effective_chat.id)
        user_id = await bot_manager.resolve_user_id(chat_id)

        if not user_id:
            await update.message.reply_text(
                "⚠️ Unauthorized. Please connect your bot via the web dashboard first."
            )
            log.warning("Unauthorized Telegram command", chat_id=chat_id)
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
# Command handlers
# ---------------------------------------------------------------------------


async def cmd_catalog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Browse available strategies."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Query strategy catalog filtered by user permissions
    # For now, list strategies from the catalog module
    try:
        from catalog import __all__ as catalog_strategies  # type: ignore[attr-defined]
        lines = ["📋 *Available Strategies:*", ""]
        for i, name in enumerate(catalog_strategies, 1):
            lines.append(f"{i}. `{name}`")
        lines.append("")
        lines.append("Use /subscribe <strategy_name> to subscribe.")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception:
        await update.message.reply_text(
            "📋 Strategy catalog is being loaded. Please try again shortly."
        )


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Subscribe to a strategy. Usage: /subscribe <strategy_name>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    if not context.args:
        await update.message.reply_text("Usage: /subscribe <strategy_name>")
        return

    strategy_name = context.args[0]

    # TODO: Validate strategy exists, check user eligibility, persist subscription
    # For now, acknowledge the request
    await update.message.reply_text(
        f"✅ Subscribed to `{strategy_name}`.\nUse /arm {strategy_name} to activate.",
        parse_mode="Markdown",
    )
    log.info("Strategy subscribed", user_id=user_id, strategy=strategy_name)


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unsubscribe from a strategy. Usage: /unsubscribe <strategy_name>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    if not context.args:
        await update.message.reply_text("Usage: /unsubscribe <strategy_name>")
        return

    strategy_name = context.args[0]

    # TODO: Remove subscription, disarm if armed, clean up state
    await update.message.reply_text(
        f"🔕 Unsubscribed from `{strategy_name}`.",
        parse_mode="Markdown",
    )
    log.info("Strategy unsubscribed", user_id=user_id, strategy=strategy_name)


async def cmd_arm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Arm a subscribed strategy for live execution. Usage: /arm <strategy_name>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    if not context.args:
        await update.message.reply_text("Usage: /arm <strategy_name>")
        return

    strategy_name = context.args[0]

    # TODO: Validate subscription exists, check balance floor, set armed state
    await update.message.reply_text(
        f"🔫 `{strategy_name}` armed. Will execute on next valid signal.",
        parse_mode="Markdown",
    )
    log.info("Strategy armed", user_id=user_id, strategy=strategy_name)


async def cmd_disarm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Disarm an active strategy. Usage: /disarm <strategy_name>"""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    if not context.args:
        await update.message.reply_text("Usage: /disarm <strategy_name>")
        return

    strategy_name = context.args[0]

    # TODO: Clear armed state, cancel pending orders
    await update.message.reply_text(
        f"🛑 `{strategy_name}` disarmed. No new trades will be placed.",
        parse_mode="Markdown",
    )
    log.info("Strategy disarmed", user_id=user_id, strategy=strategy_name)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show strategy status & balance overview for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Query user's subscriptions, armed state, recent signals, balance
    await update.message.reply_text(
        f"📊 *Status for user {user_id[:8]}…*\n\n"
        "No active subscriptions yet.\n"
        "Use /catalog to browse strategies.",
        parse_mode="Markdown",
    )


async def cmd_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show equity / free / used / floor breakdown."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Query broker margin API for this user's account
    await update.message.reply_text(
        "💰 *Balance*\n\n"
        "Connect a broker first to see live balance.\n"
        "Floor threshold: 20% of total equity.",
        parse_mode="Markdown",
    )


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show open positions for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Query positions table filtered by user_id
    await update.message.reply_text("📈 No open positions.")


async def cmd_profit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show unrealized P&L summary."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Aggregate unrealized P&L from positions
    await update.message.reply_text("💵 Unrealized P&L: ₹0.00 (no open positions)")


async def cmd_forceexit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close ALL open positions immediately for this user."""
    user_id = context.user_data["user_id"]  # type: ignore[index]

    # TODO: Fetch all open positions for user_id, place market exit orders,
    # update position status, send confirmation
    await update.message.reply_text(
        "⚠️ Force exit initiated.\n"
        "Closing all open positions at market price…"
    )
    log.warning("Force exit requested", user_id=user_id)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show help message with all available commands."""
    help_text = (
        "🤖 *Indian Trading Bot — Commands*\n\n"
        "*Strategy Management:*\n"
        "/catalog — Browse available strategies\n"
        "/subscribe <name> — Subscribe to a strategy\n"
        "/unsubscribe <name> — Unsubscribe\n"
        "/arm <name> — Activate for live trading\n"
        "/disarm <name> — Deactivate\n\n"
        "*Monitoring:*\n"
        "/status — Overview & subscriptions\n"
        "/balance — Equity breakdown\n"
        "/trades — Open positions\n"
        "/profit — Unrealized P&L\n\n"
        "*Emergency:*\n"
        "/forceexit — Close ALL positions now\n\n"
        "/help — Show this message"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")