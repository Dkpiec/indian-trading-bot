"""Telegram bot connection endpoints: connect, disconnect, status."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class ConnectRequest(BaseModel):
    bot_token: str
    chat_id: str


@router.post("/connect")
async def connect_bot(req: ConnectRequest):
    """Connect a user's Telegram bot for alerts and commands."""
    # TODO: Phase 4 — store encrypted token + chat_id in users table,
    # spawn BotManager instance for this token
    return {"status": "connected", "chat_id": req.chat_id}


@router.delete("/disconnect")
async def disconnect_bot():
    """Disconnect the authenticated user's Telegram bot."""
    # TODO: Phase 4 — clear telegram_token/chat_id, stop BotManager
    return {"status": "disconnected"}


@router.get("/status")
async def bot_status():
    """Get Telegram bot connection status for the authenticated user."""
    # TODO: Phase 4 — check if bot is running, last message time
    return {"connected": False, "last_message": None}