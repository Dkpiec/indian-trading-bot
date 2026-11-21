"""Centralized configuration — all secrets from env, no hardcoded values."""
from __future__ import annotations

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    database_url: str = "postgresql+asyncpg://itb_user:itb_pass@localhost:5432/indian_trading_bot_db"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Security
    master_encryption_key: str = ""  # Fernet key, 32 bytes base64
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24h

    # Telegram (default bot for platform alerts; users connect their own)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Broker defaults
    upstox_api_key: str = ""
    upstox_api_secret: str = ""
    upstox_access_token: str = ""
    dhan_client_id: str = ""
    dhan_access_token: str = ""

    # Telegram (user's trading bot)
    trading_telegram_bot_token: str = ""
    telegram_allowed_users: str = ""

    # Balance floor
    balance_floor_pct: float = 0.20  # 20% of total balance

    # Market hours (IST)
    market_open: str = "09:15"
    market_close: str = "15:30"
    entry_window_start: str = "09:45"
    entry_window_end: str = "13:15"

    class Config:
        env_file = ".env"
        env_prefix = ""
        case_sensitive = False


settings = Settings()