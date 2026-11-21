"""SQLAlchemy ORM models for Indian Trading Bot — asyncpg-compatible types."""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Shared declarative base for all models."""
    pass


# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(256), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    telegram_token: Mapped[str | None] = mapped_column(Text, nullable=True)  # Fernet-encrypted
    telegram_chat_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    master_override_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    master_override_broker: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # relationships
    credentials = relationship("UserCredential", back_populates="user", cascade="all, delete-orphan")
    subscriptions = relationship("UserSubscription", back_populates="user", cascade="all, delete-orphan")
    strategy_instances = relationship("StrategyInstance", back_populates="user", cascade="all, delete-orphan")
    risk_limits = relationship("RiskLimit", back_populates="user", uselist=False, cascade="all, delete-orphan")
    daily_pnls = relationship("DailyPnl", back_populates="user", cascade="all, delete-orphan")


# ---------------------------------------------------------------------------
# user_credentials
# ---------------------------------------------------------------------------
class UserCredential(Base):
    __tablename__ = "user_credentials"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    broker: Mapped[str] = mapped_column(String(32), nullable=False)
    encrypted_creds: Mapped[str] = mapped_column(Text, nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_master_override: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_overridden: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    overridden_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("master_credentials.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("user_id", "broker", name="uq_user_credentials_user_broker"),
    )

    user = relationship("User", back_populates="credentials")


# ---------------------------------------------------------------------------
# master_credentials
# ---------------------------------------------------------------------------
class MasterCredential(Base):
    __tablename__ = "master_credentials"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    broker: Mapped[str] = mapped_column(String(32), nullable=False)
    encrypted_creds: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[str] = mapped_column(String(16), nullable=False, default="ALL")  # 'ALL' or 'SELECTED'
    applies_to: Mapped[dict | None] = mapped_column(JSONB, nullable=True)  # NULL when scope='ALL'; JSONB array of UUIDs when 'SELECTED'
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        Index(
            "idx_master_creds_broker_active",
            "broker",
            "is_active",
            postgresql_where=(is_active == True),  # noqa: E712
        ),
    )


# ---------------------------------------------------------------------------
# strategy_catalog
# ---------------------------------------------------------------------------
class StrategyCatalog(Base):
    __tablename__ = "strategy_catalog"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)  # e.g. 'nifty_slhunt_v41b'
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    plugin_file: Mapped[str] = mapped_column(String(256), nullable=False)
    underlying: Mapped[str] = mapped_column(String(32), nullable=False)  # 'NIFTY', 'SENSEX', 'ANY'
    segment: Mapped[str] = mapped_column(String(16), nullable=False, default="NSE_FNO")
    strategy_type: Mapped[str] = mapped_column(String(32), nullable=False)  # 'SLHUNT', 'ORB', 'STRADDLE'
    default_params: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    version: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    subscriptions = relationship("UserSubscription", back_populates="catalog")


# ---------------------------------------------------------------------------
# user_subscriptions
# ---------------------------------------------------------------------------
class UserSubscription(Base):
    __tablename__ = "user_subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    catalog_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("strategy_catalog.id", ondelete="CASCADE"), nullable=False
    )
    custom_params: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    is_armed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    broker_override: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("user_id", "catalog_id", name="uq_user_subscriptions_user_catalog"),
        Index(
            "idx_user_subs_armed",
            "user_id",
            "is_armed",
            postgresql_where=(is_armed == True),  # noqa: E712
        ),
    )

    user = relationship("User", back_populates="subscriptions")
    catalog = relationship("StrategyCatalog", back_populates="subscriptions")


# ---------------------------------------------------------------------------
# strategy_instances
# ---------------------------------------------------------------------------
class StrategyInstance(Base):
    __tablename__ = "strategy_instances"

    instance_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    catalog_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="IDLE")  # IDLE, RUNNING, PAUSED, ERROR
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="PAPER")  # PAPER, LIVE
    parameters: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    net_qty: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_ltp: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    user = relationship("User", back_populates="strategy_instances")


# ---------------------------------------------------------------------------
# trade_order_ledger
# ---------------------------------------------------------------------------
class TradeOrderLedger(Base):
    __tablename__ = "trade_order_ledger"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    instance_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    broker: Mapped[str] = mapped_column(String(32), nullable=False)
    order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)  # BUY, SELL
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)  # MARKET, LIMIT, SL
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")  # PENDING, FILLED, CANCELLED, REJECTED
    filled_quantity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    average_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    __table_args__ = (
        Index("idx_ledger_user_instance", "user_id", "instance_id"),
    )


# ---------------------------------------------------------------------------
# daily_pnl
# ---------------------------------------------------------------------------
class DailyPnl(Base):
    __tablename__ = "daily_pnl"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    trade_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    starting_equity: Mapped[float] = mapped_column(Float, nullable=False)
    ending_equity: Mapped[float] = mapped_column(Float, nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    total_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    winning_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    losing_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("user_id", "trade_date", name="uq_daily_pnl_user_date"),
        Index("idx_daily_pnl_user_date", "user_id", "trade_date"),
    )

    user = relationship("User", back_populates="daily_pnls")


# ---------------------------------------------------------------------------
# risk_limits
# ---------------------------------------------------------------------------
class RiskLimit(Base):
    __tablename__ = "risk_limits"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    max_open_positions: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    max_daily_loss_pct: Mapped[float] = mapped_column(Float, default=3.0, nullable=False)
    max_single_trade_risk_pct: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    balance_floor_pct: Mapped[float] = mapped_column(Float, default=0.20, nullable=False)
    max_leverage: Mapped[float] = mapped_column(Float, default=3.0, nullable=False)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    circuit_breaker_threshold: Mapped[float] = mapped_column(Float, default=5.0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    user = relationship("User", back_populates="risk_limits")


# ---------------------------------------------------------------------------
# strategy_runs (legacy — preserved from prior scaffold)
# ---------------------------------------------------------------------------
class StrategyRun(Base):
    """Tracks individual strategy execution runs."""
    __tablename__ = "strategy_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(128), nullable=False)
    broker: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )