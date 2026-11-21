"""initial_schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-11-21 00:00:00.000000+00:00
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -- master_credentials (must come before user_credentials due to FK)
    op.create_table(
        "master_credentials",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("broker", sa.String(32), nullable=False),
        sa.Column("encrypted_creds", sa.Text(), nullable=False),
        sa.Column("scope", sa.String(16), nullable=False, server_default="ALL"),
        sa.Column("applies_to", JSONB(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "idx_master_creds_broker_active",
        "master_credentials",
        ["broker", "is_active"],
        postgresql_where=sa.text("is_active = true"),
    )

    # -- users
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(256), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("telegram_token", sa.Text(), nullable=True),
        sa.Column("telegram_chat_id", sa.String(64), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("master_override_active", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("master_override_broker", sa.String(32), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("email", name="uq_users_email"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    # -- user_credentials
    op.create_table(
        "user_credentials",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("broker", sa.String(32), nullable=False),
        sa.Column("encrypted_creds", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_master_override", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_overridden", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "overridden_by",
            UUID(as_uuid=True),
            sa.ForeignKey("master_credentials.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "broker", name="uq_user_credentials_user_broker"),
    )
    op.create_index("ix_user_credentials_user_id", "user_credentials", ["user_id"])

    # -- strategy_catalog
    op.create_table(
        "strategy_catalog",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("plugin_file", sa.String(256), nullable=False),
        sa.Column("underlying", sa.String(32), nullable=False),
        sa.Column("segment", sa.String(16), nullable=False, server_default="NSE_FNO"),
        sa.Column("strategy_type", sa.String(32), nullable=False),
        sa.Column("default_params", JSONB(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("version", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # -- user_subscriptions
    op.create_table(
        "user_subscriptions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "catalog_id",
            sa.String(64),
            sa.ForeignKey("strategy_catalog.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("custom_params", JSONB(), nullable=True),
        sa.Column("is_armed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("broker_override", sa.String(32), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "catalog_id", name="uq_user_subscriptions_user_catalog"),
    )
    op.create_index(
        "idx_user_subs_armed",
        "user_subscriptions",
        ["user_id", "is_armed"],
        postgresql_where=sa.text("is_armed = true"),
    )

    # -- strategy_instances
    op.create_table(
        "strategy_instances",
        sa.Column("instance_id", sa.String(64), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("catalog_id", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="IDLE"),
        sa.Column("execution_mode", sa.String(16), nullable=False, server_default="PAPER"),
        sa.Column("parameters", JSONB(), nullable=False, server_default="{}"),
        sa.Column("net_qty", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("entry_price", sa.Float(), nullable=True),
        sa.Column("last_ltp", sa.Float(), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_strategy_instances_user_id", "strategy_instances", ["user_id"])

    # -- trade_order_ledger
    op.create_table(
        "trade_order_ledger",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("instance_id", sa.String(64), nullable=False),
        sa.Column("broker", sa.String(32), nullable=False),
        sa.Column("order_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(64), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("order_type", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("filled_quantity", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("average_price", sa.Float(), nullable=True),
        sa.Column("pnl", sa.Float(), nullable=True),
        sa.Column("tags", JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_trade_order_ledger_user_id", "trade_order_ledger", ["user_id"])
    op.create_index("ix_trade_order_ledger_instance_id", "trade_order_ledger", ["instance_id"])
    op.create_index("ix_trade_order_ledger_created_at", "trade_order_ledger", ["created_at"])
    op.create_index(
        "idx_ledger_user_instance",
        "trade_order_ledger",
        ["user_id", "instance_id"],
    )

    # -- daily_pnl
    op.create_table(
        "daily_pnl",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("trade_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("starting_equity", sa.Float(), nullable=False),
        sa.Column("ending_equity", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("total_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("winning_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("losing_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_drawdown", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "trade_date", name="uq_daily_pnl_user_date"),
    )
    op.create_index("idx_daily_pnl_user_date", "daily_pnl", ["user_id", "trade_date"])

    # -- risk_limits
    op.create_table(
        "risk_limits",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "user_id",
            UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("max_open_positions", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("max_daily_loss_pct", sa.Float(), nullable=False, server_default="3.0"),
        sa.Column("max_single_trade_risk_pct", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("balance_floor_pct", sa.Float(), nullable=False, server_default="0.20"),
        sa.Column("max_leverage", sa.Float(), nullable=False, server_default="3.0"),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="60"),
        sa.Column("circuit_breaker_threshold", sa.Float(), nullable=False, server_default="5.0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # -- strategy_runs (legacy table preserved from prior scaffold)
    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("strategy_name", sa.String(128), nullable=False),
        sa.Column("broker", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("payload", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("strategy_runs")
    op.drop_table("risk_limits")
    op.drop_table("daily_pnl")
    op.drop_table("trade_order_ledger")
    op.drop_table("strategy_instances")
    op.drop_table("user_subscriptions")
    op.drop_table("strategy_catalog")
    op.drop_table("master_credentials")
    op.drop_table("user_credentials")
    op.drop_table("users")