"""Shared balance pool gate with 20% floor and entry locking.

Invariants enforced here:
- Balance MUST be fresh (<5s old) before any entry evaluation.
- 20% floor: if available_balance < total_equity * 0.20, block ALL new entries.
- Entry lock via Redis SETNX with 15s TTL to prevent race conditions between
  parallel strategies evaluating the same user simultaneously.

The floor is computed from total_equity (free + used), NOT free_balance alone,
to prevent the death spiral described in plan §7.1.
"""
from __future__ import annotations

import time
from typing import Optional

import redis.asyncio as aioredis
import structlog

from core.config import settings
from layers.brokers.base import BalanceSnapshot, BrokerBase

log = structlog.get_logger(__name__)

BALANCE_CACHE_TTL = 5  # seconds — matches plan §7.2 max_age_seconds
ENTRY_LOCK_TTL = 15  # seconds — prevents parallel strategy races
FLOOR_PCT = settings.balance_floor_pct  # 0.20


class BalanceGate:
    """Pre-trade gate: freshness check → 20% floor → margin sufficiency → lock."""

    def __init__(self, broker: BrokerBase, redis_client: aioredis.Redis):
        self._broker = broker
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_available_balance(self, user_id: str) -> float:
        """Return available balance above the 20% floor, or 0.0 if below.

        Always fetches a fresh snapshot; callers that need cached values
        should use the executor-level cache instead.
        """
        snap = await self._fetch_fresh_balance(user_id)
        floor = snap.total_equity * FLOOR_PCT
        return max(0.0, snap.free_balance - floor)

    async def check_entry_allowed(
        self, user_id: str, required_margin: float
    ) -> bool:
        """Full pre-entry gate. Returns True only if all checks pass.

        Checks in order:
          1. Balance freshness (<5s)
          2. 20% floor not breached
          3. Sufficient margin above floor for this trade
          4. Atomic entry lock acquired (SETNX, 15s TTL)
        """
        # 1. Fresh balance
        snap = await self._fetch_fresh_balance(user_id)
        if snap.age_seconds > BALANCE_CACHE_TTL:
            log.warning(
                "balance_stale",
                user_id=user_id,
                age_s=round(snap.age_seconds, 1),
            )
            return False

        floor = snap.total_equity * FLOOR_PCT

        # 2. 20% floor
        if snap.free_balance <= floor:
            log.info(
                "entry_blocked_floor",
                user_id=user_id,
                free=round(snap.free_balance, 2),
                floor=round(floor, 2),
            )
            return False

        # 3. Margin sufficiency
        available = snap.free_balance - floor
        if required_margin > available:
            log.info(
                "entry_blocked_margin",
                user_id=user_id,
                required=round(required_margin, 2),
                available=round(available, 2),
            )
            return False

        # 4. Atomic entry lock
        lock_key = f"entry_lock:{user_id}"
        acquired = await self._redis.set(lock_key, "1", nx=True, ex=ENTRY_LOCK_TTL)
        if not acquired:
            log.info("entry_blocked_lock", user_id=user_id)
            return False

        log.debug(
            "entry_allowed",
            user_id=user_id,
            required=round(required_margin, 2),
            available=round(available, 2),
        )
        return True

    async def release_entry_lock(self, user_id: str) -> None:
        """Release the entry lock early (e.g. after order placement completes)."""
        await self._redis.delete(f"entry_lock:{user_id}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_fresh_balance(self, user_id: str) -> BalanceSnapshot:
        """Fetch balance directly from broker — always fresh, never cached here."""
        try:
            return await self._broker.get_balance()
        except Exception as exc:
            log.error("balance_fetch_failed", user_id=user_id, error=str(exc))
            # Return a zeroed snapshot with current timestamp so the freshness
            # check passes but the floor check blocks (safe failure mode).
            return BalanceSnapshot(
                total_equity=0.0,
                free_balance=0.0,
                used_margin=0.0,
                timestamp=time.time(),
            )