"""Abstract broker interface for the Indian Trading Bot platform.

Every broker adapter (Dhan, Upstox, FYERS, etc.) must implement this ABC so
the execution router and balance gate can operate on a uniform contract.
Credentials are always decrypted *before* being passed to the constructor —
adapters never touch encrypted blobs or the security module directly.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class BalanceSnapshot:
    """Immutable point-in-time balance reading from a broker.

    Attributes:
        total_equity: Free + used margin (the true account size).
        free_balance: Cash available to open new positions.
        used_margin: Margin locked by open positions.
        timestamp: Epoch seconds when the broker returned these numbers.
        currency: ISO currency code (always INR for this platform).
    """

    total_equity: float
    free_balance: float
    used_margin: float
    timestamp: float
    currency: str = "INR"

    @property
    def age_seconds(self) -> float:
        return max(0.0, time.time() - self.timestamp)


class BrokerBase(ABC):
    """Uniform async interface every broker adapter must satisfy."""

    @abstractmethod
    async def get_balance(self) -> BalanceSnapshot:
        """Fetch live balances from the broker.

        Must return a fresh snapshot; caching is the caller's responsibility
        (the balance gate owns TTL enforcement).
        """

    @abstractmethod
    async def place_order(
        self,
        *,
        symbol: str,
        security_id: str,
        side: str,
        quantity: int,
        order_type: str = "MARKET",
        price: float = 0.0,
        product_type: str = "INTRADAY",
        validity: str = "DAY",
        exchange_segment: str = "NSE_FO",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit an order and return the normalized response dict.

        Required keys in the returned dict: status, broker_order_id, message.
        Adapters should NOT enforce rate limits here — the execution router
        owns SEBI 9-OPS throttling per segment.
        """

    @abstractmethod
    async def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        """Poll order status. Returns dict with at least 'status' key."""

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        """Cancel a pending order."""

    @abstractmethod
    async def close(self) -> None:
        """Release HTTP connections / resources."""
