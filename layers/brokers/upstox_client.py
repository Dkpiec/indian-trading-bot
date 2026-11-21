"""Upstox v2 broker adapter.

API base: https://api.upstox.com/v2
Auth: Bearer token in Authorization header.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx
import structlog

from layers.brokers.base import BalanceSnapshot, BrokerBase

log = structlog.get_logger(__name__)

UPSTOX_BASE = "https://api.upstox.com/v2"


class UpstoxClient(BrokerBase):
    """Async adapter for Upstox v2 REST API."""

    def __init__(self, *, access_token: str, timeout: float = 10.0):
        self._access_token = access_token
        self._http = httpx.AsyncClient(timeout=timeout)

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def get_balance(self) -> BalanceSnapshot:
        url = f"{UPSTOX_BASE}/user/get-funds-and-margin"
        resp = await self._http.get(url, headers=self._headers())
        if resp.status_code != 200:
            raise RuntimeError(f"Upstox balance failed: {resp.status_code} {resp.text}")
        data = resp.json().get("data", {})
        # Upstox returns nested fund summary; normalize defensively
        total = float(data.get("total_equity", data.get("net", 0)))
        free = float(data.get("available_margin", data.get("available_live_balance", 0)))
        used = float(data.get("used_margin", data.get("margin_used", 0)))
        return BalanceSnapshot(
            total_equity=total,
            free_balance=free,
            used_margin=used,
            timestamp=time.time(),
        )

    async def place_order(
        self,
        *,
        symbol: str,
        security_id: str,
        side: str,
        quantity: int,
        order_type: str = "MARKET",
        price: float = 0.0,
        product_type: str = "I",
        validity: str = "DAY",
        exchange_segment: str = "NSE_FO",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "quantity": quantity,
            "product": product_type,
            "validity": validity,
            "price": price if order_type == "LIMIT" else 0,
            "tag": client_order_id or "",
            "instrument_token": security_id,
            "order_type": order_type,
            "transaction_type": side.upper(),
            "disclosed_quantity": 0,
            "trigger_price": 0,
            "is_amo": False,
        }
        resp = await self._http.post(
            f"{UPSTOX_BASE}/order/place", json=payload, headers=self._headers()
        )
        data = resp.json()
        if resp.status_code in (200, 201) and data.get("status") == "success":
            order_id = data.get("data", {}).get("order_id")
            return {"status": "ACCEPTED", "broker_order_id": order_id, "message": ""}
        return {
            "status": "REJECTED",
            "broker_order_id": None,
            "message": data.get("errors", [{}])[0].get("message", resp.text)
            if isinstance(data.get("errors"), list)
            else str(data),
        }

    async def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        url = f"{UPSTOX_BASE}/order/history"
        params = {"order_id": broker_order_id}
        resp = await self._http.get(url, headers=self._headers(), params=params)
        if resp.status_code != 200:
            return {"status": "UNKNOWN", "message": resp.text}
        data = resp.json().get("data", [])
        if not data:
            return {"status": "UNKNOWN", "message": "no history"}
        latest = data[-1] if isinstance(data, list) else data
        return {"status": str(latest.get("status", "UNKNOWN")).upper(), "raw": latest}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        url = f"{UPSTOX_BASE}/order/cancel"
        params = {"order_id": broker_order_id}
        resp = await self._http.delete(url, headers=self._headers(), params=params)
        if resp.status_code == 200:
            return {"status": "CANCELLED", "message": ""}
        return {"status": "ERROR", "message": resp.text}

    async def close(self) -> None:
        await self._http.aclose()