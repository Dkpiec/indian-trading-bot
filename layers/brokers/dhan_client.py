"""DhanHQ v2 broker adapter.

API base: https://api.dhan.co/v2
Auth: access-token header (NOT Bearer).
NEVER call historical data endpoints — they are chargeable.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import httpx
import structlog

from layers.brokers.base import BalanceSnapshot, BrokerBase

log = structlog.get_logger(__name__)

DHAN_BASE = "https://api.dhan.co/v2"


class DhanClient(BrokerBase):
    """Async adapter for DhanHQ v2 REST API."""

    def __init__(self, *, client_id: str, access_token: str, timeout: float = 10.0):
        self._client_id = client_id
        self._access_token = access_token
        self._http = httpx.AsyncClient(timeout=timeout)

    def _headers(self) -> dict:
        return {
            "access-token": self._access_token,
            "Content-Type": "application/json",
            "client-id": self._client_id,
        }

    async def get_balance(self) -> BalanceSnapshot:
        url = f"{DHAN_BASE}/fundlimit"
        resp = await self._http.get(url, headers=self._headers())
        if resp.status_code != 200:
            raise RuntimeError(f"Dhan balance failed: {resp.status_code} {resp.text}")
        data = resp.json()
        # Dhan fundlimit response keys vary; normalize defensively
        total = float(data.get("totalBalance", data.get("totalEquity", 0)))
        free = float(data.get("availabelBalance", data.get("availableBalance", data.get("freeCash", 0))))
        used = float(data.get("usedMargin", data.get("marginUsed", 0)))
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
        product_type: str = "INTRADAY",
        validity: str = "DAY",
        exchange_segment: str = "NSE_FO",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "dhanClientId": self._client_id,
            "transactionType": side.upper(),
            "exchangeSegment": exchange_segment,
            "productType": product_type,
            "orderType": order_type,
            "validity": validity,
            "securityId": str(security_id),
            "quantity": quantity,
        }
        if order_type == "LIMIT" and price > 0:
            payload["price"] = price
        if client_order_id:
            payload["correlationId"] = client_order_id

        resp = await self._http.post(f"{DHAN_BASE}/orders", json=payload, headers=self._headers())
        data = resp.json()
        if resp.status_code in (200, 201) and "orderId" in data:
            return {"status": "ACCEPTED", "broker_order_id": data["orderId"], "message": ""}
        return {
            "status": "REJECTED",
            "broker_order_id": None,
            "message": data.get("errorMessage", resp.text),
        }

    async def get_option_ltp(self, symbol: str, security_id: str = "") -> Optional[float]:
        """Fetch latest LTP for an option contract via Dhan option chain.

        Dhan's option chain endpoint is free (not historical data).
        Falls back to quote API if option chain fails.
        """
        if not security_id:
            return None
        try:
            # Dhan quote endpoint: free, returns LTP for a security
            url = f"{DHAN_BASE}/quotes"
            payload = {"securityId": str(security_id), "exchangeSegment": "NSE_FNO"}
            resp = await self._http.post(url, json=payload, headers=self._headers())
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("data", {}).get("data", [])
                if items:
                    ltp = items[0].get("lastPrice")
                    return float(ltp) if ltp else None
        except Exception as e:
            log.debug("Dhan LTP poll failed", symbol=symbol, error=str(e))
        return None

    async def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        url = f"{DHAN_BASE}/orders/{broker_order_id}"
        params = {"dhanClientId": self._client_id}
        resp = await self._http.get(url, headers=self._headers(), params=params)
        if resp.status_code != 200:
            return {"status": "UNKNOWN", "message": resp.text}
        data = resp.json()
        dhan_status = str(data.get("orderStatus", data.get("status", ""))).upper()
        return {"status": dhan_status, "raw": data}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        url = f"{DHAN_BASE}/orders/{broker_order_id}"
        params = {"dhanClientId": self._client_id}
        resp = await self._http.delete(url, headers=self._headers(), params=params)
        if resp.status_code == 200:
            return {"status": "CANCELLED", "message": ""}
        return {"status": "ERROR", "message": resp.text}

    async def close(self) -> None:
        await self._http.aclose()