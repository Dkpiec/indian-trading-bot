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
        resp = await self._http.post(url, json={}, headers=self._headers())
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

    # Free index spot source (Yahoo v8 chart API — no Dhan charge).
    # Mirrors the free source the SLHUNT plugins use, so ORB strategies can
    # evaluate breakout/breakdown signals without a resolved option contract.
    # Yahoo has NIFTY (and BANKNIFTY) index tickers but NO free SENSEX one.
    # SENSEX falls back to the Dhan poll only; the Dhan index LTP is the
    # primary source for SENSEX.
    YAHOO_INDEX = {"NIFTY": "%5ENSEI", "BANKNIFTY": "%5ENSEIBANKNIFTY"}

    async def get_index_ltp(self, symbol: str = "NSE") -> Optional[float]:
        """Fetch the index spot price for NIFTY/SENSEX/BANKNIFTY.

        Primary: Dhan /v2/quotes index poll (free). Fallback: the FREE
        Yahoo v8 chart API (the same source the SLHUNT plugins use for
        5m bars) — never a chargeable Dhan historical endpoint.
        """
        dhan_val = await self._dhan_index_ltp(symbol)
        if dhan_val is not None:
            return dhan_val
        return await self._yahoo_index_ltp(symbol)

    async def _dhan_index_ltp(self, symbol: str) -> Optional[float]:
        """Dhan /v2/quotes index poll (free, not historical data)."""
        # Dhan index security IDs (public constants from the Dhan v2 API docs)
        index_ids = {"NIFTY": "mid3101", "SENSEX": "mid3040"}
        code = symbol.upper()
        for suffix in ("_CE_PE", "_PE", "_CE"):
            code = code.replace(suffix, "")
        code = code.replace("_", "")
        for known in index_ids:
            if code.startswith(known):
                code = known
                break
        sid = index_ids.get(code)
        if not sid:
            return None
        try:
            # Dhan v2 live quote endpoint (free, not historical data).
            url = f"{DHAN_BASE}/market/quote"
            payload = {"securityId": sid, "exchangeSegment": "INDEX"}
            resp = await self._http.post(url, json=payload, headers=self._headers())
            if resp.status_code == 200:
                items = resp.json().get("data", {}).get("data", [])
                if items:
                    last = items[0].get("lastPrice")
                    if last:
                        return float(last)
                log.debug("Dhan index LTP empty response", symbol=symbol,
                          status=resp.status_code, body=str(resp.text)[:200])
            else:
                log.warning("Dhan index LTP HTTP error", symbol=symbol,
                            status=resp.status_code, body=str(resp.text)[:200])
        except Exception as e:
            log.warning("Dhan index LTP poll failed", symbol=symbol, error=str(e))
        return None

    async def _yahoo_index_ltp(self, symbol: str) -> Optional[float]:
        """Free Yahoo v8 chart API — last close as the spot proxy."""
        import json as _json
        import urllib.request
        code = symbol.upper().replace("_", "")
        for suffix in ("_CE_PE", "_PE", "_CE"):
            code = code.replace(suffix, "")
        for known in self.YAHOO_INDEX:
            if code.startswith(known):
                code = known
                break
        ticker = self.YAHOO_INDEX.get(code)
        if not ticker:
            return None
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1m&range=1d"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = _json.loads(resp.read().decode())
            result = (data.get("chart", {}).get("result") or [None])[0]
            if not result:
                return None
            close = (result.get("indicators", {}).get("quote", [{}])[0].get("close") or [None])
            last = [v for v in close if v is not None]
            return float(last[-1]) if last else None
        except Exception as e:
            log.warning("Yahoo index LTP poll failed", symbol=symbol, error=str(e))
        return None

    async def get_option_ltp(self, symbol: str, security_id: str = "") -> Optional[float]:
        """Fetch latest LTP for an option contract via Dhan option chain.

        Dhan's option chain endpoint is free (not historical data).
        Falls back to quote API if option chain fails.
        """
        if not security_id:
            return None
        try:
            # Dhan quote endpoint: free, returns LTP for a security
            # Dhan v2 live quote endpoint (free, not historical data).
            url = f"{DHAN_BASE}/market/quote"
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
        url = f"{DHAN_BASE}/orderbook/trade/{broker_order_id}"
        payload = {"dhanClientId": self._client_id}
        resp = await self._http.post(url, json=payload, headers=self._headers())
        if resp.status_code != 200:
            return {"status": "UNKNOWN", "message": resp.text}
        data = resp.json()
        trades = data.get("data", {}).get("trades", [])
        dhan_status = str(data.get("status", "")).upper() if isinstance(data.get("status"), str) else ""
        return {"status": dhan_status or "OPEN", "raw": data}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        url = f"{DHAN_BASE}/orderbook/trade/cancel"
        payload = {
            "dhanClientId": self._client_id,
            "transactionId": broker_order_id,
        }
        resp = await self._http.post(url, json=payload, headers=self._headers())
        if resp.status_code == 200:
            return {"status": "CANCELLED", "message": ""}
        return {"status": "ERROR", "message": resp.text}

    async def close(self) -> None:
        await self._http.aclose()