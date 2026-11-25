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
    """Async adapter for DhanHQ v2 REST API.

    Hot-reloads the access token from disk before every API call so that
    a background token renewal (dhan-renew.py writing to coolify.env) is
    picked up without restarting the application. The token file is checked
    at most once per _TOKEN_CHECK_INTERVAL seconds to avoid per-request
    filesystem overhead; when the file's mtime changes, the new token is
    read and swapped in atomically.
    """

    # Check the credential sources at most once every 60 seconds. Token
    # renewals happen once per day at 08:35 IST, so this interval catches
    # every renewal within one tick cycle while keeping I/O negligible.
    _TOKEN_CHECK_INTERVAL = 60.0

    # Where a Dhan access token can live. The refresh tool (dhan-renew.py)
    # writes the newest token to the disk files; Coolify may ALSO inject a
    # (possibly stale) DHAN_ACCESS_TOKEN into the container's environment.
    # The previous bug: the container started with the env token, but the
    # hot-reload only ever read the disk files, so a stale env token was
    # never corrected -> persistent 401 "Invalid_Authentication".
    # We now treat ALL of these as candidate sources and always keep the
    # freshest *valid* one (by JWT expiry).
    _TOKEN_FILES = (
        "/app/coolify.env",
        "/opt/data/trading-engine/coolify.env",
        "/opt/data/secrets/coolify.env",
    )

    def __init__(self, *, client_id: str, access_token: str, timeout: float = 10.0):
        self._client_id = client_id
        self._http = httpx.AsyncClient(timeout=timeout)
        # Per-instance failure state (see class comment on _INDEX_POLL_RETRY_S).
        self._index_poll_disabled: Dict[str, float] = {}
        self._yahoo_ltp_cache: Dict[str, tuple] = {}
        self._YAHOO_MIN_INTERVAL = 30.0
        # Hot-reload state.
        self._last_token_scan: float = 0.0
        # Start with the caller-supplied token (usually the env var), but
        # immediately reconcile it against every source so we begin with the
        # freshest valid token rather than a possibly-stale env one.
        self._access_token = self._resolve_freshest_token(access_token)
        log.info(
            "Dhan client initialised",
            token_prefix=self._access_token[:12] if self._access_token else "(none)",
        )

    # ------------------------------------------------------------------
    # Token source scanning (env + disk files, freshest-valid wins)
    # ------------------------------------------------------------------
    @staticmethod
    def _jwt_exp(token: Optional[str]) -> float:
        """Decode a Dhan JWT's exp claim -> unix ts, or 0 if not parseable.

        Dhan access tokens are JWTs (issuer "dhan"). exp is the most reliable
        signal of which token is newest; it never needs a network call.
        """
        import base64
        import json as _json
        if not token or "." not in token:
            return 0.0
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            claims = _json.loads(base64.urlsafe_b64decode(payload))
            return float(claims.get("exp", 0) or 0)
        except Exception:
            return 0.0

    def _candidate_tokens(self) -> Dict[str, str]:
        """Return {source_name: token} for every available source.

        Sources: the running environment variable, and each disk file the
        renew tool writes. Only non-empty tokens are returned.
        """
        import os as _os
        out: Dict[str, str] = {}
        env_tok = _os.environ.get("DHAN_ACCESS_TOKEN", "").strip()
        if env_tok:
            out["env"] = env_tok
        for path in self._TOKEN_FILES:
            tok = self._parse_token_from_env(path)
            if tok:
                out[path] = tok
        return out

    def _resolve_freshest_token(self, fallback: str) -> str:
        """Pick the candidate token with the latest expiry (valid beats dead).

        Ties / unparseable expiry fall back to: the caller-supplied token
        (typically the env value), then the first disk token. This is what
        lets a freshly-minted disk token win over a stale env token without
        a container restart.
        """
        cands = self._candidate_tokens()
        if not cands and fallback:
            return fallback
        # Rank by exp descending; the newest valid token wins. A dead
        # (exp in the past) token only wins if it is the only one.
        ranked = sorted(
            cands.items(),
            key=lambda kv: self._jwt_exp(kv[1]),
            reverse=True,
        )
        if not ranked:
            return fallback
        return ranked[0][1]

    def _maybe_reload_token(self, force: bool = False) -> None:
        """Reconcile the in-memory token against every source.

        Called before every API request (rate-limited) and forced after a
        401. We re-scan ALL sources (env + disk files) and swap to the
        freshest valid token when it differs from what we hold. This is what
        fixes the stale-env-token bug: a disk refresh now wins even when the
        container started with an env token.
        """
        now = time.time()
        if not force and now - self._last_token_scan < self._TOKEN_CHECK_INTERVAL:
            return
        self._last_token_scan = now

        cands = self._candidate_tokens()
        if not cands:
            return
        ranked = sorted(
            cands.items(), key=lambda kv: self._jwt_exp(kv[1]), reverse=True,
        )
        best_src, best_tok = ranked[0]

        if best_tok and best_tok != self._access_token:
            old_exp = self._jwt_exp(self._access_token)
            new_exp = self._jwt_exp(best_tok)
            old_prefix = self._access_token[:12] if self._access_token else "(none)"
            self._access_token = best_tok
            log.warning(
                "Dhan access token reconciled from source",
                source=best_src,
                old_expiry=int(old_exp),
                new_expiry=int(new_exp),
                old_prefix=old_prefix,
            )

    @staticmethod
    def _parse_token_from_env(path: str) -> Optional[str]:
        """Extract DHAN_ACCESS_TOKEN from a shell-style env file."""
        try:
            with open(path) as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    if k.strip() == "DHAN_ACCESS_TOKEN":
                        v = v.strip()
                        if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                            v = v[1:-1]
                        return v if v else None
        except Exception:
            return None
        return None

    def _headers(self) -> dict:
        self._maybe_reload_token()
        return {
            "access-token": self._access_token,
            "Content-Type": "application/json",
            "client-id": self._client_id,
        }

    async def get_balance(self) -> BalanceSnapshot:
        # Dhan v2 fundlimit is a GET with NO body (the official DhanHQ-py SDK
        # calls self.dhan_http.get("/fundlimit")). A POST with an empty body
        # returns DH-905 "Missing required fields" — this is the root cause of
        # /balance and the arm-gate silently failing.
        url = f"{DHAN_BASE}/fundlimit"
        resp = await self._http.get(url, headers=self._headers())
        # 401 Invalid_Authentication: the token we hold is dead. Force a
        # full re-scan of every source (env + disk) and retry ONCE. This is
        # what recovers a container that started with a stale env token:
        # the freshest disk token now takes over without a restart.
        if resp.status_code == 401:
            self._maybe_reload_token(force=True)
            resp = await self._http.get(url, headers=self._headers())
        if resp.status_code != 200:
            raise RuntimeError(f"Dhan balance failed: {resp.status_code} {resp.text}")
        data = resp.json()
        # Dhan fundlimit fields (real API shape):
        #   availabelBalance (note: Dhan's actual spelling) — cash available to trade
        #   sodLimit         — start-of-day balance (true account size = free + used)
        #   utilizedAmount   — margin locked by open positions
        free = float(data.get("availabelBalance", data.get("availableBalance", 0)))
        used = float(data.get("utilizedAmount", data.get("usedMargin", 0)))
        total = float(data.get("sodLimit", free + used))
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
        exchange_segment: str = "NSE_FNO",
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
    YAHOO_INDEX = {"NIFTY": "%5ENSEI", "SENSEX": "%5EBSESN", "BANKNIFTY": "%5ENSEI"}

    # Per-symbol failure state so we stop hammering a dead Dhan endpoint.
    # Instance-level, NOT class-level: a class attribute would be shared
    # across every DhanClient instance, so one failing token would silently
    # disable the poll for everyone. Initialized in __init__ below.
    _INDEX_POLL_RETRY_S = 300.0  # re-try the Dhan poll every 5 min

    async def get_index_ltp(self, symbol: str = "NSE") -> Optional[float]:
        """Fetch the index spot price for NIFTY/SENSEX/BANKNIFTY.

        Primary: Dhan live-quote poll (free). Fallback: the FREE Yahoo v8
        chart API (the same source the SLHUNT plugins use for 5m bars) —
        never a chargeable Dhan historical endpoint.

        If the Dhan poll keeps failing (e.g. /market/quote 404s for this
        token), it is disabled for 5 minutes and the Yahoo source is used
        directly, so we don't log a 404 storm on every tick.
        """
        now = time.time()
        last_fail = self._index_poll_disabled.get(symbol, 0.0)
        if now - last_fail > self._INDEX_POLL_RETRY_S:
            dhan_val = await self._dhan_index_ltp(symbol)
            if dhan_val is not None:
                self._index_poll_disabled.pop(symbol, None)
                return dhan_val
            self._index_poll_disabled[symbol] = now
            log.warning(
                "Dhan index poll failing — using Yahoo source for 5 min",
                symbol=symbol,
            )
        return await self._yahoo_index_ltp(symbol)

    async def _dhan_index_ltp(self, symbol: str) -> Optional[float]:
        """Dhan /v2/marketfeed/ltp index poll (free, not historical data).

        Dhan v2 index security IDs (plain integers, segment IDX_I):
          NIFTY     → IDX_I, sid 13
          SENSEX    → IDX_I, sid 51
          BANKNIFTY → IDX_I, sid 25

        The old v1 API used 'mid3101'/'mid3040' with segment 'INDEX'.
        """
        index_ids = {
            "NIFTY": 13,
            "SENSEX": 51,
            "BANKNIFTY": 25,
        }
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
            url = f"{DHAN_BASE}/marketfeed/ltp"
            payload = {"IDX_I": [sid]}
            resp = await self._http.post(
                url, json=payload,
                headers=self._headers(),
            )
            if resp.status_code == 200:
                body = resp.json()
                data = body.get("data", {})
                for seg_key, items in data.items():
                    if isinstance(items, dict):
                        for _sid, quote in items.items():
                            lp = quote.get("last_price")
                            if lp:
                                return float(lp)
                log.debug("Dhan index LTP empty response", symbol=symbol,
                          status=resp.status_code, body=str(body)[:200])
            else:
                log.debug("Dhan index LTP HTTP error", symbol=symbol,
                          status=resp.status_code, body=str(resp.text)[:200])
        except Exception as e:
            log.debug("Dhan index LTP poll failed", symbol=symbol, error=str(e))
        return None

    # Rate-limit Yahoo index polls: max 1 call per 30s per symbol
    # (instance-level state, initialized in __init__).

    async def _yahoo_index_ltp(self, symbol: str) -> Optional[float]:
        """Free Yahoo v8 chart API — last close as the spot proxy.

        Rate-limited to 1 call per 30s per symbol to avoid hammering Yahoo.
        """
        import json as _json
        import urllib.request
        code = symbol.upper().replace("_", "")
        # Rate-limit check
        cached_entry = self._yahoo_ltp_cache.get(symbol)
        if cached_entry and (time.time() - cached_entry[0]) < self._YAHOO_MIN_INTERVAL:
            return cached_entry[1]
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
            meta = result.get("meta") or {}
            close = (result.get("indicators", {}).get("quote", [{}])[0].get("close") or [None])
            last = [v for v in close if v is not None]
            if last:
                val = float(last[-1])
                # Staleness guard: after close / on weekends the last 1m
                # bar can be hours (or days) old. Trust it only when the
                # chart reports a recent lastTradeTime (<= 300s), or when
                # the market is open and the series has data at all.
                import datetime as _dt
                last_trade_ts = meta.get("lastTradeTime") or meta.get("regularMarketTime")
                if last_trade_ts:
                    age = time.time() - int(last_trade_ts)
                    if age > 300:
                        log.debug("Yahoo index LTP stale", symbol=symbol, age_s=round(age, 1))
                        return None
                self._yahoo_ltp_cache[symbol] = (time.time(), val)
                log.debug("Yahoo index LTP resolved", symbol=symbol, ltp=val,
                          last_trade_ts=last_trade_ts)
                return val
            log.debug("Yahoo index LTP empty", symbol=symbol)
            return None
        except Exception as e:
            log.warning("Yahoo index LTP poll failed", symbol=symbol, error=str(e))
        return None

    async def get_option_ltp(self, symbol: str, security_id: str = "",
                             segment: str = "", expiry: str = "",
                             strike: float = 0.0, option_type: str = "") -> Optional[float]:
        """Fetch latest LTP for an option contract via Dhan.

        Primary: Dhan /market/quote (free, requires security_id).
        Fallback: Dhan /option-chain (free) to resolve the security_id
        from (segment, expiry, strike, CE/PE) when security_id is empty.

        SENSEX options live on BSE_FNO; NIFTY/BANKNIFTY on NSE_FNO.
        """
        # If no security_id, try to resolve it from the option chain.
        if not security_id and segment and strike and option_type:
            security_id = await self._resolve_option_security_id(
                segment, expiry, strike, option_type
            )
        if not security_id:
            return None
        try:
            # Dhan v2 live-quote endpoint (free, not historical data).
            url = f"{DHAN_BASE}/market/quote"
            # Normalize segment: Upstox BSE_FO → Dhan BSE_FNO, NSE_FO → NSE_FNO
            dhan_seg = segment.replace("_FO", "_FNO") if segment else "NSE_FNO"
            payload = {"securityId": str(security_id), "exchangeSegment": dhan_seg}
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

    async def _resolve_option_security_id(self, segment: str, expiry: str,
                                          strike: float, option_type: str) -> Optional[str]:
        """Resolve a Dhan security_id from the free option-chain endpoint.

        SENSEX options live on BSE_FNO; NIFTY/BANKNIFTY on NSE_FNO.
        The option chain returns a list of contracts with securityId,
        strikePrice, expiry, and contract type (CE/PE).

        Matches the REQUESTED expiry exactly (falling back to the nearest
        available expiry) — the old code ignored the expiry argument and
        returned the first matching strike across all expiries, which
        resolved to the wrong (usually nearest-week) contract for DTE-5
        or monthly legs.
        """
        dhan_seg = segment.replace("_FO", "_FNO") if segment else "NSE_FNO"
        opt_type = option_type.upper()
        target_expiry = (expiry or "").strip()
        try:
            url = f"{DHAN_BASE}/option-chain"
            # Dhan option chain: exchangeSegment, underSecurityId, expiry
            # For SENSEX: underSecurityId=31 (BSE index), segment=BSE_FNO
            # For NIFTY:   underSecurityId=13 (NSE index), segment=NSE_FNO
            underlying_ids = {"NSE_FNO": 13, "BSE_FNO": 31}
            under_id = underlying_ids.get(dhan_seg, 13)
            resp = await self._http.post(
                url,
                json={"exchangeSegment": dhan_seg, "underSecurityId": under_id},
                headers=self._headers(),
            )
            if resp.status_code == 200:
                data = resp.json()
                inner = data.get("data", {})
                ce_pe_key = "CE" if opt_type == "CE" else "PE"
                if target_expiry:
                    # Exact-expiry match first.
                    for opt in inner.get(target_expiry, {}).get("data", {}).get(ce_pe_key, []) or []:
                        if abs(float(opt.get("strikePrice", 0)) - strike) < 0.01:
                            sec_id = str(opt.get("securityId", ""))
                            if sec_id:
                                log.debug("Dhan option chain resolved security_id",
                                          segment=dhan_seg, strike=strike,
                                          opt=opt_type, expiry=target_expiry,
                                          security_id=sec_id)
                                return sec_id
                    # No exact expiry in the chain — fall back to the nearest
                    # expiry that carries this strike (logged, not silent).
                    import datetime as _dt
                    try:
                        target_dt = _dt.date.fromisoformat(target_expiry)
                    except ValueError:
                        target_dt = None
                    best: Optional[tuple] = None
                    for expiry_key, expiry_data in inner.items():
                        if not isinstance(expiry_data, dict):
                            continue
                        for opt in expiry_data.get("data", {}).get(ce_pe_key, []) or []:
                            if abs(float(opt.get("strikePrice", 0)) - strike) < 0.01:
                                sec_id = str(opt.get("securityId", ""))
                                if not sec_id:
                                    continue
                                try:
                                    delta = abs((_dt.date.fromisoformat(expiry_key) - target_dt).days) if target_dt else 0
                                except Exception:
                                    delta = 10_000
                                if best is None or delta < best[0]:
                                    best = (delta, sec_id, expiry_key)
                    if best:
                        log.warning("Dhan option chain: requested expiry not found; using nearest",
                                    segment=dhan_seg, requested=expiry,
                                    used=best[2], security_id=best[1])
                        return best[1]
                else:
                    # No expiry requested: keep the old behavior (first match).
                    for expiry_key, expiry_data in inner.items():
                        if not isinstance(expiry_data, dict):
                            continue
                        for opt in expiry_data.get("data", {}).get(ce_pe_key, []) or []:
                            if abs(float(opt.get("strikePrice", 0)) - strike) < 0.01:
                                sec_id = str(opt.get("securityId", ""))
                                if sec_id:
                                    log.debug("Dhan option chain resolved security_id",
                                              segment=dhan_seg, strike=strike,
                                              opt=opt_type, security_id=sec_id)
                                    return sec_id
        except Exception as e:
            log.debug("Dhan option chain resolve failed", error=str(e))
        return None

    async def get_order_status(self, broker_order_id: str) -> Dict[str, Any]:
        # Dhan v2: order history is a POST to /orders/history with the
        # dhanClientId; the v1-style GET /orderbook/trade/{id} no longer
        # exists and 404s. (v2 has no per-txnId lookup — we request the
        # day's history and pick our txn id out of it.)
        url = f"{DHAN_BASE}/orders/history"
        payload = {"dhanClientId": self._client_id}
        resp = await self._http.post(url, json=payload, headers=self._headers())
        if resp.status_code != 200:
            return {"status": "UNKNOWN", "message": resp.text}
        data = resp.json()
        orders = data.get("data", {}).get("orders", [])
        for o in orders or []:
            if str(o.get("txnId") or o.get("orderId")) == str(broker_order_id):
                state = str(o.get("state") or o.get("status") or "").upper()
                # Dhan v2 states: OPEN, CLOSED, CANCELLED, REJECTED, ...
                return {"status": state or "OPEN", "raw": o}
        return {"status": "UNKNOWN", "message": "txn id not in today's history", "raw": data}

    async def cancel_order(self, broker_order_id: str) -> Dict[str, Any]:
        # Dhan v2 cancel endpoint (v1 /orderbook/trade/cancel is gone).
        url = f"{DHAN_BASE}/orders/cancel"
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