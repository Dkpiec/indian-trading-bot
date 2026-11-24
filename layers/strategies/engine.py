"""Strategy execution engine — the missing piece.

Wires armed subscriptions → strategy plugins → market data → order execution.

Data sources:
  1. Upstox WS V3 (primary) — real-time LTP ticks via protobuf stream
  2. Dhan REST polling (fallback) — option chain + LTP when WS is down

Flow:
  _supervisor_loop() tick (1s)
    → query armed subscriptions
    → for each: load plugin, build state, call evaluate_tick()
    → if ENTRY: check balance gate, place Dhan order, create StrategyInstance
    → if EXIT: close position, update P&L, log to TradeOrderLedger
    → update /trades, /profit, /status from live data
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog

log = structlog.get_logger(__name__)

# Rate-limited logging: max 1 log per key per 30s (prevents per-tick spam)
_log_throttle: Dict[str, float] = {}
_LOG_THROTTLE_S = 30.0


def _log_throttled(key: str, level: str, msg: str, **kwargs) -> None:
    now = time.time()
    last = _log_throttle.get(key, 0.0)
    if now - last < _LOG_THROTTLE_S:
        return
    _log_throttle[key] = now
    if level == "warning":
        log.warning(msg, **kwargs)
    elif level == "debug":
        log.debug(msg, **kwargs)
    else:
        log.info(msg, **kwargs)

IST = timezone(timedelta(hours=5, minutes=30))

# ---------------------------------------------------------------------------
# Market Data
# ---------------------------------------------------------------------------


class MarketDataFeed:
    """Aggregated LTP feed: Upstox WS V3 primary, Dhan REST fallback.

    Maintains a dict of instrument_key → latest tick so the engine can
    query LTP for any option contract.
    """

    def __init__(self, *, dhan_client=None, upstox_ws=None):
        self._dhan = dhan_client
        self._upstox_ws = upstox_ws
        # instrument_key → {"ltp": float, "ts": float, "src": str}
        self._ticks: Dict[str, Dict[str, Any]] = {}
        self._ws_running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._last_poll_ts = 0.0
        self._ws_subscribed: set = set()  # keys already subscribed to the WS
        self._last_ws_reconnect_ts = 0.0
        self._ws_unhealthy_ts = 0.0

    async def _maybe_reconnect_ws(self) -> None:
        """Drive a WS reconnect when the feed is running but has gone stale
        (the 'socket is already closed' flap). Reconnect at most once every
        60s and only if the socket was unhealthy for >= 30s, so we don't
        hammer the endpoint on a persistent network outage.

        Also recovers from ws_close/ws_error events: when the Upstox WS
        adapter sets _running=False (close/error), _ws_running here still
        says the feed is up — we re-issue start() to get a fresh socket +
        subscription, which restores the WS as the primary LTP source
        within one tick cycle (~5s) instead of falling back to Dhan."""
        ws = self._upstox_ws
        if not ws:
            return
        # Case 1: the WS adapter itself reports it is no longer running
        # (socket closed or errored out). Re-start immediately.
        if self._ws_running and not getattr(ws, "_running", False):
            log.warning("Upstox WS adapter stopped — restarting feed",
                        instruments=len(self._ws_subscribed))
            self._ws_running = False
            self._ws_unhealthy_ts = 0.0
            await self.start(list(self._ws_subscribed))
            return
        if not self._ws_running:
            return
        if getattr(ws, "healthy", None) and ws.healthy():
            self._ws_unhealthy_ts = 0.0
            return
        # Feed is running but unhealthy. Wait 30s of continuous unhealthiness
        # before reconnecting, and space reconnects by >= 60s.
        now = time.time()
        if not self._ws_unhealthy_ts:
            self._ws_unhealthy_ts = now
            return
        if now - self._ws_unhealthy_ts < 30.0:
            return
        if now - self._last_ws_reconnect_ts < 60.0:
            return
        log.warning("Upstox WS stale (no ticks >= 45s) — reconnecting",
                    last_tick_age=(now - getattr(ws, "_last_tick_ts", 0)) or "never")
        self._last_ws_reconnect_ts = now
        try:
            await ws.reconnect(self._ws_subscribed or list(self._ws_subscribed))
            self._ws_unhealthy_ts = 0.0
            log.info("Upstox WS reconnect issued", instruments=len(self._ws_subscribed))
        except Exception as e:
            log.error("Upstox WS reconnect error", error=str(e))

    async def start(self, instrument_keys: List[str]) -> None:
        """Start the Upstox WS feed (or subscribe new keys if already running).

        Idempotent: on a second call with the WS already running, only the
        NEW keys are subscribed. A no-key call is a no-op when the WS is
        already running (nothing new to add).
        """
        if not self._upstox_ws:
            if not self._ws_running:
                log.info("WS feed disabled — all LTP via Dhan poll")
            return
        new_keys = [k for k in instrument_keys if k and k not in self._ws_subscribed]
        # Also subscribe the index LTP keys so the WS is the primary source
        # for BOTH option and index LTPs (the option auto-resolver reads
        # index LTPs from this same feed).
        index_keys = [
            "NSE_INDEX|NIFTY", "BSE_INDEX|SENSEX", "NSE_INDEX|BANKNIFTY",
        ]
        new_keys.extend(k for k in index_keys if k not in self._ws_subscribed and k not in instrument_keys)
        if self._ws_running:
            if new_keys:
                try:
                    await self._upstox_ws.subscribe(new_keys, mode="ltpc")
                    self._ws_subscribed.update(new_keys)
                    log.info("Upstox WS: subscribed new instruments",
                             count=len(new_keys), keys=new_keys[:5])
                except Exception as e:
                    log.warning("Upstox WS subscribe failed", error=str(e))
            return
        # First start
        all_keys = list(instrument_keys) + [k for k in index_keys if k not in instrument_keys]
        if not all_keys:
            log.info("WS feed has no instrument keys — all LTP via Dhan poll")
            return
        try:
            await self._upstox_ws.start(all_keys, mode="ltpc")
            self._ws_running = True
            self._ws_subscribed.update(all_keys)
            self._ws_task = asyncio.create_task(self._ws_consume())
            log.info("Upstox WS started", instruments=len(all_keys),
                     sample=all_keys[:4])
        except Exception as e:
            log.warning("Upstox WS failed to start, using Dhan fallback", error=str(e))
            self._ws_running = False

    async def stop(self) -> None:
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._upstox_ws:
            await self._upstox_ws.stop()
        self._ws_running = False

    async def _ws_consume(self) -> None:
        """Consume ticks from the Upstox WS queue into _ticks.

        Each tick is a dict shaped like:
            {type, instrument_key, last_price, change_percent, ts}
        produced by UpstoxWSV3._on_message.

        Index ticks (NSE_INDEX|NIFTY, BSE_INDEX|SENSEX, ...) are also
        routed into _index_ltp_cache so the option auto-resolver has a
        spot to compute strikes — this makes the WS the primary source
        for BOTH option LTPs and index LTPs, as intended.
        """
        # Map Upstox index instrument keys -> underlying code for the cache.
        index_key_map = {
            "NSE_INDEX|NIFTY": "NIFTY",
            "BSE_INDEX|SENSEX": "SENSEX",
            "NSE_INDEX|BANKNIFTY": "BANKNIFTY",
        }
        try:
            async for tick in self._upstox_ws.ticks():
                ttype = tick.get("type", "tick")
                if ttype in ("ws_close", "ws_error"):
                    # The socket closed or errored. The UpstoxWSV3 adapter
                    # sets _running=False in its close/error callback. The
                    # next _maybe_reconnect_ws() call (every tick cycle)
                    # sees the adapter stopped and re-issues start() for a
                    # fresh socket + re-subscription — keeping the WS as the
                    # primary LTP source instead of parking on the Dhan poll.
                    log.warning("Upstox WS closed/error — restart on next tick",
                                code=tick.get("code"), error=tick.get("error"))
                    self._ws_running = False
                    self._ws_unhealthy_ts = 0.0
                    continue
                if ttype == "ws_open":
                    continue
                key = tick.get("instrument_key", "")
                ltp = tick.get("last_price")
                if key and ltp:
                    self._ticks[key] = {
                        "ltp": float(ltp),
                        "ts": tick.get("ts", time.time()),
                        "src": "upstox_ws",
                    }
                    # Index tick → populate the index LTP cache for the
                    # option auto-resolver (WS is now the primary index source).
                    underlying_code = index_key_map.get(key)
                    if underlying_code:
                        _index_ltp_cache[underlying_code] = (float(ltp), time.time())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Upstox WS consume error", error=str(e))

    async def get_ltp(self, instrument_key: str, security_id: str = "", symbol: str = "",
                      segment: str = "", expiry: str = "", strike: float = 0.0,
                      option_type: str = "") -> Optional[float]:
        """Get latest LTP for an instrument. Tries WS cache, then Dhan poll.

        When the WS is running and the key is subscribed, wait up to ~3s for
        the first tick before falling back to Dhan (avoids the chargeable
        option-chain call on cold start).
        """
        # 1. Check WS cache (fresh tick < 5s old)
        cached = self._ticks.get(instrument_key)
        if cached and (time.time() - cached["ts"]) < 5.0:
            return cached["ltp"]

        # 1b. WS is running and this key is subscribed — give the feed a
        #     short grace period for the first tick (cold start / re-sub).
        if self._ws_running and instrument_key in getattr(self, "_ws_subscribed", set()):
            for _ in range(6):
                await asyncio.sleep(0.5)
                cached = self._ticks.get(instrument_key)
                if cached and (time.time() - cached["ts"]) < 5.0:
                    return cached["ltp"]
            # Still no tick — fall through to Dhan (the key is valid but the
            # feed may be slow or the instrument delisted).

        # 2. Dhan fallback: poll quotes for the security_id
        if self._dhan:
            try:
                if security_id:
                    ltp = await self._dhan.get_option_ltp(
                        symbol or security_id, security_id=security_id,
                        segment=segment, expiry=expiry, strike=strike,
                        option_type=option_type,
                    )
                    if ltp is not None:
                        self._ticks[instrument_key] = {"ltp": ltp, "ts": time.time(), "src": "dhan"}
                        return ltp
                elif segment and strike and option_type:
                    # No security_id but we have the contract specs — let
                    # Dhan resolve the security_id from the option chain
                    # (free endpoint) and fetch the LTP.
                    ltp = await self._dhan.get_option_ltp(
                        symbol or instrument_key, security_id="",
                        segment=segment, expiry=expiry, strike=strike,
                        option_type=option_type,
                    )
                    if ltp is not None:
                        self._ticks[instrument_key] = {"ltp": ltp, "ts": time.time(), "src": "dhan"}
                        return ltp
                else:
                    # No contract resolved (arming not done): use the index spot
                    # so breakout/breakdown strategies can still evaluate.
                    spot = await self._dhan.get_index_ltp(symbol)
                    if spot is not None:
                        self._ticks[instrument_key] = {"ltp": spot, "ts": time.time(), "src": "dhan_spot"}
                        # Cache the index LTP so the option auto-resolver can
                        # compute the OTM strike for this underlying.
                        if symbol:
                            global _index_ltp_cache
                            _index_ltp_cache[symbol.upper()] = (float(spot), time.time())
                        return spot
                    _log_throttled(
                        f"no_ltp:{instrument_key}", "debug",
                        "No LTP available (contract not armed, index poll failed)",
                        instrument_key=instrument_key, symbol=symbol)
            except Exception as e:
                log.debug("Dhan LTP poll failed", security_id=security_id, error=str(e))

        # 3. Stale cache is better than nothing
        if cached:
            return cached["ltp"]
        return None

    @property
    def source(self) -> str:
        """Current primary data source label."""
        if self._ws_running:
            return "upstox_ws"
        if self._dhan:
            return "dhan_poll"
        return "none"


# ---------------------------------------------------------------------------
# Instrument Resolution
# ---------------------------------------------------------------------------


# Upstox instruments JSON (cached). Keys are always "SEGMENT|numeric_id"
# (e.g. NSE_FO|49492, NSE_INDEX|399001) — see the Upstox V3 docs: the REST
# /v3/market-quote/quotes endpoint and the WS v3 feed both use this exact
# format. A formatted string like "NSE_FNO|25000_25DEC26_CE_NIFTY" is NOT a
# valid Upstox key and the WS server silently drops such subscriptions.
_UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
_instruments_index: Dict[tuple, str] = {}
_instruments_loaded_at: float = 0.0
# Secondary index: (segment, underlying, option_type) -> list of (expiry_iso, strike, instrument_key)
# Used by the auto-resolver to pick the nearest real OTM contract when the
# computed expiry/strike isn't in the primary index.
_instruments_by_underlying: Dict[tuple, list] = {}
# Cached auto-resolved option keys: (segment, underlying, option_type) -> instrument_key
_auto_key_cache: Dict[tuple, str] = {}

_index_ltp_cache: Dict[str, tuple] = {}

# Process-local cache of Dhan option-chain security_id resolutions,
# keyed by (segment, expiry, strike, option_type). Populated by the
# engine's async resolver when the arm script has not stored the id.
_security_id_cache: Dict[tuple, str] = {}


def _strike_interval(underlying: str) -> float:
    """Option strike spacing per underlying (NIFTY 50, SENSEX 100, BANKNIFTY 100)."""
    u = (underlying or "").upper()
    if u.startswith("SENSEX"):
        return 100.0
    if u.startswith("BANKNIFTY") or u.startswith("BANK"):
        return 100.0
    return 50.0


def _next_weekly_expiry(underlying: str, dte_days: int = 5) -> str:
    """Compute the next weekly expiry ISO date.

    NSE (NIFTY/BANKNIFTY) options expire Thursday; BSE (SENSEX) expire Friday.
    For a 'dte 5' strategy we want the expiry ~5 days out on the next weekly
    cycle.
    """
    from datetime import date, timedelta
    today = date.today()
    u = (underlying or "NIFTY").upper()
    target_weekday = 4 if u.startswith("SENSEX") else 3  # BSE=Fri, NSE=Thu
    offset = max(dte_days - 5, 0)
    target = today + timedelta(days=offset)
    while target.weekday() != target_weekday:
        target += timedelta(days=1)
    return target.isoformat()


def _find_nearest_option_key(
    segment: str, underlying: str, option_type: str,
    target_strike: float, target_expiry: str,
) -> Optional[str]:
    """Pick the real 'SEG|numeric_id' option contract closest to
    (target_strike, target_expiry) from the instruments JSON.

    Tries the exact (segment, expiry, strike, opt_type) first; on a miss it
    scans the same underlying+segment for the strike nearest target_strike
    (prefer expiry within 2 days of target, else the nearest expiry that has
    that strike). Returns None when the underlying has no options at all
    (e.g. SENSEX, which has no Upstox options contract).
    """
    global _instruments_index
    opt = option_type.upper()
    u = (underlying or "").upper()
    # 1. Exact hit.
    exact = _instruments_index.get((segment, target_expiry, float(target_strike), opt))
    if exact:
        return exact
    # 2. Nearest strike in the same underlying + segment.
    cands = _instruments_by_underlying.get((segment, u, opt), [])
    if not cands:
        return None
    # Prefer strikes within 2 days of target_expiry, else all.
    import datetime as _dt
    target_date = _dt.date.fromisoformat(target_expiry)
    near = [
        (exp, st, k) for (exp, st, k) in cands
        if abs((_dt.date.fromisoformat(exp) - target_date).days) <= 2
    ]
    pool = near or cands
    # Among the pool, pick the strike closest to target_strike; tie-break to
    # the most OTM side (CE -> strike >= spot, PE -> strike <= spot).
    def _score(triple):
        exp, st, k = triple
        st = float(st)
        if opt == "PE":
            # Prefer strikes <= target; penalty grows as we go OTM (below).
            diff = target_strike - st
            return (abs(diff), st)
        # CE: prefer strikes >= target; penalty grows as we go ITM (above).
        diff = st - target_strike
        return (abs(diff), st)
    pool.sort(key=lambda t: _score(t))
    return pool[0][2]


def _compute_option_strike(index_ltp: float, underlying: str, option_type: str) -> int:
    """Nearest OTM option strike to the index LTP.

    CE: strike at/just above spot (first OTM call).
    PE: strike at/just below spot (first OTM put).
    """
    step = _strike_interval(underlying)
    spot = float(index_ltp)
    atm = int(round(spot / step)) * step
    opt = option_type.upper()
    if opt == "PE":
        # First OTM put: strike below spot.
        return int(atm if atm < spot else atm - step)
    # CE: first OTM call: strike above spot.
    return int(atm if atm > spot else atm + step)


def _resolve_option_key(
    underlying: str, segment: str, option_type: str,
    strike: float, expiry: str, index_ltp: Optional[float] = None,
) -> str:
    """Resolve an Upstox option instrument key.

    If strike/expiry are missing, compute them from the index LTP, then look
    up the real "SEG|numeric_id". If the exact (expiry, strike) isn't in the
    index, fall back to the nearest real OTM contract for the same
    underlying+segment+option_type. Returns a readable label only when the
    underlying has no Upstox options at all (e.g. SENSEX).
    """
    u = (underlying or "NIFTY").upper()
    if not strike or strike <= 0:
        strike = _compute_option_strike(index_ltp, u, option_type) if index_ltp else 0
    if not expiry:
        expiry = _next_weekly_expiry(u, 5)
    # 1. Try the exact key, then the nearest real contract.
    resolved = _find_nearest_option_key(segment, u, option_type, strike, expiry)
    if resolved:
        return resolved
    # 2. No options for this underlying on this segment — readable label.
    log.warning("No Upstox option found for underlying — key will be dropped by WS",
                underlying=u, segment=segment, option_type=option_type,
                strike=strike, expiry=expiry)
    return f"{segment}|{int(strike)}_{u}_{option_type.upper()}"


def _load_upstox_instruments(force: bool = False) -> bool:
    """Fetch + index the Upstox complete instruments JSON (gzip, ~60MB).

    The index maps (segment, expiry, strike, option_type) -> instrument_key
    (the full "SEGMENT|exchange_token" string, e.g. "NSE_FO|12345").
    Cached for 1 hour — the file refreshes daily at ~6 AM IST.
    """
    global _instruments_index, _instruments_loaded_at, _instruments_by_underlying
    import time as _time
    now = _time.time()
    if not force and _instruments_index and (now - _instruments_loaded_at) < 3600:
        return True
    try:
        import urllib.request, gzip
        req = urllib.request.Request(
            _UPSTOX_INSTRUMENTS_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Referer": "https://upstox.com/",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(gzip.decompress(resp.read()).decode())
        _instruments_loaded_at = now
        _instruments_index.clear()
        _instruments_by_underlying.clear()
        from datetime import datetime as _dt, timezone as _tz
        for entry in data:
            seg = entry.get("segment", "")
            if seg not in ("NSE_FO", "BSE_FO"):
                continue
            instrument_key = entry.get("instrument_key", "")  # e.g. NSE_FO|12345
            # expiry is epoch MILLISECONDS in the JSON — convert to ISO date (UTC).
            expiry_ms = entry.get("expiry", 0)
            if not expiry_ms:
                continue
            expiry_iso = _dt.fromtimestamp(expiry_ms / 1000, tz=_tz.utc).strftime("%Y-%m-%d")
            strike = entry.get("strike_price", 0.0)
            opt_type = str(entry.get("instrument_type", "")).upper()  # CE / PE / FUT
            underlying = str(entry.get("asset_symbol", "")).upper()
            if not instrument_key or opt_type not in ("CE", "PE"):
                continue
            _instruments_index[(seg, expiry_iso, float(strike or 0), opt_type)] = instrument_key
            # Secondary index for the nearest-option fallback.
            _instruments_by_underlying.setdefault((seg, underlying, opt_type), []).append(
                (expiry_iso, float(strike or 0), instrument_key)
            )
        log.info("Upstox instruments indexed", count=len(_instruments_index))
        return True
    except Exception as e:
        log.warning("Upstox instruments fetch failed — using stale index", error=str(e))
        return len(_instruments_index) > 0


def resolve_instrument_key(
    underlying: str,
    strike: float,
    expiry: str,
    option_type: str,
    segment: str = "NSE_FO",
) -> str:
    """Resolve a real Upstox instrument key: SEGMENT|numeric_id.

    Looks up the instrument in the Upstox instruments JSON (cached 5 min).
    The Upstox WS V3 feed and the REST /v3/market-quote/quotes endpoint both
    use the "SEGMENT|numeric_id" format (e.g. NSE_FO|49492). SENSEX options
    live on BSE_FO.

    Falls back to a clearly-marked readable label when the instrument is not
    in the JSON — that label is valid for Dhan-facing code but NOT for an
    Upstox WS subscription (the server will drop it).
    """
    if not _load_upstox_instruments():
        return f"{segment}|{int(strike)}_{underlying}_{option_type.upper()}"  # label fallback
    opt_type = option_type.upper()
    from datetime import datetime as _dt, timezone as _tz
    # Normalize expiry to ISO date (params may pass ISO or epoch ms).
    exp_raw = expiry
    if isinstance(exp_raw, (int, float)) and exp_raw > 1e12:
        expiry_iso = _dt.fromtimestamp(exp_raw / 1000, tz=_tz.utc).strftime("%Y-%m-%d")
    elif isinstance(exp_raw, str) and exp_raw.isdigit() and len(exp_raw) > 10:
        expiry_iso = _dt.fromtimestamp(int(exp_raw) / 1000, tz=_tz.utc).strftime("%Y-%m-%d")
    else:
        expiry_iso = str(exp_raw)  # already ISO or short date
    resolved = _instruments_index.get((segment, expiry_iso, float(strike), opt_type))
    if resolved:
        return resolved
    log.warning(
        "Upstox instrument not in JSON — WS subscription will be dropped",
        segment=segment, expiry=expiry_iso, strike=strike, opt_type=opt_type,
    )
    return f"{segment}|{int(strike)}_{underlying}_{opt_type}"


def _format_expiry(date_str: str) -> str:
    """Convert '2026-12-25' → '25DEC26' for Upstox instrument keys."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return d.strftime("%d%b%y").upper()
    except (ValueError, TypeError):
        return date_str


# ---------------------------------------------------------------------------
# Execution Engine
# ---------------------------------------------------------------------------


class ExecutionEngine:
    """Core strategy execution loop.

    Manages the full lifecycle:
      armed subscription → plugin load → market data → evaluate_tick()
      → order placement → position tracking → P&L → /trades /profit
    """

    def __init__(
        self,
        session_factory,
        dhan_client=None,
        upstox_ws=None,
        balance_floor_pct: float = 0.20,
    ):
        self._session_factory = session_factory
        self._dhan = dhan_client
        self._upstox_ws = upstox_ws
        self._floor_pct = balance_floor_pct

        self._feed = MarketDataFeed(dhan_client=dhan_client, upstox_ws=upstox_ws)
        self._plugins: Dict[str, Any] = {}  # instance_id → StrategyPlugin
        self._running = False
        self._last_tick_log = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        # Pre-fetch the Upstox instruments JSON so resolve_instrument_key()
        # can map (segment, expiry, strike, opt_type) -> "SEG|numeric_id"
        # before we collect armed keys. Without this the index is empty and
        # every option key falls back to a readable label the WS server drops.
        # Run in a worker thread: the ~60MB download with a 60s timeout must
        # not block the event loop (which would stall the whole 1s tick).
        await asyncio.to_thread(_load_upstox_instruments)
        # Start the WS feed with the instrument keys for all currently armed
        # subscriptions. New keys are re-subscribed on each tick (the
        # _ws_subscribed set in tick() below), so keys that arm later are
        # picked up within one second.
        keys = await self._collect_armed_keys()
        await self._feed.start(keys)
        log.info("ExecutionEngine started", feed_source=self._feed.source,
                 ws_instruments=len(keys),
                 sample_keys=keys[:4])

    async def stop(self) -> None:
        self._running = False
        await self._feed.stop()
        self._plugins.clear()
        log.info("ExecutionEngine stopped")

    # ------------------------------------------------------------------
    # Main tick
    # ------------------------------------------------------------------

    async def tick(self) -> None:
        """One execution cycle. Called by _supervisor_loop() every second."""
        if not self._running:
            return

        if not self._in_market_hours():
            return

        from layers.state.models import UserSubscription, StrategyCatalog

        # 1. Find all armed subscriptions
        from sqlalchemy import select
        async with self._session_factory() as session:
            result = await session.execute(
                select(UserSubscription).join(
                    StrategyCatalog,
                    UserSubscription.catalog_id == StrategyCatalog.id,
                ).where(
                    UserSubscription.is_armed == True,
                    StrategyCatalog.is_active == True,
                )
            )
            armed_subs = result.scalars().all()

        if not armed_subs:
            return

        # 1b. Drive WS liveness/reconnect: if the WS feed is running but the
        #     socket flapped (the 'socket is already closed' race), force a
        #     fresh streamer + socket so option LTPs flow again. This runs
        #     every tick so a stale feed self-heals within ~60s.
        await self._feed._maybe_reconnect_ws()

        # 2. Process each armed subscription
        for sub in armed_subs:
            try:
                await self._process_subscription(sub)
            except Exception as e:
                log.error("Strategy execution error",
                          catalog_id=sub.catalog_id,
                          error_type=type(e).__name__,
                          error=str(e),
                          traceback=__import__('traceback').format_exc())

    # ------------------------------------------------------------------
    # Per-subscription processing
    # ------------------------------------------------------------------

    async def _process_subscription(self, sub) -> None:
        from layers.state.models import StrategyInstance, StrategyCatalog
        from sqlalchemy import select

        # sub is a UserSubscription; its catalog is joined in the query
        cat = sub  # The sub already has catalog data via the join
        # Actually, sub is a UserSubscription row. We need the StrategyCatalog.
        # Let me re-query the catalog separately since the join returns UserSubscription objects.
        async with self._session_factory() as session:
            result = await session.execute(
                select(StrategyCatalog).where(StrategyCatalog.id == sub.catalog_id)
            )
            cat = result.scalar_one_or_none()
            if not cat:
                return

        uid = sub.user_id

        # Re-subscribe this key to the WS feed if it's new (armed after
        # engine start). feed.start() is idempotent: it only subscribes
        # keys not already in _ws_subscribed.
        key = self._get_instrument_key(cat)
        if key not in getattr(self._feed, "_ws_subscribed", set()):
            await self._feed.start([key])

        # Load or create the strategy instance
        instance_id = f"{uid.hex[:8]}_{cat.id}"
        plugin = self._get_plugin(instance_id, cat)
        if not plugin:
            log.warning("Failed to load plugin", catalog_id=cat.id)
            return

        # Build state for evaluate_tick
        state = await self._build_state(instance_id, uid, cat)

        # Get LTP
        symbol = self._get_symbol(cat)
        security_id = self._get_security_id(cat)
        underlying = (cat.underlying or "NIFTY").upper()
        instrument_key = self._get_instrument_key(cat)
        ltp = await self._feed.get_ltp(instrument_key, security_id=security_id, symbol=underlying)
        if ltp is None:
            return

        # Call the strategy plugin
        signal = await plugin.evaluate_tick(ltp, state)
        if signal is None:
            return

        action = signal.get("action", "")
        log.info(
            "Strategy signal",
            catalog_id=cat.id,
            action=action,
            side=signal.get("side"),
            qty=signal.get("qty"),
            ltp=ltp,
        )

        if action == "ENTRY":
            await self._execute_entry(plugin, instance_id, uid, cat, signal, ltp)
        elif action == "EXIT":
            await self._execute_exit(plugin, instance_id, uid, cat, signal, ltp)

    # ------------------------------------------------------------------
    # State building
    # ------------------------------------------------------------------

    async def _build_state(
        self, instance_id: str, uid, cat
    ) -> Dict[str, Any]:
        """Build the state dict passed to evaluate_tick()."""
        from layers.state.models import StrategyInstance, TradeOrderLedger
        from sqlalchemy import select

        state: Dict[str, Any] = {
            "net_qty": 0.0,
            "entry_price": 0.0,
            "unrealized_pnl": 0.0,
            "spot_ltp": 0.0,
            "trade_taken_today": False,
            "entry_window_open": self._entry_window_open(),
        }

        # Live index LTP (the spot proxy for OR-range / sweep logic). The old
        # code left spot_ltp at 0 forever, which silently disabled the ORB
        # v6e midpoint-invalidation exit and the slhunt spot fallback.
        underlying_code = (cat.underlying or "NIFTY").upper()
        spot_cached = _index_ltp_cache.get(underlying_code)
        if spot_cached:
            spot_ltp, spot_ts = spot_cached
            # Use the cached value only while it is fresh (<60s); the index
            # poll refreshes it every second via the Dhan/Yahoo feed.
            if time.time() - spot_ts < 60:
                state["spot_ltp"] = spot_ltp

        async with self._session_factory() as session:
            # Load instance if it exists
            result = await session.execute(
                select(StrategyInstance).where(
                    StrategyInstance.instance_id == instance_id,
                    StrategyInstance.status == "RUNNING",
                )
            )
            inst = result.scalar_one_or_none()
            if inst:
                state["net_qty"] = inst.net_qty
                state["entry_price"] = inst.entry_price or 0.0
                state["unrealized_pnl"] = inst.unrealized_pnl or 0.0

            # Check if a trade was taken today.
            # TradeOrderLedger.created_at is stored in UTC (Postgres
            # timestamptz via asyncpg). Compare against IST midnight converted
            # to UTC — the old code used an IST-aware boundary, so the
            # "one trade per day" guard misfired 00:00-05:30 IST.
            from sqlalchemy import func as _func
            today_start_utc = datetime.now(IST).replace(
                hour=0, minute=0, second=0, microsecond=0
            ).astimezone(timezone.utc)
            result = await session.execute(
                select(_func.count()).where(
                    TradeOrderLedger.instance_id == instance_id,
                    TradeOrderLedger.created_at >= today_start_utc,
                    TradeOrderLedger.status == "FILLED",
                    TradeOrderLedger.side == "BUY",
                )
            )
            state["trade_taken_today"] = (result.scalar() or 0) > 0

        # Auto-enter flag: armed strategies are expected to take entries
        # automatically when a valid signal fires. The catalog plugins
        # (ORB v6e, ORB v2, universal_momentum_straddle) gate their entry
        # branches on this flag — it was never set, making entry unreachable.
        state["auto_enter"] = True
        state["regime_filter_failed"] = False

        # OR range parameters (from catalog default_params)
        if cat.default_params:
            state["or_high"] = cat.default_params.get("or_high", 0)
            state["or_low"] = cat.default_params.get("or_low", 0)
            state["or_mid"] = cat.default_params.get("or_mid", 0)

        return state

    async def _collect_armed_keys(self) -> List[str]:
        """Collect Upstox instrument keys for all armed subscriptions.

        Arming is the is_armed boolean on user_subscriptions (there is no
        status column). Mirrors the join in tick().

        On the first call, fetch index LTPs so _get_instrument_key() can
        auto-resolve option strikes (the index LTP cache starts empty; the
        Dhan poll may already be disabled for this process if it 404'd
        during a prior window).
        """
        from layers.state.models import UserSubscription, StrategyCatalog
        from sqlalchemy import select

        # Prime the index LTP cache for every distinct underlying so that
        # _get_instrument_key() has a spot to compute the OTM strike. This
        # runs only on the first _collect_armed_keys() call (the flag
        # prevents re-fetching on later calls).
        if not getattr(self, "_index_ltp_primed", False):
            self._index_ltp_primed = True
            await self._prime_index_ltps()

        keys: List[str] = []
        async with self._session_factory() as session:
            result = await session.execute(
                select(UserSubscription).join(
                    StrategyCatalog,
                    UserSubscription.catalog_id == StrategyCatalog.id,
                ).where(
                    UserSubscription.is_armed == True,   # noqa: E712
                    StrategyCatalog.is_active == True,   # noqa: E712
                )
            )
            subs = result.scalars().all()
            for sub in subs:
                cat_res = await session.execute(
                    select(StrategyCatalog).where(StrategyCatalog.id == sub.catalog_id)
                )
                cat = cat_res.scalar_one_or_none()
                if not cat:
                    continue
                keys.append(self._get_instrument_key(cat))
        return keys

    async def _prime_index_ltps(self) -> None:
        """Fetch index LTPs for all distinct underlyings (NIFTY, SENSEX, ...)
        so the option auto-resolver has a spot to compute strikes.

        Uses the Dhan index poll (free for SENSEX). For NIFTY a free Yahoo
        fallback is used (Yahoo has no SENSEX ticker, so SENSEX relies on
        the Dhan poll). Failures are non-fatal — the strategy will just log
        'no index LTP yet' until the next tick.
        """
        from layers.state.models import StrategyCatalog
        from sqlalchemy import select, distinct

        try:
            async with self._session_factory() as session:
                result = await session.execute(
                    select(distinct(StrategyCatalog.underlying))
                )
                underlyings = [u for (u,) in result.all() if u]
            for u in underlyings:
                spot = await self._fetch_index_ltp(u.upper())
                if spot:
                    _index_ltp_cache[u.upper()] = (float(spot), time.time())
                    log.debug("Index LTP primed for option auto-resolve",
                              underlying=u, ltp=spot)
        except Exception as e:  # noqa: BLE001
            log.debug("Index LTP priming failed", error=str(e))

    async def _fetch_index_ltp(self, underlying: str) -> Optional[float]:
        """Fetch the index LTP for an underlying: Dhan poll first, free
        Yahoo fallback for NIFTY/SENSEX/BANKNIFTY."""
        u = underlying.upper()
        try:
            if self._dhan:
                spot = await self._dhan.get_index_ltp(u)
                if spot:
                    return spot
            # Yahoo fallback (NIFTY=^NSEI, SENSEX=^BSESN, BANKNIFTY=^NSEI).
            if u in ("NIFTY", "SENSEX", "BANKNIFTY") and self._dhan:
                spot = await self._dhan._yahoo_index_ltp(u)
                if spot:
                    return spot
        except Exception as e:  # noqa: BLE001
            log.debug("Index LTP fetch failed", underlying=u, error=str(e))
        return None

    def _entry_window_open(self) -> bool:
        """True only inside the 09:45:05-13:15:00 IST weekday entry window."""
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5:
            return False
        t = now_ist.time()
        from datetime import time as _t
        return _t(9, 45, 5) <= t <= _t(13, 15, 0)

    # ------------------------------------------------------------------
    # Entry execution
    # ------------------------------------------------------------------

    async def _execute_entry(
        self,
        plugin,
        instance_id: str,
        uid,
        cat,
        signal: Dict[str, Any],
        ltp: float,
    ) -> None:
        """Place an entry order and create the StrategyInstance."""
        from layers.state.models import StrategyInstance, TradeOrderLedger
        from sqlalchemy import select

        # Balance gate: keep >= floor_pct of TOTAL EQUITY (free + used) as a
        # margin buffer. The buffer protects against margin-call death
        # spirals, so it is a fraction of total_equity, not of free_balance
        # (free < free*0.2 was unreachable and never blocked anything).
        if self._dhan:
            try:
                balance = await self._dhan.get_balance()
                total_equity = balance.total_equity
                free_balance = balance.free_balance
                floor = total_equity * self._floor_pct
                if total_equity > 0 and free_balance < floor:
                    # Insufficient margin buffer above the floor — block the
                    # entry and let the operator /telegram adjust.
                    log.warning(
                        "Entry blocked: free balance below margin floor",
                        free=round(free_balance, 2),
                        floor=round(floor, 2),
                        total=round(total_equity, 2),
                        floor_pct=self._floor_pct,
                        catalog_id=cat.id,
                    )
                    return
                log.debug(
                    "Balance gate passed",
                    free=free_balance,
                    floor=floor,
                    catalog_id=cat.id,
                )
            except Exception as e:
                # Fail-CLOSE: if we can't read the balance, do NOT place the
                # order. The old code logged 'proceeding' (fail-open) which
                # allowed unvalidated entries.
                log.error(
                    "Balance gate check failed — blocking entry (fail-close)",
                    error=str(e),
                    catalog_id=cat.id,
                )
                return

        # Place the order via Dhan
        symbol = self._get_symbol(cat)
        security_id = self._get_security_id(cat)
        if not security_id:
            # Dhan rejects orders with an empty securityId, so never send
            # one. The id is resolved from the option chain on the next
            # tick once the async resolver lands (or the arm script sets
            # it explicitly via /api/v1/strategies/upsert).
            log.warning(
                "Entry deferred: security_id unresolved",
                catalog_id=cat.id,
                symbol=symbol,
                hint="arm the strategy with security_id + strike + expiry",
            )
            return
        side = signal.get("side", "BUY")
        qty = int(signal.get("qty", 1))

        if not self._dhan:
            log.warning("No Dhan client, cannot place order")
            return

        try:
            segment = self._get_segment(cat)
            order_result = await self._dhan.place_order(
                symbol=symbol,
                security_id=security_id,
                side=side,
                quantity=qty,
                order_type="MARKET",
                product_type="INTRADAY",
                exchange_segment=segment,
            )
            broker_order_id = order_result.get("broker_order_id", "")
            if order_result.get("status") == "REJECTED":
                log.error("Entry order rejected", symbol=symbol, segment=segment,
                          message=order_result.get("message"))
                return
            log.info(
                "Entry order placed",
                symbol=symbol,
                side=side,
                qty=qty,
                order_id=broker_order_id,
                catalog_id=cat.id,
            )
        except Exception as e:
            log.error("Entry order failed", symbol=symbol, error=str(e))
            return

        # Create/update StrategyInstance
        async with self._session_factory() as session:
            result = await session.execute(
                select(StrategyInstance).where(
                    StrategyInstance.instance_id == instance_id
                )
            )
            inst = result.scalar_one_or_none()
            if inst is None:
                inst = StrategyInstance(
                    instance_id=instance_id,
                    user_id=uid,
                    catalog_id=cat.id,
                    status="RUNNING",
                    execution_mode="LIVE",
                    parameters={},
                )
                session.add(inst)

            inst.status = "RUNNING"
            inst.entry_price = ltp
            inst.last_ltp = ltp
            inst.net_qty = qty if side == "BUY" else -qty
            inst.unrealized_pnl = 0.0

            # Log the trade
            ledger = TradeOrderLedger(
                user_id=uid,
                instance_id=instance_id,
                broker="dhan",
                order_id=broker_order_id,
                symbol=symbol,
                side=side,
                quantity=qty,
                price=ltp,
                order_type="MARKET",
                status="FILLED",
                filled_quantity=qty,
                average_price=ltp,
            )
            session.add(ledger)
            await session.commit()

        # Notify via Telegram
        await self._notify(
            uid,
            f"🟢 ENTRY {cat.display_name}\n"
            f"Side: {side} | Qty: {qty}\n"
            f"Price: ₹{ltp:,.2f} | Symbol: {symbol}\n"
            f"Order: {broker_order_id}",
        )

    # ------------------------------------------------------------------
    # Exit execution
    # ------------------------------------------------------------------

    async def _execute_exit(
        self,
        plugin,
        instance_id: str,
        uid,
        cat,
        signal: Dict[str, Any],
        ltp: float,
    ) -> None:
        """Close the position and update P&L."""
        from layers.state.models import StrategyInstance, TradeOrderLedger
        from sqlalchemy import select

        # Full exits: qty resolved from the instance's live net_qty so a 40-unit
        # SENSEX position closes in one order, not 1. Staged (TP1 50%) exits
        # pass an explicit qty and keep the remainder open.
        side = signal.get("side", "SELL")
        qty = int(signal.get("qty", 0))
        if qty <= 0:
            async with self._session_factory() as _pre:
                _r = await _pre.execute(
                    select(StrategyInstance).where(
                        StrategyInstance.instance_id == instance_id,
                        StrategyInstance.status == "RUNNING",
                    )
                )
                _inst = _r.scalar_one_or_none()
                if _inst is not None and _inst.net_qty:
                    qty = abs(int(_inst.net_qty))
        if qty <= 0:
            log.warning("Exit skipped: no position qty resolved", instance_id=instance_id)
            return
        symbol = self._get_symbol(cat)
        security_id = self._get_security_id(cat)
        if not security_id:
            # Cannot close a position without the contract id; the next tick
            # retries once the option-chain resolver has populated it.
            log.warning("Exit deferred: security_id unresolved",
                        instance_id=instance_id, catalog_id=cat.id)
            return

        if not self._dhan:
            log.warning("No Dhan client, cannot place exit order")
            return

        try:
            segment = self._get_segment(cat)
            order_result = await self._dhan.place_order(
                symbol=symbol,
                security_id=security_id,
                side=side,
                quantity=qty,
                order_type="MARKET",
                product_type="INTRADAY",
                exchange_segment=segment,
            )
            broker_order_id = order_result.get("broker_order_id", "")
            if order_result.get("status") == "REJECTED":
                log.error("Exit order rejected", symbol=symbol, segment=segment,
                          message=order_result.get("message"))
                return
            log.info(
                "Exit order placed",
                symbol=symbol,
                side=side,
                qty=qty,
                order_id=broker_order_id,
                catalog_id=cat.id,
            )
        except Exception as e:
            log.error("Exit order failed", symbol=symbol, error=str(e))
            return

        # Update instance P&L
        async with self._session_factory() as session:
            result = await session.execute(
                select(StrategyInstance).where(
                    StrategyInstance.instance_id == instance_id
                )
            )
            inst = result.scalar_one_or_none()
            if inst:
                entry_px = inst.entry_price or 0.0
                if entry_px > 0:
                    if side == "SELL":
                        pnl = (ltp - entry_px) * qty
                    else:
                        pnl = (entry_px - ltp) * qty
                    inst.realized_pnl = (inst.realized_pnl or 0.0) + pnl
                    remaining = abs(inst.net_qty or 0.0) - qty
                    if remaining <= 0:
                        # Full close: reset the instance
                        inst.net_qty = 0.0
                        inst.unrealized_pnl = 0.0
                        inst.status = "IDLE"
                    else:
                        # Staged exit (e.g. TP1 50%): keep the remainder open.
                        # The remainder keeps its direction: selling part of a
                        # LONG leaves a smaller LONG (still +); buying back
                        # part of a SHORT leaves a smaller SHORT (still -).
                        # The old code inverted the sign (-remaining), so the
                        # next tick saw a long remainder as short and a later
                        # "full exit" placed the wrong-side order.
                        inst.net_qty = remaining if (inst.net_qty or 0.0) > 0 else -remaining
                    inst.last_ltp = ltp

                    # Log the trade
                    ledger = TradeOrderLedger(
                        user_id=uid,
                        instance_id=instance_id,
                        broker="dhan",
                        order_id=broker_order_id,
                        symbol=symbol,
                        side=side,
                        quantity=qty,
                        price=ltp,
                        order_type="MARKET",
                        status="FILLED",
                        filled_quantity=qty,
                        average_price=ltp,
                        pnl=pnl,
                    )
                    session.add(ledger)
                    await session.commit()

                    # Notify
                    emoji = "✅" if pnl >= 0 else "❌"
                    await self._notify(
                        uid,
                        f"{emoji} EXIT {cat.display_name}\n"
                        f"Side: {side} | Qty: {qty}\n"
                        f"Price: ₹{ltp:,.2f} | P&L: {pnl:+,.2f}\n"
                        f"Order: {broker_order_id}",
                    )

    # ------------------------------------------------------------------
    # Plugin loading
    # ------------------------------------------------------------------

    # Maps catalog strategy IDs to their actual plugin file in catalog/
    _PLUGIN_MAP = {
        "NIFTY101": "orb_invalidation_strategy_v6e",
        "NIFTY102": "nifty_slhunt_reversal",
        "BANKNIFTY101": "orb_invalidation_strategy_v2",
        "SENSEX101": "orb_invalidation_strategy_v2",
        "SENSEX102": "sensex_slhunt_reversal",
    }

    def _get_plugin(self, instance_id: str, cat) -> Optional[Any]:
        """Load or return a cached StrategyPlugin instance."""
        if instance_id in self._plugins:
            return self._plugins[instance_id]

        # Resolve the plugin module: use _PLUGIN_MAP, fall back to plugin_file column
        module_name = self._PLUGIN_MAP.get(cat.id)
        if not module_name:
            # Fall back to the DB column value (strip .py if present)
            pf = (cat.plugin_file or "").replace(".py", "")
            module_name = pf if pf else None

        if not module_name:
            log.warning("No plugin mapping for catalog entry", catalog_id=cat.id)
            return None

        try:
            module = importlib.import_module(f"catalog.{module_name}")
            plugin_cls = getattr(module, "StrategyPlugin")
            # Merge default_params with any subscription overrides
            params = dict(cat.default_params or {})
            # Armed strategies are expected to take entries automatically when
            # a valid signal fires. ORB v6e, ORB v2, and universal_momentum_straddle
            # gate their entry branches on this flag — it was never set, making
            # entry unreachable even with a working LTP feed.
            params.setdefault("auto_enter", True)
            self._plugins[instance_id] = plugin_cls(instance_id, params)
            log.info("Plugin loaded", instance_id=instance_id, module=module_name)
            return self._plugins[instance_id]
        except Exception as e:
            log.error("Plugin load failed", instance_id=instance_id, module=module_name, error=str(e))
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_symbol(self, cat) -> str:
        """Derive a Dhan symbol string from the catalog entry."""
        underlying = cat.underlying or "NIFTY"
        version = cat.version or ""
        legs = (cat.default_params or {}).get("legs", ["CE"])
        leg_str = "_".join(legs)
        return f"{underlying}_{version}_{leg_str}"

    def _get_security_id(self, cat) -> str:
        """Get the Dhan security_id for the option contract.

        The arm process should resolve and store security_id in
        default_params. If that is missing (the common case for freshly
        seeded catalog rows), fall back to Dhan's free option-chain
        endpoint so orders carry a real contract id instead of "".
        An empty security_id makes Dhan reject every order, which was
        the silent cause of "no trade ever fills".
        """
        params = cat.default_params or {}
        sid = str(params.get("security_id", "") or "")
        if sid:
            return sid
        # Resolve strike/expiry/option_type from the leg params, exactly as
        # _get_instrument_key() does, then ask Dhan's option chain for the
        # contract's numeric security id (free endpoint, not historical).
        strike = params.get("strike", 0)
        expiry = params.get("expiry", "")
        option_type = str(params.get("option_type", "CE") or "CE").upper()
        if (not strike or not expiry) and isinstance(params.get("legs"), list) and params["legs"]:
            first_leg = params["legs"][0]
            if isinstance(first_leg, dict):
                strike = first_leg.get("strike", strike)
                expiry = first_leg.get("expiry", expiry)
                option_type = str(first_leg.get("option_type", option_type) or option_type).upper()
        if not strike or not expiry:
            _log_throttled(
                f"no_sid:{cat.id}", "warning",
                "security_id unset and no strike/expiry to resolve",
                catalog_id=cat.id, params_keys=list(params.keys()))
            return ""
        # Cache resolved ids in a process-local dict (keyed by
        # (segment, expiry, strike, option_type)) — this helper is called
        # from sync paths on the running event loop, so it cannot await;
        # the async resolver below schedules the lookup and remembers the
        # result for the next call.
        cache_key = (self._get_segment(cat), str(expiry), float(strike), option_type)
        if cache_key in _security_id_cache:
            params["security_id"] = _security_id_cache[cache_key]
            return _security_id_cache[cache_key]
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            # No running loop — safe to block briefly on a fresh one.
            from layers.brokers.dhan_client import DhanClient
            async def _resolve_once() -> str:
                client = DhanClient(client_id="", access_token="")
                sec_id = await client._resolve_option_security_id(
                    self._get_segment(cat), str(expiry), float(strike), option_type)
                await client.close()
                return sec_id or ""
            try:
                sec_id = asyncio.run(_resolve_once())
            except Exception as e:
                log.warning("security_id resolve failed", catalog_id=cat.id, error=str(e))
                return ""
            if sec_id:
                _security_id_cache[cache_key] = sec_id
                params["security_id"] = sec_id
            return sec_id
        # On the running loop: schedule the resolve and return the cached
        # value if already known; otherwise return "" for this call and let
        # the next tick (1s later) pick up the resolved id. The async
        # callback stores the result in the shared cache.
        try:
            from layers.brokers.dhan_client import DhanClient
            client = self._dhan  # authenticated DhanClient from main.py

            async def _resolve_async() -> None:
                if client is None:
                    return
                sec_id = await client._resolve_option_security_id(
                    self._get_segment(cat), str(expiry), float(strike), option_type)
                if sec_id and cache_key not in _security_id_cache:
                    _security_id_cache[cache_key] = sec_id
                    log.info("security_id resolved via option chain",
                             catalog_id=cat.id, security_id=sec_id)
            loop.create_task(_resolve_async())
        except Exception as e:
            log.warning("security_id resolve deferred", catalog_id=cat.id, error=str(e))
        return ""


    def _get_segment(self, cat) -> str:
        """Return the Dhan exchange_segment for this catalog entry.

        SENSEX → BSE_FNO; NIFTY / BANKNIFTY → NSE_FNO.
        """
        underlying = (cat.underlying or "NIFTY").upper()
        if underlying.startswith("SENSEX") or underlying.startswith("BSE"):
            return "BSE_FNO"
        return "NSE_FNO"

    def _get_instrument_key(self, cat) -> str:
        """Best-effort Upstox instrument key.

        The Upstox WS V3 and REST quotes endpoints both use "SEGMENT|numeric_id"
        format. For option strategies, the strike/expiry live in the DB
        row's default_params, NOT in the static strategy metadata — so we
        must read cat.default_params for strike/expiry/legs.
        """
        underlying = cat.underlying or "NIFTY"
        segment = cat.segment
        if not segment:
            segment = "BSE_FO" if underlying.upper().startswith("SENSEX") else "NSE_FO"
        segment = segment.replace("_FNO", "_FO")
        params = cat.default_params or {}
        # strike may be at top level or inside a nested legs[0] structure.
        strike = params.get("strike", 0)
        if not strike and params.get("legs"):
            first_leg = params["legs"][0]
            if isinstance(first_leg, dict):
                strike = first_leg.get("strike", strike)
        expiry = params.get("expiry", "")
        if not expiry and params.get("legs"):
            first_leg = params["legs"][0]
            if isinstance(first_leg, dict):
                expiry = first_leg.get("expiry", expiry)
        option_type = params.get("option_type", "CE")
        if not option_type and params.get("legs"):
            legs = params["legs"]
            option_type = legs[0].get("option_type", "CE") if isinstance(legs[0], dict) else "CE"
        try:
            strike_f = float(strike)
        except (TypeError, ValueError):
            strike_f = 0.0
        if strike_f > 0 and expiry:
            return resolve_instrument_key(underlying, strike_f, expiry, option_type, segment)
        # No strike/expiry in params — auto-resolve the OTM option strike and
        # weekly expiry from the live index LTP so the Upstox WS gets a real
        # "SEG|numeric_id" key (a readable label would be silently dropped).
        # The index LTP is cached per-underlying (populated in get_ltp()).
        # For SENSEX the index LTP comes from the Dhan poll (Yahoo has no
        # free SENSEX ticker); NIFTY uses the free Yahoo source.
        if self._auto_resolve_index_ltp(underlying):
            cached_key = _auto_key_cache.get((segment, underlying, option_type.upper()))
            if cached_key:
                return cached_key
            index_ltp = _index_ltp_cache.get(underlying.upper(), (0, 0))[0]
            auto_strike = _compute_option_strike(index_ltp, underlying, option_type)
            auto_expiry = _next_weekly_expiry(underlying, 5)
            auto_key = _resolve_option_key(underlying, segment, option_type, auto_strike, auto_expiry, index_ltp=index_ltp)
            _auto_key_cache[(segment, underlying, option_type.upper())] = auto_key
            log.info("Auto-resolved option key", segment=segment, underlying=underlying,
                     option_type=option_type, strike=auto_strike, expiry=auto_expiry,
                     index_ltp=index_ltp, instrument_key=auto_key)
            return auto_key
        # No index LTP available yet — fall back to the readable label (WS
        # will drop it until the index LTP lands and we re-resolve).
        _log_throttled(
            f"no_key:{underlying}:{option_type}", "warning",
            "Instrument key unresolved (no strike/expiry in params, no index LTP yet)",
            underlying=underlying, segment=segment,
            strike=strike, expiry=expiry, option_type=option_type)
        return f"{segment}|{strike}_{underlying}_{option_type}"

    def _auto_resolve_index_ltp(self, underlying: str) -> bool:
        """Return True when a fresh index LTP is cached for the underlying.

        The actual fetch happens in MarketDataFeed.get_ltp() (async, via
        Dhan/Yahoo index poll). This helper only checks the cache so it can
        be called from the sync _get_instrument_key() without awaiting. The
        cache is populated by the async _process_subscription() path which
        calls get_ltp() for the index symbol each tick.
        """
        u = (underlying or "").upper()
        now = time.time()
        cached = _index_ltp_cache.get(u)
        if cached and (now - cached[1]) < 300:  # 5-min freshness
            return cached[0] > 0
        return False

    def _in_market_hours(self) -> bool:
        """Check if NSE options market is open (09:15-15:30 IST, Mon-Fri)."""
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5:
            return False
        return (9, 15) <= (now_ist.hour, now_ist.minute) < (15, 30)

    async def _notify(self, uid, text: str) -> None:
        """Send a Telegram notification for the user."""
        try:
            # The notification goes through the BotManager if available
            # For now, log it; the /trades command will show the trade
            log.info("Trade notification", user_id=str(uid), text=text[:200])
        except Exception as e:
            log.warning("Notification failed", error=str(e))
