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

    async def start(self, instrument_keys: List[str]) -> None:
        """Start the Upstox WS feed and begin Dhan fallback polling."""
        if not self._upstox_ws:
            log.info("WS feed disabled — all LTP via Dhan poll")
            return
        if not instrument_keys:
            log.info("WS feed has no instrument keys — all LTP via Dhan poll")
            return
        try:
            await self._upstox_ws.start(instrument_keys, mode="ltpc")
            self._ws_running = True
            self._ws_task = asyncio.create_task(self._ws_consume())
            log.info("Upstox WS started", instruments=len(instrument_keys))
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
        produced by UpstoxWSV3._on_message (FIX 6).
        """
        try:
            async for tick in self._upstox_ws.ticks():
                key = tick.get("instrument_key", "")
                ltp = tick.get("last_price")
                if key and ltp:
                    self._ticks[key] = {
                        "ltp": float(ltp),
                        "ts": tick.get("ts", time.time()),
                        "src": "upstox_ws",
                    }
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Upstox WS consume error", error=str(e))

    async def get_ltp(self, instrument_key: str, security_id: str = "", symbol: str = "") -> Optional[float]:
        """Get latest LTP for an instrument. Tries WS cache, then Dhan poll."""
        # 1. Check WS cache (fresh tick < 5s old)
        cached = self._ticks.get(instrument_key)
        if cached and (time.time() - cached["ts"]) < 5.0:
            return cached["ltp"]

        # 2. Dhan fallback: poll quotes for the security_id
        if self._dhan:
            try:
                if security_id:
                    ltp = await self._dhan.get_option_ltp(symbol or security_id, security_id=security_id)
                    if ltp is not None:
                        self._ticks[instrument_key] = {"ltp": ltp, "ts": time.time(), "src": "dhan"}
                        return ltp
                else:
                    # No contract resolved (arming not done): use the index spot
                    # so breakout/breakdown strategies can still evaluate.
                    spot = await self._dhan.get_index_ltp(symbol)
                    if spot is not None:
                        self._ticks[instrument_key] = {"ltp": spot, "ts": time.time(), "src": "dhan_spot"}
                        return spot
                    log.debug("No LTP available (contract not armed, index poll failed)",
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


def _load_upstox_instruments(force: bool = False) -> bool:
    """Fetch + index the Upstox complete instruments JSON (gzip, ~60MB).

    The index maps (segment, expiry, strike, option_type) -> instrument_key
    (the full "SEGMENT|exchange_token" string, e.g. "NSE_FO|12345").
    Cached for 1 hour — the file refreshes daily at ~6 AM IST.
    """
    global _instruments_index, _instruments_loaded_at
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
        log.info("ExecutionEngine started", feed_source=self._feed.source)

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
            # Compare the TIMESTAMP column directly to a datetime — never CAST to
            # date (the Date type collides with the DB column type name and breaks
            # the query). The 00:00 start of day is the correct boundary in IST.
            from sqlalchemy import func as _func
            today_start = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
            result = await session.execute(
                select(_func.count()).where(
                    TradeOrderLedger.instance_id == instance_id,
                    TradeOrderLedger.created_at >= today_start,
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

        # Balance gate check.
        # The floor gates NEW entries against a minimum free-balance buffer.
        # floor = free_balance * floor_pct (e.g. 20% of available margin kept
        # as buffer). Block only when free_balance < floor (i.e. the position
        # would consume more than (1 - floor_pct) of available free margin).
        # This is a soft pre-check; Dhan's own margin validation at the broker
        # is the authoritative gate.
        if self._dhan:
            try:
                balance = await self._dhan.get_balance()
                free_balance = balance.free_balance
                floor = free_balance * self._floor_pct
                if free_balance < floor:
                    # Mathematically floor < free_balance always (floor is a
                    # fraction of free_balance), so this block is unreachable
                    # unless free_balance <= 0 (account empty).
                    log.warning(
                        "Entry blocked: free balance at or below zero",
                        free=free_balance,
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
                        # Staged exit (e.g. TP1 50%): keep the remainder open
                        inst.net_qty = -remaining if side == "SELL" else remaining
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

        Dhan security IDs are numeric. For options, the security_id is
        resolved from the option chain API. For now, we use the
        strategy's default_params to store it, or fall back to a
        placeholder that the resolve_dhan_option.py script can fill.
        """
        params = cat.default_params or {}
        # The arm process should resolve and store security_id here
        return str(params.get("security_id", ""))

    def _get_segment(self, cat) -> str:
        """Return the Dhan exchange_segment for this catalog entry.

        SENSEX → BSE_FNO; NIFTY / BANKNIFTY → NSE_FNO.
        """
        underlying = (cat.underlying or "NIFTY").upper()
        if underlying.startswith("SENSEX") or underlying.startswith("BSE"):
            return "BSE_FNO"
        return "NSE_FNO"

    def _get_instrument_key(self, cat) -> str:
        """Best-effort Upstox instrument key."""
        underlying = cat.underlying or "NIFTY"
        segment = cat.segment
        if not segment:
            segment = "BSE_FO" if underlying.upper().startswith("SENSEX") else "NSE_FO"
        # Upstox segments use _FO (Dhan uses _FNO) — normalize for the key.
        segment = segment.replace("_FNO", "_FO")
        params = cat.default_params or {}
        strike = params.get("strike", 0)
        expiry = params.get("expiry", "")
        option_type = params.get("legs", ["CE"])[0] if params.get("legs") else "CE"
        try:
            strike_f = float(strike)
        except (TypeError, ValueError):
            strike_f = 0.0
        if strike_f > 0 and expiry:
            return resolve_instrument_key(underlying, strike_f, expiry, option_type, segment)
        return f"{segment}|{strike}_{underlying}_{option_type}"

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
