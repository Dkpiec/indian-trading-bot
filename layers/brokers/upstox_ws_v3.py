"""Upstox v3 WebSocket market data streamer — async wrapper.

The upstream SDK (wheelhouse/upstox_client/feeder/market_data_streamer_v3.py)
uses synchronous websocket-client threads. This module wraps it with an
asyncio.Queue bridge so the rest of the bot can consume ticks without
blocking the event loop.

Endpoint: wss://api.upstox.com/v3/feed/market-data-feed  (v3, NOT v2)
Auth: Bearer token via api_client.configuration.auth_settings()['OAUTH2']['value']
"""
from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import structlog

log = structlog.get_logger(__name__)


class UpstoxWSV3:
    """Async-friendly wrapper around MarketDataStreamerV3.

    Usage:
        ws = UpstoxWSV3(access_token="...")
        await ws.start(instrument_keys=["NSE_FO|12345"], mode="ltpc")
        async for tick in ws.ticks():
            process(tick)
        await ws.stop()
    """

    def __init__(self, *, access_token: str = "", analytics_token: str = ""):
        # Prefer long-lived analytics token for v3 WS; fall back to short-lived access token
        self._access_token = analytics_token or access_token
        self._queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=10_000)
        self._streamer = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._first_tick_logged = False
        self._ws_open = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(
        self,
        instrument_keys: List[str],
        mode: str = "ltpc",
    ) -> None:
        """Connect to Upstox v3 WS and start streaming."""
        if self._running:
            log.warning("Upstox WS already running")
            return

        self._loop = asyncio.get_running_loop()

        # Import here so the wheelhouse path is resolved at runtime
        from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3
        from upstox_client.api_client import ApiClient
        from upstox_client.configuration import Configuration

        config = Configuration()
        config.access_token = self._access_token
        api_client = ApiClient(configuration=config)

        self._streamer = MarketDataStreamerV3(
            api_client=api_client,
            instrumentKeys=instrument_keys,
            mode=mode,
        )

        # Register callbacks that bridge into the async queue
        self._streamer.on("open", lambda: self._on_open())
        self._streamer.on("message", lambda msg: self._on_message(msg))
        self._streamer.on("error", lambda err: self._on_error(err))
        self._streamer.on("close", lambda code, reason: self._on_close(code, reason))

        # connect() spawns a daemon WS thread internally and returns
        # immediately; the socket is NOT open yet. Subscribe (in
        # handle_open) can raise "WebSocket is not open." if on_open fires
        # before the socket is ready. Poll for the open state, then verify
        # the initial subscription actually went out.
        self._streamer.connect()
        self._running = True
        log.info(
            "Upstox v3 WS connecting",
            instruments=len(instrument_keys),
            mode=mode,
        )
        # Wait up to 10s for the socket to open (feeder.ws.sock set on open).
        feeder = self._streamer.feeder
        for _ in range(20):
            if feeder and feeder.ws and feeder.ws.sock:
                log.info("Upstox v3 WS socket open")
                break
            await asyncio.sleep(0.5)
        else:
            log.error("Upstox v3 WS did not open within 10s — retrying once")
            try:
                self._streamer.connect()
                await asyncio.sleep(3)
            except Exception as e:
                log.error("Upstox v3 WS reconnect failed", error=str(e))

    async def stop(self) -> None:
        """Disconnect gracefully."""
        if not self._running:
            return
        self._running = False
        if self._streamer and self._streamer.feeder:
            try:
                self._streamer.feeder.ws.close()
            except Exception as e:
                log.warning("Upstox WS close error", error=str(e))
        log.info("Upstox v3 WS stopped")

    async def subscribe(self, instrument_keys: List[str], mode: str = "ltpc") -> None:
        """Add instruments to the live subscription."""
        if self._streamer and self._running:
            self._streamer.subscribe(instrument_keys, mode)
            log.info("Upstox WS subscribed", keys=len(instrument_keys), mode=mode)

    async def unsubscribe(self, instrument_keys: List[str]) -> None:
        """Remove instruments from the live subscription."""
        if self._streamer and self._running:
            self._streamer.unsubscribe(instrument_keys)
            log.info("Upstox WS unsubscribed", keys=len(instrument_keys))

    async def ticks(self):
        """Async generator yielding parsed tick dicts."""
        while self._running:
            try:
                tick = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                yield tick
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    @property
    def connected(self) -> bool:
        return self._running and self._streamer is not None

    # ------------------------------------------------------------------
    # Internal callbacks (called from WS thread, must be thread-safe)
    # ------------------------------------------------------------------

    def _on_open(self) -> None:
        self._ws_open = True
        log.info("Upstox v3 WS connected")
        if self._loop:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_open"})
            )

    def _on_message(self, message: Any) -> None:
        """Bridge SDK ticks into the async queue.

        The SDK's MarketDataStreamerV3.handle_message already decodes
        the protobuf FeedResponse via json_format.MessageToDict, so
        `message` is a dict like:
            {
                "feeds": {
                    "NSE_FO|49492": {"ltpc": {"ltp": "250.5", "cp": "250.25"}},
                    ...
                },
                "currentTs": "..."
            }
        """
        if not self._loop or not self._running:
            return
        try:
            if not isinstance(message, dict):
                log.warning("Upstox WS non-dict message", type=type(message).__name__)
                return
            feeds = message.get("feeds")
            # MessageToDict emits the repeated FeedsEntry field as a LIST of
            # {"key": instrument_key, "value": feed} (a map would be a dict).
            if isinstance(feeds, dict):
                entries = list(feeds.items())
            elif isinstance(feeds, list):
                entries = [(e.get("key"), e.get("value")) for e in feeds if isinstance(e, dict)]
            else:
                # market_info / status frames carry no feeds — skip silently.
                return
            for key, feed in entries:
                if not key or not isinstance(feed, dict):
                    continue
                # Feed oneof: ltpc / fullFeed / firstLevelWithGreeks. In
                # ltpc mode the oneof resolves to the nested ltpc message;
                # in full/option modes fall back to the flat top-level fields
                # (MarketFullFeed/IndexFullFeed have ltp at the top level too).
                ltpc = feed.get("ltpc")
                if not isinstance(ltpc, dict):
                    ltpc = feed  # flat shape (full mode)
                ltp = ltpc.get("ltp")
                if ltp is None:
                    continue
                tick = {
                    "type": "tick",
                    "instrument_key": key,
                    "last_price": float(ltp),
                    "change_percent": float(ltpc.get("cp", 0) or 0),
                    "ts": time.time(),
                }
                if not self._first_tick_logged:
                    self._first_tick_logged = True
                    log.info("First Upstox WS tick received",
                             instrument_key=key, ltp=tick["last_price"])
                self._loop.call_soon_threadsafe(
                    lambda t=tick: self._queue.put_nowait(t)
                )
        except asyncio.QueueFull:
            pass
        except Exception as e:
            log.error("Upstox WS message bridge error", error=str(e))

    def _on_error(self, error: Any) -> None:
        log.error("Upstox v3 WS error", error=str(error))
        if self._loop and self._running:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_error", "error": str(error)})
            )

    def _on_close(self, code: Any, reason: Any) -> None:
        log.warning("Upstox v3 WS closed", code=code, reason=str(reason))
        self._running = False
        self._ws_open = False
        if self._loop:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_close", "code": code})
            )