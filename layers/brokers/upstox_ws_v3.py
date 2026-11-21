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

        # connect() spawns a daemon thread internally
        self._streamer.connect()
        self._running = True
        log.info(
            "Upstox v3 WS starting",
            instruments=len(instrument_keys),
            mode=mode,
        )

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
        log.info("Upstox v3 WS connected")
        if self._loop:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_open"})
            )

    def _on_message(self, message: Any) -> None:
        if not self._loop or not self._running:
            return
        try:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "tick", "data": message})
            )
        except asyncio.QueueFull:
            pass  # drop oldest on backpressure

    def _on_error(self, error: Any) -> None:
        log.error("Upstox v3 WS error", error=str(error))
        if self._loop and self._running:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_error", "error": str(error)})
            )

    def _on_close(self, code: Any, reason: Any) -> None:
        log.warning("Upstox v3 WS closed", code=code, reason=str(reason))
        self._running = False
        if self._loop:
            self._loop.call_soon_threadsafe(
                lambda: self._queue.put_nowait({"type": "ws_close", "code": code})
            )