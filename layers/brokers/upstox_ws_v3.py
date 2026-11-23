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
        self._queue: Optional[asyncio.Queue] = None
        self._streamer = None
        self._feeder = None
        self._keys: List[str] = []
        self._mode = "ltpc"
        self._last_tick_ts = 0.0
        self._last_subscribed_ts = 0.0
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
        # Create the queue inside the running loop (lazy, so it binds to the
        # active event loop on any Python version).
        self._queue = asyncio.Queue(maxsize=10_000)

        # Import here so the wheelhouse path is resolved at runtime. If the
        # SDK was not installed into site-packages (image built before the
        # Dockerfile fix), add the in-repo wheelhouse dir to sys.path as a
        # fallback.
        import os as _os
        import sys as _sys
        # The adapter is at <app-root>/layers/brokers/upstox_ws_v3.py, so
        # three dirname levels up is the app root (which contains wheelhouse/).
        _app_root = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
        _wh = _os.path.join(_app_root, "wheelhouse")
        if _os.path.isdir(_os.path.join(_wh, "upstox_client")) and _wh not in _sys.path:
            _sys.path.insert(0, _wh)
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
        # The SDK's __init__ sets self.feeder=None; the feeder is only created
        # on connect(). Our patched connect() builds it, so create it up front
        # to avoid a NoneType reference before the first connect.
        try:
            from upstox_client.feeder.market_data_feeder_v3 import MarketDataFeederV3
            if self._streamer.feeder is None:
                self._streamer.feeder = MarketDataFeederV3(
                    api_client=api_client,
                    instrumentKeys=instrument_keys,
                    mode=mode,
                    on_open=self._streamer.on_open,
                    on_message=self._streamer.on_message,
                    on_error=self._streamer.on_error,
                    on_close=self._streamer.on_close,
                )
        except Exception as e:  # noqa: BLE001
            log.warning("Upstox WS feeder pre-init failed (will be created on connect)",
                        error=str(e))
        self._feeder = self._streamer.feeder
        self._keys = list(instrument_keys)
        self._mode = mode
        # The feed endpoint (wss://api.upstox.com/v3/feed/market-data-feed)
        # answers the WS handshake with a 302 redirect to the *authorized*
        # feed URL (wss://wsfeeder-api.upstox.com/...). websocket-client's
        # run_forever does NOT follow WebSocket redirects, so without this
        # patch every connect gets "Handshake status 403 Forbidden". We patch
        # the SDK's connect() to follow the 302 exactly once.
        self._patch_feed_redirect(self._streamer)

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
        # Re-subscribe the initial keys explicitly. The SDK's handle_open
        # calls subscribe_to_initial_keys() inside the WS on_open callback,
        # but if the socket wasn't ready at that moment the subscribe raises
        # "WebSocket is not open." and is silently swallowed by
        # websocket-client. This explicit call guarantees the keys are sent
        # once the socket is confirmed open. Retry a few times because the
        # SDK's on_open can fire on a transient socket that immediately
        # closes (the "socket is already closed" race we saw in production).
        feeder = self._streamer.feeder
        subscribed = False
        for attempt in range(6):
            if not (feeder and feeder.ws and feeder.ws.sock):
                # Socket dropped (or never opened) — ask the SDK to reconnect
                # and give it a moment to bring the socket back up.
                try:
                    self._streamer.connect()
                except Exception as e:
                    log.warning("Upstox v3 WS reconnect (pre-sub) error", error=str(e))
                await asyncio.sleep(1.5)
                feeder = self._streamer.feeder
                continue
            try:
                if self._keys:
                    feeder.subscribe(list(self._keys), self._mode)
                self._last_subscribed_ts = time.time()
                log.info("Upstox v3 WS initial subscribe sent",
                         count=len(self._keys), attempt=attempt + 1)
                subscribed = True
                break
            except Exception as e:
                log.warning("Upstox v3 WS initial subscribe retry",
                            attempt=attempt + 1, error=str(e))
                await asyncio.sleep(1.0)
        if not subscribed:
            log.error("Upstox v3 WS subscribe could not be confirmed "
                      "— the socket may flap; liveness check will drive a reconnect")
        # Verify a live tick actually arrives within ~8s. "Socket open" is
        # not enough: in production the socket flapped and the first
        # subscribe raised "socket is already closed", so no data ever
        # flowed. A tick is the only real proof the feed is usable.
        if instrument_keys:
            t0 = time.time()
            while time.time() - t0 < 8.0:
                if self._last_tick_ts and (time.time() - self._last_tick_ts) < 10.0:
                    log.info("Upstox v3 WS tick verified", instruments=len(instrument_keys))
                    break
                await asyncio.sleep(0.5)
            else:
                log.warning("Upstox v3 WS: no tick within 8s of start "
                            "— feed may be live but silent, or socket flapping")
        # Final health check: if the socket still isn't open after the retry,
        # the WS is down. Log loudly so the Dhan fallback is a conscious
        # degradation, not a silent one.
        feeder = self._streamer.feeder
        if not (feeder and feeder.ws and feeder.ws.sock):
            log.error(
                "Upstox v3 WS NOT AVAILABLE — engine will run on Dhan fallback. "
                "Check analytics token validity and network egress.",
                instruments=len(instrument_keys),
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
        """Async generator yielding parsed tick dicts.

        Yields nothing until start() has been called (the queue is created
        in start()). The engine only begins consuming ticks after start(),
        so this guard is defensive.
        """
        while self._running:
            if self._queue is None:
                await asyncio.sleep(0.2)
                continue
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

    def healthy(self) -> bool:
        """True only when the WS is running AND has received a live tick in
        the last 45s (the SENSEX/NIFTY option LTP refresh is far faster than
        45s, so a stale _last_tick_ts means the socket flapped or the
        subscription was lost). This is the liveness signal the engine and
        the Telegram 'WS alive=' report should use, not just 'running'."""
        if not self._running:
            return False
        if not self._last_tick_ts:
            return False
        return (time.time() - self._last_tick_ts) < 45.0

    async def reconnect(self, instrument_keys: Optional[List[str]] = None,
                        mode: Optional[str] = None) -> None:
        """Tear down the streamer/feeder socket and start it again.

        The SDK's auto_reconnect re-subscribes on close, but in production
        the socket flapped and the re-subscribe raised 'socket is already
        closed' and never recovered. Forcing a fresh streamer + socket is
        the reliable path. Call this whenever healthy() is False and the
        engine needs LTPs again.
        """
        keys = instrument_keys if instrument_keys is not None else self._keys
        m = mode if mode is not None else self._mode
        if not keys:
            log.warning("Upstox WS reconnect skipped — no instrument keys")
            return
        # Stop the old streamer/feeder (closes the socket, cancels the WS
        # thread's run_forever). We keep the _queue and _loop.
        self._running = False
        self._ws_open = False
        try:
            if self._streamer and self._streamer.feeder:
                try:
                    self._streamer.feeder.ws.close()
                except Exception:
                    pass
        except Exception:
            pass
        self._streamer = None
        self._feeder = None
        self._last_tick_ts = 0.0
        # Rebuild the streamer + socket from scratch.
        await self.start(list(keys), m)

    # ------------------------------------------------------------------
    # Feed 302-redirect handling
    # ------------------------------------------------------------------
    def _patch_feed_redirect(self, streamer) -> None:
        """Wrap the SDK's connect() to follow the feed's 302 redirect.

        Upstox's v3 feed endpoint responds to the WS handshake with a 302
        redirect to the authorized feed URL. websocket-client's run_forever
        does not follow WebSocket redirects (it only follows them in
        connect()), so the naive SDK connect gets "403 Forbidden" on the
        redirected host. We perform the redirect handshake ourselves (one
        attempt) and hand off the resolved URL to a fresh WebSocketApp that
        reuses the SDK's callbacks.

        This mirrors what the Upstox docs instruct: 'configure your
        WebSocket client to handle automatic redirection to the authorized
        endpoint after authentication'.
        """
        import re
        import ssl
        import socket as _socket
        import threading
        import websocket as _ws

        # Cache the resolved feed URL per-streamer so reconnects reuse it.
        resolved_url: dict[str, str] = {}

        def _resolve(url: str, headers: Dict[str, str]) -> str:
            """Handshake to `url`; if the server 3xx-redirects, follow once."""
            m = re.match(r"ws(s?)://([^/]+)(/.*)", url)
            if not m:
                return url
            scheme, host, path = m.group(1), m.group(2), m.group(3)
            import base64, os as _os
            sec_key = base64.b64encode(_os.urandom(16)).decode()
            sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            sock.settimeout(15)
            try:
                sock.connect((host, 443 if scheme == "s" else 80))
                if scheme == "s":
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    sock = ctx.wrap_socket(sock, server_hostname=host)
                req = (
                    f"GET {path} HTTP/1.1\r\nHost: {host}\r\n"
                    f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {sec_key}\r\n"
                    f"Sec-WebSocket-Version: 13\r\n"
                )
                for hk, hv in headers.items():
                    req += f"{hk}: {hv}\r\n"
                req += "\r\n"
                sock.sendall(req.encode())
                resp = sock.recv(4096).decode(errors="replace")
                status_line = resp.splitlines()[0] if resp else ""
                parts = status_line.split()
                code = parts[1] if len(parts) > 1 else "0"
                if code in ("301", "302", "307", "308"):
                    loc = re.search(r"(?i)location:\s*(\S+)", resp)
                    if loc:
                        log.info("Upstox v3 WS following feed redirect",
                                 target=loc.group(1)[:80])
                        sock.close()
                        return loc.group(1)
                # 101 or any non-redirect: close and use the original url.
                sock.close()
                return url
            except Exception as e:  # noqa: BLE001
                sock.close()
                log.warning("Upstox v3 WS redirect resolve failed — using direct URL",
                            error=str(e))
                return url

        def _connect_patched(self):
            feeder = self.feeder
            ws_url = "wss://api.upstox.com/v3/feed/market-data-feed"
            auth = self.api_client.configuration.auth_settings().get("OAUTH2", {}).get("value", "")
            headers = {"Authorization": auth, "Accept": "*/*"}
            target = resolved_url.get("url")
            if not target:
                target = _resolve(ws_url, headers)
                resolved_url["url"] = target
            if feeder.ws and feeder.ws.sock:
                return
            sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
            feeder.ws = _ws.WebSocketApp(
                target,
                header=headers,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            threading.Thread(target=feeder.ws.run_forever, kwargs={"sslopt": sslopt}).start()

        streamer.connect = _connect_patched.__get__(streamer, type(streamer))
        log.debug("Upstox v3 WS feed redirect patch applied")

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
                self._last_tick_ts = time.time()
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