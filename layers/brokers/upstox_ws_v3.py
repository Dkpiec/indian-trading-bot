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
import json
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
        self._last_reconnect_ts = 0.0
        self._reconnect_lock = asyncio.Lock()
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
        # to avoid a NoneType reference before the first connect. Use the
        # streamer's real callback methods (handle_open/message/error/close)
        # that the SDK's own connect() would attach.
        try:
            from upstox_client.feeder.market_data_feeder_v3 import MarketDataFeederV3

            if self._streamer.feeder is None:
                self._streamer.feeder = MarketDataFeederV3(
                    api_client=api_client,
                    instrumentKeys=instrument_keys,
                    mode=mode,
                    on_open=self._streamer.handle_open,
                    on_message=self._streamer.handle_message,
                    on_error=self._streamer.handle_error,
                    on_close=self._streamer.handle_close,
                )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "Upstox WS feeder pre-init failed (will be created on connect)", error=str(e)
            )
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
            # Socket didn't open within 10s. The ?r= token from /authorize
            # may have expired — re-resolve and reconnect ONCE.
            log.error("Upstox v3 WS did not open within 10s — re-resolving and retrying")
            try:
                # Close the orphaned WebSocketApp thread (if it started).
                if feeder and getattr(feeder, "ws", None) and getattr(feeder.ws, "sock", None):
                    try:
                        feeder.ws.sock.close()
                    except Exception:
                        pass
                self._streamer.connect()
                await asyncio.sleep(3)
            except Exception as e:
                log.error("Upstox v3 WS reconnect failed", error=str(e))
        # Re-subscribe the initial keys explicitly. The SDK's handle_open
        # calls subscribe_to_initial_keys() inside the WS on_open callback,
        # but if the socket wasn't ready at that moment the subscribe raises
        # "WebSocket is not open." and is silently swallowed by
        # websocket-client. This explicit call guarantees the keys are sent
        # once the socket is confirmed open.
        #
        # NOTE: we do NOT re-call connect() in the subscribe retry loop —
        # that re-resolves the /authorize URL (fresh ?r= token) and orphans
        # the in-flight WebSocketApp thread, causing the 403 storm. Instead,
        # we just wait for the socket to open and try the subscribe.
        feeder = self._streamer.feeder
        subscribed = False
        for attempt in range(6):
            if not (feeder and feeder.ws and feeder.ws.sock):
                # Socket not open yet — give the in-flight run_forever thread
                # time to complete the handshake (redirect + ?r= validation).
                log.debug("Upstox v3 WS socket not open yet, waiting", attempt=attempt + 1)
                await asyncio.sleep(2.0)
                feeder = self._streamer.feeder
                continue
            try:
                if self._keys:
                    feeder.subscribe(list(self._keys), self._mode)
                self._last_subscribed_ts = time.time()
                log.info(
                    "Upstox v3 WS initial subscribe sent",
                    count=len(self._keys),
                    attempt=attempt + 1,
                )
                subscribed = True
                break
            except Exception as e:
                log.warning(
                    "Upstox v3 WS initial subscribe retry", attempt=attempt + 1, error=str(e)
                )
                await asyncio.sleep(1.0)
        if not subscribed:
            log.error(
                "Upstox v3 WS subscribe could not be confirmed "
                "— the socket may flap; liveness check will drive a reconnect"
            )
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
                log.warning(
                    "Upstox v3 WS: no tick within 8s of start "
                    "— feed may be live but silent, or socket flapping"
                )
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
                # Propagate so task cancellation / feed.stop() actually
                # terminates this consumer. The old `break` swallowed the
                # cancellation and left a zombie task with a full queue.
                self._running = False
                raise

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

    def in_cooldown(self) -> bool:
        """Return True if the WS is in a post-failure cooldown window.

        The Upstox gateway tracks open connections per user (2-5 slots).
        A failed handshake consumes a slot AND the one-time ?code= token;
        the gateway 403s new tokens until the slots clear (5-30 min).
        This method exposes the internal _last_ws_fail_ts so the engine's
        reconnect loop can skip attempts during the cooldown window
        instead of hammering the endpoint and creating a 403 storm.
        """
        import time as _time

        last_fail = getattr(self, "_last_ws_fail_ts", 0.0)
        if not last_fail:
            return False
        return (_time.time() - last_fail) < 900  # 15-min cooldown

    def _maybe_reconnect(self, instrument_keys: Optional[List[str]] = None) -> None:
        """Auto-heal: if the WS is running but stalled (>120s without a tick),
        tear it down and re-subscribe. The SDK's auto_reconnect does NOT detect
        a silently dead socket (observed: last_tick frozen for 80+ min while
        running=True). Call this from the engine's 1s tick loop so a stale
        feed self-heals within one reconnect cycle.

        Uses a 30s grace period after the last reconnect attempt so the
        fresh socket has time to deliver at least one tick before we re-trigger.
        """
        import time as _time

        now = _time.time()
        if now - self._last_reconnect_ts < 30.0:
            return  # grace period after a reconnect
        # Stalled? Check: running but no tick for >120s, OR not started yet.
        if self._running and self._last_tick_ts and (now - self._last_tick_ts) > 120.0:
            log.warning(
                "Upstox WS auto-reconnect: no tick for %ds — restarting streamer",
                int(now - self._last_tick_ts),
            )
            self._last_reconnect_ts = now
            # Fire-and-forget: reconnect is async, so schedule it on the loop.
            try:
                loop = self._loop
                if loop and loop.is_running():
                    loop.create_task(self.reconnect(instrument_keys))
                else:
                    log.warning("Upstox WS auto-reconnect: no running loop, skipping")
            except Exception as e:
                log.warning("Upstox WS auto-reconnect failed: %s", str(e))
        elif not self._running and not self._last_reconnect_ts:
            # Never started (token present but start() not called yet).
            # Don't auto-start here — the engine's start() handles initial connect.
            pass

    async def reconnect(
        self, instrument_keys: Optional[List[str]] = None, mode: Optional[str] = None
    ) -> None:
        """Tear down the streamer/feeder socket and start it again.

        The SDK's auto_reconnect re-subscribes on close, but in production
        the socket flapped and the re-subscribe raised 'socket is already
        closed' and never recovered. Forcing a fresh streamer + socket is
        the reliable path. Call this whenever healthy() is False and the
        engine needs LTPs again.

        Respects the Upstox gateway cooldown: if the last handshake
        failed within the last 15 min, the gateway has consumed the
        one-time ?code= token and connection slots. Reconnecting now
        would 403 and further poison the slots. Wait for the cooldown
        to clear instead.
        """
        if self.in_cooldown():
            log.debug("Upstox WS reconnect skipped — in gateway cooldown", cooldown_s=900)
            return
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
        self._last_reconnect_ts = time.time()  # mark reconnect so _maybe_reconnect backs off
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
        import websocket as _ws

        # Cache the resolved feed URL per-streamer so reconnects reuse it.
        # (Now always overwritten on each connect() — see _connect_patched.)
        resolved_url: dict[str, str] = {}

        def _resolve(url: str, headers: Dict[str, str], force: bool = False) -> str:
            """Handshake to `url`; if the server 3xx-redirects, follow once.

            `force` is accepted but always performs a fresh handshake —
            the redirect target carries a session token that goes stale,
            so we never reuse a cached URL across reconnects.
            """
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
                        log.info("Upstox v3 WS following feed redirect", target=loc.group(1)[:80])
                        sock.close()
                        return loc.group(1)
                # 101 or any non-redirect: close and use the original url.
                sock.close()
                return url
            except Exception as e:  # noqa: BLE001
                sock.close()
                log.warning("Upstox v3 WS redirect resolve failed — using direct URL", error=str(e))
                return url

        def _resolve_via_authorize(auth: str) -> Optional[str]:
            """Call GET /v3/feed/market-data-feed/authorize to get the
            authorized wss:// URL directly (no redirect race).

            The authorize endpoint returns:
                {"status":"success","data":{"authorized_redirect_uri":"wss://..."}}

            The code in the query string is single-use, so this must be
            called fresh on every connect(). Returns None on failure.

            Cloudflare blocks the default Python urllib User-Agent (1010),
            so we send a browser-like UA to get through.
            """
            import urllib.request
            import urllib.error

            url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
            req = urllib.request.Request(
                url,
                headers={
                    "Authorization": auth,
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
                method="GET",
            )
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    body = json.loads(resp.read())
                    uri = (body.get("data") or {}).get("authorized_redirect_uri")
                    if uri:
                        log.info(
                            "Upstox v3 WS authorize endpoint returned feed URL", target=uri[:80]
                        )
                        return uri
                log.warning("Upstox v3 WS authorize endpoint returned no URI", body=body)
            except urllib.error.HTTPError as e:
                # Log the HTTP status + body so 403/401 entitlement issues
                # are visible without digging through the raw exception.
                err_body = ""
                try:
                    err_body = e.read().decode(errors="replace")[:300]
                except Exception:  # noqa: BLE001
                    pass
                log.warning(
                    "Upstox v3 WS authorize endpoint HTTP error", status=e.code, body=err_body
                )
            except Exception as e:  # noqa: BLE001
                log.warning("Upstox v3 WS authorize endpoint failed", error=str(e))
            return None

        def _connect_patched(self):
            feeder = self.feeder
            if feeder is None:
                from upstox_client.feeder.market_data_feeder_v3 import MarketDataFeederV3

                feeder = MarketDataFeederV3(
                    api_client=self.api_client,
                    instrumentKeys=self.instrumentKeys,
                    mode=self.mode,
                    on_open=self.handle_open,
                    on_message=self.handle_message,
                    on_error=self.handle_error,
                    on_close=self.handle_close,
                )
                self.feeder = feeder
            if not feeder:
                log.error("Upstox WS feeder unavailable — cannot start feed")
                return
            # Cooldown: if we just failed a handshake within the last 15 min,
            # skip this attempt. Upstox's gateway tracks open connections per
            # user (2–5 slots). Our repeated failed handshakes consumed the
            # one-time ?code= tokens AND the connection slots; the gateway
            # now 403s new tokens until the slots clear (5–30 min). Waiting
            # avoids further poisoning.
            now = time.time()
            _last_fail = getattr(self, "_last_ws_fail_ts", 0.0)
            if _last_fail and (now - _last_fail) < 900:
                log.debug(
                    "Upstox v3 WS in cooldown (waiting for gateway slots to clear)",
                    retry_in_s=int(900 - (now - _last_fail)),
                )
                return
            auth = self.api_client.configuration.auth_settings().get("OAUTH2", {}).get("value", "")
            if feeder.ws and feeder.ws.sock:
                return
            headers = {"Authorization": auth, "Accept": "*/*"}
            sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
            # SDK-native: connect to wss://api.upstox.com/v3/feed/market-data-feed
            # and let websocket-client follow the 302 redirect to wsfeeder-api
            # in-session. Byte-identical to the SDK's own feeder.connect().
            ws_url = "wss://api.upstox.com/v3/feed/market-data-feed"
            log.info(
                "Upstox v3 WS connecting (SDK-native, cooldown cleared)",
                target=ws_url,
                instruments=len(self.instrumentKeys),
            )
            feeder.ws = _ws.WebSocketApp(
                ws_url,
                header=headers,
                on_open=feeder.on_open,
                on_message=feeder.on_message,
                on_error=feeder.on_error,
                on_close=feeder.on_close,
            )
            threading.Thread(
                target=feeder.ws.run_forever,
                kwargs={"sslopt": sslopt},
            ).start()

        # Mark a WS failure so _connect_patched enters cooldown
        _orig_error = streamer.handle_error

        def _error_cooled(self, error):
            self._last_ws_fail_ts = time.time()
            log.warning("Upstox v3 WS error — entering 15-min cooldown", error=str(error)[:150])
            _orig_error(error)

        streamer.handle_error = _error_cooled.__get__(streamer, type(streamer))

        streamer.connect = _connect_patched.__get__(streamer, type(streamer))
        log.debug("Upstox v3 WS feed redirect patch applied")

    # ------------------------------------------------------------------
    # Internal callbacks (called from WS thread, must be thread-safe)
    # ------------------------------------------------------------------

    def _on_open(self) -> None:
        self._ws_open = True
        log.info("Upstox v3 WS connected")
        if self._loop:
            self._loop.call_soon_threadsafe(lambda: self._queue.put_nowait({"type": "ws_open"}))

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
                    log.info(
                        "First Upstox WS tick received", instrument_key=key, ltp=tick["last_price"]
                    )
                self._loop.call_soon_threadsafe(lambda t=tick: self._queue.put_nowait(t))
        except asyncio.QueueFull:
            # The consumer (the engine's 1s tick) is behind — the WS stream
            # is faster than the poll loop can drain it. Drop the overflow
            # but LOG it: a full queue for >60s means the event loop is
            # stuck (blocking call elsewhere) and LTPs are going stale.
            if time.time() - getattr(self, "_last_queue_full_log", 0.0) > 60.0:
                self._last_queue_full_log = time.time()
                log.warning(
                    "Upstox WS tick queue full — dropping ticks " "(consumer blocked or too slow)"
                )
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
