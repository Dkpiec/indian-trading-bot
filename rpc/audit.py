"""Temporary clean-room audit of the Upstox WS handshake (read-only,
runs in-container so the egress IP is the whitelisted one). DELETE AFTER USE."""
import json, ssl, sys, threading, time, urllib.request
from fastapi import APIRouter

router = APIRouter()

@router.post("/api/v1/debug/ws-audit")
async def ws_audit():
    import os
    sys.path.insert(0, os.path.join(os.getcwd(), "wheelhouse"))
    import websocket
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    TOKEN = os.environ.get("UPSTOX_ANALYTICS_TOKEN", "")
    out = {"token_len": len(TOKEN)}
    if not TOKEN:
        return {"ok": False, "error": "no UPSTOX_ANALYTICS_TOKEN", **out}
    # 1. /authorize (v3)
    try:
        req = urllib.request.Request(
            "https://api.upstox.com/v3/feed/market-data-feed/authorize",
            headers={"Authorization": "Bearer " + TOKEN,
                     "Accept": "application/json",
                     "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"})
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            body = json.loads(resp.read())
            uri = (body.get("data") or {}).get("authorizedRedirectUri")
        out["authorize"] = {"ok": bool(uri), "ms": int((time.time()-t0)*1000), "uri": (uri or "")[:120]}
    except Exception as e:
        out["authorize"] = {"ok": False, "error": str(e)[:150]}
        return {"ok": False, **out}
    # 2. WS handshake on the authorized URI (no header, per docs)
    events = []
    def on_open(ws): events.append("OPEN")
    def on_message(ws, m): events.append("MSG:" + m[:120])
    def on_error(ws, e): events.append("ERROR:" + str(e)[:150])
    def on_close(ws, c, r): events.append(f"CLOSE:code={c}")
    ws = websocket.WebSocketApp(uri, header={}, on_open=on_open, on_message=on_message,
                                on_error=on_error, on_close=on_close)
    sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
    th = threading.Thread(target=ws.run_forever, kwargs={"sslopt": sslopt}, daemon=True)
    th.start()
    time.sleep(15)
    ws.close()
    out["ws_events"] = events[:8]
    out["ws_ok"] = any(e == "OPEN" or e.startswith("MSG") for e in events)
    return {"ok": True, **out}
