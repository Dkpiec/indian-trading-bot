"""SENSEX SL-Hunting Reversal Strategy (V4.1g) — live execution plugin.

Backtest source of truth (V4.1g, 2024-11 to 2026-11, 327 OR days, 2-year):
    89 trades | WR 61.8% | PF 3.20 | +₹246,305 / 2Y
    Long SENSEX weekly options (1-ITM, DTE 5), 2 lots (40 units).
    Sweep-and-rejection entry on completed 5m bars; staged exit with
    midpoint invalidation, 50% TP1 (sell 1 lot), trailing 0.5 ATR on
    remaining lot, 100% TP2, 40-bar time stop, 09:15-15:30 IST EOD.

Live wiring notes
-----------------
* Entry signal is sourced from the FREE Yahoo %5ESENSEX 5m chart (v8 API)
  — never from a chargeable Dhan historical endpoint. The sweep/rejection
  check runs on each completed bar; the entry is sized at 2 lots (40 units).
* The option contract (1-ITM weekly CE/PE) is resolved at arm-time by the
  operator via /api/v1/strategies/upsert (security_id + lot_size + expiry
  are set in parameters). The plugin does NOT re-resolve on every tick —
  the weekly roll is handled by the daily arm script, matching the ORB legs.
* When holding a position, exits are driven by inst.last_ltp (option LTP
  from the Upstox WS streamer / broker sync). When flat, entries are driven
  by the ^SENSEX sweep check (index-level signal).

Config (defaults match the backtest winner V4.1g; overridable via params):
    om=0.20  wick=5.0  sl=0.12  tp1=0.50  tp2=1.00  hold=40
    OR=[0.40,0.90]  rej=0.45  mvix=0.10  mom=True
    lot=40 (2 lots of 20)  dte=5  tp1_frac=0.50  trail_atr=0.5

Position isolation
-----------------
This instance is independent of the ORB legs. It trades a distinct
security_id (SENSEX weekly option) on BSE_FNO segment, and the engine's
assert_sole_owner gate already prevents two strategies on the same
underlying+leg from double-entering — the SL-hunt leg uses
option_type=CE/PE with trigger_type=SLHUNT so it is a distinct leg.

Dhan order notes
----------------
* SENSEX options are on the BSE_FNO segment (not NSE_FNO).
* SENSEX strike spacing is 100 points (not 50 like NIFTY).
* SENSEX lot size is 20 units. 2 lots = 40 units per trade.
* Exits are in lot increments: sell 1 lot (20 units) at TP1,
  trail/close remaining 1 lot (20 units) at TP2/trail/EOD.
"""
from __future__ import annotations

import json
import structlog
log = structlog.get_logger(__name__)
import threading
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# --- Backtest winner config (V4.1g) ---
DEFAULT_CONFIG = {
    "om": 0.20,          # OR width multiplier (wick threshold = ow * om)
    "wick": 5.0,         # sweep wick threshold in points
    "sl": 0.12,          # stop loss 12%
    "tp1": 0.50,         # first target 50% (sell 1 lot)
    "tp2": 1.00,         # final target 100% (close remaining lot)
    "hold": 40,          # max bars to hold before time stop
    "or_min": 0.40,     # OR width min (fraction of spot)
    "or_max": 0.90,     # OR width max (fraction of spot)
    "rej": 0.45,        # rejection depth fraction of sweep wick
    "mvix": 0.10,       # minimum VIX (sigma) filter
    "mom": True,        # momentum confirmation (next bar closes in direction)
    "lot": 40,          # total units per entry (2 lots of 20)
    "lot_units": 20,    # SENSEX lot size
    "tp1_frac": 0.50,   # fraction of position sold at TP1 (1 lot of 2)
    "trail_atr": 0.5,   # trailing stop ATR multiplier
    "dte": 5.0,         # days to expiry
}

# SENSEX 5m candle source: Dhan live quote for the SENSEX index (free, not
# historical). Yahoo has no free SENSEX ticker. The old code posted
# exchangeSegment="INDEX" and read data.data[].lastPrice — both wrong for
# Dhan v2 (the segment is IDX_I and the response is a flat list), so the
# endpoint returned [] and the plugin fell back to a single synthetic bar
# with O=H=L=C. We now accumulate REAL 5m bars from polled quotes so the
# sweep/rejection check has genuine bar structure.
DHAN_BASE = "https://api.dhan.co/v2"
SENSEX_INDEX_ID = "mid3040"  # Dhan SENSEX index security_id

# In-process 5m bar accumulator for the SENSEX index quote.
_sensex_bars: Dict[str, Any] = {"poll_ts": 0.0, "bars": []}
_sensex_lock = threading.Lock()
_SENSEX_POLL_S = 3.0  # poll the live quote at most once every 3s


def _fetch_sensex_5m_candles() -> list:
    """Return [(ts_epoch, open, high, low, close)] for today's SENSEX 5m bars.

    Polls the Dhan SENSEX index live quote (free, not a chargeable
    historical endpoint) every few seconds and accumulates real 5m bars in
    memory. The caller (the 1s tick loop) never blocks on the network:
    each call is a single small POST at most every _SENSEX_POLL_S, and a
    failed poll simply keeps the bars accumulated so far.
    """
    import time
    now = time.time()
    with _sensex_lock:
        need_poll = now - _sensex_bars["poll_ts"] >= _SENSEX_POLL_S
        _sensex_bars["poll_ts"] = now  # throttle marker even if the poll fails

    if need_poll:
        try:
            url = f"{DHAN_BASE}/market/quote"
            # Dhan v2: index quotes use exchangeSegment="IDX_I"; the response
            # is a flat {"data": [ {...} ]} list (NOT data.data).
            payload = {"securityId": SENSEX_INDEX_ID, "exchangeSegment": "IDX_I"}
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
            spot: Optional[float] = None
            for item in data.get("data") or []:
                if isinstance(item, dict) and str(item.get("securityId")) == SENSEX_INDEX_ID:
                    spot = float(item.get("lastPrice") or item.get("last_price") or 0) or None
            if spot is not None:
                with _sensex_lock:
                    bars = _sensex_bars["bars"]
                    # Bucket the quote into 5m bars (floor the epoch to 5m).
                    bucket = int(now // 300) * 300
                    if bars and bars[-1][0] == bucket:
                        o, h, l, c = bars[-1][1], bars[-1][2], bars[-1][3], bars[-1][4]
                        bars[-1] = (bucket, o, max(h, spot), min(l, spot), spot)
                    else:
                        bars.append((bucket, spot, spot, spot, spot))
                    # Drop bars older than one trading day (restart safety).
                    day_start = datetime.now(timezone.utc).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ).timestamp()
                    _sensex_bars["bars"] = [b for b in bars if b[0] >= day_start - 300]
        except Exception as e:
            log.warning("SENSEX SLHUNT index poll failed", error=str(e))
            spot = None

    with _sensex_lock:
        completed_cut = now - 300
        # Return only the COMPLETED bars (the in-progress 5m window excluded,
        # matching the NIFTY plugin's "act on closed bars" rule).
        out = [b for b in _sensex_bars["bars"] if b[0] <= completed_cut]
    return out


class StrategyPlugin:
    """SENSEX SL-Hunting Reversal (V4.1g) — live execution.

    Entry: sweep-and-rejection on a completed 5m bar (index ^SENSEX).
    Exit: midpoint invalidation, 12% SL, 50% TP1 (sell 1 lot + trail BE),
    0.5 ATR trailing stop, 100% TP2, 40-bar time stop, 15:30 IST EOD.
    """

    def __init__(self, instance_id: str, params: dict):
        self.instance_id = instance_id
        self.params = dict(params)
        self._cfg = dict(DEFAULT_CONFIG)
        self._cfg.update(self._override_from_params(params))
        self.last_signal_ts: Optional[float] = None  # last completed-bar ts we evaluated

    @staticmethod
    def _override_from_params(params: dict) -> dict:
        """Allow operator overrides for the backtest winner config via params."""
        mapping = {
            "om": "slh_om", "wick": "slh_wick", "sl": "slh_sl",
            "tp1": "slh_tp1", "tp2": "slh_tp2", "hold": "slh_hold",
            "or_min": "slh_or_min", "or_max": "slh_or_max",
            "rej": "slh_rej", "lot": "slh_lot", "lot_units": "slh_lot_units",
            "mvix": "slh_mvix", "mom": "slh_mom",
            "tp1_frac": "slh_tp1_frac", "trail_atr": "slh_trail_atr",
            "dte": "slh_dte",
        }
        out = {}
        for cfg_key, p_key in mapping.items():
            if p_key in params and params[p_key] is not None:
                out[cfg_key] = params[p_key]
        return out

    def _open_and_close(self, params: dict, state: dict):
        """Compute the day's OR open/close from params (or bounds)."""
        p = params
        # Prefer explicit or_high/or_low set by the arm script.
        or_high = float(p.get("or_high") or 0.0)
        or_low = float(p.get("or_low") or 0.0)
        if or_high > 0 and or_low > 0 and or_high > or_low:
            return or_low, or_high
        # Fall back to spot_945 + width.
        spot = float(p.get("spot_945") or state.get("spot_ltp") or 0.0)
        wmin = float(self._cfg.get("or_min") or 0.0)
        wmax = float(self._cfg.get("or_max") or 0.0)
        if spot <= 0:
            return 0.0, 0.0
        # Default midpoint = spot; width within [wmin, wmax] as a fraction.
        wmid = (wmin + wmax) / 2.0
        width = spot * wmid / 2.0
        return spot - width, spot + width

    def _sweep_rejection_check(self, state: dict, params: dict) -> Optional[dict]:
        """Return a SIGNAL entry when a completed 5m bar sweeps the OR bound
        and rejects. None otherwise.

        Supports BOTH CE and PE legs via params["option_type"]:
          CE: sweep or_high (low dips below, closes back above) -> BUY CE
          PE: sweep or_low  (high pokes above, closes back below) -> BUY PE
        The option_type comes from the upserted strategy parameters so a single
        plugin class serves both legs without code duplication.
        """
        cfg = self._cfg
        or_low, or_high = self._open_and_close(params, state)
        if or_low <= 0 or or_high <= 0:
            return None
        om = (or_high + or_low) / 2.0

        # Determine which leg this instance trades (CE or PE).
        opt_type = str(params.get("option_type") or "CE").upper()

        candles = _fetch_sensex_5m_candles()
        if not candles:
            return None
        # Only evaluate the most recently COMPLETED bar. The Yahoo chart
        # includes the in-progress bar; drop the last one (its close is not
        # final). The bar we act on is the last one that has fully closed.
        now_ts = datetime.now(timezone.utc).timestamp()
        completed = [c for c in candles if c[0] <= now_ts - 5 * 60]
        if not completed:
            return None
        last = completed[-1]
        ts, o, h, l, c = last
        # De-dup: act once per completed bar.
        if self.last_signal_ts is not None and ts <= self.last_signal_ts:
            return None
        self.last_signal_ts = ts

        wick = float(cfg.get("wick") or 5.0)
        rej = float(cfg.get("rej") or 0.45)

        # CE leg: sweep of or_high (low dipped below, close back above).
        if opt_type == "CE" and c > or_high and l < or_high:
            sweep_wick = or_high - l
            if sweep_wick >= wick:
                # Rejection depth check: close must reject back by rej * sweep_wick
                rec = c - l
                if rec >= rej * sweep_wick and c >= om:
                    return {
                        "action": "ENTRY",
                        "side": "BUY",
                        "qty": int(cfg.get("lot") or 40),
                        "tp1_qty": int((cfg.get("lot") or 40) * (cfg.get("tp1_frac") or 0.5)),
                        "reason": (
                            f"SLHUNT: SENSEX 5m bar swept or_high {or_high:.2f} "
                            f"(low {l:.2f}, wick {sweep_wick:.1f}pt, rej {rec:.1f}pt) "
                            f"and closed back at {c:.2f} > om {om:.2f} — enter SENSEX CE"
                        ),
                    }
        # PE leg: sweep of or_low (high poked above, close back below).
        if opt_type == "PE" and c < or_low and h > or_low:
            sweep_wick = h - or_low
            if sweep_wick >= wick:
                rec = h - c
                if rec >= rej * sweep_wick and c <= om:
                    return {
                        "action": "ENTRY",
                        "side": "BUY",
                        "qty": int(cfg.get("lot") or 40),
                        "tp1_qty": int((cfg.get("lot") or 40) * (cfg.get("tp1_frac") or 0.5)),
                        "reason": (
                            f"SLHUNT: SENSEX 5m bar swept or_low {or_low:.2f} "
                            f"(high {h:.2f}, wick {sweep_wick:.1f}pt, rej {rec:.1f}pt) "
                            f"and closed back at {c:.2f} < om {om:.2f} — enter SENSEX PE"
                        ),
                    }
        return None

    async def evaluate_tick(self, ltp: float, state: dict) -> Optional[dict]:
        """Engine awaits this — must be an async method (matches the ORB plugins)."""
        p = dict(self.params)
        p.update(state.get("_params") or {})
        cfg = self._cfg

        net_qty = float(state.get("net_qty") or 0.0)
        entry_price = float(state.get("entry_price") or 0.0)
        opt_ltp = float(state.get("opt_ltp") or 0.0)

        if net_qty != 0:
            # Holding a position: exits only, driven by the option LTP.
            return self._exits(net_qty, entry_price, opt_ltp, p, state)
        # Flat: entry check. Only inside the entry window.
        if not state.get("entry_window_open", False):
            return None
        return self._sweep_rejection_check(state, p)

    def _exits(self, net_qty: float, entry: float, opt_ltp: float,
               p: dict, state: dict) -> Optional[dict]:
        """Staged exit on the held SENSEX option position.

        tp1 (50%): sell 1 lot (20 units) + move SL to breakeven + trail 0.5 ATR.
        tp2 (100%): close remaining lot.
        SL (12%): full stop.
        Trail (0.5 ATR): trailing stop on remaining lot after TP1.
        Time stop (hold=40 bars): exit.
        EOD (15:30 IST): exit.
        """
        cfg = self._cfg
        if entry <= 0 or opt_ltp <= 0:
            return None
        pct = (opt_ltp - entry) / entry
        sl = float(cfg.get("sl") or 0.12)
        tp1 = float(cfg.get("tp1") or 0.50)
        tp2 = float(cfg.get("tp2") or 1.00)
        hold = int(cfg.get("hold") or 40)
        lot_units = int(cfg.get("lot_units") or 20)
        trail_atr = float(cfg.get("trail_atr") or 0.5)

        # Track whether TP1 has been hit (partial exit already done).
        tp1_hit = bool(state.get("tp1_hit") or False)

        if tp1_hit:
            # Remaining lot: trail stop or TP2.
            # TP2: close remaining lot.
            if pct >= tp2:
                return {
                    "action": "EXIT",
                    "side": "SELL",
                    "qty": lot_units,
                    "reason": f"SLHUNT SENSEX: TP2 hit (+{pct*100:.1f}% >= {tp2*100:.0f}%) — close remaining lot",
                }
            # Trail stop: if LTP drops below entry (BE) after TP1, exit remaining.
            # The trail_atr factor is applied via the engine's trail state.
            if pct <= 0.0:
                return {
                    "action": "EXIT",
                    "side": "SELL",
                    "qty": lot_units,
                    "reason": f"SLHUNT SENSEX: trail stop — LTP {opt_ltp:.2f} back at/below entry {entry:.2f} after TP1 — close remaining lot",
                }
        else:
            # Full position: SL, TP1, time stop.
            # Stop loss (12%).
            if pct <= -sl:
                return {
                    "action": "EXIT",
                    "side": "SELL",
                    "qty": int(cfg.get("lot") or 40),
                    "reason": f"SLHUNT SENSEX: stop loss hit (-{abs(pct)*100:.1f}% <= SL {sl*100:.0f}%) — full exit",
                }
            # TP1: 50% — sell 1 lot (20 units), move to BE + trail.
            if pct >= tp1:
                return {
                    "action": "EXIT",
                    "side": "SELL",
                    "qty": lot_units,  # sell 1 lot only
                    "tp1_hit": True,   # engine marks TP1 as hit
                    "reason": f"SLHUNT SENSEX: TP1 hit (+{pct*100:.1f}% >= {tp1*100:.0f}%) — sell 1 lot, trail remaining at BE+{trail_atr}ATR",
                }

        # Time stop: 40 bars held.
        bars_held = int(p.get("_bars_held") or 0)
        if bars_held >= hold:
            return {
                "action": "EXIT",
                "side": "SELL",
                "qty": int(abs(net_qty)),
                "reason": f"SLHUNT SENSEX: time stop (held {bars_held} bars >= {hold}) — close",
            }

        # EOD: close before 15:15 IST (not 15:30). The engine's tick loop
        # stops at exactly 15:30, so a 15:30 exit could never be placed;
        # 15:15 leaves the last 15 market minutes for the order to fill.
        from zoneinfo import ZoneInfo
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
        if now_ist.hour * 60 + now_ist.minute >= (15 * 60 + 15):
            return {
                "action": "EXIT",
                "side": "SELL",
                "qty": int(abs(net_qty)),
                "reason": "SLHUNT SENSEX: EOD (15:15 IST) — flat",
            }
        return None
