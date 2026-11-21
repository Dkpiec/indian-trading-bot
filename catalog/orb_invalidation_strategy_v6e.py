class StrategyPlugin:
    def __init__(self, instance_id: str, params: dict):
        self.instance_id = instance_id
        self.params = params
        # Per-trade peak tracker kept in instance attributes, NOT in params.
        # If stored in self.params the supervisor param-sync can overwrite the
        # peak tracker mid-trade, silently breaking the trail and BE lock.
        self._highest_pnl_pct: float = 0.0
        self._tracked_entry_px: float = 0.0

    @staticmethod
    def _safe_float(value, default: float) -> float:
        """Coerce a possibly-empty-string/None param to float (default on failure).

        The supervisor writes option_ltp_ref='' to force a fresh LTP capture
        (main.py recapture path), so a bare float() on the raw value would
        raise ValueError and crash evaluate_tick for the whole instance.
        """
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    async def evaluate_tick(self, ltp: float, state: dict) -> dict | None:
        # v6e params (from sweep winner): SL 60%, TGT_W 120%, TGT_M 75%, TRAIL_ACT 35%, TRAIL_BUF 0.25, BE_AT 8%, DISPLACE 0.15
        # Every numeric read goes through _safe_float: a present-but-None param
        # (JSON null, or the recapture path's option_ltp_ref='') must not raise
        # and silently disable risk management for the whole instance.
        p = self.params
        dte = int(self._safe_float(p.get('dte'), 5))
        
        # DTE-Adaptive Regimes (v6e uses same targets as v5/v6b for monthly vs weekly)
        if dte <= 6:
            sl_pct = self._safe_float(p.get('sl_pct'), 50.0)  # 60 -> 50 (2026-11 sweep: SL50 median PF 10.7 > SL60; every top-10 config = SL50)
            target_pct = self._safe_float(p.get('target_pct'), 0.0) or 125.0  # 120 -> 125 (sweep weekly target optimum)
        else:
            sl_pct = self._safe_float(p.get('sl_pct'), 50.0)  # 60 -> 50
            target_pct = self._safe_float(p.get('target_pct'), 100.0)  # intraday: 150 was too aggressive (dead ceiling, TGT rarely fills on a monthly premium intraday); 100 is reachable-ish; trail+BE do the real work

        # Default to 1 lot (not 30): see v2 — the engine's lot-clamp is the sole
        # sizing authority; per-instance qty is set explicitly by the arm script.
        qty = self._safe_float(p.get('qty'), 1.0)
        default_side = str(p.get('default_side') or 'BUY').upper()
        trigger_type = str(self.params.get('trigger_type', 'BREAKOUT')).upper()

        # v6e exit params
        trail_activation = 40.0  # 35 -> 40 (2026-11 sweep: wider activation = let winners run; doubles PF)
        trail_buffer = 0.30  # 0.25 -> 0.30 (tighter stick to high-water mark)
        be_at = 10.0  # 8 -> 10 (lock profit earlier, cut max-consec-loss)

        # 1. ENTRY LOGIC
        if state['net_qty'] == 0 and self.params.get('auto_enter', False):
            # Check market entry time window: Strictly 09:45:05 to 13:15:00 IST on weekdays
            # (the supervisor can override via the entry_window_open state key so the
            # 15:15-09:44 post-market re-entry guard stays in effect; when that key is
            # absent the wall-clock check is the fallback).
            from datetime import datetime, time as dtime
            from zoneinfo import ZoneInfo
            if "entry_window_open" not in state:
                now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
                if now_ist.weekday() >= 5 or not (dtime(9, 45, 5) <= now_ist.time() <= dtime(13, 15, 0)):
                    return None
            elif not state.get("entry_window_open", False):
                return None

            # Check 1 trade per day invariant
            if self.params.get('trade_taken_today', False):
                return None

            # OR Width Regime Filter (v6e: wider band [0.10%, 1.10%])
            if self.params.get('regime_filter_failed', False):
                return None

            or_high = self._safe_float(p.get('or_high'), 0.0)
            or_low = self._safe_float(p.get('or_low'), 0.0)
            or_width = or_high - or_low if (or_high > 0 and or_low > 0) else 0.0
            displacement_buffer = 0.20 * or_width  # 0.25 -> 0.20 (2026-11 sweep: every top-10 config = 0.20; 0.25 over-selective)

            # Entry side consistency guard (same as v2): a CE leg only trades
            # BREAKOUT, a PE leg only BREAKDOWN — a stale armed trigger_type
            # against the wrong option_type would "break out" the wrong side.
            option_type = str(p.get('option_type') or '').upper()
            if trigger_type == 'BREAKOUT' and option_type and option_type == 'PE':
                return None
            if trigger_type == 'BREAKDOWN' and option_type and option_type == 'CE':
                return None

            # F2: minimum-premium floor — a near-expiry / near-zero-premium option has
            # no meaningful edge and can't clear the Rule-2 ±10% bracket honestly. Skip
            # entry when the option reference premium is below the floor (default 5.0,
            # overridable via the min_premium param). Defense-in-depth: this closes the
            # gap where option_ltp_ref==0 lets the supervisor's Rule-2 bracket through.
            min_premium = self._safe_float(p.get('min_premium'), 5.0)
            ref_prem = self._safe_float(self.params.get('option_ltp_ref', 0.0), 0.0)
            if min_premium > 0 and ref_prem > 0 and ref_prem < min_premium:
                return None

            # Bullish Breakout (CE Entry): Spot > OR_HIGH + (0.15 * OR_WIDTH)
            if trigger_type == 'BREAKOUT':
                entry_level = or_high + displacement_buffer
                if or_high > 0 and ltp > entry_level:
                    return {'action': 'ENTRY', 'side': default_side, 'qty': qty}

            # Bearish Breakdown (PE Entry): Spot < OR_LOW - (0.15 * OR_WIDTH)
            elif trigger_type == 'BREAKDOWN':
                entry_level = or_low - displacement_buffer
                if or_low > 0 and ltp < entry_level:
                    return {'action': 'ENTRY', 'side': default_side, 'qty': qty}

        # 2. EXIT LOGIC & RISK MANAGEMENT
        if state['net_qty'] != 0 and state['entry_price'] > 0:
            entry_px = state['entry_price']
            net_qty = abs(state['net_qty'])
            total_cost = entry_px * net_qty
            # Close-side helper: long positions close with SELL, short with BUY.
            close_side = 'SELL' if state['net_qty'] > 0 else 'BUY'

            # Primary: Compute exact PnL % from broker unrealized PnL
            if 'unrealized_pnl' in state and total_cost > 0:
                pnl_pct = (state['unrealized_pnl'] / total_cost) * 100.0
            else:
                pnl_pct = 0.0

            # Fallback: a broker feed that reports a tiny non-zero but stale PnL
            # (e.g. +0.005% off a dead mark price) would otherwise leave the
            # SL/target/trail/BE stops operating on a bad number. Use a band
            # (abs < 0.01%) instead of an exact-zero check — matching v2 — so any
            # implausibly-small PnL falls through to the LTP-derived value when
            # the LTP is inside a realistic option-premium range.
            # SAFETY: a 0.0 LTP (rate-limited feed, dead contract) must never
            # fabricate a -100% PnL and fire a bogus stop-loss exit.
            if abs(pnl_pct) < 0.01:
                if 0.20 * entry_px <= ltp <= 3.0 * entry_px:
                    if state['net_qty'] > 0:
                        pnl_pct = ((ltp - entry_px) / entry_px) * 100.0
                    else:
                        pnl_pct = ((entry_px - ltp) / entry_px) * 100.0
                elif ltp > 0:
                    pnl_pct = 0.0

            # 2.1 Hard Stop Loss (v6e: 60%)
            if pnl_pct <= -sl_pct:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 2.2 Take Profit Target (v6e: 120% weekly / 75% monthly)
            if pnl_pct >= target_pct:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 2.3 Dynamic Trailing Stop-Loss (v6e: activate at +35%, buffer 25% of peak)
            # Per-trade peak tracker in instance attributes; reset on new entry_px.
            entry_px_key = entry_px
            if self._tracked_entry_px != entry_px_key:
                self._highest_pnl_pct = 0.0
                self._tracked_entry_px = entry_px_key

            if pnl_pct > self._highest_pnl_pct:
                self._highest_pnl_pct = pnl_pct
            highest_pnl = self._highest_pnl_pct

            # Breakeven lock: once trade reaches +8%, stop moves to entry (0% PnL).
            # SAFETY: only honor a 0.0 PnL readout when a live premium is available.
            if highest_pnl >= be_at and pnl_pct <= 0.0 and ltp > 0:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # Continuous trail: after +35% peak, exit when PnL drops 25% from peak
            if highest_pnl >= trail_activation:
                trail_stop_pct = highest_pnl * (1.0 - trail_buffer)
                if pnl_pct <= trail_stop_pct:
                    return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 2.4 Midpoint Invalidation Stop (v6e: off when or_mid is un-armed; when or_mid is
            # armed it defaults to a DTE-adaptive buffer — the wide 25-pt monthly buffer is
            # meaningless on a high-gamma short-dated option, where 25 pts can be the whole
            # premium and the rule silently never fires. An explicit mid_inval_pts overrides).
            or_mid = self._safe_float(p.get('or_mid'), 0.0)
            spot_ltp = float(state.get('spot_ltp') or 0.0)
            or_high = self._safe_float(p.get('or_high'), 0.0)
            or_low = self._safe_float(p.get('or_low'), 0.0)
            or_width = or_high - or_low if (or_high > 0 and or_low > 0) else 0.0
            mid_inval_pts = self._safe_float(p.get('mid_inval_pts'), 0.0)
            if or_mid > 0 and mid_inval_pts <= 0:
                # DTE-adaptive default: ~2.5% of OR width (min 5 pts) for high-gamma
                # short-dated; 25 pts for the monthly regime.
                mid_inval_pts = max(0.025 * or_width, 5.0) if dte <= 6 else 25.0
            if or_mid > 0 and spot_ltp > 0 and mid_inval_pts > 0:
                if trigger_type == 'BREAKOUT' and spot_ltp <= (or_mid - mid_inval_pts):
                    return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}
                elif trigger_type == 'BREAKDOWN' and spot_ltp >= (or_mid + mid_inval_pts):
                    return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 2.5 Post-market exit guard: never fire exit orders after 15:15 IST
            # When the market is closed the broker rejects the order and the
            # strategy stays stranded; the broker's own EOD square-off handles it.
            from datetime import datetime as _dt, time as _dtime
            from zoneinfo import ZoneInfo as _ZI
            _now_ist = _dt.now(_ZI("Asia/Kolkata"))
            if _now_ist.weekday() < 5 and _now_ist.time() > _dtime(15, 15, 0):
                return None

        return None