class StrategyPlugin:
    def __init__(self, instance_id: str, params: dict):
        self.instance_id = instance_id
        self.params = params
        # Mutable per-trade state kept in instance attributes, NOT in params,
        # to prevent accidental leakage between trades or serialization issues.
        self._highest_pnl_pct: float = 0.0
        self._tracked_entry_px: float = 0.0

    @staticmethod
    def _safe_float(value, default: float) -> float:
        """Parse a float from config/state; return default on any failure."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    async def evaluate_tick(self, ltp: float, state: dict) -> dict | None:
        qty = self._safe_float(self.params.get('qty', 1.0), 1.0)
        default_side = str(self.params.get('default_side') or 'BUY').upper()
        trigger_type = str(self.params.get('trigger_type') or 'BREAKOUT').upper()

        net_qty = self._safe_float(state.get('net_qty', 0), 0.0)
        entry_px = self._safe_float(state.get('entry_price', 0.0), 0.0)

        # --- ENTRY LOGIC (only when flat and auto_enter enabled) ---
        # Market entry window: weekdays only, 09:45:05–13:15:00 IST (ORB session).
        from datetime import datetime as _dt, time as _dtime
        from zoneinfo import ZoneInfo as _ZI
        _now_ist = _dt.now(_ZI("Asia/Kolkata"))
        _market_open = _now_ist.weekday() < 5 and (_dtime(9, 45, 5) <= _now_ist.time() <= _dtime(13, 15, 0))

        if net_qty == 0 and self.params.get('auto_enter', False):
            if not _market_open:
                return None
            # One trade per session per strategy (set by the arming script / dispatch).
            # 1 trade per day: read from STATE (the engine sets it there),
            # with the static param as a legacy fallback.
            if state.get('trade_taken_today', self.params.get('trade_taken_today', False)):
                return None
            or_high = self._safe_float(self.params.get('or_high') or state.get('or_high', 0.0), 0.0)
            or_low = self._safe_float(self.params.get('or_low') or state.get('or_low', 0.0), 0.0)

            if trigger_type == 'BREAKDOWN':
                if or_low > 0 and ltp < or_low:
                    return {'action': 'ENTRY', 'side': default_side, 'qty': qty}
            elif trigger_type == 'BREAKOUT':
                if or_high > 0 and ltp > or_high:
                    return {'action': 'ENTRY', 'side': default_side, 'qty': qty}
            elif or_high > 0 and ltp > or_high and default_side == 'BUY':
                return {'action': 'ENTRY', 'side': 'BUY', 'qty': qty}
            elif or_low > 0 and ltp < or_low and default_side == 'SELL':
                return {'action': 'ENTRY', 'side': 'SELL', 'qty': qty}
            elif or_high == 0 and or_low == 0 and self.params.get('market_trigger', False):
                return {'action': 'ENTRY', 'side': default_side, 'qty': qty}

        # --- v6e-STYLE EXIT RULES (single-pass param parsing) ---
        sl_pct = self._safe_float(self.params.get('sl_pct', 30.0), 30.0)
        target_pct = self._safe_float(self.params.get('target_pct', 75.0), 75.0)
        be_at_pct = self._safe_float(self.params.get('be_at_pct', 8.0), 8.0)
        trail_activation = self._safe_float(self.params.get('trail_activation', 25.0), 25.0)
        trail_buffer = self._safe_float(self.params.get('trail_buffer', 0.20), 0.20)

        # Reset per-trade peak tracker when position is flat
        if net_qty == 0:
            self._highest_pnl_pct = 0.0
            self._tracked_entry_px = 0.0
            return None

        # No valid entry price — cannot compute PnL
        if entry_px <= 0:
            return None

        abs_net_qty = abs(net_qty)
        total_cost = entry_px * abs_net_qty

        # Compute pnl_pct: prefer unrealized_pnl from state; if that value is
        # implausibly small (abs < 0.01%, i.e. a dead/stale mark feed reporting
        # ~0) AND the LTP is inside a realistic option-premium band, fall back to
        # the LTP-derived PnL so a real 20%+ loss is not frozen at 0% (the
        # 2026-11-09 SENSEX CE stranding class of bug). Matches v2/v6e.
        unrealized_raw = state.get('unrealized_pnl')
        ltp_band = total_cost > 0 and 0.20 * entry_px <= ltp <= 3.0 * entry_px
        if unrealized_raw is not None and total_cost > 0:
            unrealized = self._safe_float(unrealized_raw, 0.0)
            pnl_pct = (unrealized / total_cost) * 100.0
            if abs(pnl_pct) < 0.01 and ltp_band:
                if net_qty > 0:
                    pnl_pct = ((ltp - entry_px) / entry_px) * 100.0
                else:
                    pnl_pct = ((entry_px - ltp) / entry_px) * 100.0
        else:
            # No unrealized_pnl key at all — derive from LTP when in band.
            if ltp_band:
                if net_qty > 0:
                    pnl_pct = ((ltp - entry_px) / entry_px) * 100.0
                else:
                    pnl_pct = ((entry_px - ltp) / entry_px) * 100.0
            else:
                pnl_pct = 0.0

        # Per-trade peak tracking: keyed by entry_px AND reset on new trade detection
        if self._tracked_entry_px != entry_px:
            self._highest_pnl_pct = 0.0
            self._tracked_entry_px = entry_px

        if pnl_pct > self._highest_pnl_pct:
            self._highest_pnl_pct = pnl_pct

        highest_pnl = self._highest_pnl_pct

        # 1. Hard Stop-Loss
        if pnl_pct <= -sl_pct:
            return {'action': 'EXIT', 'side': 'SELL' if net_qty > 0 else 'BUY', 'qty': abs_net_qty}

        # 2. Profit Target
        if pnl_pct >= target_pct:
            return {'action': 'EXIT', 'side': 'SELL' if net_qty > 0 else 'BUY', 'qty': abs_net_qty}

        # 3. Breakeven Lock: once trade reaches +be_at_pct, stop moves to entry (0% PnL).
        # Requires a live premium (ltp > 0): a dead 0.0 LTP that freezes PnL at 0.0
        # must not trigger the breakeven close of a position that has since lost money.
        if highest_pnl >= be_at_pct and pnl_pct <= 0.0 and ltp > 0:
            return {'action': 'EXIT', 'side': 'SELL' if net_qty > 0 else 'BUY', 'qty': abs_net_qty}

        # 4. Continuous Trailing Stop: after +trail_activation peak, exit when PnL drops trail_buffer from peak.
        # Also gated on a live premium so a stale-zero LTP readout can't fabricate the exit.
        if highest_pnl >= trail_activation and ltp > 0:
            trail_stop_pct = highest_pnl * (1.0 - trail_buffer)
            if pnl_pct <= trail_stop_pct:
                return {'action': 'EXIT', 'side': 'SELL' if net_qty > 0 else 'BUY', 'qty': abs_net_qty}

        # 5. Post-market exit guard: never dispatch EXIT orders after 15:15 IST on
        #    weekdays — the broker rejects them and the strategy strands at
        #    SQUARED_OFF with a locked-in loss. The broker's own EOD square-off
        #    (15:15:00 IST) closes the position.
        if _now_ist.weekday() < 5 and _now_ist.time() > _dtime(15, 15, 0):
            return None

        return None