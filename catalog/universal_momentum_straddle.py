class StrategyPlugin:
    def __init__(self, instance_id: str, params: dict):
        self.instance_id = instance_id
        self.params = params
        # Per-trade peak tracker kept in instance attributes, NOT in params,
        # so the supervisor param-sync cannot overwrite it mid-trade.
        self._highest_pnl_pct: float = 0.0
        self._tracked_entry_px: float = 0.0

    @staticmethod
    def _safe_float(value, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    async def evaluate_tick(self, ltp: float, state: dict) -> dict | None:
        sl_pct = self._safe_float(self.params.get('sl_pct', 50.0), 50.0)
        target_pct = self._safe_float(self.params.get('target_pct', 100.0), 100.0)
        qty = self._safe_float(self.params.get('qty', 1.0), 1.0)
        default_side = str(self.params.get('default_side', 'BUY')).upper()
        net_qty = self._safe_float(state.get('net_qty', 0), 0.0)
        entry_px = self._safe_float(state.get('entry_price', 0.0), 0.0)

        # Market entry window: weekdays only, 09:45:05–13:15:00 IST.
        from datetime import datetime as _dt, time as _dtime
        from zoneinfo import ZoneInfo as _ZI
        _now_ist = _dt.now(_ZI("Asia/Kolkata"))
        _market_open = _now_ist.weekday() < 5 and (_dtime(9, 45, 5) <= _now_ist.time() <= _dtime(13, 15, 0))

        # ENTRY: strictly flat + auto_enter + market open + one-trade guard
        if net_qty == 0 and self.params.get('auto_enter', False):
            if not _market_open:
                return None
            if self.params.get('trade_taken_today', False):
                return None
            or_high = self._safe_float(self.params.get('or_high', 0.0), 0.0)
            or_low = self._safe_float(self.params.get('or_low', 0.0), 0.0)
            if or_high > 0 and ltp > or_high and default_side == 'BUY':
                return {'action': 'ENTRY', 'side': 'BUY', 'qty': qty}
            elif or_low > 0 and ltp < or_low and default_side == 'SELL':
                return {'action': 'ENTRY', 'side': 'SELL', 'qty': qty}
            elif or_high == 0 and or_low == 0 and self.params.get('market_trigger', False):
                return {'action': 'ENTRY', 'side': default_side, 'qty': qty}

        # EXIT RULES: SL floor, target, breakeven lock, trailing, post-market guard
        if net_qty != 0 and entry_px > 0:
            total_cost = entry_px * abs(net_qty)

            # PnL: prefer broker unrealized PnL; fall back to LTP when the
            # broker value is implausibly small (dead/stale feed) and LTP is
            # in a realistic band.
            unrealized_raw = state.get('unrealized_pnl')
            ltp_band = total_cost > 0 and 0.20 * entry_px <= ltp <= 3.0 * entry_px
            if unrealized_raw is not None and total_cost > 0:
                pnl_pct = (self._safe_float(unrealized_raw, 0.0) / total_cost) * 100.0
                if abs(pnl_pct) < 0.01 and ltp_band:
                    if net_qty > 0:
                        pnl_pct = ((ltp - entry_px) / entry_px) * 100.0
                    else:
                        pnl_pct = ((entry_px - ltp) / entry_px) * 100.0
            else:
                pnl_pct = 0.0
                if ltp_band:
                    if net_qty > 0:
                        pnl_pct = ((ltp - entry_px) / entry_px) * 100.0
                    else:
                        pnl_pct = ((entry_px - ltp) / entry_px) * 100.0

            # Per-trade peak tracker (instance attribute, not self.params).
            if self._tracked_entry_px != entry_px:
                self._highest_pnl_pct = 0.0
                self._tracked_entry_px = entry_px
            if pnl_pct > self._highest_pnl_pct:
                self._highest_pnl_pct = pnl_pct
            highest_pnl = self._highest_pnl_pct
            close_side = 'SELL' if net_qty > 0 else 'BUY'

            # 1. Hard Stop-Loss
            if pnl_pct <= -sl_pct:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 2. Profit Target
            if pnl_pct >= target_pct:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 3. Breakeven Lock: once +10% peak, exit at 0% PnL.
            # Gate on a live premium so a dead 0.0 LTP (PnL frozen at 0.0) can't
            # fabricate the close of a position that has since lost money.
            be_at_pct = self._safe_float(self.params.get('be_at_pct', 10.0), 10.0)
            if highest_pnl >= be_at_pct and pnl_pct <= 0.0 and ltp > 0:
                return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 4. Trailing Stop: after +25% peak, exit when PnL drops 20% from peak.
            # Also gated on a live premium.
            trail_activation = self._safe_float(self.params.get('trail_activation', 25.0), 25.0)
            trail_buffer = self._safe_float(self.params.get('trail_buffer', 0.20), 0.20)
            if highest_pnl >= trail_activation and ltp > 0:
                trail_stop_pct = highest_pnl * (1.0 - trail_buffer)
                if pnl_pct <= trail_stop_pct:
                    return {'action': 'EXIT', 'side': close_side, 'qty': abs(net_qty)}

            # 5. Post-market guard: no exit dispatch after 15:15 IST on weekdays
            if _now_ist.weekday() < 5 and _now_ist.time() > _dtime(15, 15, 0):
                return None

        return None
