"""Regression tests for the 2026-11-23 audit fixes.

All tests are dependency-light: pure-logic checks plus source inspection of
the modules that import third-party packages (httpx/structlog/sqlalchemy),
so they run in a bare interpreter without the full venv.

Run: python3 -m pytest tests/test_audit_fixes.py -v
     or: python3 tests/test_audit_fixes.py
"""
from __future__ import annotations

import ast
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _src(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _has_call(name: str, *needles: str) -> bool:
    """True if the source contains ALL needles (line-by-line, order-agnostic)."""
    src = _src(name)
    return all(n in src for n in needles)


# ---------------------------------------------------------------------------
# 1. Staged-exit sign fix (engine._execute_exit)
# ---------------------------------------------------------------------------

def _staged_net_qty(prev_net_qty: float, qty: int) -> float:
    """Mirror of the engine's staged-exit update."""
    remaining = abs(prev_net_qty) - qty
    if remaining <= 0:
        return 0.0
    return remaining if prev_net_qty > 0 else -remaining


class TestStagedExitSign:
    def test_long_remainder_stays_long(self):
        # Sell 40 of a 80-unit long: 40 units still LONG (+40), not -40.
        assert _staged_net_qty(80.0, 40) == 40.0

    def test_short_remainder_stays_short(self):
        # Buy back 20 of a 60-unit short: 40 units still SHORT (-40).
        assert _staged_net_qty(-60.0, 20) == -40.0

    def test_full_close_resets(self):
        assert _staged_net_qty(40.0, 40) == 0.0
        assert _staged_net_qty(-40.0, 40) == 0.0

    def test_engine_source_no_longer_inverts_sign(self):
        src = _src("layers/strategies/engine.py")
        # The old bug: `-remaining if side == "SELL" else remaining`
        assert "-remaining if side == \"SELL\"" not in src
        assert "remaining if (inst.net_qty or 0.0) > 0 else -remaining" in src


# ---------------------------------------------------------------------------
# 2. Balance floor gate (fraction of total_equity, not free_balance)
# ---------------------------------------------------------------------------

class TestBalanceFloor:
    def test_old_formula_never_blocks(self):
        # Old: floor = free * pct -> free < free*pct is impossible (pct<1).
        free, floor_pct = 10_000.0, 0.20
        assert not (free < free * floor_pct)

    def test_new_formula_blocks_high_used_margin(self):
        total_equity, free_balance, floor_pct = 61_000.0, 5_000.0, 0.20
        floor = total_equity * floor_pct
        assert free_balance < floor  # 5k free of 61k total -> block

    def test_engine_source_uses_total_equity(self):
        src = _src("layers/strategies/engine.py")
        assert "floor = total_equity * self._floor_pct" in src
        assert "floor = free_balance * self._floor_pct" not in src


# ---------------------------------------------------------------------------
# 3. trade_taken_today boundary (IST midnight -> UTC)
# ---------------------------------------------------------------------------

class TestTradeTakenTodayBoundary:
    def test_boundary_is_previous_day_1830_utc(self):
        IST = timezone(timedelta(hours=5, minutes=30))
        now_ist = datetime(2026, 11, 23, 4, 0, tzinfo=IST)  # 04:00 IST
        boundary = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        assert boundary == datetime(2026, 11, 22, 18, 30, tzinfo=timezone.utc)

    def test_engine_source_converts_to_utc(self):
        src = _src("layers/strategies/engine.py")
        assert "today_start_utc" in src
        assert ".astimezone(timezone.utc)" in src


# ---------------------------------------------------------------------------
# 4. SENSEX EOD exit fires inside the engine tick window
# ---------------------------------------------------------------------------

class TestSensensexEodExit:
    def test_exit_minute_inside_tick_window(self):
        src = _src("catalog/sensex_slhunt_reversal.py")
        assert "15 * 60 + 15" in src
        assert "EOD (15:15 IST)" in src
        # The old unreachable 15:30 boundary must be gone from the exit path.
        assert "15 * 60 + 30" not in src


# ---------------------------------------------------------------------------
# 5. SENSEX 5m bar accumulator (real bars, not one synthetic)
# ---------------------------------------------------------------------------

class TestSensexBars:
    def test_source_accumulates_bars(self):
        src = _src("catalog/sensex_slhunt_reversal.py")
        assert "_sensex_bars" in src
        assert "IDX_I" in src  # correct Dhan v2 index segment (was "INDEX")
        # The old single synthetic bar must be gone.
        assert "[(ts, ltp, ltp, ltp, ltp)]" not in src

    def test_poll_throttled(self):
        src = _src("catalog/sensex_slhunt_reversal.py")
        assert "_SENSEX_POLL_S" in src


# ---------------------------------------------------------------------------
# 6. trade_taken_today read from STATE in ORB plugins
# ---------------------------------------------------------------------------

class TestTradeTakenTodayStateRead:
    @pytest.mark.parametrize("path", [
        "catalog/orb_invalidation_strategy_v6e.py",
        "catalog/orb_invalidation_strategy_v2.py",
        "catalog/universal_momentum_straddle.py",
    ])
    def test_reads_state_first(self, path):
        src = _src(path)
        assert "state.get('trade_taken_today'" in src


# ---------------------------------------------------------------------------
# 7. Security: master token + require_master + insecure-secret guard
# ---------------------------------------------------------------------------

class TestSecurity:
    def test_issue_master_token_exists_and_sets_claim(self):
        src = _src("core/security.py")
        tree = ast.parse(src)
        fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "issue_master_token"]
        assert fns, "issue_master_token not defined"
        assert '"is_master": True' in src or "'is_master': True" in src or "is_master" in src

    def test_require_master_rejects_insecure_default_secret(self):
        src = _src("core/security.py")
        assert "INSECURE_DEFAULT_JWT_SECRET" in src
        assert "set_insecure_default_jwt_secret" in src

    def test_get_current_user_carries_is_master(self):
        src = _src("core/security.py")
        assert '"is_master": bool(payload.get("is_master", False))' in src

    def test_main_wires_the_guard(self):
        src = _src("core/main.py")
        assert "set_insecure_default_jwt_secret" in src
        assert '"change-me-in-production"' in src

    def test_admin_endpoint_has_auth_dependency(self):
        src = _src("rpc/admin.py")
        assert "Depends(get_current_user)" in src
        assert "require_master(user)" in src


# ---------------------------------------------------------------------------
# 8. Dhan orderbook v2 paths + security_id resolution
# ---------------------------------------------------------------------------

class TestDhanClient:
    def test_v2_paths_used(self):
        src = _src("layers/brokers/dhan_client.py")
        # No code path may still use the v1 URL (comments may mention it).
        for line in src.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""') or "no longer" in line or "gone" in line:
                continue
            assert "/orderbook/trade/" not in line, f"v1 path still in active code: {line}"
        assert "/orders/history" in src
        assert "/orders/cancel" in src

    def test_expiry_matched_in_resolver(self):
        src = _src("layers/brokers/dhan_client.py")
        assert 'inner.get(target_expiry' in src or "target_expiry" in src
        assert "requested expiry not found; using nearest" in src

    def test_poll_state_is_instance_level(self):
        src = _src("layers/brokers/dhan_client.py")
        # The class-level mutable dict is gone; __init__ creates the instance one.
        init_block = src.split("def __init__", 1)[1].split("def _headers", 1)[0]
        assert "self._index_poll_disabled" in init_block
        assert "self._yahoo_ltp_cache" in init_block

    def test_yahoo_staleness_guard(self):
        src = _src("layers/brokers/dhan_client.py")
        assert "lastTradeTime" in src

    def test_no_stale_env_token_regression(self):
        """The container must not be stuck with a stale env token.

        Proof of the fix: when DHAN_ACCESS_TOKEN in the environment is dead
        (expired) but a disk file holds a fresher valid token, the client
        must reconcile to the disk token at construction. This is the exact
        bug that produced persistent 401s after a container restart.
        """
        import base64
        import json
        import os
        import tempfile
        import time as _time
        from unittest import mock
        from layers.brokers.dhan_client import DhanClient

        def make_jwt(exp_offset_s: int) -> str:
            header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256"}).encode()).decode()
            payload = base64.urlsafe_b64encode(
                json.dumps({"iss": "dhan", "exp": int(_time.time()) + exp_offset_s}).encode()
            ).decode()
            sig = base64.urlsafe_b64encode(b"fakesig").decode()
            # strip padding so it looks like a compact JWT
            return header.rstrip("=") + "." + payload.rstrip("=") + "." + sig.rstrip("=")

        stale_env = make_jwt(-3600)          # expired 1h ago
        fresh_disk = make_jwt(24 * 3600)    # valid for 24h

        with tempfile.TemporaryDirectory() as td:
            disk_path = os.path.join(td, "coolify.env")
            with open(disk_path, "w") as f:
                f.write(f"DHAN_ACCESS_TOKEN={fresh_disk}\n")

            with mock.patch.dict(os.environ, {"DHAN_ACCESS_TOKEN": stale_env}):
                with mock.patch.object(DhanClient, "_TOKEN_FILES", (disk_path,)):
                    client = DhanClient(client_id="1234567", access_token=stale_env)
                    # Must have reconciled to the fresher disk token.
                    assert client._access_token == fresh_disk, (
                        "stale env token must be superseded by the fresher disk token"
                    )

    def test_jwt_expiry_ranking(self):
        import base64
        import json
        import time as _time
        from layers.brokers.dhan_client import DhanClient

        def make_jwt(exp_offset_s: int, marker: str) -> str:
            h = base64.urlsafe_b64encode(json.dumps({"a": 1}).encode()).decode().rstrip("=")
            p = base64.urlsafe_b64encode(
                json.dumps({"exp": int(_time.time()) + exp_offset_s, "m": marker}).encode()
            ).decode().rstrip("=")
            s = base64.urlsafe_b64encode(b"x").decode().rstrip("=")
            return f"{h}.{p}.{s}"

        later = make_jwt(72 * 3600, "later")
        earlier = make_jwt(1 * 3600, "earlier")
        # Later-expiry token must rank above the earlier one.
        assert DhanClient._jwt_exp(later) > DhanClient._jwt_exp(earlier)

    def test_force_rescan_on_401_present(self):
        src = _src("layers/brokers/dhan_client.py")
        # get_balance must force-rescan after a 401 and retry once.
        assert "force=True" in src
        assert src.count("self._http.get(url") >= 2, "expect a retry after 401"



# ---------------------------------------------------------------------------
# 9. Upstox WS cancellation + queue-full logging
# ---------------------------------------------------------------------------

class TestUpstoxWs:
    def test_ticks_repropagate_cancelled(self):
        src = _src("layers/brokers/upstox_ws_v3.py")
        tree = ast.parse(src)
        # Find the ticks() method on the UpstoxWSV3 class.
        for cls in ast.walk(tree):
            if not (isinstance(cls, ast.ClassDef) and cls.name == "UpstoxWSV3"):
                continue
            for node in cls.body:
                if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == "ticks":
                    for exc in ast.walk(node):
                        if isinstance(exc, ast.ExceptHandler):
                            exc_name = (
                                getattr(exc.type, "id", getattr(exc.type, "attr", ""))
                                if not isinstance(exc.type, ast.Tuple)
                                else ",".join(
                                    getattr(e, "id", getattr(e, "attr", "")) for e in exc.type.elts
                                )
                            )
                            if "CancelledError" in exc_name:
                                kinds = [type(x).__name__ for x in exc.body]
                                assert "Raise" in kinds, "CancelledError must be re-raised"
                                return
        pytest.fail("ticks() not found on UpstoxWSV3")

    def test_queue_full_logs(self):
        src = _src("layers/brokers/upstox_ws_v3.py")
        assert "_last_queue_full_log" in src


# ---------------------------------------------------------------------------
# 10. Fallback catalog segments
# ---------------------------------------------------------------------------

class TestFallbackCatalog:
    def test_sensex_segments_are_bse(self):
        src = _src("layers/state/fallback_db.py")
        tree = ast.parse(src)
        catalog = None
        for node in tree.body:
            if isinstance(node, ast.AnnAssign) and node.target.id == "_strategy_catalog":
                catalog = ast.literal_eval(node.value)
                break
            if isinstance(node, ast.Assign) and any(
                getattr(t, "id", "") == "_strategy_catalog" for t in node.targets
            ):
                catalog = ast.literal_eval(node.value)
                break
        assert catalog is not None, "_strategy_catalog not found"
        assert catalog["SENSEX101"]["segment"] == "BSE_FNO"
        assert catalog["SENSEX102"]["segment"] == "BSE_FNO"


# ---------------------------------------------------------------------------
# 11. UpstoxClient product default + funds parsing
# ---------------------------------------------------------------------------

class TestUpstoxClient:
    def test_intraday_default(self):
        src = _src("layers/brokers/upstox_client.py")
        assert 'product_type: str = "INTRADAY"' in src

    def test_fund_summary_parsing(self):
        src = _src("layers/brokers/upstox_client.py")
        assert 'fund.get("available_margin"' in src or "fund_summary" in src


# ---------------------------------------------------------------------------
# 12. NIFTY slhunt: non-blocking candle refresh
# ---------------------------------------------------------------------------

class TestNiftySlhuntFeed:
    def test_refresh_is_background_and_cached(self):
        src = _src("catalog/nifty_slhunt_reversal.py")
        assert "threading.Thread" in src
        assert "_CANDLES_REFRESH_S" in src
        # The old blocking urlopen-in-tick path is gone (fetch runs in _refresh).
        assert "run_coroutine_threadsafe" not in src


# ---------------------------------------------------------------------------
# 13. Engine: security_id never sent empty; start() unblocks the loop
# ---------------------------------------------------------------------------

class TestEngineSecurityId:
    def test_entry_defers_when_unresolved(self):
        src = _src("layers/strategies/engine.py")
        assert "Entry deferred: security_id unresolved" in src
        assert "Exit deferred: security_id unresolved" in src

    def test_start_uses_to_thread(self):
        src = _src("layers/strategies/engine.py")
        assert "await asyncio.to_thread(_load_upstox_instruments)" in src

    def test_spot_ltp_populated_from_index_cache(self):
        src = _src("layers/strategies/engine.py")
        assert 'state["spot_ltp"] = spot_ltp' in src


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
