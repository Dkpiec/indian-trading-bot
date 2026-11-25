"""Positions, P&L, fills, orders, stop-loss, kill-all, and log endpoints.

These endpoints back the Telegram bot commands that are forwarded from the
V0 engine's TG handler. Each queries the DB models or the Dhan client for
live data.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()

log = logging.getLogger(__name__)


def _session(request: Request):
    return getattr(request.app.state, "async_session_factory", None)


def _dhan(request: Request):
    return getattr(request.app.state, "dhan_client", None)


# ---------------------------------------------------------------------------
# /api/v1/positions — open positions from StrategyInstance
# ---------------------------------------------------------------------------
@router.get("/positions")
async def get_positions(request: Request):
    """List open positions (StrategyInstance rows with net_qty != 0)."""
    from layers.state.models import StrategyInstance, StrategyCatalog
    from sqlalchemy import select

    session_factory = _session(request)
    if not session_factory:
        return {"positions": [], "note": "DB unavailable"}

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(StrategyInstance, StrategyCatalog)
                .join(StrategyCatalog, StrategyInstance.catalog_id == StrategyCatalog.id, isouter=True)
                .where(StrategyInstance.net_qty != 0)
                .order_by(StrategyInstance.updated_at.desc())
            )
            rows = result.all()
            positions = [
                {
                    "instance_id": inst.instance_id,
                    "catalog_id": inst.catalog_id,
                    "display_name": cat.display_name if cat else inst.catalog_id,
                    "status": inst.status,
                    "net_qty": inst.net_qty,
                    "entry_price": inst.entry_price,
                    "last_ltp": inst.last_ltp,
                    "realized_pnl": inst.realized_pnl,
                    "unrealized_pnl": inst.unrealized_pnl,
                    "updated_at": inst.updated_at.isoformat() if inst.updated_at else None,
                }
                for inst, cat in rows
            ]
        return {"positions": positions, "count": len(positions)}
    except Exception as e:
        log.warning("positions query failed: %s", e)
        return {"positions": [], "error": str(e)}


# ---------------------------------------------------------------------------
# /api/v1/profit — P&L summary
# ---------------------------------------------------------------------------
@router.get("/profit")
async def get_profit(request: Request):
    """Unrealized + realized P&L summary across all strategy instances."""
    from layers.state.models import StrategyInstance, DailyPnl
    from sqlalchemy import select, func

    session_factory = _session(request)
    if not session_factory:
        return {"unrealized": 0.0, "realized": 0.0, "note": "DB unavailable"}

    try:
        async with session_factory() as session:
            # All-time P&L from strategy instances
            r = await session.execute(
                select(
                    func.coalesce(func.sum(StrategyInstance.unrealized_pnl), 0.0).label("unrealized"),
                    func.coalesce(func.sum(StrategyInstance.realized_pnl), 0.0).label("realized"),
                    func.count(StrategyInstance.instance_id).label("instances"),
                )
            )
            unreal, realized, count = r.one()

            # Today's P&L from DailyPnl (most recent row per user, summed)
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            r2 = await session.execute(
                select(
                    func.coalesce(func.sum(DailyPnl.realized_pnl), 0.0).label("realized"),
                    func.coalesce(func.sum(DailyPnl.unrealized_pnl), 0.0).label("unrealized"),
                    func.coalesce(func.sum(DailyPnl.total_trades), 0).label("trades"),
                ).where(DailyPnl.trade_date >= today_start)
            )
            today_realized, today_unrealized, today_trades = r2.one()

        return {
            "total_unrealized": float(unreal),
            "total_realized": float(realized),
            "active_instances": int(count),
            "today_realized": float(today_realized),
            "today_unrealized": float(today_unrealized),
            "today_trades": int(today_trades),
        }
    except Exception as e:
        log.warning("profit query failed: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# /api/v1/fills — fill history from TradeOrderLedger
# ---------------------------------------------------------------------------
@router.get("/fills")
async def get_fills(request: Request, limit: int = Query(20, le=200)):
    """Recent fills (TradeOrderLedger rows with status=FILLED)."""
    from layers.state.models import TradeOrderLedger
    from sqlalchemy import select

    session_factory = _session(request)
    if not session_factory:
        return {"fills": [], "note": "DB unavailable"}

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(TradeOrderLedger)
                .where(TradeOrderLedger.status == "FILLED")
                .order_by(TradeOrderLedger.created_at.desc())
                .limit(limit)
            )
            fills = [
                {
                    "order_id": f.order_id,
                    "symbol": f.symbol,
                    "side": f.side,
                    "quantity": f.filled_quantity,
                    "price": f.average_price,
                    "pnl": f.pnl,
                    "created_at": f.created_at.isoformat() if f.created_at else None,
                }
                for f in result.scalars().all()
            ]
        return {"fills": fills, "count": len(fills)}
    except Exception as e:
        log.warning("fills query failed: %s", e)
        return {"fills": [], "error": str(e)}


# ---------------------------------------------------------------------------
# /api/v1/orders — recent orders from TradeOrderLedger
# ---------------------------------------------------------------------------
@router.get("/orders")
async def get_orders(request: Request, limit: int = Query(10, le=200)):
    """Recent orders (all statuses) from TradeOrderLedger."""
    from layers.state.models import TradeOrderLedger
    from sqlalchemy import select

    session_factory = _session(request)
    if not session_factory:
        return {"orders": [], "note": "DB unavailable"}

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(TradeOrderLedger)
                .order_by(TradeOrderLedger.created_at.desc())
                .limit(limit)
            )
            orders = [
                {
                    "order_id": o.order_id,
                    "symbol": o.symbol,
                    "side": o.side,
                    "quantity": o.quantity,
                    "price": o.price,
                    "status": o.status,
                    "filled_quantity": o.filled_quantity,
                    "pnl": o.pnl,
                    "created_at": o.created_at.isoformat() if o.created_at else None,
                }
                for o in result.scalars().all()
            ]
        return {"orders": orders, "count": len(orders)}
    except Exception as e:
        log.warning("orders query failed: %s", e)
        return {"orders": [], "error": str(e)}


# ---------------------------------------------------------------------------
# /api/v1/stop-loss — per-position stop-losses
# ---------------------------------------------------------------------------
@router.get("/stop-loss")
async def get_stop_loss(request: Request):
    """Per-position stop-loss levels (derived from StrategyInstance params)."""
    from layers.state.models import StrategyInstance
    from sqlalchemy import select

    session_factory = _session(request)
    if not session_factory:
        return {"stops": [], "note": "DB unavailable"}

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(StrategyInstance)
                .where(StrategyInstance.net_qty != 0)
                .order_by(StrategyInstance.updated_at.desc())
            )
            stops = []
            for inst in result.scalars().all():
                params = inst.parameters or {}
                sl = params.get("stop_loss") or params.get("sl")
                sl_pct = params.get("stop_loss_pct") or params.get("sl_pct")
                stops.append({
                    "instance_id": inst.instance_id,
                    "catalog_id": inst.catalog_id,
                    "net_qty": inst.net_qty,
                    "entry_price": inst.entry_price,
                    "last_ltp": inst.last_ltp,
                    "stop_loss": sl,
                    "stop_loss_pct": sl_pct,
                    "params_keys": list(params.keys()),
                })
        return {"stops": stops, "count": len(stops)}
    except Exception as e:
        log.warning("stop-loss query failed: %s", e)
        return {"stops": [], "error": str(e)}


# ---------------------------------------------------------------------------
# /api/v1/strategies/kill-all — close all open positions
# ---------------------------------------------------------------------------
@router.post("/strategies/kill-all")
async def kill_all(request: Request):
    """Close ALL open positions immediately (force-exit every instance)."""
    from layers.state.models import StrategyInstance
    from sqlalchemy import select, update

    session_factory = _session(request)
    if not session_factory:
        raise HTTPException(503, "Database unavailable — cannot kill all")

    dhan = _dhan(request)
    if not dhan:
        raise HTTPException(503, "No broker client — cannot close positions")

    closed = 0
    errors = []
    try:
        async with session_factory() as session:
            result = await session.execute(
                select(StrategyInstance).where(StrategyInstance.net_qty != 0)
            )
            instances = result.scalars().all()
            for inst in instances:
                try:
                    side = "SELL" if inst.net_qty > 0 else "BUY"
                    qty = int(abs(inst.net_qty))
                    if qty <= 0:
                        continue
                    params = inst.parameters or {}
                    security_id = params.get("security_id", "")
                    symbol = params.get("symbol", inst.catalog_id)
                    segment = params.get("segment", "NSE_FNO")
                    if not security_id:
                        errors.append(f"{inst.instance_id}: no security_id")
                        continue
                    r = await dhan.place_order(
                        symbol=symbol,
                        security_id=security_id,
                        side=side,
                        quantity=qty,
                        order_type="MARKET",
                        product_type="INTRADAY",
                        exchange_segment=segment,
                    )
                    if r.get("status") in ("ACCEPTED", "PLACED"):
                        await session.execute(
                            update(StrategyInstance)
                            .where(StrategyInstance.instance_id == inst.instance_id)
                            .values(net_qty=0.0, status="CLOSED")
                        )
                        closed += 1
                    else:
                        errors.append(f"{inst.instance_id}: {r.get('message', 'order failed')}")
                except Exception as e:
                    errors.append(f"{inst.instance_id}: {str(e)[:120]}")
            await session.commit()
    except Exception as e:
        log.warning("kill-all failed: %s", e)
        raise HTTPException(500, f"Kill-all failed: {str(e)[:200]}") from e

    return {"status": "complete", "closed": closed, "errors": errors, "total": closed + len(errors)}


# ---------------------------------------------------------------------------
# /api/v1/logs — recent log lines
# ---------------------------------------------------------------------------
@router.get("/logs")
async def get_logs(request: Request, lines: int = Query(50, le=500)):
    """Return the last N lines from the in-memory log buffer.

    The ITB doesn't persist logs to disk inside the container (Coolify
    captures stdout); this endpoint returns a ring buffer of recent
    log records captured via a logging handler installed at startup.
    """
    buf = getattr(request.app.state, "log_buffer", None)
    if not buf:
        return {"logs": [], "note": "No in-memory log buffer (use Coolify logs API)"}
    return {"logs": list(buf)[-lines:], "count": min(lines, len(buf))}
