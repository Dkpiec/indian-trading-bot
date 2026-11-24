"""Strategy management endpoints: arm, disarm, params, status.

The arm/disarm endpoints persist is_armed on the matching UserSubscription
row so the execution engine's 1s tick loop (engine.py tick() → 
_load_armed_subs) picks up the strategy within one second.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class ArmRequest(BaseModel):
    subscription_id: str
    broker_override: Optional[str] = None


class ParamOverride(BaseModel):
    key: str
    value: str


@router.get("/")
async def list_strategies(request: Request):
    """List all strategy instances + armed subscriptions for the system user.

    Queries the DB for UserSubscription rows joined to StrategyCatalog so
    the operator can see which strategies are armed and running.
    """
    from layers.state.models import UserSubscription, StrategyCatalog
    from sqlalchemy import select

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        return {"instances": [], "note": "DB unavailable — no instances to list"}
    try:
        async with session_factory() as session:
            result = await session.execute(
                select(UserSubscription, StrategyCatalog)
                .join(
                    StrategyCatalog,
                    UserSubscription.catalog_id == StrategyCatalog.id,
                )
            )
            rows = result.all()
            instances = [
                {
                    "subscription_id": sub.id,
                    "catalog_id": sub.catalog_id,
                    "user_id": str(sub.user_id),
                    "is_armed": sub.is_armed,
                    "display_name": cat.display_name if cat else None,
                }
                for sub, cat in rows
            ]
        return {"instances": instances}
    except Exception as e:
        return {"instances": [], "error": str(e)}


@router.post("/arm")
async def arm_strategy(req: ArmRequest, request: Request):
    """Arm a subscribed strategy for live execution.

    Sets UserSubscription.is_armed=True in the DB. The execution engine's
    1s tick loop polls this flag, so the strategy goes live within ~1s.
    """
    from layers.state.models import UserSubscription
    from sqlalchemy import update

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        raise HTTPException(503, "Database unavailable — cannot arm strategy")

    try:
        async with session_factory() as session:
            result = await session.execute(
                update(UserSubscription)
                .where(UserSubscription.id == req.subscription_id)
                .values(is_armed=True)
            )
            await session.commit()
            rows = result.rowcount
        if rows == 0:
            raise HTTPException(404, f"Subscription {req.subscription_id} not found")
        return {"status": "armed", "subscription_id": req.subscription_id,
                "message": "Strategy will go live on the next engine tick (~1s)"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to arm strategy: {e}")


@router.post("/disarm")
async def disarm_strategy(subscription_id: str, request: Request):
    """Disarm a strategy (keep subscription, stop live execution).

    Sets UserSubscription.is_armed=False. Open positions are NOT closed —
    the engine stops evaluating entries for this strategy but existing
    positions continue to be managed by their exit rules until closed.
    """
    from layers.state.models import UserSubscription
    from sqlalchemy import update

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        raise HTTPException(503, "Database unavailable — cannot disarm strategy")

    try:
        async with session_factory() as session:
            result = await session.execute(
                update(UserSubscription)
                .where(UserSubscription.id == subscription_id)
                .values(is_armed=False)
            )
            await session.commit()
            rows = result.rowcount
        if rows == 0:
            raise HTTPException(404, f"Subscription {subscription_id} not found")
        return {"status": "disarmed", "subscription_id": subscription_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to disarm strategy: {e}")


@router.put("/{instance_id}/params")
async def update_params(instance_id: str, override: ParamOverride, request: Request):
    """Override a single parameter on a running strategy instance.

    Merges the key/value into the subscription's custom_params JSONB so the
    execution engine picks up the new value on the next tick.
    """
    from layers.state.models import UserSubscription
    from sqlalchemy import select
    import json as _json

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        raise HTTPException(503, "Database unavailable")

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(UserSubscription).where(UserSubscription.id == instance_id)
            )
            sub = result.scalar_one_or_none()
            if not sub:
                raise HTTPException(404, f"Subscription {instance_id} not found")
            params = dict(sub.custom_params or {})
            params[override.key] = override.value
            sub.custom_params = params
            await session.commit()
        return {"instance_id": instance_id, "param": override.key,
                "value": override.value, "status": "updated"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Failed to update params: {e}")
