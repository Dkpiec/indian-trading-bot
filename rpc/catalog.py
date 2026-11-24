"""Strategy catalog endpoints: list, subscribe, unsubscribe."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import select

router = APIRouter()


class SubscribeRequest(BaseModel):
    catalog_id: str
    custom_params: Optional[dict] = None


class UnsubscribeRequest(BaseModel):
    subscription_id: str


@router.get("/")
async def list_catalog(request: Request):
    """List all available strategies in the catalog."""
    from layers.state.models import StrategyCatalog

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        # Use in-memory fallback catalog
        from layers.state.fallback_db import get_catalog
        strategies = await get_catalog()
        return {
            "strategies": strategies,
            "status": "degraded",
            "message": "Database unavailable — using in-memory fallback catalog",
        }

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(StrategyCatalog).where(StrategyCatalog.is_active == True).order_by(StrategyCatalog.id)  # noqa: E712
            )
            strategies = result.scalars().all()
            return {
                "strategies": [
                    {
                        "id": s.id,
                        "display_name": s.display_name,
                        "description": s.description,
                        "underlying": s.underlying,
                        "segment": s.segment,
                        "strategy_type": s.strategy_type,
                        "version": s.version,
                        "is_active": s.is_active,
                    }
                    for s in strategies
                ],
                "status": "ok",
            }
    except Exception as e:
        # DB unavailable at runtime — fall back to in-memory catalog
        from layers.state.fallback_db import get_catalog
        strategies = await get_catalog()
        return {
            "strategies": strategies,
            "status": "degraded",
            "message": f"Database unavailable ({e}) — using in-memory fallback catalog",
        }


@router.post("/subscribe")
async def subscribe(req: SubscribeRequest, request: Request):
    """Subscribe to a catalog strategy (creates user_subscription)."""
    from layers.state.models import StrategyCatalog, UserSubscription, User
    import uuid as _uuid
    import hashlib

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        # Use in-memory fallback
        from layers.state.fallback_db import get_catalog_entry, subscribe as fallback_subscribe
        entry = await get_catalog_entry(req.catalog_id)
        if not entry:
            raise HTTPException(status_code=404, detail=f"Strategy {req.catalog_id} not found or inactive")
        sub_id = await fallback_subscribe(req.catalog_id, "system", req.custom_params)
        return {"status": "subscribed", "catalog_id": req.catalog_id, "subscription_id": sub_id, "fallback": True}

    # Verify catalog entry exists
    async with session_factory() as session:
        cat_result = await session.execute(
            select(StrategyCatalog).where(StrategyCatalog.id == req.catalog_id, StrategyCatalog.is_active == True)  # noqa: E712
        )
        catalog_entry = cat_result.scalar_one_or_none()

        if not catalog_entry:
            raise HTTPException(status_code=404, detail=f"Strategy {req.catalog_id} not found or inactive")

        # Get or create system user
        user_uuid = _uuid.uuid5(_uuid.NAMESPACE_URL, "trading-bot:user:system")
        user_result = await session.execute(select(User).where(User.id == user_uuid))
        user = user_result.scalar_one_or_none()

        if not user:
            user = User(
                id=user_uuid,
                email="system@trading-bot.local",
                password_hash=hashlib.sha256(b"system").hexdigest(),
                is_active=True,
            )
            session.add(user)

        # Check if already subscribed
        sub_result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == user_uuid,
                UserSubscription.catalog_id == req.catalog_id,
            )
        )
        if sub_result.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Already subscribed")

        # Create subscription
        sub = UserSubscription(
            user_id=user_uuid,
            catalog_id=req.catalog_id,
            custom_params=req.custom_params,
            is_armed=False,
        )
        session.add(sub)
        await session.commit()

        return {"status": "subscribed", "catalog_id": req.catalog_id, "subscription_id": str(sub.id)}


@router.delete("/unsubscribe/{catalog_id}")
async def unsubscribe(catalog_id: str, request: Request):
    """Remove subscription and stop associated instance."""
    from layers.state.models import UserSubscription
    import uuid as _uuid

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        raise HTTPException(status_code=503, detail="Database unavailable")

    user_uuid = _uuid.uuid5(_uuid.NAMESPACE_URL, "trading-bot:user:system")

    async with session_factory() as session:
        sub_result = await session.execute(
            select(UserSubscription).where(
                UserSubscription.user_id == user_uuid,
                UserSubscription.catalog_id == catalog_id,
            )
        )
        sub = sub_result.scalar_one_or_none()

        if not sub:
            raise HTTPException(status_code=404, detail="Subscription not found")

        await session.delete(sub)
        await session.commit()

        return {"status": "unsubscribed", "catalog_id": catalog_id}


@router.get("/{catalog_id}/params")
async def get_default_params(catalog_id: str, request: Request):
    """Get default parameters for a catalog strategy."""
    from layers.state.models import StrategyCatalog

    session_factory = getattr(request.app.state, "async_session_factory", None)
    if not session_factory:
        return {"catalog_id": catalog_id, "default_params": {}, "status": "degraded"}

    try:
        async with session_factory() as session:
            result = await session.execute(
                select(StrategyCatalog).where(StrategyCatalog.id == catalog_id)
            )
            catalog_entry = result.scalar_one_or_none()

            if not catalog_entry:
                raise HTTPException(status_code=404, detail=f"Strategy {catalog_id} not found")

            return {
                "catalog_id": catalog_id,
                "default_params": catalog_entry.default_params,
                "status": "ok",
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {e}")