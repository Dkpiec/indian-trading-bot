"""Strategy catalog endpoints: list, subscribe, unsubscribe."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class SubscribeRequest(BaseModel):
    catalog_id: str
    custom_params: Optional[dict] = None


@router.get("/")
async def list_catalog():
    """List all available strategies in the catalog."""
    # TODO: Phase 2 — query strategy_catalog WHERE is_active=True
    return {"strategies": []}


@router.post("/subscribe")
async def subscribe(req: SubscribeRequest):
    """Subscribe to a catalog strategy (creates user_subscription + instance)."""
    # TODO: Phase 2 — insert user_subscriptions row, spawn default instance
    return {"status": "subscribed", "catalog_id": req.catalog_id}


@router.delete("/unsubscribe/{subscription_id}")
async def unsubscribe(subscription_id: str):
    """Remove subscription and stop associated instance."""
    # TODO: Phase 2 — delete user_subscriptions row, cancel instance task
    return {"status": "unsubscribed", "subscription_id": subscription_id}


@router.get("/{catalog_id}/params")
async def get_default_params(catalog_id: str):
    """Get default parameters for a catalog strategy."""
    # TODO: Phase 2 — query strategy_catalog.default_params
    return {"catalog_id": catalog_id, "default_params": {}}