"""Strategy management endpoints: arm, disarm, params, status."""
from __future__ import annotations

from fastapi import APIRouter, Depends
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
async def list_strategies():
    """List all strategy instances for the authenticated user."""
    # TODO: Phase 3 — query strategy_instances by user_id
    return {"instances": []}


@router.post("/arm")
async def arm_strategy(req: ArmRequest):
    """Arm a subscribed strategy for live execution."""
    # TODO: Phase 3 — set is_armed=True, spawn asyncio task
    return {"status": "armed", "subscription_id": req.subscription_id}


@router.post("/disarm")
async def disarm_strategy(subscription_id: str):
    """Disarm a strategy (keep subscription, stop live execution)."""
    # TODO: Phase 3 — set is_armed=False, cancel asyncio task
    return {"status": "disarmed", "subscription_id": subscription_id}


@router.put("/{instance_id}/params")
async def update_params(instance_id: str, override: ParamOverride):
    """Override a single parameter on a running strategy instance."""
    # TODO: Phase 3 — merge into instance.parameters, hot-reload
    return {"instance_id": instance_id, "param": override.key, "value": override.value}