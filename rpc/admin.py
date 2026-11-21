"""Admin endpoints: master credential management, user oversight."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class MasterCredentialRequest(BaseModel):
    broker: str
    encrypted_creds: str
    scope: str = "ALL"  # 'ALL' or 'SELECTED'
    applies_to: Optional[list] = None


@router.post("/master-credentials")
async def set_master_credential(req: MasterCredentialRequest):
    """Set a master credential override for a broker."""
    from core.security import encrypt_secret
    from core.main import cred_router

    await cred_router.set_master_override(
        broker=req.broker,
        encrypted_creds=req.encrypted_creds,
        scope=req.scope,
        applies_to=req.applies_to,
    )
    return {"status": "master_credential_set", "broker": req.broker, "scope": req.scope}


@router.get("/users")
async def list_users():
    """List all registered users (admin only)."""
    # TODO: Phase 2 — query users table
    return {"users": []}


@router.get("/users/{user_id}/strategies")
async def get_user_strategies(user_id: str):
    """Get all strategy instances for a specific user (admin only)."""
    # TODO: Phase 3 — query strategy_instances by user_id
    return {"user_id": user_id, "instances": []}