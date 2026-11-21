"""Auth endpoints: register, login, token refresh."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

router = APIRouter()


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


@router.post("/register", response_model=TokenResponse)
async def register(req: RegisterRequest):
    # TODO: Phase 2 — hash password, create user row, return JWT
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Phase 2")


@router.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest):
    # TODO: Phase 2 — verify password, return JWT
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Phase 2")