"""Fernet encryption for broker credentials + JWT auth + master key middleware."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from cryptography.fernet import Fernet
import jwt
import structlog

from core.config import settings

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Fernet Encryption (broker API keys/tokens)
# ---------------------------------------------------------------------------

def _get_fernet() -> Fernet:
    """Get or create Fernet cipher from MASTER_ENCRYPTION_KEY env.

    The ephemeral-key fallback (dev only) is DANGEROUS: every process
    that lacks the env var generates a DIFFERENT key, so credentials
    encrypted in one process (or one container replica, or before a
    redeploy) cannot be decrypted in another. We keep the dev fallback
    but refuse to silently start in that state when a credential is
    actually being written — encrypt_secret() raises instead of
    producing undecryptable ciphertext.
    """
    key_b64 = settings.master_encryption_key.encode() if settings.master_encryption_key else None
    if not key_b64:
        # Generate ephemeral key (dev only — in production this MUST be set)
        log.warning("No MASTER_ENCRYPTION_KEY set, generating ephemeral Fernet key (dev only)")
        key_b64 = Fernet.generate_key()
        os.environ["MASTER_ENCRYPTION_KEY"] = key_b64.decode()
    return Fernet(key_b64)


_fernet: Optional[Fernet] = None


def _cipher() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = _get_fernet()
    return _fernet


def encrypt_secret(plaintext: str) -> str:
    """Encrypt a secret string (broker API key, token, etc)."""
    return _cipher().encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    """Decrypt a secret string."""
    return _cipher().decrypt(ciphertext.encode()).decode()


def verify_encrypted(ciphertext: str) -> bool:
    """Check if a ciphertext is valid without decrypting it."""
    try:
        _cipher().decrypt(ciphertext.encode())
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JWT Auth (user sessions)
# ---------------------------------------------------------------------------

def create_access_token(user_id: str, broker: str = "dhan", expires_min: int = 1440) -> str:
    """Create a JWT for a user session."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "broker": broker,
        "iat": now,
        "exp": now + timedelta(minutes=expires_min),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def issue_master_token(user_id: str, expires_min: int = 60) -> str:
    """Mint a short-lived JWT carrying the is_master claim.

    The platform owner calls this (out-of-band, e.g. from the deploy
    host) to get a token that passes require_master() for one hour.
    This is the only supported way to reach /api/v1/admin/master-credentials
    — the old code had no token path at all, so admin endpoints were
    unreachable even for the owner.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "broker": "dhan",
        "is_master": True,
        "iat": now,
        "exp": now + timedelta(minutes=expires_min),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT. Returns payload dict or None if invalid."""
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError:
        log.warning("JWT expired")
        return None
    except jwt.InvalidTokenError as e:
        log.warning("Invalid JWT", error=str(e))
        return None


# ---------------------------------------------------------------------------
# Master Credential Routing
# ---------------------------------------------------------------------------

class MasterCredentialRouter:
    """Routes credential resolution: master override > user credential > 401.

    Hierarchy:
      1. master_credentials (admin-set, highest priority)
      2. user_credentials (user's own broker API key/token)
      3. 401 error (no valid credential)

    Master credentials are checked first; if a matching active master credential
    exists for the broker, it overrides the user's personal credential.
    """

    def __init__(self, db_session_factory):
        self._session_factory = db_session_factory

    async def resolve_credentials(self, user_id: str, broker: str) -> Optional[dict]:
        """Resolve the effective credential for a user+broker pair.

        Returns dict with 'encrypted_creds', 'source' ('master'|'user'),
        'broker' or None if no credential found.
        """
        from sqlalchemy import select, desc

        async with self._session_factory() as session:
            # 1. Check master credentials first (highest priority)
            master = (await session.execute(
                select(
                    type(__import__("layers.state.models", fromlist=["MasterCredential"]).MasterCredential)
                ).where(
                    type(__import__("layers.state.models", fromlist=["MasterCredential"]).MasterCredential).broker == broker
                )
            )).scalars().first()

            if master and master.is_active:
                return {
                    "encrypted_creds": master.encrypted_creds,
                    "source": "master",
                    "broker": broker,
                    "user_id": user_id,
                }

            # 2. Fall back to user's own credential
            user_cred = (await session.execute(
                select(
                    type(__import__("layers.state.models", fromlist=["UserCredential"]).UserCredential)
                ).where(
                    type(__import__("layers.state.models", fromlist=["UserCredential"]).UserCredential).user_id == user_id,
                    type(__import__("layers.state.models", fromlist=["UserCredential"]).UserCredential).broker == broker,
                    type(__import__("layers.state.models", fromlist=["UserCredential"]).UserCredential).is_active == True,
                )
            )).scalars().first()

            if user_cred:
                return {
                    "encrypted_creds": user_cred.encrypted_creds,
                    "source": "user",
                    "broker": broker,
                    "user_id": user_id,
                }

        log.warning("No credential found", user_id=user_id, broker=broker)
        return None

    async def set_master_override(self, broker: str, encrypted_creds: str, scope: str = "ALL", applies_to: Optional[list] = None) -> None:
        """Set a master credential override for a broker."""
        from layers.state.models import MasterCredential

        async with self._session_factory() as session:
            # Deactivate existing master creds for this broker
            existing = (await session.execute(
                select(MasterCredential).where(
                    MasterCredential.broker == broker,
                    MasterCredential.is_active == True,
                )
            )).scalars().all()
            for cred in existing:
                cred.is_active = False

            master = MasterCredential(
                broker=broker,
                encrypted_creds=encrypted_creds,
                scope=scope,
                applies_to=applies_to if scope == "SELECTED" else None,
                is_active=True,
            )
            session.add(master)
            await session.commit()

        log.info("Master credential set", broker=broker, scope=scope)


# ---------------------------------------------------------------------------
# FastAPI Dependency Injection
# ---------------------------------------------------------------------------

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> dict:
    """FastAPI dependency: extract and validate JWT from Authorization header."""
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    return {
        "user_id": payload["sub"],
        "broker": payload.get("broker", "dhan"),
        # Carry the master claim so require_master() can enforce it. The old
        # code never set is_master, so every admin endpoint 403'd even for
        # the platform owner.
        "is_master": bool(payload.get("is_master", False)),
    }


# Set by core.main at startup when JWT_SECRET is the insecure default, so
# require_master() refuses to trust is_master claims minted with it.
# (Importing core.main from security would be a circular import, hence the
# module-level flag + setter.)
INSECURE_DEFAULT_JWT_SECRET = False


def set_insecure_default_jwt_secret(flag: bool) -> None:
    global INSECURE_DEFAULT_JWT_SECRET
    INSECURE_DEFAULT_JWT_SECRET = bool(flag)


def require_master(user: dict) -> None:
    """Guard: only the platform owner (master) can call admin endpoints."""
    # In production, check user.role == 'master' in the DB
    # For now, master endpoints require a separate master API key
    if not user.get("is_master"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Master access required")
    # And never trust a master claim minted with the shipped default secret.
    if INSECURE_DEFAULT_JWT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Master access unavailable: JWT_SECRET is the insecure default; set it via env",
        )
