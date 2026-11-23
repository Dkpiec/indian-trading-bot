"""In-memory fallback database layer for when Postgres/Redis are unavailable.

Provides minimal implementations of the database-dependent operations so the
bot can continue functioning in degraded mode.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

# In-memory storage
_strategy_catalog: Dict[str, Dict[str, Any]] = {
    "NIFTY101": {
        "id": "NIFTY101",
        "display_name": "NIFTY ORB v6e",
        "description": "NIFTY ORB v6e CE+PE legs",
        "underlying": "NIFTY",
        "segment": "NSE_FNO",
        "strategy_type": "orb",
        "version": "6e",
        "is_active": True,
        "default_params": {"legs": ["CE", "PE"]},
    },
    "NIFTY102": {
        "id": "NIFTY102",
        "display_name": "NIFTY SL-Hunt v4.1b",
        "description": "NIFTY SL-Hunt v4.1b CE leg",
        "underlying": "NIFTY",
        "segment": "NSE_FNO",
        "strategy_type": "slhunt",
        "version": "4.1b",
        "is_active": True,
        "default_params": {"legs": ["CE"]},
    },
    "BANKNIFTY101": {
        "id": "BANKNIFTY101",
        "display_name": "BankNifty ORB v2",
        "description": "BankNifty ORB Invalidation v2 CE+PE monthly",
        "underlying": "BANKNIFTY",
        "segment": "NSE_FNO",
        "strategy_type": "orb",
        "version": "2",
        "is_active": True,
        "default_params": {"legs": ["CE", "PE"]},
    },
    "SENSEX101": {
        "id": "SENSEX101",
        "display_name": "SENSEX ORB Invalidation",
        "description": "SENSEX ORB Invalidation CE+PE legs",
        "underlying": "SENSEX",
        "segment": "NSE_FNO",
        "strategy_type": "orb",
        "version": "5",
        "is_active": True,
        "default_params": {"legs": ["CE", "PE"]},
    },
    "SENSEX102": {
        "id": "SENSEX102",
        "display_name": "SENSEX SL-Hunt V4.1g",
        "description": "SENSEX SL-Hunt V4.1g CE legs",
        "underlying": "SENSEX",
        "segment": "NSE_FNO",
        "strategy_type": "slhunt",
        "version": "4.1g",
        "is_active": True,
        "default_params": {"legs": ["CE"]},
    },
}

_user_subscriptions: Dict[str, Dict[str, Any]] = {}


async def get_catalog() -> List[Dict[str, Any]]:
    """Return all active strategies from in-memory catalog."""
    return [s for s in _strategy_catalog.values() if s.get("is_active")]


async def get_catalog_entry(catalog_id: str) -> Optional[Dict[str, Any]]:
    """Get a single catalog entry by ID."""
    return _strategy_catalog.get(catalog_id)


async def subscribe(catalog_id: str, user_id: str, custom_params: Optional[Dict] = None) -> str:
    """Create a subscription (in-memory). Returns subscription ID."""
    sub_id = f"{user_id}:{catalog_id}"
    _user_subscriptions[sub_id] = {
        "user_id": user_id,
        "catalog_id": catalog_id,
        "custom_params": custom_params or {},
        "is_armed": False,
    }
    return sub_id


async def unsubscribe(catalog_id: str, user_id: str) -> bool:
    """Remove a subscription (in-memory). Returns True if found."""
    sub_id = f"{user_id}:{catalog_id}"
    if sub_id in _user_subscriptions:
        del _user_subscriptions[sub_id]
        return True
    return False
