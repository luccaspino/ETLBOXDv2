from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from src.db.connection import get_connection

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/db")
def db_health_check() -> dict[str, str]:
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok", "database": "reachable"}
    except Exception as err:
        logger.warning("Database health check failed: %s", err)
        raise HTTPException(status_code=503, detail="Database unavailable.") from err
