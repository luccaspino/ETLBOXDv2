from __future__ import annotations

from fastapi import APIRouter

from src.db.connection import get_cursor

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/db")
def db_health_check() -> dict[str, str]:
    with get_cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"status": "ok", "database": "reachable"}
