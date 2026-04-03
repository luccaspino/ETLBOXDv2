from __future__ import annotations

from fastapi import HTTPException, Query

from src.db import get_user_id_by_username


def require_user_id(
    username: str = Query(..., description="Username da tabela users"),
) -> str:
    user_id = get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail=f"Usuario '{username}' nao encontrado.")
    return user_id
