from __future__ import annotations

from fastapi import APIRouter, HTTPException

from src.api.schemas import UserLookupResponse
from src.db import get_user_lookup

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/{username}", response_model=UserLookupResponse)
def get_user_by_username(username: str) -> UserLookupResponse:
    user = get_user_lookup(username)
    if not user:
        raise HTTPException(status_code=404, detail=f"Usuario '{username}' nao encontrado.")
    return UserLookupResponse(**user)
