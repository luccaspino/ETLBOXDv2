from __future__ import annotations

from pydantic import BaseModel


class UserLookupResponse(BaseModel):
    username: str
    has_data: bool
    total_filmes: int
    total_watchlist: int
