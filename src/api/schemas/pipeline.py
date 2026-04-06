from __future__ import annotations

from pydantic import BaseModel


class PipelineRunResponse(BaseModel):
    username: str
    films_upserted_from_scrape: int
    user_films_loaded: int
    watchlist_loaded: int
