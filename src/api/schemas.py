from __future__ import annotations

from pydantic import BaseModel, Field


class PipelineRunRequest(BaseModel):
    zip_path: str = Field(..., description="Caminho absoluto para o ZIP exportado do Letterboxd")
    workers: int = 20
    timeout: int = 10
    retries: int = 1
    retry_backoff: float = 0.25
    request_interval: float = 0.0
    progress_every: int = 50
    errors_out: str | None = "scrape_errors.csv"
    auto_retry_failed: bool = True
    retry_failed_passes: int = 6
    allow_partial: bool = False


class PipelineRunResponse(BaseModel):
    films_upserted_from_scrape: int
    user_films_loaded: int
    watchlist_loaded: int


class MainKpisResponse(BaseModel):
    total_filmes: int
    media_nota_pessoal: float | None
    total_horas: float


class RatingGapResponse(BaseModel):
    diferenca_media: float | None
    media_pessoal: float | None
    media_letterboxd: float | None


class MonthlyLogItem(BaseModel):
    mes: str
    total: int
