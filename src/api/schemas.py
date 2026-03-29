from __future__ import annotations

from pydantic import BaseModel


class PipelineRunResponse(BaseModel):
    films_upserted_from_scrape: int
    user_films_loaded: int
    watchlist_loaded: int


class UserLookupResponse(BaseModel):
    username: str
    has_data: bool
    total_filmes: int
    total_watchlist: int


class MainKpisResponse(BaseModel):
    total_filmes: int
    media_nota_pessoal: float | None
    total_horas: float


class RatingGapResponse(BaseModel):
    diferenca_media: float | None
    media_pessoal: float | None
    media_letterboxd: float | None


class ReleaseYearResponse(BaseModel):
    ano_medio_lancamento: float | None


class MonthlyLogItem(BaseModel):
    mes: int
    total: int


class YearlyLogItem(BaseModel):
    ano: int
    total: int


class RatingDistributionItem(BaseModel):
    rating: float | None
    total: int


class CountryCountItem(BaseModel):
    country_code: str
    total_filmes: int


class GenreCountItem(BaseModel):
    genero: str
    total_filmes: int


class PersonRankingItem(BaseModel):
    nome: str
    filmes_assistidos: int
    media_nota_pessoal: float | None


class FilteredFilmItem(BaseModel):
    film_id: int
    title: str
    year: int | None
    runtime_min: int | None
    user_rating: float | None
    letterboxd_avg_rating: float | None
    watched_date: str | None
    tagline: str | None
    letterboxd_url: str
