from __future__ import annotations

from pydantic import BaseModel


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
    country_name: str
    total_filmes: int


class GenreCountItem(BaseModel):
    genero: str
    total_filmes: int


class PersonRankingItem(BaseModel):
    nome: str
    filmes_assistidos: int
    media_nota_pessoal: float | None


class CategoryRankingItem(BaseModel):
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
    poster_url: str | None
    letterboxd_url: str


class RandomReviewItem(BaseModel):
    film_id: int
    title: str
    year: int | None
    watched_date: str | None
    review_text: str
    letterboxd_url: str


class WatchlistFilmItem(BaseModel):
    film_id: int
    title: str
    year: int | None
    runtime_min: int | None
    original_language: str | None
    tagline: str | None
    poster_url: str | None
    letterboxd_url: str
    letterboxd_avg_rating: float | None
    director: str | None
    genres: str | None
    cast_top3: str | None
    added_date: str | None


class FilterCountryOptionItem(BaseModel):
    code: str
    name: str


class RuntimeRangeItem(BaseModel):
    min: int | None
    max: int | None


class FilterOptionsResponse(BaseModel):
    personal_ratings: list[float]
    letterboxd_ratings: list[float]
    watched_years: list[int]
    watched_months: list[int]
    release_years: list[int]
    release_decades: list[int]
    genres: list[str]
    countries: list[str]
    country_options: list[FilterCountryOptionItem]
    directors: list[str]
    actors: list[str]
    runtime: RuntimeRangeItem
