from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import (
    CategoryRankingItem,
    CountryCountItem,
    FilteredFilmItem,
    GenreCountItem,
    MainKpisResponse,
    MonthlyLogItem,
    PersonRankingItem,
    RandomReviewItem,
    RatingDistributionItem,
    RatingGapResponse,
    ReleaseYearResponse,
    YearlyLogItem,
)
from src.db.repository import (
    get_country_counts,
    get_country_rankings,
    get_filtered_films,
    get_genre_counts,
    get_genre_rankings,
    get_logs_by_month,
    get_logs_by_year,
    get_main_kpis,
    get_people_rankings,
    get_random_review,
    get_random_watchlist_film,
    get_rating_distribution,
    get_rating_gap_kpis,
    get_release_year_kpi,
    get_user_id_by_username,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _require_user_id(username: str) -> str:
    user_id = get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail=f"Usuario '{username}' nao encontrado.")
    return user_id


def _film_filters(
    min_rating: float | None = None,
    max_rating: float | None = None,
    min_runtime: int | None = None,
    max_runtime: int | None = None,
    decade_start: int | None = None,
    director_name: str | None = None,
    actor_name: str | None = None,
    country_code: str | None = None,
    genre_name: str | None = None,
    watched_month: int | None = None,
    watched_year: int | None = None,
) -> dict[str, float | int | str | None]:
    return {
        "min_rating": min_rating,
        "max_rating": max_rating,
        "min_runtime": min_runtime,
        "max_runtime": max_runtime,
        "decade_start": decade_start,
        "director_name": director_name,
        "actor_name": actor_name,
        "country_code": country_code,
        "genre_name": genre_name,
        "watched_month": watched_month,
        "watched_year": watched_year,
    }


@router.get("/kpis/main", response_model=MainKpisResponse)
def get_kpis_main(username: str = Query(..., description="Username da tabela users")) -> MainKpisResponse:
    user_id = _require_user_id(username)
    return MainKpisResponse(**get_main_kpis(user_id))


@router.get("/kpis/rating-gap", response_model=RatingGapResponse)
def get_kpis_rating_gap(username: str = Query(..., description="Username da tabela users")) -> RatingGapResponse:
    user_id = _require_user_id(username)
    return RatingGapResponse(**get_rating_gap_kpis(user_id))


@router.get("/kpis/release-year", response_model=ReleaseYearResponse)
def get_kpis_release_year(username: str = Query(..., description="Username da tabela users")) -> ReleaseYearResponse:
    user_id = _require_user_id(username)
    return ReleaseYearResponse(**get_release_year_kpi(user_id))


@router.get("/random", response_model=FilteredFilmItem)
def get_random_watchlist_pick(username: str = Query(..., description="Username da tabela users")) -> FilteredFilmItem:
    user_id = _require_user_id(username)
    film = get_random_watchlist_film(user_id)
    if not film:
        raise HTTPException(status_code=404, detail="Nenhum filme encontrado para este usuario.")
    return FilteredFilmItem(**film)


@router.get("/reviews/random", response_model=RandomReviewItem)
def get_random_review_pick(username: str = Query(..., description="Username da tabela users")) -> RandomReviewItem:
    user_id = _require_user_id(username)
    review = get_random_review(user_id)
    if not review:
        raise HTTPException(status_code=404, detail="Nenhuma review encontrada para este usuario.")
    return RandomReviewItem(**review)


@router.get("/logs/monthly", response_model=list[MonthlyLogItem])
def get_monthly_logs(username: str = Query(..., description="Username da tabela users")) -> list[MonthlyLogItem]:
    user_id = _require_user_id(username)
    return [MonthlyLogItem(**row) for row in get_logs_by_month(user_id)]


@router.get("/logs/yearly", response_model=list[YearlyLogItem])
def get_yearly_logs(username: str = Query(..., description="Username da tabela users")) -> list[YearlyLogItem]:
    user_id = _require_user_id(username)
    return [YearlyLogItem(**row) for row in get_logs_by_year(user_id)]


@router.get("/distribution/ratings", response_model=list[RatingDistributionItem])
def get_ratings_distribution(username: str = Query(..., description="Username da tabela users")) -> list[RatingDistributionItem]:
    user_id = _require_user_id(username)
    return [RatingDistributionItem(**row) for row in get_rating_distribution(user_id)]


@router.get("/distribution/countries", response_model=list[CountryCountItem])
def get_countries_distribution(username: str = Query(..., description="Username da tabela users")) -> list[CountryCountItem]:
    user_id = _require_user_id(username)
    return [CountryCountItem(**row) for row in get_country_counts(user_id)]


@router.get("/distribution/genres", response_model=list[GenreCountItem])
def get_genres_distribution(username: str = Query(..., description="Username da tabela users")) -> list[GenreCountItem]:
    user_id = _require_user_id(username)
    return [GenreCountItem(**row) for row in get_genre_counts(user_id)]


@router.get("/rankings/countries/most-watched", response_model=list[CategoryRankingItem])
def get_countries_most_watched(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[CategoryRankingItem]:
    user_id = _require_user_id(username)
    return [
        CategoryRankingItem(**row)
        for row in get_country_rankings(user_id, order_by="most_watched", min_films=min_films)
    ]


@router.get("/rankings/countries/best-rated", response_model=list[CategoryRankingItem])
def get_countries_best_rated(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[CategoryRankingItem]:
    user_id = _require_user_id(username)
    return [
        CategoryRankingItem(**row)
        for row in get_country_rankings(user_id, order_by="best_rated", min_films=min_films)
    ]


@router.get("/rankings/genres/most-watched", response_model=list[CategoryRankingItem])
def get_genres_most_watched(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[CategoryRankingItem]:
    user_id = _require_user_id(username)
    return [
        CategoryRankingItem(**row)
        for row in get_genre_rankings(user_id, order_by="most_watched", min_films=min_films)
    ]


@router.get("/rankings/genres/best-rated", response_model=list[CategoryRankingItem])
def get_genres_best_rated(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[CategoryRankingItem]:
    user_id = _require_user_id(username)
    return [
        CategoryRankingItem(**row)
        for row in get_genre_rankings(user_id, order_by="best_rated", min_films=min_films)
    ]


@router.get("/rankings/directors/most-watched", response_model=list[PersonRankingItem])
def get_directors_most_watched(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[PersonRankingItem]:
    user_id = _require_user_id(username)
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(user_id, role="director", min_films=min_films, order_by="most_watched")
    ]


@router.get("/rankings/directors/best-rated", response_model=list[PersonRankingItem])
def get_directors_best_rated(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[PersonRankingItem]:
    user_id = _require_user_id(username)
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(user_id, role="director", min_films=min_films, order_by="best_rated")
    ]


@router.get("/rankings/actors/most-watched", response_model=list[PersonRankingItem])
def get_actors_most_watched(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[PersonRankingItem]:
    user_id = _require_user_id(username)
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(user_id, role="actor", min_films=min_films, order_by="most_watched")
    ]


@router.get("/rankings/actors/best-rated", response_model=list[PersonRankingItem])
def get_actors_best_rated(
    username: str = Query(..., description="Username da tabela users"),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[PersonRankingItem]:
    user_id = _require_user_id(username)
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(user_id, role="actor", min_films=min_films, order_by="best_rated")
    ]


@router.get("/films", response_model=list[FilteredFilmItem])
def get_films_table(
    username: str = Query(..., description="Username da tabela users"),
    min_rating: float | None = Query(None, ge=0.5, le=5.0),
    max_rating: float | None = Query(None, ge=0.5, le=5.0),
    min_runtime: int | None = Query(None, ge=1),
    max_runtime: int | None = Query(None, ge=1),
    decade_start: int | None = Query(None, ge=1880, le=2100),
    director_name: str | None = Query(None),
    actor_name: str | None = Query(None),
    country_code: str | None = Query(None, min_length=2, max_length=2),
    genre_name: str | None = Query(None),
    watched_month: int | None = Query(None, ge=1, le=12),
    watched_year: int | None = Query(None, ge=1880, le=2100),
) -> list[FilteredFilmItem]:
    user_id = _require_user_id(username)
    filters = _film_filters(
        min_rating=min_rating,
        max_rating=max_rating,
        min_runtime=min_runtime,
        max_runtime=max_runtime,
        decade_start=decade_start,
        director_name=director_name,
        actor_name=actor_name,
        country_code=country_code,
        genre_name=genre_name,
        watched_month=watched_month,
        watched_year=watched_year,
    )
    return [FilteredFilmItem(**row) for row in get_filtered_films(user_id, **filters)]
