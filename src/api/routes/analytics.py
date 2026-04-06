from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.dependencies import require_user_id
from src.api.schemas import (
    CategoryRankingItem,
    CountryCountItem,
    FilterCountryOptionItem,
    FilteredFilmItem,
    FilterOptionsResponse,
    GenreCountItem,
    LoggedFilmItem,
    MainKpisResponse,
    MonthlyLogItem,
    PersonRankingItem,
    RandomReviewItem,
    RatingDistributionItem,
    RuntimeRangeItem,
    RatingGapResponse,
    ReleaseYearResponse,
    WatchlistFilmItem,
    YearlyLogItem,
)
from src.db import (
    get_country_counts,
    get_country_rankings,
    get_filtered_films,
    get_genre_counts,
    get_filter_options,
    get_genre_rankings,
    get_logged_films,
    get_logs_by_month,
    get_logs_by_year,
    get_main_kpis,
    get_people_rankings,
    get_random_review,
    get_random_watchlist_film,
    get_rating_distribution,
    get_watchlist_films,
    get_rating_gap_kpis,
    get_release_year_kpi,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/kpis/main", response_model=MainKpisResponse)
def get_kpis_main(user_id: str = Depends(require_user_id)) -> MainKpisResponse:
    return MainKpisResponse(**get_main_kpis(user_id))


@router.get("/kpis/rating-gap", response_model=RatingGapResponse)
def get_kpis_rating_gap(user_id: str = Depends(require_user_id)) -> RatingGapResponse:
    return RatingGapResponse(**get_rating_gap_kpis(user_id))


@router.get("/kpis/release-year", response_model=ReleaseYearResponse)
def get_kpis_release_year(user_id: str = Depends(require_user_id)) -> ReleaseYearResponse:
    return ReleaseYearResponse(**get_release_year_kpi(user_id))


@router.get("/random", response_model=FilteredFilmItem)
def get_random_watchlist_pick(user_id: str = Depends(require_user_id)) -> FilteredFilmItem:
    film = get_random_watchlist_film(user_id)
    if not film:
        raise HTTPException(status_code=404, detail="Nenhum filme encontrado para este usuario.")
    return FilteredFilmItem(**film)


@router.get("/reviews/random", response_model=RandomReviewItem)
def get_random_review_pick(user_id: str = Depends(require_user_id)) -> RandomReviewItem:
    review = get_random_review(user_id)
    if not review:
        raise HTTPException(status_code=404, detail="Nenhuma review encontrada para este usuario.")
    return RandomReviewItem(**review)


@router.get("/logs/monthly", response_model=list[MonthlyLogItem])
def get_monthly_logs(user_id: str = Depends(require_user_id)) -> list[MonthlyLogItem]:
    return [MonthlyLogItem(**row) for row in get_logs_by_month(user_id)]


@router.get("/logs/yearly", response_model=list[YearlyLogItem])
def get_yearly_logs(user_id: str = Depends(require_user_id)) -> list[YearlyLogItem]:
    return [YearlyLogItem(**row) for row in get_logs_by_year(user_id)]


@router.get("/logs/films", response_model=list[LoggedFilmItem])
def get_logged_films_table(
    user_id: str = Depends(require_user_id),
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
) -> list[LoggedFilmItem]:
    return [
        LoggedFilmItem(**row)
        for row in get_logged_films(
            user_id,
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
    ]


@router.get("/distribution/ratings", response_model=list[RatingDistributionItem])
def get_ratings_distribution(user_id: str = Depends(require_user_id)) -> list[RatingDistributionItem]:
    return [RatingDistributionItem(**row) for row in get_rating_distribution(user_id)]


@router.get("/distribution/countries", response_model=list[CountryCountItem])
def get_countries_distribution(user_id: str = Depends(require_user_id)) -> list[CountryCountItem]:
    return [CountryCountItem(**row) for row in get_country_counts(user_id)]


@router.get("/distribution/genres", response_model=list[GenreCountItem])
def get_genres_distribution(user_id: str = Depends(require_user_id)) -> list[GenreCountItem]:
    return [GenreCountItem(**row) for row in get_genre_counts(user_id)]


@router.get("/rankings/countries/most-watched", response_model=list[CategoryRankingItem])
def get_countries_most_watched(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[CategoryRankingItem]:
    return [
        CategoryRankingItem(**row)
        for row in get_country_rankings(user_id, order_by="most_watched", min_films=min_films)
    ]


@router.get("/rankings/countries/best-rated", response_model=list[CategoryRankingItem])
def get_countries_best_rated(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[CategoryRankingItem]:
    return [
        CategoryRankingItem(**row)
        for row in get_country_rankings(user_id, order_by="best_rated", min_films=min_films)
    ]


@router.get("/rankings/genres/most-watched", response_model=list[CategoryRankingItem])
def get_genres_most_watched(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
) -> list[CategoryRankingItem]:
    return [
        CategoryRankingItem(**row)
        for row in get_genre_rankings(user_id, order_by="most_watched", min_films=min_films)
    ]


@router.get("/rankings/genres/best-rated", response_model=list[CategoryRankingItem])
def get_genres_best_rated(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
) -> list[CategoryRankingItem]:
    return [
        CategoryRankingItem(**row)
        for row in get_genre_rankings(user_id, order_by="best_rated", min_films=min_films)
    ]


@router.get("/rankings/directors/most-watched", response_model=list[PersonRankingItem])
def get_directors_most_watched(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
    limit: int = Query(25, ge=1, le=100, description="Quantidade maxima de diretores retornados"),
) -> list[PersonRankingItem]:
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(
            user_id,
            role="director",
            min_films=min_films,
            order_by="most_watched",
            limit=limit,
        )
    ]


@router.get("/rankings/directors/best-rated", response_model=list[PersonRankingItem])
def get_directors_best_rated(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
    limit: int = Query(25, ge=1, le=100, description="Quantidade maxima de diretores retornados"),
) -> list[PersonRankingItem]:
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(
            user_id,
            role="director",
            min_films=min_films,
            order_by="best_rated",
            limit=limit,
        )
    ]


@router.get("/rankings/actors/most-watched", response_model=list[PersonRankingItem])
def get_actors_most_watched(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(1, ge=1, description="Minimo de filmes para entrar no ranking"),
    limit: int = Query(25, ge=1, le=100, description="Quantidade maxima de atores retornados"),
) -> list[PersonRankingItem]:
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(
            user_id,
            role="actor",
            min_films=min_films,
            order_by="most_watched",
            limit=limit,
        )
    ]


@router.get("/rankings/actors/best-rated", response_model=list[PersonRankingItem])
def get_actors_best_rated(
    user_id: str = Depends(require_user_id),
    min_films: int = Query(3, ge=1, description="Minimo de filmes avaliados para entrar no ranking"),
    limit: int = Query(25, ge=1, le=100, description="Quantidade maxima de atores retornados"),
) -> list[PersonRankingItem]:
    return [
        PersonRankingItem(**row)
        for row in get_people_rankings(
            user_id,
            role="actor",
            min_films=min_films,
            order_by="best_rated",
            limit=limit,
        )
    ]


@router.get("/watchlist", response_model=list[WatchlistFilmItem])
def get_watchlist_table(user_id: str = Depends(require_user_id)) -> list[WatchlistFilmItem]:
    return [WatchlistFilmItem(**row) for row in get_watchlist_films(user_id)]


@router.get("/filters/options", response_model=FilterOptionsResponse)
def get_filters_options(user_id: str = Depends(require_user_id)) -> FilterOptionsResponse:
    payload = get_filter_options(user_id)
    payload['country_options'] = [FilterCountryOptionItem(**row) for row in payload['country_options']]
    payload['runtime'] = RuntimeRangeItem(**payload['runtime'])
    return FilterOptionsResponse(**payload)


@router.get("/films", response_model=list[FilteredFilmItem])
def get_films_table(
    user_id: str = Depends(require_user_id),
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
    return [
        FilteredFilmItem(**row)
        for row in get_filtered_films(
            user_id,
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
    ]
