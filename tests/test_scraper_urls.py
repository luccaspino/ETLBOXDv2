from __future__ import annotations

from src.ingestion.scraper_urls import _normalize_film_url, _to_global_film_url


def test_normalize_film_url_strips_user_page_number() -> None:
    assert _normalize_film_url("https://letterboxd.com/cavszinha/film/hitch-2005/1/") == (
        "https://letterboxd.com/film/hitch-2005/"
    )
    assert _normalize_film_url("https://letterboxd.com/cavszinha/film/shes-all-that/1/") == (
        "https://letterboxd.com/film/shes-all-that/"
    )


def test_to_global_film_url_strips_user_page_number() -> None:
    assert _to_global_film_url("https://letterboxd.com/cavszinha/film/hitch-2005/1/") == (
        "https://letterboxd.com/film/hitch-2005/"
    )
    assert _to_global_film_url("https://letterboxd.com/cavszinha/film/shes-all-that/1/") == (
        "https://letterboxd.com/film/shes-all-that/"
    )


def test_to_global_film_url_preserves_non_numeric_suffixes() -> None:
    assert _to_global_film_url("https://letterboxd.com/cavszinha/film/hitch-2005/reviews/") == (
        "https://letterboxd.com/cavszinha/film/hitch-2005/reviews"
    )
    assert _to_global_film_url("https://letterboxd.com/cavszinha/film/hitch-2005/activity/") == (
        "https://letterboxd.com/cavszinha/film/hitch-2005/activity"
    )
