from __future__ import annotations

import pandas as pd

from src.dashboard.components.summary import (
    aggregate_logs_by_month,
    aggregate_name_counts,
    aggregate_rating_distribution,
    build_logged_films_dataframe,
    compute_summary_metrics,
    extract_selected_rating,
    extract_selected_year,
    extract_selected_month,
    filter_logged_films,
)


def test_build_logged_films_dataframe_derives_month_year_and_lists() -> None:
    df = build_logged_films_dataframe(
        [
            {
                "film_id": 1,
                "title": "Possession",
                "year": 1981,
                "runtime_min": 124,
                "user_rating": 4.5,
                "letterboxd_avg_rating": 4.1,
                "watched_date": "2026-12-15",
                "genres": ["Drama", "Horror"],
                "countries": ["France"],
            }
        ]
    )

    assert int(df.iloc[0]["watched_month"]) == 12
    assert int(df.iloc[0]["watched_year"]) == 2026
    assert df.iloc[0]["genres_list"] == ["Drama", "Horror"]
    assert df.iloc[0]["countries_list"] == ["France"]


def test_compute_summary_metrics_uses_filtered_logs() -> None:
    df = pd.DataFrame(
        [
            {
                "runtime_min": 120,
                "user_rating": 4.0,
                "letterboxd_avg_rating": 3.5,
                "year": 2000,
            },
            {
                "runtime_min": 90,
                "user_rating": 2.0,
                "letterboxd_avg_rating": 2.5,
                "year": 2010,
            },
        ]
    )

    metrics = compute_summary_metrics(df)

    assert metrics == {
        "total_filmes": 2,
        "media_nota_pessoal": 3.0,
        "total_horas": 3.5,
        "diferenca_media": 0.0,
        "media_letterboxd": 3.0,
        "ano_medio_lancamento": 2005.0,
    }


def test_aggregate_helpers_keep_months_and_count_lists() -> None:
    df = pd.DataFrame(
        [
            {
                "watched_month": 12,
                "user_rating": 4.0,
                "countries_list": ["France", "West Germany"],
                "genres_list": ["Drama"],
            },
            {
                "watched_month": 12,
                "user_rating": 4.0,
                "countries_list": ["France"],
                "genres_list": ["Drama", "Horror"],
            },
        ]
    )

    monthly = aggregate_logs_by_month(df)
    ratings = aggregate_rating_distribution(df)
    countries = aggregate_name_counts(df, list_column="countries_list", output_label="country_name")

    assert monthly.loc[monthly["mes"] == 12, "total"].item() == 2
    assert monthly["total"].sum() == 2
    assert ratings.to_dict("records") == [{"rating": 4.0, "total": 2}]
    assert countries[0] == {"country_name": "France", "total_filmes": 2}


def test_extract_selected_month_accepts_multiple_event_shapes() -> None:
    assert extract_selected_month({"selection": {"month_select": {"mes": 12}}}) == 12
    assert extract_selected_month({"selection": {"month_select": [{"mes": 11}]}}) == 11
    assert extract_selected_month({"selection": {"month_select": {"mes": [10]}}}) == 10
    assert extract_selected_month({"selection": {"month_select": {}}}) is None


def test_filter_logged_films_combines_dimensions_and_can_exclude_one() -> None:
    df = pd.DataFrame(
        [
            {"title": "A", "watched_month": 12, "watched_year": 2025, "user_rating": 4.0},
            {"title": "B", "watched_month": 12, "watched_year": 2024, "user_rating": 4.0},
            {"title": "C", "watched_month": 11, "watched_year": 2025, "user_rating": 3.5},
        ]
    )

    filtered = filter_logged_films(df, month=12, year=2025, rating=4.0)
    yearly_view = filter_logged_films(df, month=12, year=2025, rating=4.0, exclude={"year"})

    assert filtered["title"].tolist() == ["A"]
    assert yearly_view["title"].tolist() == ["A", "B"]


def test_extract_selected_year_accepts_multiple_event_shapes() -> None:
    assert extract_selected_year({"selection": {"year_select": {"ano": 2025}}}) == 2025
    assert extract_selected_year({"selection": {"year_select": [{"ano": 2024}]}}) == 2024
    assert extract_selected_year({"selection": {"year_select": {"ano": [2023]}}}) == 2023
    assert extract_selected_year({"selection": {"year_select": {}}}) is None


def test_extract_selected_rating_accepts_multiple_event_shapes() -> None:
    assert extract_selected_rating({"selection": {"rating_select": {"rating": 4.5}}}) == 4.5
    assert extract_selected_rating({"selection": {"rating_select": [{"rating": 3.0}]}}) == 3.0
    assert extract_selected_rating({"selection": {"rating_select": {"rating": [2.5]}}}) == 2.5
    assert extract_selected_rating({"selection": {"rating_select": {}}}) is None
