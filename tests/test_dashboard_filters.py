from __future__ import annotations

import pandas as pd

from src.dashboard.components.filters import filter_watchlist_dataframe


def test_filter_watchlist_dataframe_treats_title_query_as_literal_text() -> None:
    df = pd.DataFrame(
        [
            {
                "title": "Spider-Man [Homecoming]",
                "year": 2017,
                "runtime_min": 133,
                "letterboxd_avg_rating": 3.8,
                "original_language": "en",
                "genres_list": [],
                "directors_list": [],
                "actors_list": [],
            },
            {
                "title": "C+ +",
                "year": 2000,
                "runtime_min": 90,
                "letterboxd_avg_rating": 3.0,
                "original_language": "en",
                "genres_list": [],
                "directors_list": [],
                "actors_list": [],
            },
        ]
    )

    bracket_result = filter_watchlist_dataframe(df, title_query="[")
    plus_result = filter_watchlist_dataframe(df, title_query="C+ +")

    assert bracket_result["title"].tolist() == ["Spider-Man [Homecoming]"]
    assert plus_result["title"].tolist() == ["C+ +"]
