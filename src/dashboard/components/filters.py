from __future__ import annotations

from typing import Iterable

import pandas as pd


def split_values(value: str | None, *, separator: str = ",") -> list[str]:
    if not isinstance(value, str):
        return []
    return [item.strip() for item in value.split(separator) if item.strip()]


def unique_values(values: Iterable[str]) -> list[str]:
    return sorted({value.strip() for value in values if isinstance(value, str) and value.strip()})


def build_watchlist_dataframe(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["year"] = pd.to_numeric(df.get("year"), errors="coerce")
    df["runtime_min"] = pd.to_numeric(df.get("runtime_min"), errors="coerce")
    df["letterboxd_avg_rating"] = pd.to_numeric(df.get("letterboxd_avg_rating"), errors="coerce")
    df["genres_list"] = df.get("genres", pd.Series(dtype="object")).apply(split_values)
    df["directors_list"] = df.get("director", pd.Series(dtype="object")).apply(split_values)
    df["actors_list"] = df.get("cast_top3", pd.Series(dtype="object")).apply(
        lambda value: split_values(value, separator="|")
    )
    return df


def filter_watchlist_dataframe(
    df: pd.DataFrame,
    *,
    title_query: str | None = None,
    min_year: int | None = None,
    max_year: int | None = None,
    min_runtime: int | None = None,
    max_runtime: int | None = None,
    min_avg_rating: float | None = None,
    max_avg_rating: float | None = None,
    original_language: str | None = None,
    selected_genres: list[str] | None = None,
    selected_directors: list[str] | None = None,
    selected_actors: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()

    if title_query:
        filtered = filtered[
            filtered["title"].fillna("").str.contains(
                title_query.strip(),
                case=False,
                na=False,
                regex=False,
            )
        ]
    if min_year is not None:
        filtered = filtered[filtered["year"].fillna(-1) >= min_year]
    if max_year is not None:
        filtered = filtered[filtered["year"].fillna(9999) <= max_year]
    if min_runtime is not None:
        filtered = filtered[filtered["runtime_min"].fillna(-1) >= min_runtime]
    if max_runtime is not None:
        filtered = filtered[filtered["runtime_min"].fillna(9999) <= max_runtime]
    if min_avg_rating is not None:
        filtered = filtered[filtered["letterboxd_avg_rating"].fillna(-1) >= min_avg_rating]
    if max_avg_rating is not None:
        filtered = filtered[filtered["letterboxd_avg_rating"].fillna(9999) <= max_avg_rating]
    if original_language:
        filtered = filtered[filtered["original_language"].fillna("") == original_language]

    if selected_genres:
        filtered = filtered[
            filtered["genres_list"].apply(lambda values: all(item in values for item in selected_genres))
        ]
    if selected_directors:
        filtered = filtered[
            filtered["directors_list"].apply(lambda values: all(item in values for item in selected_directors))
        ]
    if selected_actors:
        filtered = filtered[
            filtered["actors_list"].apply(lambda values: all(item in values for item in selected_actors))
        ]

    return filtered
