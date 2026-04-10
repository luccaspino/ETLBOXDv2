from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd

from src.dashboard.components.collages import month_label


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def build_logged_films_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "film_id",
                "title",
                "year",
                "runtime_min",
                "user_rating",
                "letterboxd_avg_rating",
                "watched_date",
                "watched_month",
                "watched_year",
                "genres_list",
                "countries_list",
            ]
        )

    df["year"] = pd.to_numeric(df.get("year"), errors="coerce")
    df["runtime_min"] = pd.to_numeric(df.get("runtime_min"), errors="coerce")
    df["user_rating"] = pd.to_numeric(df.get("user_rating"), errors="coerce")
    df["letterboxd_avg_rating"] = pd.to_numeric(df.get("letterboxd_avg_rating"), errors="coerce")
    df["watched_date"] = pd.to_datetime(df.get("watched_date"), errors="coerce")
    df["watched_month"] = df["watched_date"].dt.month.astype("Int64")
    df["watched_year"] = df["watched_date"].dt.year.astype("Int64")
    df["genres_list"] = df.get("genres", pd.Series(dtype="object")).apply(_as_list)
    df["countries_list"] = df.get("countries", pd.Series(dtype="object")).apply(_as_list)
    return df


def filter_logged_films(
    df: pd.DataFrame,
    *,
    month: int | None = None,
    year: int | None = None,
    rating: float | None = None,
    exclude: set[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    exclude = exclude or set()
    filtered = df.copy()

    if month is not None and "month" not in exclude:
        filtered = filtered[filtered["watched_month"] == month]
    if year is not None and "year" not in exclude:
        filtered = filtered[filtered["watched_year"] == year]
    if rating is not None and "rating" not in exclude:
        filtered = filtered[filtered["user_rating"].round(2) == round(float(rating), 2)]

    return filtered.reset_index(drop=True)


def aggregate_logs_by_month(df: pd.DataFrame) -> pd.DataFrame:
    base = pd.DataFrame({"mes": list(range(1, 13))})
    if df.empty:
        base["total"] = 0
        base["month_label"] = base["mes"].apply(month_label)
        return base

    counts = (
        df["watched_month"]
        .dropna()
        .astype(int)
        .value_counts()
        .rename_axis("mes")
        .reset_index(name="total")
    )
    merged = base.merge(counts, on="mes", how="left").fillna({"total": 0})
    merged["total"] = merged["total"].astype(int)
    merged["month_label"] = merged["mes"].apply(month_label)
    return merged


def aggregate_logs_by_year(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["ano", "total"])

    grouped = (
        df["watched_year"]
        .dropna()
        .astype(int)
        .value_counts()
        .rename_axis("ano")
        .reset_index(name="total")
        .sort_values("ano")
        .reset_index(drop=True)
    )
    grouped["total"] = grouped["total"].astype(int)
    return grouped


def aggregate_rating_distribution(df: pd.DataFrame) -> pd.DataFrame:
    rated = df[df["user_rating"].notna()]
    if rated.empty:
        return pd.DataFrame(columns=["rating", "total"])

    grouped = (
        rated.groupby("user_rating", dropna=False)
        .size()
        .reset_index(name="total")
        .sort_values("user_rating")
        .reset_index(drop=True)
        .rename(columns={"user_rating": "rating"})
    )
    grouped["total"] = grouped["total"].astype(int)
    return grouped


def aggregate_name_counts(
    df: pd.DataFrame,
    *,
    list_column: str,
    output_label: str,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    if df.empty or list_column not in df.columns:
        return []

    exploded = df[[list_column]].explode(list_column).dropna()
    if exploded.empty:
        return []

    grouped = (
        exploded.groupby(list_column)
        .size()
        .reset_index(name="total_filmes")
        .sort_values(["total_filmes", list_column], ascending=[False, True])
        .head(top_n)
        .reset_index(drop=True)
        .rename(columns={list_column: output_label})
    )
    grouped["total_filmes"] = grouped["total_filmes"].astype(int)
    return grouped.to_dict("records")


def compute_summary_metrics(df: pd.DataFrame) -> dict[str, Any]:
    rated = df[df["user_rating"].notna()]
    rating_gap = df[df["user_rating"].notna() & df["letterboxd_avg_rating"].notna()]
    valid_years = df[df["year"].notna()]

    media_nota_pessoal = round(float(rated["user_rating"].mean()), 2) if not rated.empty else None
    total_horas = round(float(df["runtime_min"].fillna(0).sum()) / 60.0, 2)
    diferenca_media = (
        round(float((rating_gap["user_rating"] - rating_gap["letterboxd_avg_rating"]).mean()), 2)
        if not rating_gap.empty
        else None
    )
    media_letterboxd = (
        round(float(rating_gap["letterboxd_avg_rating"].mean()), 2)
        if not rating_gap.empty
        else None
    )
    ano_medio_lancamento = round(float(valid_years["year"].mean()), 1) if not valid_years.empty else None

    return {
        "total_filmes": int(len(df)),
        "media_nota_pessoal": media_nota_pessoal,
        "total_horas": total_horas,
        "diferenca_media": diferenca_media,
        "media_letterboxd": media_letterboxd,
        "ano_medio_lancamento": ano_medio_lancamento,
    }


def build_month_selection_chart(monthly_df: pd.DataFrame, selected_month: int | None) -> alt.Chart:
    chart_df = monthly_df.copy()
    chart_df["is_selected"] = chart_df["mes"].eq(selected_month)
    month_select = alt.selection_point(
        name="month_select",
        fields=["mes"],
        on="click",
        clear="dblclick",
        toggle=False,
    )

    return (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X(
                "mes:O",
                sort=list(range(1, 13)),
                title="Mês assistido",
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y("total:Q", title="Total de filmes logados"),
            color=alt.condition(
                alt.datum.is_selected,
                alt.value("#82c96b"),
                alt.value("#78b7f0"),
            ),
            opacity=alt.condition(
                alt.datum.is_selected,
                alt.value(1.0),
                alt.value(0.72),
            ),
            tooltip=[
                alt.Tooltip("month_label:N", title="Mês"),
                alt.Tooltip("total:Q", title="Total"),
            ],
        )
        .add_params(month_select)
        .properties(height=320)
        .configure_view(strokeWidth=0)
    )


def build_year_selection_chart(yearly_df: pd.DataFrame, selected_year: int | None) -> alt.Chart:
    chart_df = yearly_df.copy()
    chart_df["is_selected"] = chart_df["ano"].eq(selected_year)
    year_select = alt.selection_point(
        name="year_select",
        fields=["ano"],
        on="click",
        clear="dblclick",
        toggle=False,
    )

    return (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X("ano:O", title="Ano assistido", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("total:Q", title="Total de filmes logados"),
            color=alt.condition(
                alt.datum.is_selected,
                alt.value("#f5a45d"),
                alt.value("#78b7f0"),
            ),
            opacity=alt.condition(
                alt.datum.is_selected,
                alt.value(1.0),
                alt.value(0.72),
            ),
            tooltip=[
                alt.Tooltip("ano:O", title="Ano"),
                alt.Tooltip("total:Q", title="Total"),
            ],
        )
        .add_params(year_select)
        .properties(height=320)
        .configure_view(strokeWidth=0)
    )


def build_rating_selection_chart(rating_df: pd.DataFrame, selected_rating: float | None) -> alt.Chart:
    chart_df = rating_df.copy()
    rounded_rating = round(float(selected_rating), 2) if selected_rating is not None else None
    chart_df["is_selected"] = chart_df["rating"].round(2).eq(rounded_rating)
    rating_select = alt.selection_point(
        name="rating_select",
        fields=["rating"],
        on="click",
        clear="dblclick",
        toggle=False,
    )

    return (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
        .encode(
            x=alt.X("rating:O", title="Nota pessoal", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("total:Q", title="Total de filmes"),
            color=alt.condition(
                alt.datum.is_selected,
                alt.value("#82c96b"),
                alt.value("#78b7f0"),
            ),
            opacity=alt.condition(
                alt.datum.is_selected,
                alt.value(1.0),
                alt.value(0.72),
            ),
            tooltip=[
                alt.Tooltip("rating:O", title="Nota"),
                alt.Tooltip("total:Q", title="Total"),
            ],
        )
        .add_params(rating_select)
        .properties(height=320)
        .configure_view(strokeWidth=0)
    )


def extract_selected_month(event: Any) -> int | None:
    return _extract_selected_value(
        event,
        selection_name="month_select",
        field_name="mes",
        converter=_coerce_month,
    )


def extract_selected_year(event: Any) -> int | None:
    return _extract_selected_value(
        event,
        selection_name="year_select",
        field_name="ano",
        converter=_coerce_year,
    )


def extract_selected_rating(event: Any) -> float | None:
    return _extract_selected_value(
        event,
        selection_name="rating_select",
        field_name="rating",
        converter=_coerce_rating,
    )


def format_rating_label(value: float | None) -> str:
    if value is None:
        return "-"

    normalized = round(float(value), 2)
    if normalized.is_integer():
        return f"{normalized:.1f}"
    return f"{normalized:.2f}".rstrip("0").rstrip(".")


def _extract_selected_value(
    event: Any,
    *,
    selection_name: str,
    field_name: str,
    converter,
) -> Any:
    if event is None:
        return None

    selection = getattr(event, "selection", None)
    if selection is None and isinstance(event, dict):
        selection = event.get("selection")
    if selection is None:
        return None

    raw_selection = getattr(selection, selection_name, None)
    if raw_selection is None and isinstance(selection, dict):
        raw_selection = selection.get(selection_name)
    return _coerce_selection_value(raw_selection, field_name=field_name, converter=converter)


def _coerce_selection_value(value: Any, *, field_name: str, converter) -> Any:
    if value in (None, {}, []):
        return None

    candidates: list[Any] = [value]
    while candidates:
        candidate = candidates.pop(0)
        if candidate in (None, {}, []):
            continue
        if isinstance(candidate, list):
            candidates = list(candidate) + candidates
            continue
        if isinstance(candidate, tuple):
            candidates = list(candidate) + candidates
            continue
        if isinstance(candidate, dict):
            if field_name in candidate:
                candidates.insert(0, candidate[field_name])
            if "value" in candidate:
                candidates.insert(0, candidate["value"])
            if "values" in candidate:
                candidates.insert(0, candidate["values"])
            continue

        converted = converter(candidate)
        if converted is not None:
            return converted

    return None


def _coerce_month(value: Any) -> int | None:
    try:
        month = int(value)
    except (TypeError, ValueError):
        return None
    if 1 <= month <= 12:
        return month
    return None


def _coerce_year(value: Any) -> int | None:
    try:
        year = int(value)
    except (TypeError, ValueError):
        return None
    if 1880 <= year <= 2100:
        return year
    return None


def _coerce_rating(value: Any) -> float | None:
    try:
        rating = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    if 0.5 <= rating <= 5.0:
        return rating
    return None
