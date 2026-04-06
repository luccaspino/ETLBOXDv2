from __future__ import annotations

from typing import Iterable

import pandas as pd
import streamlit as st

COLUMN_LABELS = {
    "title": "Título",
    "year": "Ano",
    "username": "Usuário",
    "user_rating": "Nota pessoal",
    "letterboxd_avg_rating": "Média Letterboxd",
    "avg_user_rating": "Média pessoal",
    "avg_letterboxd_rating": "Média Letterboxd",
    "runtime_min": "Duração (min)",
    "watched_date": "Data assistida",
    "release_year": "Ano de lançamento",
    "country": "País",
    "country_name": "País",
    "genre": "Gênero",
    "genre_name": "Gênero",
    "genres": "Gêneros",
    "director": "Diretor",
    "director_name": "Diretor",
    "actor_name": "Ator",
    "cast_top3": "Elenco principal",
    "review_text": "Texto da review",
    "letterboxd_url": "Link do Letterboxd",
    "original_language": "Idioma original",
    "tagline": "Tagline",
    "count": "Quantidade",
}


def _format_column_label(column: str) -> str:
    if column in COLUMN_LABELS:
        return COLUMN_LABELS[column]
    return column.replace("_", " ").title()


def render_records_table(
    rows: Iterable[dict],
    *,
    link_columns: list[str] | None = None,
    hidden_columns: list[str] | None = None,
) -> None:
    df = pd.DataFrame(list(rows))
    render_dataframe(df, link_columns=link_columns, hidden_columns=hidden_columns)


def render_dataframe(
    df: pd.DataFrame,
    *,
    link_columns: list[str] | None = None,
    hidden_columns: list[str] | None = None,
) -> None:
    if df.empty:
        st.dataframe(df, width="stretch", hide_index=True)
        return

    display_df = df.copy()
    display_df = display_df.rename(columns={column: _format_column_label(column) for column in display_df.columns})
    if hidden_columns:
        hidden_labels = {_format_column_label(column) for column in hidden_columns}
        keep_columns = [column for column in display_df.columns if column not in hidden_labels]
        display_df = display_df[keep_columns]

    column_config: dict[str, object] = {}
    for column in link_columns or []:
        column_label = _format_column_label(column)
        if column_label in display_df.columns:
            column_config[column_label] = st.column_config.LinkColumn(column_label)

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config=column_config,
    )
