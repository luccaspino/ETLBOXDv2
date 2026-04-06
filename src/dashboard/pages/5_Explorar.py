from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

from src.dashboard.api_client import ApiClientError, get_filter_options, get_filtered_films, get_watchlist
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.filters import (
    build_watchlist_dataframe,
    filter_watchlist_dataframe,
    unique_values,
)
from src.dashboard.components.messages import render_api_error, render_empty_state
from src.dashboard.components.tables import render_dataframe, render_records_table
from src.dashboard.state import get_active_username, initialize_state

configure_page("ETLboxd | Explorar")
initialize_state()

username = get_active_username()

st.title("Explorar")
st.caption("Explore filmes vistos com filtros da API e a sua watchlist com filtros locais.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Selecione um usuário",
        "Abra o Menu e escolha um usuário antes de usar o explorador.",
    )
    st.stop()

collection_mode = st.radio(
    "Coleção",
    options=["Filmes vistos", "Watchlist"],
    horizontal=True,
)

if collection_mode == "Filmes vistos":
    try:
        with st.spinner("Carregando filtros e filmes..."):
            filter_options = get_filter_options(username)
    except ApiClientError as err:
        render_api_error(err)
        st.stop()

    country_options = {
        item["name"]: item["code"]
        for item in filter_options.get("country_options", [])
    }
    personal_ratings = filter_options.get("personal_ratings", [])
    runtime_range = filter_options.get("runtime", {})

    with st.form("films-filter-form"):
        top_col1, top_col2, top_col3 = st.columns(3)
        with top_col1:
            min_rating = st.selectbox("Nota pessoal mínima", options=[None] + personal_ratings, format_func=lambda value: "Qualquer" if value is None else str(value))
            watched_year = st.selectbox("Ano assistido", options=[None] + filter_options.get("watched_years", []), format_func=lambda value: "Todos" if value is None else str(value))
            watched_month = st.selectbox("Mês assistido", options=[None] + filter_options.get("watched_months", []), format_func=lambda value: "Todos" if value is None else str(value))
        with top_col2:
            max_rating = st.selectbox("Nota pessoal máxima", options=[None] + personal_ratings, format_func=lambda value: "Qualquer" if value is None else str(value))
            decade_start = st.selectbox("Década de lançamento", options=[None] + filter_options.get("release_decades", []), format_func=lambda value: "Todas" if value is None else str(value))
            genre_name = st.selectbox("Gênero", options=[None] + filter_options.get("genres", []), format_func=lambda value: "Todos" if value is None else value)
        with top_col3:
            min_runtime = st.number_input(
                "Runtime mínimo",
                min_value=0,
                value=int(runtime_range.get("min") or 0),
                step=5,
            )
            max_runtime_default = int(runtime_range.get("max") or 0)
            max_runtime = st.number_input(
                "Runtime máximo",
                min_value=0,
                value=max_runtime_default,
                step=5,
            )
            country_name = st.selectbox("País", options=[None] + sorted(country_options.keys()), format_func=lambda value: "Todos" if value is None else value)

        extra_col1, extra_col2 = st.columns(2)
        with extra_col1:
            director_name = st.selectbox("Diretor", options=[None] + filter_options.get("directors", []), format_func=lambda value: "Todos" if value is None else value)
        with extra_col2:
            actor_name = st.selectbox("Ator", options=[None] + filter_options.get("actors", []), format_func=lambda value: "Todos" if value is None else value)

        st.form_submit_button("Aplicar filtros", width="stretch")

    filters = {
        "min_rating": min_rating,
        "max_rating": max_rating,
        "min_runtime": min_runtime or None,
        "max_runtime": max_runtime or None,
        "decade_start": decade_start,
        "director_name": director_name,
        "actor_name": actor_name,
        "country_code": country_options.get(country_name),
        "genre_name": genre_name,
        "watched_month": watched_month,
        "watched_year": watched_year,
    }

    try:
        with st.spinner("Consultando filmes filtrados..."):
            film_rows = get_filtered_films(username, **filters)
    except ApiClientError as err:
        render_api_error(err)
        st.stop()

    st.subheader(f"Resultados ({len(film_rows)})")
    render_records_table(film_rows, link_columns=["letterboxd_url"])

else:
    try:
        with st.spinner("Carregando watchlist..."):
            watchlist_rows = get_watchlist(username)
    except ApiClientError as err:
        render_api_error(err)
        st.stop()

    watchlist_df = build_watchlist_dataframe(watchlist_rows)
    if watchlist_df.empty:
        render_empty_state("Watchlist vazia", "Não há itens na watchlist para explorar.")
        st.stop()

    available_genres = unique_values(genre for values in watchlist_df["genres_list"] for genre in values)
    available_directors = unique_values(director for values in watchlist_df["directors_list"] for director in values)
    available_actors = unique_values(actor for values in watchlist_df["actors_list"] for actor in values)
    available_languages = unique_values(watchlist_df["original_language"].dropna().tolist())

    with st.form("watchlist-filter-form"):
        st.caption("Campos numéricos com valor 0 não aplicam filtro.")
        top_col1, top_col2, top_col3 = st.columns(3)
        with top_col1:
            title_query = st.text_input("Buscar por título")
            min_year = st.number_input("Ano mínimo", min_value=0, value=0, step=1)
            max_year = st.number_input("Ano máximo", min_value=0, value=0, step=1)
        with top_col2:
            min_runtime = st.number_input("Runtime mínimo", min_value=0, value=0, step=5)
            max_runtime = st.number_input("Runtime máximo", min_value=0, value=0, step=5)
            original_language = st.selectbox("Idioma original", options=[None] + available_languages, format_func=lambda value: "Todos" if value is None else value)
        with top_col3:
            min_avg_rating = st.number_input("Média Letterboxd mínima", min_value=0.0, value=0.0, step=0.1)
            max_avg_rating = st.number_input("Média Letterboxd máxima", min_value=0.0, value=0.0, step=0.1)

        selected_genres = st.multiselect("Gêneros", options=available_genres)
        selected_directors = st.multiselect("Diretores", options=available_directors)
        selected_actors = st.multiselect("Atores", options=available_actors)

        st.form_submit_button("Aplicar filtros", width="stretch")

    filtered_watchlist = filter_watchlist_dataframe(
        watchlist_df,
        title_query=title_query,
        min_year=min_year or None,
        max_year=max_year or None,
        min_runtime=min_runtime or None,
        max_runtime=max_runtime or None,
        min_avg_rating=min_avg_rating or None,
        max_avg_rating=max_avg_rating or None,
        original_language=original_language,
        selected_genres=selected_genres,
        selected_directors=selected_directors,
        selected_actors=selected_actors,
    )

    st.subheader(f"Resultados ({len(filtered_watchlist)})")
    render_dataframe(
        filtered_watchlist.drop(columns=["genres_list", "directors_list", "actors_list"], errors="ignore"),
        link_columns=["letterboxd_url"],
    )
