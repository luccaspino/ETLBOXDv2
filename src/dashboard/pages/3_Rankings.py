from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

from src.dashboard.api_client import ApiClientError, get_filter_options, get_filtered_films, get_rankings
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.collages import render_film_grid
from src.dashboard.components.messages import render_api_error, render_empty_state
from src.dashboard.state import get_active_username, initialize_state

configure_page("ETLboxd | Rankings")
initialize_state()

username = get_active_username()

st.title("Rankings")
st.caption("Rankings do usuário ativo em mais vistos e melhor avaliados.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Selecione um usuário",
        "Abra o Menu e escolha um usuário antes de consultar os rankings.",
    )
    st.stop()

st.session_state.setdefault("ranking_drilldown", None)

ranking_sections = [
    ("directors", "Diretores"),
    ("actors", "Atores"),
    ("genres", "Gêneros"),
    ("countries", "Países"),
]


def _set_drilldown(category_key: str, item_name: str) -> None:
    st.session_state["ranking_drilldown"] = {
        "category": category_key,
        "name": item_name,
    }


def _render_ranking_list(title: str, rows: list[dict], category_key: str, *, key_prefix: str) -> None:
    st.subheader(title)
    if not rows:
        render_empty_state("Sem resultados", "Nenhum item atendeu a esse recorte.")
        return

    header_col1, header_col2, header_col3 = st.columns([3, 1, 1])
    header_col1.caption("Nome")
    header_col2.caption("Filmes")
    header_col3.caption("Média")

    for index, row in enumerate(rows):
        row_col1, row_col2, row_col3 = st.columns([3, 1, 1])
        item_name = str(row.get("nome") or "-")
        with row_col1:
            if st.button(
                item_name,
                key=f"{key_prefix}:{index}:{item_name}",
                width="stretch",
            ):
                _set_drilldown(category_key, item_name)
        row_col2.write(str(row.get("filmes_assistidos", 0)))
        row_col3.write(str(row.get("media_nota_pessoal", "-")))


min_col1, min_col2 = st.columns(2)
with min_col1:
    min_most_watched = st.number_input("Mínimo para mais vistos", min_value=1, value=1, step=1)
with min_col2:
    min_best_rated = st.number_input("Mínimo para melhor avaliados", min_value=1, value=3, step=1)

country_name_to_code: dict[str, str] = {}

try:
    with st.spinner("Carregando rankings..."):
        filter_options = get_filter_options(username)
        country_name_to_code = {
            item["name"]: item["code"]
            for item in filter_options.get("country_options", [])
            if item.get("name") and item.get("code")
        }
        rankings_by_category = {
            category_key: {
                "most_watched": get_rankings(
                    username,
                    category_key,
                    "most-watched",
                    min_films=int(min_most_watched),
                ),
                "best_rated": get_rankings(
                    username,
                    category_key,
                    "best-rated",
                    min_films=int(min_best_rated),
                ),
            }
            for category_key, _ in ranking_sections
        }
except ApiClientError as err:
    render_api_error(err)
    st.stop()

for category_key, category_label in ranking_sections:
    st.markdown("---")
    st.header(category_label)
    rank_col1, rank_col2 = st.columns(2)
    with rank_col1:
        _render_ranking_list(
            "Mais vistos",
            rankings_by_category[category_key]["most_watched"],
            category_key,
            key_prefix=f"{category_key}:most",
        )
    with rank_col2:
        _render_ranking_list(
            "Melhor avaliados",
            rankings_by_category[category_key]["best_rated"],
            category_key,
            key_prefix=f"{category_key}:best",
        )

drilldown = st.session_state.get("ranking_drilldown")
if drilldown:
    selected_category = str(drilldown.get("category") or "")
    selected_name = str(drilldown.get("name") or "").strip()
    if selected_category and selected_name:
        filter_payload: dict[str, object] = {}
        if selected_category == "countries":
            country_code = country_name_to_code.get(selected_name)
            if country_code:
                filter_payload["country_code"] = country_code
        elif selected_category == "genres":
            filter_payload["genre_name"] = selected_name
        elif selected_category == "directors":
            filter_payload["director_name"] = selected_name
        elif selected_category == "actors":
            filter_payload["actor_name"] = selected_name

        st.markdown("---")
        st.subheader(f"Filmes relacionados: {selected_name}")

        if not filter_payload:
            render_empty_state(
                "Não foi possível filtrar",
                "Esse item não pôde ser convertido em filtro para listar os filmes.",
            )
        else:
            try:
                with st.spinner("Carregando filmes do item selecionado..."):
                    filtered_rows = get_filtered_films(username, **filter_payload)
            except ApiClientError as err:
                render_api_error(err)
            else:
                if not filtered_rows:
                    render_empty_state(
                        "Sem filmes encontrados",
                        "Nenhum filme foi encontrado para esse recorte.",
                    )
                else:
                    st.caption(f"{len(filtered_rows)} filme(s) encontrados.")
                    render_film_grid(filtered_rows)
