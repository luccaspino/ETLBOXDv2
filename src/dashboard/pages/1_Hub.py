from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

from src.dashboard.api_client import ApiClientError, get_user_lookup
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.cards import render_nav_card
from src.dashboard.components.messages import render_api_error, render_empty_state
from src.dashboard.state import get_active_username, initialize_state

configure_page("ETLboxd | Hub")
initialize_state()

username = get_active_username()

st.title("Hub do Dashboard")
st.caption("Ponto de entrada para navegar pelas análises e ferramentas.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Nenhum usuário selecionado",
        "Volte para o Menu, faça upload do ZIP ou consulte um usuário existente antes de abrir as páginas do dashboard.",
    )
    if st.button("Voltar para o Menu", width="stretch"):
        st.switch_page("app.py")
    st.stop()

try:
    user_lookup = get_user_lookup(username)
except ApiClientError as err:
    render_api_error(err)
    st.stop()

stats_col1, stats_col2, stats_col3 = st.columns(3)
stats_col1.metric("Usuário", user_lookup["username"])
stats_col2.metric("Filmes", user_lookup["total_filmes"])
stats_col3.metric("Watchlist", user_lookup["total_watchlist"])

st.markdown("---")

row_one = st.columns(2)
with row_one[0]:
    render_nav_card(
        "Resumo",
        "KPIs, distribuições e histórico mensal e anual do usuário ativo.",
        "pages/2_Resumo.py",
    )
with row_one[1]:
    render_nav_card(
        "Rankings",
        "Países, gêneros, diretores e atores em rankings de mais vistos e melhor avaliados.",
        "pages/3_Rankings.py",
    )

row_two = st.columns(2)
with row_two[0]:
    render_nav_card(
        "Ferramentas",
        "Ferramentas divertidas, como adivinhar o filme pela sua review e usar a roleta de filmes baseada na sua watchlist. Mais novidades estão por vir.",
        "pages/4_Ferramentas.py",
    )
with row_two[1]:
    render_nav_card(
        "Explorar",
        "Explorador com múltiplos filtros para filmes já assistidos e watchlist.",
        "pages/5_Explorar.py",
    )
