from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

from src.dashboard.api_client import ApiClientError, get_random_review_pick, get_random_watchlist_pick
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.messages import render_api_error, render_empty_state
from src.dashboard.state import get_active_username, initialize_state

configure_page("ETLboxd | Ferramentas")
initialize_state()

username = get_active_username()

st.title("Ferramentas")
st.caption("Espaço para as funcionalidades mais divertidas do dashboard.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Selecione um usuário",
        "Abra o Menu e escolha um usuário antes de usar as ferramentas.",
    )
    st.stop()

st.session_state.setdefault("random_review_pick", None)
st.session_state.setdefault("review_answer_revealed", False)
st.session_state.setdefault("random_watchlist_pick", None)

review_tab, watchlist_tab = st.tabs(["Adivinhar pela review", "Watchlist aleatória"])

with review_tab:
    st.subheader("Adivinhe o filme pela review")
    st.write("Sorteie uma review aleatória e tente adivinhar o filme antes de revelar a resposta.")
    if st.button("Sortear review", key="draw-review", width="stretch"):
        try:
            st.session_state["random_review_pick"] = get_random_review_pick(username)
            st.session_state["review_answer_revealed"] = False
        except ApiClientError as err:
            render_api_error(err)

    review_pick = st.session_state.get("random_review_pick")
    if review_pick:
        st.text_area(
            "Texto da review",
            value=review_pick["review_text"],
            height=220,
            disabled=True,
        )
        if st.button("Revelar resposta", key="reveal-review", width="stretch"):
            st.session_state["review_answer_revealed"] = True
        if st.session_state.get("review_answer_revealed"):
            st.success(f"{review_pick['title']} ({review_pick.get('year') or '-'})")
            st.caption(f"Assistido em: {review_pick.get('watched_date') or '-'}")
            st.link_button("Abrir no Letterboxd", review_pick["letterboxd_url"])

with watchlist_tab:
    st.subheader("Watchlist aleatória")
    st.write("Sorteie um filme da sua watchlist para decidir o que assistir agora.")
    if st.button("Sortear filme", key="draw-watchlist", width="stretch"):
        try:
            st.session_state["random_watchlist_pick"] = get_random_watchlist_pick(username)
        except ApiClientError as err:
            render_api_error(err)

    watchlist_pick = st.session_state.get("random_watchlist_pick")
    if watchlist_pick:
        header_col, image_col = st.columns([2, 1])
        with header_col:
            st.success(f"{watchlist_pick['title']} ({watchlist_pick.get('year') or '-'})")
            st.caption(f"Runtime: {watchlist_pick.get('runtime_min') or '-'} min")
            st.caption(f"Média Letterboxd: {watchlist_pick.get('letterboxd_avg_rating') or '-'}")
            if watchlist_pick.get("tagline"):
                st.write(watchlist_pick["tagline"])
            st.link_button("Abrir no Letterboxd", watchlist_pick["letterboxd_url"])
        with image_col:
            if watchlist_pick.get("poster_url"):
                st.image(watchlist_pick["poster_url"], width="stretch")
