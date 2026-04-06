from __future__ import annotations

from pathlib import Path

import streamlit as st

SITE_URL = "https://etlboxd.streamlit.app"
LOGO_PATH = Path(__file__).resolve().parent / "assets" / "etlboxd.png"
NAV_ITEMS = [
    ("Menu", "app.py"),
    ("Hub", "pages/1_Hub.py"),
    ("Resumo", "pages/2_Resumo.py"),
    ("Rankings", "pages/3_Rankings.py"),
    ("Ferramentas", "pages/4_Ferramentas.py"),
    ("Explorar", "pages/5_Explorar.py"),
]


def configure_page(page_title: str) -> None:
    try:
        st.set_page_config(
            page_title=page_title,
            page_icon=str(LOGO_PATH),
            layout="wide",
        )
    except Exception:
        pass
    try:
        st.logo(str(LOGO_PATH), icon_image=str(LOGO_PATH), link=SITE_URL)
    except Exception:
        pass
    st.markdown(
        """
        <style>
        [data-testid="stSidebarHeader"] img {
            height: 4rem !important;
            max-height: 4rem !important;
            width: auto !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_nav(active_username: str | None = None) -> None:
    for label, page in NAV_ITEMS:
        st.page_link(page, label=label, width="stretch")

    st.markdown("---")
    if active_username:
        st.success(f"Usuário ativo: {active_username}")
