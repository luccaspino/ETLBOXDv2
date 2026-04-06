from __future__ import annotations

import streamlit as st


def render_nav_card(title: str, description: str, page_path: str, *, button_label: str = "Abrir") -> None:
    with st.container(border=True):
        st.subheader(title)
        st.write(description)
        if st.button(button_label, key=f"nav:{page_path}", width="stretch"):
            st.switch_page(page_path)
