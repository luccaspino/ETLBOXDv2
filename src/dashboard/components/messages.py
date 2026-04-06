from __future__ import annotations

import streamlit as st


def render_api_error(err: Exception, *, message: str = "Ocorreu um erro ao carregar os dados.") -> None:
    detail = ""

    err_detail = getattr(err, "detail", None)
    if isinstance(err_detail, str) and err_detail.strip():
        detail = err_detail.strip()
    else:
        text = str(err).strip()
        if text:
            detail = text

    if detail and detail != message:
        st.error(f"{message}\n\n{detail}")
        return

    st.error(message)


def render_empty_state(title: str, body: str) -> None:
    st.info(f"**{title}**\n\n{body}")
