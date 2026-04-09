from __future__ import annotations

import streamlit as st


def render_api_error(err: Exception, *, message: str = "Ocorreu um erro ao carregar os dados.") -> None:
    def _looks_like_html_document(text: str) -> bool:
        normalized = text.lstrip().lower()
        return normalized.startswith("<!doctype html") or normalized.startswith("<html")

    detail = ""

    err_detail = getattr(err, "detail", None)
    if isinstance(err_detail, str) and err_detail.strip():
        detail = err_detail.strip()
    else:
        text = str(err).strip()
        if text:
            detail = text

    if detail and _looks_like_html_document(detail):
        status_code = getattr(err, "status_code", None)
        if status_code in {502, 503, 504}:
            detail = f"A API esta temporariamente indisponivel (HTTP {status_code}). Tente novamente em instantes."
        else:
            detail = "A API retornou uma pagina de erro inesperada. Tente novamente em instantes."

    if detail and detail != message:
        st.error(f"{message}\n\n{detail}")
        return

    st.error(message)


def render_empty_state(title: str, body: str) -> None:
    st.info(f"**{title}**\n\n{body}")
