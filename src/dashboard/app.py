from __future__ import annotations

import csv
import sys
import zipfile
from io import BytesIO, TextIOWrapper
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    import streamlit as st
except Exception as err:  # pragma: no cover
    raise RuntimeError(
        "Streamlit não instalado. Adicione `streamlit` ao requirements para usar o dashboard."
    ) from err

from src.dashboard.api_client import (
    ApiClientError,
    get_backend_status,
    get_user_lookup,
    run_pipeline_upload,
)
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.messages import render_api_error
from src.dashboard.state import (
    get_active_username,
    get_last_pipeline_summary,
    initialize_state,
    set_active_username,
    set_last_pipeline_summary,
)


def _extract_username_from_zip(file_bytes: bytes) -> str | None:
    try:
        with zipfile.ZipFile(BytesIO(file_bytes)) as archive:
            with archive.open("profile.csv") as raw_file:
                with TextIOWrapper(raw_file, encoding="utf-8-sig", newline="") as text_file:
                    first_row = next(csv.DictReader(text_file), None)
    except Exception:
        return None

    if not first_row:
        return None

    for column_name, value in first_row.items():
        normalized = str(column_name).strip().lower().replace(" ", "_")
        if normalized == "username":
            cleaned = str(value or "").strip()
            return cleaned or None

    return None


def _resolve_upload_username(
    summary: dict[str, object],
    fallback_username: str | None,
    inferred_username: str | None = None,
) -> str | None:
    candidate = summary.get("username")
    if candidate is None:
        candidate = inferred_username
    if candidate is None:
        candidate = fallback_username
    if candidate is None:
        candidate = get_active_username()
    if candidate is None:
        try:
            candidate = st.query_params.get("user")
        except Exception:
            candidate = None
    if candidate is None:
        return None
    cleaned = str(candidate).strip()
    return cleaned or None


configure_page("ETLboxd | Menu")
initialize_state()

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.25rem;
        padding-bottom: 2rem;
    }
    h1 {
        margin-bottom: 0.25rem;
    }
    [data-testid="stFileUploaderDropzone"] {
        min-height: 5rem;
    }
    [data-testid="stTextInputRootElement"] > div {
        min-height: 3.25rem;
    }
    [data-testid="stTextInputRootElement"] input {
        min-height: 3.25rem;
    }
    [data-testid="stVerticalBlockBorderWrapper"] {
        min-height: 24rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

active_username = get_active_username()

with st.sidebar:
    render_sidebar_nav(active_username)

st.title("ETLboxd")
st.caption("Envie seu export do Letterboxd ou abra um dashboard já carregado pelo nome de usuário.")

st.link_button(
    "Baixar export do Letterboxd",
    "https://letterboxd.com/data/export/",
    width="content",
)

with st.container(border=True):
    status_col, refresh_col = st.columns([4, 1])
    with status_col:
        st.subheader("Status do backend")
    with refresh_col:
        if st.button("Atualizar status", key="refresh-backend-status", width="stretch"):
            clear = getattr(get_backend_status, "clear", None)
            if callable(clear):
                clear()
            st.rerun()

    backend_status = get_backend_status()
    backend_detail = str(backend_status.get("detail", "")).strip()
    backend_label = str(backend_status.get("label", "Status desconhecido")).strip()
    backend_state = str(backend_status.get("state", "")).strip()

    if backend_state == "online":
        st.success(f"{backend_label}. {backend_detail}")
    elif backend_state == "warming":
        st.warning(f"{backend_label}. {backend_detail}")
    else:
        st.error(f"{backend_label}. {backend_detail}")

current_summary = get_last_pipeline_summary()
current_user = get_active_username()
lookup_result: dict[str, object] | None = None

if current_user:
    try:
        lookup_result = get_user_lookup(current_user)
    except ApiClientError:
        lookup_result = None

if current_user:
    open_col, helper_col = st.columns([1, 2])
    with open_col:
        if st.button("Abrir Hub", width="stretch"):
            st.switch_page("pages/1_Hub.py")
    with helper_col:
        st.caption("O usuário ativo também fica salvo na URL para recarregamento e compartilhamento.")

if lookup_result:
    st.markdown("---")
    st.subheader("Usuário ativo")
    user_col1, user_col2, user_col3 = st.columns(3)
    user_col1.metric("Usuário", lookup_result.get("username", "-"))
    user_col2.metric("Filmes", lookup_result.get("total_filmes", 0))
    user_col3.metric("Watchlist", lookup_result.get("total_watchlist", 0))

upload_col, lookup_col = st.columns(2, gap="large")

with upload_col:
    with st.container(border=True):
        st.subheader("Upload ZIP")
        st.write("Envie o arquivo ZIP exportado pelo Letterboxd para criar ou atualizar seus dados.")
        with st.form("upload-zip-form"):
            uploaded_file = st.file_uploader(
                "Arquivo ZIP do Letterboxd",
                type=["zip"],
                accept_multiple_files=False,
            )
            upload_submitted = st.form_submit_button("Processar upload", width="stretch")

    if upload_submitted:
        if uploaded_file is None:
            st.warning("Selecione um arquivo ZIP antes de enviar.")
        else:
            try:
                file_bytes = uploaded_file.getvalue()
                inferred_username = _extract_username_from_zip(file_bytes)
                with st.spinner("Processando upload e sincronizando os dados..."):
                    summary = run_pipeline_upload(uploaded_file.name, file_bytes)

                resolved_username = _resolve_upload_username(summary, current_user, inferred_username)
                summary_with_username = dict(summary)
                if resolved_username:
                    summary_with_username["username"] = resolved_username

                set_last_pipeline_summary(summary_with_username)
                current_summary = summary_with_username

                if resolved_username:
                    set_active_username(resolved_username)
                    current_user = resolved_username

                st.rerun()
            except KeyError:
                st.error(
                    "O upload foi processado, mas a API não retornou o nome de usuário esperado. "
                    "Tente recarregar a página ou buscar o usuário manualmente."
                )
            except ApiClientError as err:
                render_api_error(err)

with lookup_col:
    with st.container(border=True):
        st.subheader("Consultar usuário existente")
        st.write("Use o nome de usuário exato para abrir um dashboard que já foi carregado ao menos uma vez.")
        with st.form("lookup-user-form"):
            lookup_username = st.text_input("Usuário do Letterboxd")
            lookup_submitted = st.form_submit_button("Buscar usuário", width="stretch")

    if lookup_submitted:
        if not lookup_username.strip():
            st.warning("Digite um usuário para consultar.")
        else:
            try:
                with st.spinner("Buscando usuário..."):
                    lookup_result = get_user_lookup(lookup_username)
                set_active_username(lookup_result["username"])
                current_user = str(lookup_result["username"])
                st.rerun()
            except ApiClientError as err:
                if err.status_code == 404:
                    st.warning("Usuário não existente. Faça upload do ZIP para carregar esse usuário.")
                else:
                    render_api_error(err)

if lookup_result is None and current_user:
    try:
        lookup_result = get_user_lookup(current_user)
    except ApiClientError:
        lookup_result = None

if current_summary:
    st.markdown("---")
    st.subheader("Último upload processado")
    summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
    summary_username = _resolve_upload_username(current_summary, current_user)
    summary_col1.metric("Usuário", summary_username or "-")
    summary_col2.metric("Filmes importados do scrape", current_summary.get("films_upserted_from_scrape", 0))
    summary_col3.metric("Filmes carregados", current_summary.get("user_films_loaded", 0))
    summary_col4.metric("Watchlist carregada", current_summary.get("watchlist_loaded", 0))
