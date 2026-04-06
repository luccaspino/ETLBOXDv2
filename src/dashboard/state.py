from __future__ import annotations

from typing import Any

import streamlit as st

_DEFAULTS: dict[str, Any] = {
    "active_username": None,
    "last_pipeline_summary": None,
}


def _coerce_query_value(value: Any) -> str | None:
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def initialize_state() -> None:
    for key, default in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default
    sync_active_username()


def sync_active_username() -> None:
    username_from_url = _coerce_query_value(st.query_params.get("user"))
    active_username = _coerce_query_value(st.session_state.get("active_username"))

    if username_from_url and username_from_url != active_username:
        st.session_state["active_username"] = username_from_url
    elif active_username and not username_from_url:
        st.query_params["user"] = active_username


def get_active_username() -> str | None:
    return _coerce_query_value(st.session_state.get("active_username"))


def set_active_username(username: str | None) -> None:
    cleaned = _coerce_query_value(username)
    st.session_state["active_username"] = cleaned
    if cleaned:
        st.query_params["user"] = cleaned
        return

    try:
        del st.query_params["user"]
    except Exception:
        pass


def clear_active_username() -> None:
    set_active_username(None)


def set_last_pipeline_summary(summary: dict[str, Any] | None) -> None:
    st.session_state["last_pipeline_summary"] = summary


def get_last_pipeline_summary() -> dict[str, Any] | None:
    summary = st.session_state.get("last_pipeline_summary")
    return summary if isinstance(summary, dict) else None

