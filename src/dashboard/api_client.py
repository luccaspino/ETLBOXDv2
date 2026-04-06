from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx
from src.config import get_env

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None


class ApiClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, detail: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


def get_api_base_url() -> str:
    env_value = (get_env("API_BASE_URL", "") or "").strip()
    if env_value:
        return env_value.rstrip("/")

    if st is not None:
        try:
            secret_value = st.secrets.get("API_BASE_URL")
        except Exception:
            secret_value = None
        if secret_value:
            return str(secret_value).strip().rstrip("/")

    raise ApiClientError(
        "API_BASE_URL não configurada. Defina no .env local ou em st.secrets no Streamlit Cloud.",
    )


def _extract_error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or "Resposta de erro sem detalhe."

    detail = payload.get("detail")
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    return "Erro desconhecido retornado pela API."


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    files: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> Any:
    url = f"{get_api_base_url()}{path}"
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.request(
                method=method,
                url=url,
                params=params,
                data=data,
                files=files,
            )
    except httpx.RequestError as err:
        raise ApiClientError(
            f"Não foi possível conectar à API em {get_api_base_url()}.",
        ) from err

    if response.is_error:
        detail = _extract_error_detail(response)
        raise ApiClientError(
            f"{detail}",
            status_code=response.status_code,
            detail=detail,
        )

    try:
        return response.json()
    except ValueError as err:
        raise ApiClientError("A API retornou uma resposta JSON inválida.") from err


def run_pipeline_upload(file_name: str, file_bytes: bytes) -> dict[str, Any]:
    files = {
        "file": (file_name, file_bytes, "application/zip"),
    }
    payload = _request_json("POST", "/pipeline/run", files=files, timeout=300.0)
    return dict(payload)


def get_user_lookup(username: str) -> dict[str, Any]:
    encoded = quote(username.strip(), safe="")
    payload = _request_json("GET", f"/users/{encoded}")
    return dict(payload)


def _analytics_get(path: str, username: str, **params: Any) -> Any:
    query_params = {"username": username}
    for key, value in params.items():
        if value is None or value == "":
            continue
        query_params[key] = value
    return _request_json("GET", path, params=query_params)


def get_main_kpis(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/main", username))


def get_rating_gap_kpis(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/rating-gap", username))


def get_release_year_kpi(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/release-year", username))


def get_monthly_logs(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/logs/monthly", username))


def get_yearly_logs(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/logs/yearly", username))


def get_logged_films(
    username: str,
    *,
    allow_legacy_fallback: bool = True,
    **filters: Any,
) -> list[dict[str, Any]]:
    try:
        payload = _analytics_get("/analytics/logs/films", username, **filters)
        return list(payload)
    except ApiClientError as err:
        if err.status_code != 404 or not allow_legacy_fallback:
            raise

    # Fallback para ambientes cujo backend ainda nao publicou /analytics/logs/films.
    legacy_rows = get_filtered_films(username, **filters)
    normalized_rows: list[dict[str, Any]] = []
    for row in legacy_rows:
        normalized = dict(row)
        normalized.setdefault("genres", [])
        normalized.setdefault("countries", [])
        normalized_rows.append(normalized)
    return normalized_rows


def get_rating_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/ratings", username))


def get_country_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/countries", username))


def get_genre_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/genres", username))


def get_rankings(username: str, category: str, order_by: str, *, min_films: int) -> list[dict[str, Any]]:
    path = f"/analytics/rankings/{category}/{order_by}"
    return list(_analytics_get(path, username, min_films=min_films))


def get_random_watchlist_pick(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/random", username))


def get_random_review_pick(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/reviews/random", username))


def get_watchlist(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/watchlist", username))


def get_filter_options(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/filters/options", username))


def get_filtered_films(username: str, **filters: Any) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/films", username, **filters))
