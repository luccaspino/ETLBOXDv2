from __future__ import annotations

import atexit
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable
from urllib.parse import quote

import httpx

from src.config import get_env

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

_SHARED_HTTP_CLIENTS: dict[str, httpx.Client] = {}
_SHARED_HTTP_CLIENTS_LOCK = threading.Lock()


def _cache_data(ttl: int = 120, max_entries: int = 128):
    if st is None:  # pragma: no cover
        def decorator(func):
            func.clear = lambda: None
            return func

        return decorator
    return st.cache_data(show_spinner=False, ttl=ttl, max_entries=max_entries)


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
        "API_BASE_URL nao configurada. Defina no .env local ou em st.secrets no Streamlit Cloud.",
    )


def _get_http_client(base_url: str) -> httpx.Client:
    with _SHARED_HTTP_CLIENTS_LOCK:
        client = _SHARED_HTTP_CLIENTS.get(base_url)
        if client is None:
            client = httpx.Client(
                base_url=base_url,
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=40),
            )
            _SHARED_HTTP_CLIENTS[base_url] = client
        return client


def _close_http_clients() -> None:
    with _SHARED_HTTP_CLIENTS_LOCK:
        clients = list(_SHARED_HTTP_CLIENTS.values())
        _SHARED_HTTP_CLIENTS.clear()

    for client in clients:
        try:
            client.close()
        except Exception:
            pass


atexit.register(_close_http_clients)


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
    base_url = get_api_base_url()
    client = _get_http_client(base_url)

    try:
        response = client.request(
            method=method,
            url=path,
            params=params,
            data=data,
            files=files,
            timeout=timeout,
        )
    except httpx.RequestError as err:
        raise ApiClientError(
            f"Nao foi possivel conectar a API em {base_url}.",
        ) from err

    if response.is_error:
        detail = _extract_error_detail(response)
        raise ApiClientError(
            detail,
            status_code=response.status_code,
            detail=detail,
        )

    try:
        return response.json()
    except ValueError as err:
        raise ApiClientError("A API retornou uma resposta JSON invalida.") from err


def _run_parallel_calls(calls: dict[str, Callable[[], Any]]) -> dict[str, Any]:
    if not calls:
        return {}
    if len(calls) == 1:
        key, func = next(iter(calls.items()))
        return {key: func()}

    results: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(calls))) as executor:
        future_to_key = {executor.submit(func): key for key, func in calls.items()}
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            results[key] = future.result()
    return results


def clear_dashboard_caches() -> None:
    for func in [
        get_user_lookup,
        get_main_kpis,
        get_rating_gap_kpis,
        get_release_year_kpi,
        get_monthly_logs,
        get_yearly_logs,
        get_rating_distribution,
        get_country_distribution,
        get_genre_distribution,
        get_rankings,
        get_watchlist,
        get_filter_options,
        get_summary_bundle,
        get_rankings_bundle,
    ]:
        clear = getattr(func, "clear", None)
        if callable(clear):
            clear()


def run_pipeline_upload(file_name: str, file_bytes: bytes) -> dict[str, Any]:
    files = {
        "file": (file_name, file_bytes, "application/zip"),
    }
    payload = _request_json("POST", "/pipeline/run", files=files, timeout=300.0)
    clear_dashboard_caches()
    return dict(payload)


@_cache_data()
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


@_cache_data()
def get_main_kpis(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/main", username))


@_cache_data()
def get_rating_gap_kpis(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/rating-gap", username))


@_cache_data()
def get_release_year_kpi(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/kpis/release-year", username))


@_cache_data()
def get_monthly_logs(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/logs/monthly", username))


@_cache_data()
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

    legacy_rows = get_filtered_films(username, **filters)
    normalized_rows: list[dict[str, Any]] = []
    for row in legacy_rows:
        normalized = dict(row)
        normalized.setdefault("genres", [])
        normalized.setdefault("countries", [])
        normalized_rows.append(normalized)
    return normalized_rows


@_cache_data()
def get_rating_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/ratings", username))


@_cache_data()
def get_country_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/countries", username))


@_cache_data()
def get_genre_distribution(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/distribution/genres", username))


@_cache_data(max_entries=256)
def get_rankings(username: str, category: str, order_by: str, *, min_films: int) -> list[dict[str, Any]]:
    path = f"/analytics/rankings/{category}/{order_by}"
    return list(_analytics_get(path, username, min_films=min_films))


def get_random_watchlist_pick(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/random", username))


def get_random_review_pick(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/reviews/random", username))


@_cache_data()
def get_watchlist(username: str) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/watchlist", username))


@_cache_data()
def get_filter_options(username: str) -> dict[str, Any]:
    return dict(_analytics_get("/analytics/filters/options", username))


def get_filtered_films(username: str, **filters: Any) -> list[dict[str, Any]]:
    return list(_analytics_get("/analytics/films", username, **filters))


@_cache_data(max_entries=64)
def get_summary_bundle(username: str) -> dict[str, Any]:
    payload = _run_parallel_calls(
        {
            "main_kpis": lambda: get_main_kpis(username),
            "rating_gap": lambda: get_rating_gap_kpis(username),
            "release_year": lambda: get_release_year_kpi(username),
            "monthly_logs": lambda: get_monthly_logs(username),
            "yearly_logs": lambda: get_yearly_logs(username),
            "rating_distribution_rows": lambda: get_rating_distribution(username),
            "country_distribution": lambda: get_country_distribution(username),
            "genre_distribution": lambda: get_genre_distribution(username),
        }
    )

    try:
        payload["logged_film_rows"] = get_logged_films(username, allow_legacy_fallback=False)
        payload["logged_films_supported"] = True
    except ApiClientError as err:
        if err.status_code != 404:
            raise
        payload["logged_film_rows"] = []
        payload["logged_films_supported"] = False

    return payload


@_cache_data(max_entries=64)
def get_rankings_bundle(
    username: str,
    *,
    min_most_watched: int,
    min_best_rated: int,
) -> dict[str, Any]:
    categories = ("directors", "actors", "genres", "countries")
    calls: dict[str, Callable[[], Any]] = {
        "filter_options": lambda: get_filter_options(username),
    }
    for category in categories:
        calls[f"{category}:most_watched"] = lambda category=category: get_rankings(
            username,
            category,
            "most-watched",
            min_films=min_most_watched,
        )
        calls[f"{category}:best_rated"] = lambda category=category: get_rankings(
            username,
            category,
            "best-rated",
            min_films=min_best_rated,
        )

    raw_results = _run_parallel_calls(calls)
    rankings_by_category = {
        category: {
            "most_watched": raw_results[f"{category}:most_watched"],
            "best_rated": raw_results[f"{category}:best_rated"],
        }
        for category in categories
    }
    return {
        "filter_options": raw_results["filter_options"],
        "rankings_by_category": rankings_by_category,
    }
