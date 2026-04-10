from __future__ import annotations

import httpx

from src.dashboard import api_client


def test_run_pipeline_upload_uses_api_defaults(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_request_json(method: str, path: str, **kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["data"] = kwargs.get("data")
        captured["files"] = kwargs.get("files")
        return {"username": "ppino", "films_upserted_from_scrape": 1, "user_films_loaded": 2, "watchlist_loaded": 3}

    monkeypatch.setattr(api_client, "_request_json", fake_request_json)

    payload = api_client.run_pipeline_upload("letterboxd.zip", b"zip-bytes")

    assert payload["username"] == "ppino"
    assert captured["method"] == "POST"
    assert captured["path"] == "/pipeline/run"
    assert captured["data"] is None
    assert captured["files"] == {"file": ("letterboxd.zip", b"zip-bytes", "application/zip")}


def test_get_backend_status_reports_online_when_api_and_db_are_available(monkeypatch) -> None:
    clear = getattr(api_client.get_backend_status, "clear", None)
    if callable(clear):
        clear()

    monkeypatch.setattr(api_client, "_request_json", lambda method, path, **kwargs: {"status": "ok"})

    payload = api_client.get_backend_status()

    assert payload == {
        "state": "online",
        "label": "Backend ativo",
        "detail": "API e banco estão acessíveis.",
    }


def test_get_backend_status_reports_warming_when_database_health_returns_503(monkeypatch) -> None:
    clear = getattr(api_client.get_backend_status, "clear", None)
    if callable(clear):
        clear()

    def fake_request_json(method: str, path: str, **kwargs):
        if path == "/health":
            return {"status": "ok"}
        raise api_client.ApiClientError(
            "Database unavailable",
            status_code=503,
            detail="Banco temporariamente indisponivel.",
        )

    monkeypatch.setattr(api_client, "_request_json", fake_request_json)

    payload = api_client.get_backend_status()

    assert payload == {
        "state": "warming",
        "label": "Banco acordando",
        "detail": "Banco temporariamente indisponivel.",
    }


def test_get_backend_status_reports_unavailable_when_health_check_cannot_connect(monkeypatch) -> None:
    clear = getattr(api_client.get_backend_status, "clear", None)
    if callable(clear):
        clear()

    monkeypatch.setattr(
        api_client,
        "_request_json",
        lambda method, path, **kwargs: (_ for _ in ()).throw(
            api_client.ApiClientError(
                "Backend offline",
                detail="Não foi possível conectar a API em https://api.example.com.",
            )
        ),
    )

    payload = api_client.get_backend_status()

    assert payload == {
        "state": "unavailable",
        "label": "Backend indisponível",
        "detail": "Não foi possível conectar a API em https://api.example.com.",
    }


def test_get_logged_films_uses_new_route_when_available(monkeypatch) -> None:
    monkeypatch.setattr(
        api_client,
        "_analytics_get",
        lambda path, username, **params: [
            {
                "film_id": 1,
                "title": "Possession",
                "genres": ["Drama"],
                "countries": ["France"],
            }
        ],
    )

    rows = api_client.get_logged_films("ppino", watched_year=2025)

    assert rows == [
        {
            "film_id": 1,
            "title": "Possession",
            "genres": ["Drama"],
            "countries": ["France"],
        }
    ]


def test_get_logged_films_falls_back_to_filtered_films_on_404(monkeypatch) -> None:
    def fake_analytics_get(path: str, username: str, **params):
        raise api_client.ApiClientError("Not Found", status_code=404, detail="Not Found")

    monkeypatch.setattr(api_client, "_analytics_get", fake_analytics_get)
    monkeypatch.setattr(
        api_client,
        "get_filtered_films",
        lambda username, **filters: [
            {
                "film_id": 1,
                "title": "Possession",
                "letterboxd_url": "https://letterboxd.com/film/possession/",
            }
        ],
    )

    rows = api_client.get_logged_films("ppino", watched_year=2025)

    assert rows == [
        {
            "film_id": 1,
            "title": "Possession",
            "letterboxd_url": "https://letterboxd.com/film/possession/",
            "genres": [],
            "countries": [],
        }
    ]


def test_get_logged_films_can_raise_404_when_legacy_fallback_is_disabled(monkeypatch) -> None:
    def fake_analytics_get(path: str, username: str, **params):
        raise api_client.ApiClientError("Not Found", status_code=404, detail="Not Found")

    monkeypatch.setattr(api_client, "_analytics_get", fake_analytics_get)

    try:
        api_client.get_logged_films("ppino", allow_legacy_fallback=False)
    except api_client.ApiClientError as err:
        assert err.status_code == 404
    else:  # pragma: no cover
        raise AssertionError("Era esperado propagar ApiClientError com status 404.")


def test_get_rankings_bundle_uses_languages_category(monkeypatch) -> None:
    calls: list[tuple[str, str, int]] = []

    monkeypatch.setattr(api_client, "get_filter_options", lambda username: {})

    def fake_get_rankings(username: str, category: str, order_by: str, *, min_films: int):
        calls.append((category, order_by, min_films))
        return [{"nome": f"{category}:{order_by}", "filmes_assistidos": 1, "media_nota_pessoal": 4.0}]

    monkeypatch.setattr(api_client, "get_rankings", fake_get_rankings)

    payload = api_client.get_rankings_bundle("ppino", min_most_watched=1, min_best_rated=3)

    assert "languages" in payload["rankings_by_category"]
    assert "countries" not in payload["rankings_by_category"]
    assert ("languages", "most-watched", 1) in calls
    assert ("languages", "best-rated", 3) in calls


def test_request_json_retries_get_when_backend_returns_503(monkeypatch) -> None:
    responses = iter(
        [
            httpx.Response(
                503,
                json={"detail": "Banco temporariamente indisponivel."},
                headers={"Retry-After": "0"},
            ),
            httpx.Response(200, json={"username": "ppino"}),
        ]
    )
    captured_sleeps: list[float] = []

    class FakeClient:
        def request(self, **kwargs):
            return next(responses)

    monkeypatch.setattr(api_client, "get_api_base_url", lambda: "https://api.example.com")
    monkeypatch.setattr(api_client, "_get_http_client", lambda base_url: FakeClient())
    monkeypatch.setattr(api_client.time, "sleep", lambda seconds: captured_sleeps.append(seconds))

    payload = api_client._request_json("GET", "/users/ppino")

    assert payload == {"username": "ppino"}
    assert captured_sleeps == [0.0]


def test_request_json_retries_get_when_connection_fails(monkeypatch) -> None:
    request = httpx.Request("GET", "https://api.example.com/users/ppino")
    responses = iter(
        [
            httpx.ConnectError("backend cold start", request=request),
            httpx.Response(200, json={"username": "ppino"}),
        ]
    )
    captured_sleeps: list[float] = []

    class FakeClient:
        def request(self, **kwargs):
            result = next(responses)
            if isinstance(result, Exception):
                raise result
            return result

    monkeypatch.setattr(api_client, "get_api_base_url", lambda: "https://api.example.com")
    monkeypatch.setattr(api_client, "_get_http_client", lambda base_url: FakeClient())
    monkeypatch.setattr(api_client.time, "sleep", lambda seconds: captured_sleeps.append(seconds))

    payload = api_client._request_json("GET", "/users/ppino")

    assert payload == {"username": "ppino"}
    assert captured_sleeps == [1.0]


def test_request_json_reports_helpful_message_after_connection_failures(monkeypatch) -> None:
    request = httpx.Request("GET", "https://api.example.com/users/ppino")
    captured_sleeps: list[float] = []
    calls = {"count": 0}

    class FakeClient:
        def request(self, **kwargs):
            calls["count"] += 1
            raise httpx.ConnectError("backend cold start", request=request)

    monkeypatch.setattr(api_client, "get_api_base_url", lambda: "https://api.example.com")
    monkeypatch.setattr(api_client, "_get_http_client", lambda base_url: FakeClient())
    monkeypatch.setattr(api_client.time, "sleep", lambda seconds: captured_sleeps.append(seconds))

    try:
        api_client._request_json("GET", "/users/ppino")
    except api_client.ApiClientError as err:
        assert err.detail is None
        assert str(err) == (
            "Não foi possível conectar a API. "
            "O backend ainda está acordando; tente novamente em alguns segundos."
        )
    else:  # pragma: no cover
        raise AssertionError("Era esperado propagar ApiClientError apos esgotar os retries de conexao.")

    assert calls["count"] == 3
    assert captured_sleeps == [1.0, 2.0]


def test_request_json_does_not_retry_post_when_backend_returns_503(monkeypatch) -> None:
    calls = {"count": 0}

    class FakeClient:
        def request(self, **kwargs):
            calls["count"] += 1
            return httpx.Response(
                503,
                json={"detail": "Banco temporariamente indisponivel."},
                headers={"Retry-After": "0"},
            )

    monkeypatch.setattr(api_client, "get_api_base_url", lambda: "https://api.example.com")
    monkeypatch.setattr(api_client, "_get_http_client", lambda base_url: FakeClient())

    try:
        api_client._request_json("POST", "/pipeline/run")
    except api_client.ApiClientError as err:
        assert err.status_code == 503
        assert err.detail == "Banco temporariamente indisponivel."
    else:  # pragma: no cover
        raise AssertionError("Era esperado propagar ApiClientError no POST sem retry.")

    assert calls["count"] == 1


def test_request_json_hides_html_from_gateway_502(monkeypatch) -> None:
    class FakeClient:
        def request(self, **kwargs):
            return httpx.Response(
                502,
                text="<!DOCTYPE html><html><head><title>502</title></head><body>bad gateway</body></html>",
                headers={"content-type": "text/html"},
            )

    monkeypatch.setattr(api_client, "get_api_base_url", lambda: "https://api.example.com")
    monkeypatch.setattr(api_client, "_get_http_client", lambda base_url: FakeClient())

    try:
        api_client._request_json("POST", "/pipeline/run")
    except api_client.ApiClientError as err:
        assert err.status_code == 502
        assert err.detail == "O backend retornou 502 e está temporariamente indisponível. Tente novamente em instantes."
    else:  # pragma: no cover
        raise AssertionError("Era esperado propagar ApiClientError para 502 HTML.")
