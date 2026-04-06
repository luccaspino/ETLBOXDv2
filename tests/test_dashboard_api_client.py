from __future__ import annotations

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
