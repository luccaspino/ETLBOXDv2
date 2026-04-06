from __future__ import annotations

from src.dashboard import api_client


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
