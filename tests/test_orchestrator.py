from __future__ import annotations

import pandas as pd

from src.ingestion.scraper import FilmScrapeResult
from src.pipeline import orchestrator


def test_run_stops_retry_loop_when_no_url_is_recovered(monkeypatch) -> None:
    monkeypatch.setattr(orchestrator, "fetch_existing_film_urls", lambda: set())
    monkeypatch.setattr(orchestrator, "fetch_existing_film_keys", lambda: set())
    monkeypatch.setattr(
        orchestrator,
        "parse_zip",
        lambda zip_path, existing_uris, existing_film_keys: {
            "user": pd.DataFrame([{"username": "ppino"}]),
            "user_films": pd.DataFrame(),
            "watchlist": pd.DataFrame(),
            "scrape_queue": pd.DataFrame(
                [{"letterboxd_uri": "https://boxd.it/a"}, {"letterboxd_uri": "https://boxd.it/b"}]
            ),
        },
    )
    monkeypatch.setattr(
        orchestrator,
        "load_all_to_db",
        lambda parsed, scrape_results: {
            "username": "ppino",
            "films_upserted_from_scrape": 0,
            "user_films_loaded": 0,
            "watchlist_loaded": 0,
        },
    )

    scraper_instances: list[object] = []

    class FakeScraper:
        def __init__(self, *args, **kwargs) -> None:
            scraper_instances.append(self)

        def scrape_many(self, uris):
            return [
                FilmScrapeResult(
                    letterboxd_url=str(uri),
                    requested_url=str(uri),
                    scrape_error="connection reset",
                    attempts=1,
                )
                for uri in uris
            ]

    monkeypatch.setattr(orchestrator, "LetterboxdScraper", FakeScraper)

    result = orchestrator.run(
        zip_path="dummy.zip",
        auto_retry_failed=True,
        retry_failed_passes=6,
        require_complete_scrape=False,
        errors_out=None,
    )

    assert result["username"] == "ppino"
    assert len(scraper_instances) == 2
