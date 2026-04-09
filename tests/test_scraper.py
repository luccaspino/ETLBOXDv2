from __future__ import annotations

import logging

import httpx

from src.ingestion.scraper import FilmScrapeResult, LetterboxdScraper
from src.ingestion.scraper_parser import _parse_film_page, _stars_to_rating


def test_stars_to_rating_supports_legacy_mojibake_tokens() -> None:
    assert _stars_to_rating("Film Ã¢Ëœâ€¦Ã¢Ëœâ€¦Ã‚Â½") == 2.5


def test_parse_film_page_extracts_core_fields() -> None:
    html = """
    <html>
      <head>
        <meta property="og:url" content="https://letterboxd.com/film/test-film/" />
        <meta property="og:image" content="https://img/poster.jpg" />
        <meta property="og:description" content="Overview text" />
      </head>
      <body>
        <h1 class="headline-1"><span class="name">Test Film</span></h1>
        <div class="releasedate"><a>2024</a></div>
        <section class="production-synopsis">
          <h4 class="tagline">Tagline here</h4>
        </section>
        <p class="text-footer">113 mins</p>

        <section id="tab-genres">
          <a href="/films/genre/horror/">Horror</a>
          <a href="/films/genre/thriller/">Thriller</a>
        </section>

        <section id="tab-details">
          <a href="/films/country/usa/">USA</a>
          <a href="/films/language/english/">English</a>
        </section>

        <section id="tab-cast">
          <ul class="cast-list">
            <li><a>Actor 1</a></li>
            <li><a>Actor 2</a></li>
          </ul>
        </section>

        <section id="tab-crew">
          <a href="/director/jane-doe/">Jane Doe</a>
        </section>

        <script type="application/ld+json">
        {
          "@type": "Movie",
          "name": "Test Film",
          "aggregateRating": {"ratingValue": "3.84"}
        }
        </script>
      </body>
    </html>
    """

    item = _parse_film_page("https://letterboxd.com/film/test-film/", html)

    assert item.title == "Test Film"
    assert item.year == 2024
    assert item.runtime_min == 113
    assert item.original_language == "English"
    assert item.tagline == "Tagline here"
    assert item.letterboxd_avg_rating == 3.84
    assert "Jane Doe" in item.directors
    assert "Actor 1" in item.cast
    assert "Horror" in item.genres
    assert "US" in item.countries


def test_parse_film_page_ignores_show_all_placeholders() -> None:
    html = """
    <html>
      <head>
        <meta property="og:url" content="https://letterboxd.com/film/test-film/" />
      </head>
      <body>
        <h1 class="headline-1"><span class="name">Test Film</span></h1>
        <div class="releasedate"><a>2024</a></div>

        <section id="tab-genres">
          <a href="/films/genre/horror/">Horror</a>
          <a href="/films/genre/all/">Show All...</a>
        </section>

        <section id="tab-cast">
          <ul class="cast-list">
            <li><a>Actor 1</a></li>
            <li><a>Show All...</a></li>
          </ul>
        </section>

        <section id="tab-crew">
          <a href="/director/jane-doe/">Jane Doe</a>
        </section>
      </body>
    </html>
    """

    item = _parse_film_page("https://letterboxd.com/film/test-film/", html)

    assert item.cast == ["Actor 1"]
    assert item.genres == ["Horror"]


def test_scrape_one_retries_connection_reset_error(monkeypatch) -> None:
    scraper = LetterboxdScraper(retries=1)
    fetch_calls: list[str] = []

    def fake_fetch_html(url: str) -> tuple[str, str, int, str]:
        fetch_calls.append(url)
        if len(fetch_calls) == 1:
            raise ConnectionResetError(104, "Connection reset by peer")
        return ("<html></html>", "https://letterboxd.com/film/test-film/", 200, "HTTP/1.1")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(letterboxd_url=final_url, title="Test Film")

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert result.ok
    assert result.title == "Test Film"
    assert result.attempts == 2
    assert result.http_status == 200
    assert result.http_version == "HTTP/1.1"
    assert fetch_calls == [
        "https://boxd.it/test",
        "https://boxd.it/test",
        "https://letterboxd.com/film/test-film/",
    ]


def test_scrape_one_reuses_review_page_data_when_canonical_fetch_fails(monkeypatch) -> None:
    scraper = LetterboxdScraper(retries=0)
    fetch_calls = {"count": 0}

    def fake_fetch_html(url: str) -> tuple[str, str, int, str]:
        fetch_calls["count"] += 1
        if fetch_calls["count"] == 1:
            return ("<html></html>", "https://letterboxd.com/user/film/test-film/", 200, "HTTP/1.1")
        raise ConnectionResetError(104, "Connection reset by peer")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(
            letterboxd_url="https://letterboxd.com/user/film/test-film/",
            title="Test Film",
        )

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert result.ok
    assert result.title == "Test Film"
    assert result.letterboxd_url == "https://letterboxd.com/film/test-film/"
    assert result.requested_url == "https://boxd.it/test"
    assert result.attempts == 1
    assert result.used_fallback is True
    assert result.latency_ms is not None
    assert fetch_calls["count"] == 2


def test_scrape_one_keeps_failing_when_review_title_cannot_be_cleaned(monkeypatch) -> None:
    scraper = LetterboxdScraper(retries=0)
    fetch_calls = {"count": 0}

    def fake_fetch_html(url: str) -> tuple[str, str, int, str]:
        fetch_calls["count"] += 1
        if fetch_calls["count"] == 1:
            return ("<html></html>", "https://letterboxd.com/user/film/test-film/", 200, "HTTP/1.1")
        raise ConnectionResetError(104, "Connection reset by peer")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(
            letterboxd_url="https://letterboxd.com/user/film/test-film/",
            title="Review of Test Film",
        )

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert not result.ok
    assert result.scrape_error == "canonical fetch failed for review/log URL"
    assert result.letterboxd_url == "https://letterboxd.com/film/test-film/"
    assert result.requested_url == "https://boxd.it/test"
    assert result.attempts == 1
    assert result.used_fallback is True
    assert result.latency_ms is not None


def test_scrape_one_recovers_from_403_review_redirect(monkeypatch) -> None:
    scraper = LetterboxdScraper(retries=0)

    def fake_fetch_html(url: str) -> tuple[str, str, int, str]:
        if url == "https://boxd.it/test":
            response = httpx.Response(
                403,
                request=httpx.Request("GET", "https://letterboxd.com/user/film/test-film/1/"),
            )
            raise httpx.HTTPStatusError("403", request=response.request, response=response)
        if url == "https://letterboxd.com/film/test-film/":
            return ("<html></html>", "https://letterboxd.com/film/test-film/", 200, "HTTP/2")
        raise AssertionError(f"URL inesperada: {url}")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(letterboxd_url=final_url.rstrip("/"), title="Test Film")

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert result.ok
    assert result.title == "Test Film"
    assert result.letterboxd_url == "https://letterboxd.com/film/test-film"
    assert result.requested_url == "https://boxd.it/test"
    assert result.attempts == 1
    assert result.used_fallback is True
    assert result.http_status == 200
    assert result.http_version == "HTTP/2"


def test_scrape_many_logs_startup_progress_and_metrics(monkeypatch, caplog) -> None:
    scraper = LetterboxdScraper(max_workers=4, timeout_s=8, retries=1, progress_every=10)
    monkeypatch.setattr(
        scraper,
        "scrape_one",
        lambda uri: FilmScrapeResult(
            letterboxd_url=str(uri),
            requested_url=str(uri),
            title="Test Film",
            latency_ms=150.0,
            http_status=200,
            http_version="HTTP/1.1",
        ),
    )

    with caplog.at_level(logging.INFO):
        results = scraper.scrape_many(["https://boxd.it/a", "https://boxd.it/b"])

    assert len(results) == 2
    assert "scraping: iniciado | total=2 | workers=4 | timeout=8s | retries=1" in caplog.text
    assert "scraping metrics" in caplog.text
    assert "p50=" in caplog.text
    assert "p95=" in caplog.text


def test_scraper_uses_shared_httpx_client(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, url: str) -> None:
            self.text = "<html></html>"
            self.url = url
            self.status_code = 200
            self.http_version = "HTTP/2"

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            captured["limits"] = kwargs.get("limits")
            captured["follow_redirects"] = kwargs.get("follow_redirects")
            captured["headers"] = kwargs.get("headers")
            captured["http2"] = kwargs.get("http2")

        def get(self, url: str) -> FakeResponse:
            return FakeResponse("https://letterboxd.com/film/test-film/")

        def close(self) -> None:
            return None

    monkeypatch.setattr("src.ingestion.scraper._http2_dependencies_available", lambda: True)
    monkeypatch.setattr("src.ingestion.scraper.httpx.Client", FakeClient)

    scraper = LetterboxdScraper(max_workers=2)
    html, final_url, status, http_version = scraper._fetch_html("https://boxd.it/test")
    scraper.close()

    assert html == "<html></html>"
    assert final_url == "https://letterboxd.com/film/test-film/"
    assert status == 200
    assert http_version == "HTTP/2"
    assert captured["follow_redirects"] is True
    assert captured["http2"] is True
    assert isinstance(captured["limits"], httpx.Limits)


def test_fetch_html_downgrades_to_http11_after_http2_transport_issue(monkeypatch) -> None:
    created_http2_flags: list[bool] = []

    class FakeResponse:
        def __init__(self, url: str, http_version: str) -> None:
            self.text = "<html></html>"
            self.url = url
            self.status_code = 200
            self.http_version = http_version

        def raise_for_status(self) -> None:
            return None

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            self.http2 = kwargs["http2"]
            created_http2_flags.append(self.http2)

        def get(self, url: str):
            if self.http2:
                raise KeyError(11)
            return FakeResponse("https://letterboxd.com/film/test-film/", "HTTP/1.1")

        def close(self) -> None:
            return None

    monkeypatch.setattr("src.ingestion.scraper._http2_dependencies_available", lambda: True)
    monkeypatch.setattr("src.ingestion.scraper.httpx.Client", FakeClient)

    scraper = LetterboxdScraper(max_workers=2)
    html, final_url, status, http_version = scraper._fetch_html("https://boxd.it/test")
    scraper.close()

    assert html == "<html></html>"
    assert final_url == "https://letterboxd.com/film/test-film/"
    assert status == 200
    assert http_version == "HTTP/1.1"
    assert scraper.http2_enabled is False
    assert created_http2_flags == [True, False]


def test_scrape_one_populates_latency_and_status(monkeypatch) -> None:
    scraper = LetterboxdScraper(retries=0)

    def fake_fetch_html(url: str) -> tuple[str, str, int, str]:
        return ("<html></html>", "https://letterboxd.com/film/test-film/", 200, "HTTP/1.1")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(letterboxd_url=final_url, title="Test Film")

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert result.ok
    assert result.latency_ms is not None
    assert result.latency_ms >= 0
    assert result.http_status == 200
    assert result.http_version == "HTTP/1.1"


def test_scrape_many_stores_metrics_object(monkeypatch) -> None:
    scraper = LetterboxdScraper(max_workers=2, progress_every=10)
    monkeypatch.setattr(
        scraper,
        "scrape_one",
        lambda uri: FilmScrapeResult(
            letterboxd_url=str(uri),
            requested_url=str(uri),
            title="Test Film",
            latency_ms=100.0 if str(uri).endswith("a") else 300.0,
            http_status=200,
            http_version="HTTP/2" if str(uri).endswith("a") else "HTTP/1.1",
            used_fallback=str(uri).endswith("b"),
        ),
    )

    scraper.scrape_many(["https://boxd.it/a", "https://boxd.it/b"])

    assert scraper.last_metrics is not None
    assert scraper.last_metrics.total == 2
    assert scraper.last_metrics.ok == 2
    assert scraper.last_metrics.fallbacks_used == 1
    assert scraper.last_metrics.http2_responses == 1
    assert scraper.last_metrics.http11_responses == 1
