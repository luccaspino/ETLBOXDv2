from __future__ import annotations

from src.ingestion.scraper import FilmScrapeResult, LetterboxdScraper
from src.ingestion.scraper_parser import _parse_film_page, _stars_to_rating


def test_stars_to_rating_supports_legacy_mojibake_tokens() -> None:
    assert _stars_to_rating("Film â˜…â˜…Â½") == 2.5


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
    fetch_calls = {"count": 0}

    def fake_fetch_html(url: str) -> tuple[str, str]:
        fetch_calls["count"] += 1
        if fetch_calls["count"] == 1:
            raise ConnectionResetError(104, "Connection reset by peer")
        return ("<html></html>", "https://letterboxd.com/film/test-film/")

    def fake_parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
        return FilmScrapeResult(letterboxd_url=final_url, title="Test Film")

    monkeypatch.setattr(scraper, "_fetch_html", fake_fetch_html)
    monkeypatch.setattr("src.ingestion.scraper._parse_film_page", fake_parse_film_page)

    result = scraper.scrape_one("https://boxd.it/test")

    assert result.ok
    assert result.title == "Test Film"
    assert result.attempts == 2
    assert fetch_calls["count"] == 2
