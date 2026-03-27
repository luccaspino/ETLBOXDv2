from __future__ import annotations

from src.ingestion.scraper import _parse_film_page


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
