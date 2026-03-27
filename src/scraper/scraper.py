from __future__ import annotations

import csv
import json
import logging
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

LETTERBOXD_HOSTS = {"letterboxd.com", "www.letterboxd.com", "boxd.it", "www.boxd.it"}
RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}

COUNTRY_SLUG_TO_CODE = {
    "usa": "US",
    "united-states": "US",
    "uk": "GB",
    "united-kingdom": "GB",
    "england": "GB",
    "brazil": "BR",
    "argentina": "AR",
    "australia": "AU",
    "austria": "AT",
    "belgium": "BE",
    "canada": "CA",
    "chile": "CL",
    "china": "CN",
    "colombia": "CO",
    "czech-republic": "CZ",
    "denmark": "DK",
    "finland": "FI",
    "france": "FR",
    "germany": "DE",
    "greece": "GR",
    "hong-kong": "HK",
    "hungary": "HU",
    "iceland": "IS",
    "india": "IN",
    "indonesia": "ID",
    "iran": "IR",
    "ireland": "IE",
    "israel": "IL",
    "italy": "IT",
    "japan": "JP",
    "luxembourg": "LU",
    "mexico": "MX",
    "netherlands": "NL",
    "new-zealand": "NZ",
    "norway": "NO",
    "poland": "PL",
    "portugal": "PT",
    "russia": "RU",
    "south-africa": "ZA",
    "south-korea": "KR",
    "korea-south": "KR",
    "spain": "ES",
    "sweden": "SE",
    "switzerland": "CH",
    "taiwan": "TW",
    "thailand": "TH",
    "turkey": "TR",
    "ukraine": "UA",
}


@dataclass
class FilmScrapeResult:
    letterboxd_url: str
    requested_url: str | None = None
    title: str | None = None
    year: int | None = None
    runtime_min: int | None = None
    original_language: str | None = None
    overview: str | None = None
    tagline: str | None = None
    poster_url: str | None = None
    letterboxd_avg_rating: float | None = None
    genres: list[str] = field(default_factory=list)
    directors: list[str] = field(default_factory=list)
    cast: list[str] = field(default_factory=list)
    countries: list[str] = field(default_factory=list)
    scraped_at: datetime | None = None
    scrape_error: str | None = None
    attempts: int = 0

    @property
    def ok(self) -> bool:
        return self.scrape_error is None


class _GlobalRateLimiter:
    def __init__(self, min_interval_s: float) -> None:
        self.min_interval_s = max(0.0, float(min_interval_s))
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self.min_interval_s <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_s = self._next_allowed - now
            if wait_s > 0:
                time.sleep(wait_s)
                now = time.monotonic()
            self._next_allowed = now + self.min_interval_s


def _normalize_film_url(uri: str) -> str:
    uri = uri.strip()
    if uri.startswith(("http://", "https://")):
        normalized = uri.rstrip("/")
    elif uri.startswith("/"):
        normalized = f"https://letterboxd.com{uri}".rstrip("/")
    else:
        normalized = f"https://letterboxd.com/{uri}".rstrip("/")

    # Remove sufixo /<n> em URLs de filme: /film/<slug>/<n> -> /film/<slug>
    parsed = urlparse(normalized)
    path = parsed.path or ""
    m = re.match(r"^/film/([^/]+)/\d+$", path.rstrip("/"))
    if m:
        return f"https://letterboxd.com/film/{m.group(1)}"
    return normalized


def _is_letterboxd_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    return host in LETTERBOXD_HOSTS


def _to_global_film_url(url: str) -> str:
    if not _is_letterboxd_url(url):
        return url.rstrip("/")
    parsed = urlparse(url)
    path = parsed.path or ""
    idx = path.find("/film/")
    if idx == -1:
        return url.rstrip("/")
    film_path = path[idx:].rstrip("/")
    # Remove sufixo de pagina de review/log: /film/<slug>/<n>
    # Mantem apenas /film/<slug>
    m = re.match(r"^(/film/[^/]+)(?:/\d+)?$", film_path)
    if m:
        film_path = m.group(1)
    return f"https://letterboxd.com{film_path}".rstrip("/")


def _extract_json_ld(html: str) -> list[dict]:
    pattern = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )
    blocks = pattern.findall(html)
    parsed: list[dict] = []
    for raw in blocks:
        payload = unescape(raw).strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            parsed.extend([item for item in data if isinstance(item, dict)])
        elif isinstance(data, dict):
            parsed.append(data)
    return parsed


def _movie_from_json_ld(items: list[dict]) -> dict | None:
    for item in items:
        typ = item.get("@type")
        if isinstance(typ, list):
            is_movie = "Movie" in typ
        else:
            is_movie = typ == "Movie"
        if is_movie:
            return item
    return None


def _movie_from_review_json_ld(items: list[dict]) -> dict | None:
    for item in items:
        typ = item.get("@type")
        if isinstance(typ, list):
            is_review = "Review" in typ
        else:
            is_review = typ == "Review"
        if not is_review:
            continue
        reviewed = item.get("itemReviewed")
        if isinstance(reviewed, dict):
            reviewed_type = reviewed.get("@type")
            if isinstance(reviewed_type, list):
                is_movie = "Movie" in reviewed_type
            else:
                is_movie = reviewed_type == "Movie"
            if is_movie:
                return reviewed
    return None


def _extract_meta_content(html: str, key: str) -> str | None:
    pattern = re.compile(
        rf'<meta[^>]+(?:property|name)=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(html)
    return unescape(m.group(1)).strip() if m else None


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    m = re.search(r"\d+", str(value))
    return int(m.group(0)) if m else None


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = str(value).replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", cleaned)
    return float(m.group(0)) if m else None


def _stars_to_rating(text: str | None) -> float | None:
    if not text:
        return None
    stars = text.count("★") + text.count("â˜…")
    half = 0.5 if ("½" in text or "Â½" in text) else 0.0
    if stars == 0 and half == 0:
        return None
    return stars + half


def _strip_year_from_title(title: str | None) -> tuple[str | None, int | None]:
    if not title:
        return None, None
    cleaned = title.strip()
    m = re.search(r"\((\d{4})\)\s*$", cleaned)
    if not m:
        return cleaned, None
    year = int(m.group(1))
    cleaned = re.sub(r"\s*\(\d{4}\)\s*$", "", cleaned).strip()
    return cleaned, year


def _looks_like_review_title(title: str | None) -> bool:
    if not title:
        return False
    low = title.lower()
    return (" review of " in low) or (" diary entry for " in low)


def _title_from_review_title(title: str | None) -> str | None:
    if not title:
        return None
    m = re.search(r"(?:review of|diary entry for)\s+(.+)$", title, flags=re.IGNORECASE)
    if not m:
        return None
    cleaned, _ = _strip_year_from_title(m.group(1).strip())
    return cleaned


def _is_review_like_page(url: str) -> bool:
    path = urlparse(url).path or ""
    idx = path.find("/film/")
    if idx == -1:
        return False
    prefix = path[:idx].strip("/")
    return bool(prefix)


def _extract_people(entries: object) -> list[str]:
    if not entries:
        return []
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        return []
    names = []
    for entry in entries:
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    return list(dict.fromkeys(names))


def _extract_movie_aggregate_rating(movie: dict) -> float | None:
    agg = movie.get("aggregateRating")
    if isinstance(agg, dict):
        return _to_float(agg.get("ratingValue"))
    return None


def _extract_genres(raw: object) -> list[str]:
    if isinstance(raw, str):
        raw_list = [raw]
    elif isinstance(raw, list):
        raw_list = [g for g in raw if isinstance(g, str)]
    else:
        return []
    return list(dict.fromkeys([g.strip() for g in raw_list if g.strip()]))


def _extract_year_from_releasedate(soup: BeautifulSoup) -> int | None:
    rel = soup.select_one(".releasedate a")
    return _to_int(rel.get_text(" ", strip=True)) if rel else None


def _extract_runtime_from_footer(soup: BeautifulSoup) -> int | None:
    for node in soup.select("p.text-footer"):
        txt = node.get_text(" ", strip=True)
        val = _to_int(txt)
        if val and "min" in txt.lower():
            return val
    return None


def _extract_tagline_from_synopsis(soup: BeautifulSoup) -> str | None:
    node = soup.select_one("section.production-synopsis h4.tagline")
    return node.get_text(" ", strip=True) if node else None


def _extract_genres_from_html(soup: BeautifulSoup) -> list[str]:
    out = []
    for a in soup.select("#tab-genres a[href*='/films/genre/']"):
        txt = a.get_text(" ", strip=True)
        if txt and txt.lower() != "show all…":
            out.append(txt)
    return list(dict.fromkeys(out))


def _extract_country_code_from_href(country_href: str) -> str | None:
    m = re.search(r"/films/country/([^/]+)/?", country_href or "")
    if not m:
        return None
    slug = unquote(m.group(1)).strip().lower()
    if not slug:
        return None
    if len(slug) == 2 and slug.isalpha():
        return slug.upper()
    return COUNTRY_SLUG_TO_CODE.get(slug)


def _extract_details_tab(soup: BeautifulSoup) -> tuple[list[str], str | None]:
    country_codes = []
    language = None
    for a in soup.select("#tab-details a[href]"):
        href = a.get("href", "")
        txt = a.get_text(" ", strip=True)
        if "/films/country/" in href:
            code = _extract_country_code_from_href(href)
            if code:
                country_codes.append(code)
        if "/films/language/" in href and txt and not language:
            language = txt
    return list(dict.fromkeys(country_codes)), language


def _extract_cast_from_html(soup: BeautifulSoup) -> list[str]:
    names = []
    for a in soup.select("#tab-cast .cast-list a"):
        txt = a.get_text(" ", strip=True)
        if txt:
            names.append(txt)
    return list(dict.fromkeys(names))


def _extract_directors_from_html(soup: BeautifulSoup) -> list[str]:
    names = []
    for a in soup.select("#tab-crew a[href*='/director/']"):
        txt = a.get_text(" ", strip=True)
        if txt:
            names.append(txt)
    # fallback para markup alternativo da aba de crew
    if not names:
        for a in soup.select("#tab-crew .text-sluglist a"):
            txt = a.get_text(" ", strip=True)
            if txt:
                names.append(txt)
    return list(dict.fromkeys(names))


def _canonical_letterboxd_url(final_url: str, html: str, movie: dict | None) -> str:
    if movie and isinstance(movie.get("url"), str) and _is_letterboxd_url(movie["url"]):
        return _to_global_film_url(movie["url"])
    og_url = _extract_meta_content(html, "og:url")
    if og_url and _is_letterboxd_url(og_url):
        return _to_global_film_url(og_url)
    return _to_global_film_url(final_url)


def _parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
    soup = BeautifulSoup(html, "html.parser")
    items = _extract_json_ld(html)
    movie = _movie_from_json_ld(items) or _movie_from_review_json_ld(items) or {}

    title = None
    title_node = soup.select_one("h1.headline-1 .name")
    if title_node:
        title = title_node.get_text(" ", strip=True)
    if not title and isinstance(movie.get("name"), str):
        title = movie.get("name")
    if not title:
        title = _extract_meta_content(html, "og:title")
        if title and " • Letterboxd" in title:
            title = title.replace(" • Letterboxd", "").strip()
        if title and " â€¢ Letterboxd" in title:
            title = title.replace(" â€¢ Letterboxd", "").strip()
    if _looks_like_review_title(title):
        title = _title_from_review_title(title) or title
        if isinstance(movie.get("name"), str):
            movie_name = movie.get("name")
            if movie_name and not _looks_like_review_title(movie_name):
                title = movie_name

    year = _extract_year_from_releasedate(soup) or _to_int(movie.get("datePublished"))
    title, year_from_title = _strip_year_from_title(title)
    if year is None:
        year = year_from_title

    review_like = _is_review_like_page(final_url)

    overview = (movie.get("description") if isinstance(movie.get("description"), str) else None)
    if not overview and not review_like:
        overview = _extract_meta_content(html, "og:description") or _extract_meta_content(html, "description")
    poster = (
        (movie.get("image") if isinstance(movie.get("image"), str) else None)
        or _extract_meta_content(html, "og:image")
    )

    genres = _extract_genres_from_html(soup) or _extract_genres(movie.get("genre"))
    directors = _extract_directors_from_html(soup) or _extract_people(movie.get("director"))
    if not directors and not _is_review_like_page(final_url):
        director_meta = _extract_meta_content(html, "twitter:data1")
        if director_meta:
            directors = [director_meta]
    cast = _extract_cast_from_html(soup) or _extract_people(movie.get("actor"))

    countries, language = _extract_details_tab(soup)
    if not language:
        language = _extract_meta_content(html, "inLanguage")

    rating = _extract_movie_aggregate_rating(movie)
    if rating is None:
        if not review_like:
            rating = _to_float(_extract_meta_content(html, "twitter:data2"))
        rating = rating or _to_float(_extract_meta_content(html, "ratingValue"))
        if not review_like:
            rating = rating or _stars_to_rating(_extract_meta_content(html, "og:title"))
    runtime = (
        _extract_runtime_from_footer(soup)
        or _to_int(movie.get("duration"))
        or _to_int(_extract_meta_content(html, "duration"))
    )
    tagline = _extract_tagline_from_synopsis(soup)

    return FilmScrapeResult(
        letterboxd_url=_canonical_letterboxd_url(final_url, html, movie),
        title=title,
        year=year,
        runtime_min=runtime,
        original_language=language,
        overview=overview,
        tagline=tagline,
        poster_url=poster,
        letterboxd_avg_rating=rating,
        genres=genres,
        directors=directors,
        cast=cast,
        countries=countries,
        scraped_at=datetime.now(timezone.utc),
    )


class LetterboxdScraper:
    def __init__(
        self,
        max_workers: int = 20,
        timeout_s: int = 10,
        retries: int = 1,
        retry_backoff_s: float = 0.25,
        request_interval_s: float = 0.0,
        progress_every: int = 50,
    ) -> None:
        self.max_workers = max(1, int(max_workers))
        self.timeout_s = max(1, int(timeout_s))
        self.retries = max(0, int(retries))
        self.retry_backoff_s = max(0.0, float(retry_backoff_s))
        self.progress_every = max(1, int(progress_every))
        self._rate_limiter = _GlobalRateLimiter(request_interval_s)
        self._user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )

    def _fetch_html(self, url: str) -> tuple[str, str]:
        self._rate_limiter.acquire()
        req = Request(
            url=url,
            headers={
                "User-Agent": self._user_agent,
                "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8",
            },
        )
        with urlopen(req, timeout=self.timeout_s) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html = response.read().decode(charset, errors="replace")
            return html, response.geturl()

    def _retry_sleep(self, attempt: int) -> None:
        if self.retry_backoff_s <= 0:
            return
        backoff = self.retry_backoff_s * (2 ** (attempt - 1))
        jitter = random.uniform(0.0, self.retry_backoff_s)
        time.sleep(backoff + jitter)

    def scrape_one(self, uri: str) -> FilmScrapeResult:
        url = _normalize_film_url(uri)
        if not _is_letterboxd_url(url):
            return FilmScrapeResult(
                letterboxd_url=url,
                requested_url=url,
                scrape_error=f"URL invalida para Letterboxd: {url}",
            )

        attempts = self.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                html, final_url = self._fetch_html(url)
                result = _parse_film_page(final_url, html)
                result.requested_url = url
                result.attempts = attempt

                global_film_url = _to_global_film_url(final_url)
                must_refetch_canonical = _is_review_like_page(final_url) or _looks_like_review_title(result.title)
                if global_film_url != final_url.rstrip("/") or must_refetch_canonical:
                    try:
                        html2, final2 = self._fetch_html(global_film_url)
                        canonical = _parse_film_page(final2, html2)
                        canonical.requested_url = url
                        canonical.attempts = attempt
                        result = canonical
                    except Exception as err:
                        logger.debug("fallback canonical fetch falhou para %s: %s", url, err)
                        if must_refetch_canonical:
                            return FilmScrapeResult(
                                letterboxd_url=global_film_url,
                                requested_url=url,
                                scrape_error="canonical fetch failed for review/log URL",
                                attempts=attempt,
                            )
                return result
            except HTTPError as err:
                if err.code not in RETRYABLE_HTTP_STATUS or attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=f"HTTP {err.code}",
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except (URLError, TimeoutError, ValueError) as err:
                if attempt >= attempts:
                    return FilmScrapeResult(
                        letterboxd_url=url,
                        requested_url=url,
                        scrape_error=str(err),
                        attempts=attempt,
                    )
                self._retry_sleep(attempt)
            except Exception as err:
                logger.exception("erro inesperado no scraping de %s", url)
                return FilmScrapeResult(
                    letterboxd_url=url,
                    requested_url=url,
                    scrape_error=f"unexpected: {err}",
                    attempts=attempt,
                )

        return FilmScrapeResult(
            letterboxd_url=url,
            requested_url=url,
            scrape_error="falha nao mapeada",
            attempts=attempts,
        )

    def scrape_many(self, uris: Iterable[str]) -> list[FilmScrapeResult]:
        uri_list = [str(uri).strip() for uri in uris if str(uri).strip()]
        total = len(uri_list)
        if total == 0:
            return []

        started = time.perf_counter()
        done = ok = err = 0
        out: list[FilmScrapeResult | None] = [None] * total

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            future_to_idx = {ex.submit(self.scrape_one, uri): i for i, uri in enumerate(uri_list)}
            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    result = fut.result()
                except Exception as ex_err:
                    result = FilmScrapeResult(
                        letterboxd_url=uri_list[idx],
                        requested_url=uri_list[idx],
                        scrape_error=f"future error: {ex_err}",
                    )
                out[idx] = result
                done += 1
                if result.ok:
                    ok += 1
                else:
                    err += 1

                if done % self.progress_every == 0 or done == total:
                    elapsed = max(1e-6, time.perf_counter() - started)
                    rate = done / elapsed
                    eta = (total - done) / rate if rate > 0 else 0.0
                    logger.info(
                        "scraping: %s/%s | ok=%s erro=%s | %.2f url/s | ETA %.1fs",
                        done,
                        total,
                        ok,
                        err,
                        rate,
                        eta,
                    )

        return [x for x in out if x is not None]


def write_scrape_failures(results: list[FilmScrapeResult], output_csv: str) -> int:
    failures = [r for r in results if not r.ok]
    if not failures:
        return 0
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["letterboxd_url", "requested_url", "scrape_error", "attempts", "title", "year"],
        )
        writer.writeheader()
        for row in failures:
            writer.writerow(
                {
                    "letterboxd_url": row.letterboxd_url,
                    "requested_url": row.requested_url,
                    "scrape_error": row.scrape_error,
                    "attempts": row.attempts,
                    "title": row.title,
                    "year": row.year,
                }
            )
    return len(failures)


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python -m src.scraper.scraper <url1> [url2] [...]")
        raise SystemExit(1)

    scraper = LetterboxdScraper()
    rows = scraper.scrape_many(sys.argv[1:])
    for item in rows:
        print(
            {
                "requested_url": item.requested_url,
                "url": item.letterboxd_url,
                "title": item.title,
                "year": item.year,
                "directors": item.directors,
                "cast": item.cast,
                "genres": item.genres,
                "countries": item.countries,
                "rating": item.letterboxd_avg_rating,
                "error": item.scrape_error,
            }
        )
