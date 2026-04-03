from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import unescape
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

from src.ingestion.scraper_urls import COUNTRY_SLUG_TO_CODE, _is_letterboxd_url, _to_global_film_url

if TYPE_CHECKING:
    from src.ingestion.scraper import FilmScrapeResult


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
        is_movie = "Movie" in typ if isinstance(typ, list) else typ == "Movie"
        if is_movie:
            return item
    return None


def _movie_from_review_json_ld(items: list[dict]) -> dict | None:
    for item in items:
        typ = item.get("@type")
        is_review = "Review" in typ if isinstance(typ, list) else typ == "Review"
        if not is_review:
            continue
        reviewed = item.get("itemReviewed")
        if isinstance(reviewed, dict):
            reviewed_type = reviewed.get("@type")
            is_movie = "Movie" in reviewed_type if isinstance(reviewed_type, list) else reviewed_type == "Movie"
            if is_movie:
                return reviewed
    return None


def _extract_meta_content(html: str, key: str) -> str | None:
    pattern = re.compile(
        rf'<meta[^>]+(?:property|name)=["\']{re.escape(key)}["\'][^>]+content=["\'](.*?)["\']',
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(html)
    return unescape(match.group(1)).strip() if match else None


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    return int(match.group(0)) if match else None


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = str(value).replace(",", ".")
    match = re.search(r"\d+(?:\.\d+)?", cleaned)
    return float(match.group(0)) if match else None


def _stars_to_rating(text: str | None) -> float | None:
    if not text:
        return None
    stars = text.count("â˜…") + text.count("Ã¢Ëœâ€¦")
    half = 0.5 if ("Â½" in text or "Ã‚Â½" in text) else 0.0
    if stars == 0 and half == 0:
        return None
    return stars + half


def _strip_year_from_title(title: str | None) -> tuple[str | None, int | None]:
    if not title:
        return None, None
    cleaned = title.strip()
    match = re.search(r"\((\d{4})\)\s*$", cleaned)
    if not match:
        return cleaned, None
    year = int(match.group(1))
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
    match = re.search(r"(?:review of|diary entry for)\s+(.+)$", title, flags=re.IGNORECASE)
    if not match:
        return None
    cleaned, _ = _strip_year_from_title(match.group(1).strip())
    return cleaned


def _is_review_like_page(url: str) -> bool:
    path = urlparse(url).path or ""
    film_idx = path.find("/film/")
    if film_idx == -1:
        return False
    prefix = path[:film_idx].strip("/")
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
    aggregate = movie.get("aggregateRating")
    if isinstance(aggregate, dict):
        return _to_float(aggregate.get("ratingValue"))
    return None


def _extract_genres(raw: object) -> list[str]:
    if isinstance(raw, str):
        raw_list = [raw]
    elif isinstance(raw, list):
        raw_list = [genre for genre in raw if isinstance(genre, str)]
    else:
        return []
    return list(dict.fromkeys([genre.strip() for genre in raw_list if genre.strip()]))


def _extract_year_from_releasedate(soup: BeautifulSoup) -> int | None:
    released = soup.select_one(".releasedate a")
    return _to_int(released.get_text(" ", strip=True)) if released else None


def _extract_runtime_from_footer(soup: BeautifulSoup) -> int | None:
    for node in soup.select("p.text-footer"):
        text = node.get_text(" ", strip=True)
        value = _to_int(text)
        if value and "min" in text.lower():
            return value
    return None


def _extract_tagline_from_synopsis(soup: BeautifulSoup) -> str | None:
    node = soup.select_one("section.production-synopsis h4.tagline")
    return node.get_text(" ", strip=True) if node else None


def _extract_genres_from_html(soup: BeautifulSoup) -> list[str]:
    out = []
    for anchor in soup.select("#tab-genres a[href*='/films/genre/']"):
        text = anchor.get_text(" ", strip=True)
        if text and text.lower() != "show allÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦":
            out.append(text)
    return list(dict.fromkeys(out))


def _extract_country_code_from_href(country_href: str) -> str | None:
    match = re.search(r"/films/country/([^/]+)/?", country_href or "")
    if not match:
        return None
    slug = unquote(match.group(1)).strip().lower()
    if not slug:
        return None
    if len(slug) == 2 and slug.isalpha():
        return slug.upper()
    return COUNTRY_SLUG_TO_CODE.get(slug)


def _extract_details_tab(soup: BeautifulSoup) -> tuple[list[str], str | None]:
    country_codes = []
    language = None
    for anchor in soup.select("#tab-details a[href]"):
        href = anchor.get("href", "")
        text = anchor.get_text(" ", strip=True)
        if "/films/country/" in href:
            code = _extract_country_code_from_href(href)
            if code:
                country_codes.append(code)
        if "/films/language/" in href and text and not language:
            language = text
    return list(dict.fromkeys(country_codes)), language


def _extract_cast_from_html(soup: BeautifulSoup) -> list[str]:
    names = []
    for anchor in soup.select("#tab-cast .cast-list a"):
        text = anchor.get_text(" ", strip=True)
        if text:
            names.append(text)
    return list(dict.fromkeys(names))


def _extract_directors_from_html(soup: BeautifulSoup) -> list[str]:
    names = []
    for anchor in soup.select("#tab-crew a[href*='/director/']"):
        text = anchor.get_text(" ", strip=True)
        if text:
            names.append(text)
    if not names:
        for anchor in soup.select("#tab-crew .text-sluglist a"):
            text = anchor.get_text(" ", strip=True)
            if text:
                names.append(text)
    return list(dict.fromkeys(names))


def _canonical_letterboxd_url(final_url: str, html: str, movie: dict | None) -> str:
    if movie and isinstance(movie.get("url"), str) and _is_letterboxd_url(movie["url"]):
        return _to_global_film_url(movie["url"])
    og_url = _extract_meta_content(html, "og:url")
    if og_url and _is_letterboxd_url(og_url):
        return _to_global_film_url(og_url)
    return _to_global_film_url(final_url)


def _parse_film_page(final_url: str, html: str) -> FilmScrapeResult:
    from src.ingestion.scraper import FilmScrapeResult

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
        if title and " â€¢ Letterboxd" in title:
            title = title.replace(" â€¢ Letterboxd", "").strip()
        if title and " Ã¢â‚¬Â¢ Letterboxd" in title:
            title = title.replace(" Ã¢â‚¬Â¢ Letterboxd", "").strip()
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
    overview = movie.get("description") if isinstance(movie.get("description"), str) else None
    if not overview and not review_like:
        overview = _extract_meta_content(html, "og:description") or _extract_meta_content(html, "description")
    poster = (
        movie.get("image") if isinstance(movie.get("image"), str) else None
    ) or _extract_meta_content(html, "og:image")

    genres = _extract_genres_from_html(soup) or _extract_genres(movie.get("genre"))
    directors = _extract_directors_from_html(soup) or _extract_people(movie.get("director"))
    if not directors and not review_like:
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
