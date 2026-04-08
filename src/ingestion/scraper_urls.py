from __future__ import annotations

import re
from urllib.parse import urlparse

LETTERBOXD_HOSTS = {"letterboxd.com", "www.letterboxd.com", "boxd.it", "www.boxd.it"}
_GLOBAL_FILM_RE = re.compile(r"^/film/([^/]+)$")
_GLOBAL_FILM_PAGE_RE = re.compile(r"^/film/([^/]+)/(\d+)$")
_USER_FILM_RE = re.compile(r"^/[^/]+/film/([^/]+)$")
_USER_FILM_PAGE_RE = re.compile(r"^/[^/]+/film/([^/]+)/(\d+)$")

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


def _extract_canonical_film_slug(path: str) -> str | None:
    clean_path = (path or "").rstrip("/")
    for pattern in (_GLOBAL_FILM_RE, _GLOBAL_FILM_PAGE_RE, _USER_FILM_RE, _USER_FILM_PAGE_RE):
        match = pattern.match(clean_path)
        if match:
            return match.group(1)
    return None


def _normalize_film_url(uri: str) -> str:
    uri = uri.strip()
    if uri.startswith(("http://", "https://")):
        normalized = uri.rstrip("/")
    elif uri.startswith("/"):
        normalized = f"https://letterboxd.com{uri}".rstrip("/")
    else:
        normalized = f"https://letterboxd.com/{uri}".rstrip("/")

    parsed = urlparse(normalized)
    slug = _extract_canonical_film_slug(parsed.path or "")
    if slug:
        return f"https://letterboxd.com/film/{slug}/"
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
    slug = _extract_canonical_film_slug(parsed.path or "")
    if slug:
        return f"https://letterboxd.com/film/{slug}/"
    return url.rstrip("/")
