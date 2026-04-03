from __future__ import annotations

import re
from urllib.parse import urlparse

LETTERBOXD_HOSTS = {"letterboxd.com", "www.letterboxd.com", "boxd.it", "www.boxd.it"}

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


def _normalize_film_url(uri: str) -> str:
    uri = uri.strip()
    if uri.startswith(("http://", "https://")):
        normalized = uri.rstrip("/")
    elif uri.startswith("/"):
        normalized = f"https://letterboxd.com{uri}".rstrip("/")
    else:
        normalized = f"https://letterboxd.com/{uri}".rstrip("/")

    parsed = urlparse(normalized)
    path = parsed.path or ""
    match = re.match(r"^/film/([^/]+)/\d+$", path.rstrip("/"))
    if match:
        return f"https://letterboxd.com/film/{match.group(1)}"
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
    film_idx = path.find("/film/")
    if film_idx == -1:
        return url.rstrip("/")

    film_path = path[film_idx:].rstrip("/")
    match = re.match(r"^(/film/[^/]+)(?:/\d+)?$", film_path)
    if match:
        film_path = match.group(1)
    return f"https://letterboxd.com{film_path}".rstrip("/")
