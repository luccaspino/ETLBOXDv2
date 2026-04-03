from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

LANGUAGE_TO_CODE = {
    "english": "en",
    "portuguese": "pt",
    "portuguese (brazil)": "pt-BR",
    "spanish": "es",
    "french": "fr",
    "german": "de",
    "italian": "it",
    "japanese": "ja",
    "korean": "ko",
    "chinese": "zh",
    "mandarin": "zh",
    "mandarin chinese": "zh",
    "cantonese": "yue",
    "hindi": "hi",
    "russian": "ru",
    "arabic": "ar",
    "turkish": "tr",
    "swedish": "sv",
    "norwegian": "no",
    "danish": "da",
    "dutch": "nl",
    "polish": "pl",
    "thai": "th",
    "greek": "el",
    "persian": "fa",
    "hebrew": "he",
    "indonesian": "id",
    "romanian": "ro",
    "ukrainian": "uk",
    "czech": "cs",
    "hungarian": "hu",
    "finnish": "fi",
}

COUNTRY_CODE_TO_NAME = {
    "AE": "United Arab Emirates",
    "AR": "Argentina",
    "AT": "Austria",
    "AU": "Australia",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "BO": "Bolivia",
    "BR": "Brazil",
    "CA": "Canada",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CO": "Colombia",
    "CS": "Serbia and Montenegro",
    "CU": "Cuba",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "DO": "Dominican Republic",
    "DZ": "Algeria",
    "EC": "Ecuador",
    "EE": "Estonia",
    "EG": "Egypt",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "GE": "Georgia",
    "GR": "Greece",
    "HK": "Hong Kong",
    "HR": "Croatia",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IN": "India",
    "IR": "Iran",
    "IS": "Iceland",
    "IT": "Italy",
    "JP": "Japan",
    "KR": "South Korea",
    "LB": "Lebanon",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MA": "Morocco",
    "ME": "Montenegro",
    "MX": "Mexico",
    "MY": "Malaysia",
    "NG": "Nigeria",
    "NL": "Netherlands",
    "NO": "Norway",
    "NZ": "New Zealand",
    "PE": "Peru",
    "PH": "Philippines",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "RS": "Serbia",
    "RU": "Russia",
    "SE": "Sweden",
    "SG": "Singapore",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "TH": "Thailand",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TW": "Taiwan",
    "UA": "Ukraine",
    "UK": "United Kingdom",
    "US": "United States",
    "UY": "Uruguay",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "ZA": "South Africa",
}


def normalize_language(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    lower = raw.lower()
    if lower in LANGUAGE_TO_CODE:
        return LANGUAGE_TO_CODE[lower]

    first_token = lower.split(",", 1)[0].strip()
    if first_token in LANGUAGE_TO_CODE:
        return LANGUAGE_TO_CODE[first_token]

    if len(raw) <= 10:
        return raw

    compact = raw.split("(", 1)[0].strip()
    if compact.lower() in LANGUAGE_TO_CODE:
        return LANGUAGE_TO_CODE[compact.lower()]

    truncated = compact[:10]
    logger.debug(
        "Idioma '%s' truncado para '%s' para caber em VARCHAR(10).",
        raw,
        truncated,
    )
    return truncated or None


def country_code(raw: str) -> str | None:
    if not raw:
        return None

    value = raw.strip().upper()
    if len(value) == 2 and value.isalpha():
        return value

    slug = raw.strip().lower().replace(" ", "-")
    manual_map = {
        "united-states": "US",
        "usa": "US",
        "united-kingdom": "GB",
        "uk": "GB",
        "brazil": "BR",
    }
    return manual_map.get(slug)


def country_name(value: str | None) -> str | None:
    if not value:
        return None

    normalized = value.strip().upper()
    return COUNTRY_CODE_TO_NAME.get(normalized, normalized)


__all__ = [
    "COUNTRY_CODE_TO_NAME",
    "LANGUAGE_TO_CODE",
    "country_code",
    "country_name",
    "normalize_language",
]
