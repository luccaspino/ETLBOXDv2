from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

import pandas as pd

LATEST_USER_FILMS_CTE = """
WITH latest_user_films AS (
    SELECT DISTINCT ON (uf.film_id)
        uf.id,
        uf.user_id,
        uf.film_id,
        uf.rating,
        uf.watched_date,
        uf.log_date,
        uf.is_rewatch,
        uf.review_text,
        uf.tags
    FROM user_films uf
    WHERE uf.user_id = %s
    ORDER BY
        uf.film_id,
        COALESCE(uf.log_date, uf.watched_date) DESC NULLS LAST,
        uf.watched_date DESC NULLS LAST,
        uf.id DESC
)
"""


def _normalize_url(url: str | None) -> str | None:
    if not isinstance(url, str):
        return None
    cleaned = url.strip()
    if not cleaned:
        return None
    cleaned = cleaned.split("?", 1)[0].rstrip("/")
    return cleaned or None


def _safe_email(letterboxd_username: str, email: str | None) -> str:
    if isinstance(email, str) and email.strip():
        return email.strip().lower()
    username = (letterboxd_username or "user").strip().lower()
    return f"{username}@letterboxd.local"


def _safe_password_hash() -> str:
    return "__imported_from_letterboxd__"


def _db_null(value: Any) -> Any:
    return None if pd.isna(value) else value


def _film_key(title: Any, year: Any) -> tuple[str, int | None] | None:
    if not isinstance(title, str):
        return None
    normalized_title = title.strip().lower()
    if not normalized_title:
        return None
    if pd.isna(year) or year is None:
        normalized_year = None
    else:
        normalized_year = int(year)
    return (normalized_title, normalized_year)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if pd.isna(value):
        return default
    return bool(value)


def _chunked(seq: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for idx in range(0, len(seq), chunk_size):
        yield seq[idx:idx + chunk_size]


def _execute_many(cur: Any, sql: str, rows: list[tuple[Any, ...]], chunk_size: int = 1000) -> int:
    if not rows:
        return 0
    for chunk in _chunked(rows, chunk_size):
        cur.executemany(sql, chunk)
    return len(rows)


def _normalize_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _normalize_text_filter(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return f"%{cleaned}%"
