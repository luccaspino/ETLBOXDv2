from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Iterable

import pandas as pd

from src.db.connection import get_cursor, get_read_connection, get_write_connection
from src.ingestion.scraper import FilmScrapeResult

logger = logging.getLogger(__name__)

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


def _normalize_language(value: str | None) -> str | None:
    """
    Garante compatibilidade com films.original_language VARCHAR(10).
    Prioriza codigo ISO curto; fallback com truncamento seguro.
    """
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None

    lower = raw.lower()
    if lower in LANGUAGE_TO_CODE:
        return LANGUAGE_TO_CODE[lower]

    # Ex.: "English, Spanish" -> tenta primeira lingua
    first_token = lower.split(",", 1)[0].strip()
    if first_token in LANGUAGE_TO_CODE:
        return LANGUAGE_TO_CODE[first_token]

    # Ja parece um codigo (en, pt-BR, zh-Hans etc.)
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


def _connect() -> Any:
    return get_write_connection()


def fetch_existing_film_urls() -> set[str]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT letterboxd_url FROM films")
            rows = cur.fetchall()
    urls = {_normalize_url(row[0]) for row in rows}
    urls.discard(None)
    return urls


def fetch_existing_film_keys() -> set[tuple[str, int | None]]:
    """
    Retorna chave lÃƒÂ³gica de filme jÃƒÂ¡ no banco para evitar re-scrape desnecessÃƒÂ¡rio.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT title, year FROM films")
            rows = cur.fetchall()
    keys: set[tuple[str, int | None]] = set()
    for title, year in rows:
        if not title:
            continue
        keys.add((str(title).strip().lower(), int(year) if year is not None else None))
    return keys


def _upsert_user(cur: Any, user_df: pd.DataFrame) -> str:
    row = user_df.iloc[0]
    letterboxd_username = str(row["username"]).strip()
    email = _safe_email(letterboxd_username, row.get("email"))
    app_username = letterboxd_username
    password_hash = _safe_password_hash()

    cur.execute(
        """
        INSERT INTO users (username, email, password_hash, letterboxd_username)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (username) DO UPDATE SET
            email = EXCLUDED.email,
            letterboxd_username = EXCLUDED.letterboxd_username,
            updated_at = NOW()
        RETURNING id
        """,
        (app_username, email, password_hash, letterboxd_username),
    )
    user_id = cur.fetchone()[0]
    logger.info("users: upsert de '%s' concluido.", app_username)
    return str(user_id)


def _upsert_films(cur: Any, results: list[FilmScrapeResult]) -> dict[str, int]:
    film_id_by_url: dict[str, int] = {}
    upserted_canonical_urls: set[str] = set()

    for item in results:
        url = _normalize_url(item.letterboxd_url)
        if not url:
            continue
        if not item.ok or not item.title:
            logger.warning("filme ignorado por scraping incompleto: %s (%s)", url, item.scrape_error)
            continue

        cur.execute(
            """
            INSERT INTO films (
                title, year, runtime_min, original_language, overview, tagline,
                poster_url, letterboxd_url, letterboxd_avg_rating, scraped_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (letterboxd_url) DO UPDATE SET
                title = EXCLUDED.title,
                year = EXCLUDED.year,
                runtime_min = EXCLUDED.runtime_min,
                original_language = EXCLUDED.original_language,
                overview = EXCLUDED.overview,
                tagline = EXCLUDED.tagline,
                poster_url = EXCLUDED.poster_url,
                letterboxd_avg_rating = EXCLUDED.letterboxd_avg_rating,
                scraped_at = EXCLUDED.scraped_at
            RETURNING id
            """,
            (
                item.title,
                item.year,
                item.runtime_min,
                _normalize_language(item.original_language),
                item.overview,
                item.tagline,
                item.poster_url,
                url,
                item.letterboxd_avg_rating,
                item.scraped_at,
            ),
        )
        film_id = cur.fetchone()[0]
        film_id_by_url[url] = film_id
        upserted_canonical_urls.add(url)
        req = _normalize_url(item.requested_url)
        if req:
            film_id_by_url[req] = film_id

    logger.info("films: %s upsert(s).", len(upserted_canonical_urls))
    return film_id_by_url


def _chunked(seq: list[Any], chunk_size: int) -> Iterable[list[Any]]:
    for idx in range(0, len(seq), chunk_size):
        yield seq[idx:idx + chunk_size]


def _execute_many(cur: Any, sql: str, rows: list[tuple[Any, ...]], chunk_size: int = 1000) -> int:
    if not rows:
        return 0
    for chunk in _chunked(rows, chunk_size):
        cur.executemany(sql, chunk)
    return len(rows)


def _fetch_name_id_map(cur: Any, table: str, names: set[str]) -> dict[str, int]:
    if not names:
        return {}
    out: dict[str, int] = {}
    for chunk in _chunked(list(names), 1000):
        cur.execute(f"SELECT id, name FROM {table} WHERE name = ANY(%s)", (chunk,))
        for row in cur.fetchall():
            out[str(row[1])] = int(row[0])
    return out


def _ensure_entities(cur: Any, table: str, names: set[str]) -> dict[str, int]:
    """
    Garante entidades por nome em lote (genres/people) e retorna mapa nome->id.
    """
    if table not in {"genres", "people"}:
        raise ValueError(f"Tabela nao suportada para _ensure_entities: {table}")
    if not names:
        return {}

    existing = _fetch_name_id_map(cur, table, names)
    missing = [name for name in names if name not in existing]
    if missing:
        _execute_many(
            cur,
            f"INSERT INTO {table} (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
            [(name,) for name in missing],
            chunk_size=2000,
        )
        existing = _fetch_name_id_map(cur, table, names)
    return existing


def _country_code(raw: str) -> str | None:
    if not raw:
        return None
    val = raw.strip().upper()
    if len(val) == 2 and val.isalpha():
        return val
    slug = raw.strip().lower().replace(" ", "-")
    manual_map = {
        "united-states": "US",
        "usa": "US",
        "united-kingdom": "GB",
        "uk": "GB",
        "brazil": "BR",
    }
    return manual_map.get(slug)


def _upsert_film_dimensions(cur: Any, results: list[FilmScrapeResult], film_id_by_url: dict[str, int]) -> None:
    genre_names: set[str] = set()
    people_names: set[str] = set()
    pending_film_genres: set[tuple[int, str]] = set()
    pending_directors: set[tuple[int, str]] = set()
    pending_countries: set[tuple[int, str]] = set()
    # Mantem primeira ocorrencia no cast para preservar ordem principal.
    pending_actors: dict[tuple[int, str], int] = {}

    for item in results:
        url = _normalize_url(item.letterboxd_url)
        if not url or url not in film_id_by_url:
            continue
        fid = film_id_by_url[url]

        for raw_genre in item.genres:
            genre = (raw_genre or "").strip()
            if not genre:
                continue
            genre_names.add(genre)
            pending_film_genres.add((fid, genre))

        for raw_director in item.directors:
            director = (raw_director or "").strip()
            if not director:
                continue
            people_names.add(director)
            pending_directors.add((fid, director))

        for order, raw_actor in enumerate(item.cast, start=1):
            actor = (raw_actor or "").strip()
            if not actor:
                continue
            people_names.add(actor)
            pending_actors.setdefault((fid, actor), order)

        for raw_country in item.countries:
            code = _country_code(raw_country)
            if code:
                pending_countries.add((fid, code))

    logger.info(
        "Dimensoes: generos=%s pessoas=%s links_genero=%s links_diretor=%s links_ator=%s links_pais=%s",
        len(genre_names),
        len(people_names),
        len(pending_film_genres),
        len(pending_directors),
        len(pending_actors),
        len(pending_countries),
    )

    genre_id_by_name = _ensure_entities(cur, "genres", genre_names)
    people_id_by_name = _ensure_entities(cur, "people", people_names)

    film_genre_rows = [
        (film_id, genre_id_by_name[genre])
        for film_id, genre in pending_film_genres
        if genre in genre_id_by_name
    ]
    _execute_many(
        cur,
        """
        INSERT INTO film_genres (film_id, genre_id)
        VALUES (%s, %s)
        ON CONFLICT (film_id, genre_id) DO NOTHING
        """,
        film_genre_rows,
    )

    film_people_rows: list[tuple[int, int, str, int | None]] = []
    for film_id, director in pending_directors:
        person_id = people_id_by_name.get(director)
        if person_id is not None:
            film_people_rows.append((film_id, person_id, "director", None))
    for (film_id, actor), cast_order in pending_actors.items():
        person_id = people_id_by_name.get(actor)
        if person_id is not None:
            film_people_rows.append((film_id, person_id, "actor", cast_order))

    _execute_many(
        cur,
        """
        INSERT INTO film_people (film_id, person_id, role, cast_order)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (film_id, person_id, role) DO UPDATE SET
            cast_order = EXCLUDED.cast_order
        """,
        film_people_rows,
    )

    _execute_many(
        cur,
        """
        INSERT INTO film_countries (film_id, country_code)
        VALUES (%s, %s)
        ON CONFLICT (film_id, country_code) DO NOTHING
        """,
        list(pending_countries),
    )


def _insert_user_films(
    cur: Any,
    user_id: str,
    user_films_df: pd.DataFrame,
    film_id_by_url: dict[str, int],
    film_id_by_key: dict[tuple[str, int | None], int],
    url_aliases: dict[str, str],
) -> int:
    rows_with_date: list[tuple[Any, ...]] = []
    rows_without_date: list[tuple[Any, ...]] = []
    for _, row in user_films_df.iterrows():
        url = _normalize_url(row.get("letterboxd_uri"))
        if not url:
            continue
        if url not in film_id_by_url and url in url_aliases:
            url = url_aliases[url]

        film_id = film_id_by_url.get(url) if url else None
        if film_id is None:
            film_key = _film_key(row.get("film_name"), row.get("film_year"))
            if film_key is not None:
                film_id = film_id_by_key.get(film_key)
        if film_id is None:
            continue

        watched_date = _db_null(row.get("watched_date"))
        row_values = (
            user_id,
            film_id,
            _db_null(row.get("rating")),
            watched_date,
            _db_null(row.get("log_date")),
            _safe_bool(row.get("is_rewatch", False)),
            _db_null(row.get("review_text")),
            _db_null(row.get("tags")),
        )
        if watched_date is None:
            rows_without_date.append(row_values)
        else:
            rows_with_date.append(row_values)

    inserted_or_updated = 0
    inserted_or_updated += _execute_many(
        cur,
        """
        INSERT INTO user_films (
            user_id, film_id, rating, watched_date, log_date, is_rewatch, review_text, tags
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id, film_id, watched_date) WHERE watched_date IS NOT NULL DO UPDATE SET
            rating = EXCLUDED.rating,
            log_date = EXCLUDED.log_date,
            is_rewatch = EXCLUDED.is_rewatch,
            review_text = EXCLUDED.review_text,
            tags = EXCLUDED.tags
        """,
        rows_with_date,
        chunk_size=1000,
    )
    inserted_or_updated += _execute_many(
        cur,
        """
        INSERT INTO user_films (
            user_id, film_id, rating, watched_date, log_date, is_rewatch, review_text, tags
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id, film_id) WHERE watched_date IS NULL DO UPDATE SET
            rating = EXCLUDED.rating,
            log_date = EXCLUDED.log_date,
            is_rewatch = EXCLUDED.is_rewatch,
            review_text = EXCLUDED.review_text,
            tags = EXCLUDED.tags
        """,
        rows_without_date,
        chunk_size=1000,
    )
    return inserted_or_updated


def _insert_watchlist(
    cur: Any,
    user_id: str,
    watchlist_df: pd.DataFrame,
    film_id_by_url: dict[str, int],
    film_id_by_key: dict[tuple[str, int | None], int],
    url_aliases: dict[str, str],
) -> int:
    rows_to_insert: list[tuple[Any, ...]] = []
    for _, row in watchlist_df.iterrows():
        url = _normalize_url(row.get("letterboxd_uri"))
        if not url:
            continue
        if url not in film_id_by_url and url in url_aliases:
            url = url_aliases[url]

        film_id = film_id_by_url.get(url) if url else None
        if film_id is None:
            film_key = _film_key(row.get("film_name"), row.get("film_year"))
            if film_key is not None:
                film_id = film_id_by_key.get(film_key)
        if film_id is None:
            continue

        rows_to_insert.append((user_id, film_id, _db_null(row.get("added_date"))))

    return _execute_many(
        cur,
        """
        INSERT INTO watchlist (user_id, film_id, added_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, film_id) DO UPDATE SET
            added_date = COALESCE(EXCLUDED.added_date, watchlist.added_date)
        """,
        rows_to_insert,
        chunk_size=1000,
    )


def _fetch_all_film_ids(cur: Any) -> tuple[dict[str, int], dict[tuple[str, int | None], int]]:
    cur.execute("SELECT id, letterboxd_url, title, year FROM films")
    url_map: dict[str, int] = {}
    key_map: dict[tuple[str, int | None], int] = {}
    for fid, url, title, year in cur.fetchall():
        norm = _normalize_url(url)
        if norm:
            url_map[norm] = fid
        film_key = _film_key(title, year)
        if film_key is not None:
            key_map[film_key] = fid
    return url_map, key_map


def _fetch_user_collection_totals(cur: Any, user_id: str) -> tuple[int, int]:
    cur.execute(
        """
        SELECT
            (
                SELECT COUNT(*)::INT
                FROM (
                    SELECT DISTINCT ON (uf.film_id) uf.film_id
                    FROM user_films uf
                    WHERE uf.user_id = %s
                    ORDER BY
                        uf.film_id,
                        COALESCE(uf.log_date, uf.watched_date) DESC NULLS LAST,
                        uf.watched_date DESC NULLS LAST,
                        uf.id DESC
                ) latest_films
            ) AS total_filmes,
            (
                SELECT COUNT(*)::INT
                FROM watchlist w
                WHERE w.user_id = %s
            ) AS total_watchlist
        """,
        (user_id, user_id),
    )
    row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def load_all_to_db(
    parsed: dict[str, pd.DataFrame],
    scrape_results: list[FilmScrapeResult],
) -> dict[str, int]:
    logger.info("DB: iniciando transacao de carga...")
    with _connect() as conn:
        with conn.cursor() as cur:
            user_id = _upsert_user(cur, parsed["user"])
            logger.info("DB: upsert de filmes...")
            film_id_by_url = _upsert_films(cur, scrape_results)
            logger.info("DB: upsert de dimensoes (genres/people/countries)...")
            _upsert_film_dimensions(cur, scrape_results, film_id_by_url)
            url_aliases = {}
            for item in scrape_results:
                req = _normalize_url(item.requested_url)
                canon = _normalize_url(item.letterboxd_url)
                if req and canon:
                    url_aliases[req] = canon

            # Garante mapeamento tambÃƒÂ©m para filmes que jÃƒÂ¡ existiam no DB.
            logger.info("DB: carregando mapa completo de filmes...")
            full_film_url_map, full_film_key_map = _fetch_all_film_ids(cur)
            logger.info("DB: carregando user_films...")
            user_films_count = _insert_user_films(
                cur,
                user_id,
                parsed["user_films"],
                full_film_url_map,
                full_film_key_map,
                url_aliases,
            )
            logger.info("DB: carregando watchlist...")
            watchlist_count = _insert_watchlist(
                cur,
                user_id,
                parsed["watchlist"],
                full_film_url_map,
                full_film_key_map,
                url_aliases,
            )
            total_filmes_loaded, total_watchlist_loaded = _fetch_user_collection_totals(cur, user_id)

        conn.commit()
        logger.info("DB: commit concluido.")

    films_upserted_from_scrape = len(
        {
            _normalize_url(item.letterboxd_url)
            for item in scrape_results
            if item.ok and item.title and _normalize_url(item.letterboxd_url)
        }
    )
    logger.info(
        "Carga user_films/watchlist: linhas_processadas user_films=%s watchlist=%s totais_finais user_films=%s watchlist=%s",
        user_films_count,
        watchlist_count,
        total_filmes_loaded,
        total_watchlist_loaded,
    )
    stats = {
        "films_upserted_from_scrape": films_upserted_from_scrape,
        "user_films_loaded": total_filmes_loaded,
        "watchlist_loaded": total_watchlist_loaded,
    }
    logger.info("Carga concluida: %s", stats)
    return stats


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


def _build_filtered_clause(
    user_id: str,
    *,
    min_rating: float | None = None,
    max_rating: float | None = None,
    min_runtime: int | None = None,
    max_runtime: int | None = None,
    decade_start: int | None = None,
    director_name: str | None = None,
    actor_name: str | None = None,
    country_code: str | None = None,
    genre_name: str | None = None,
    watched_month: int | None = None,
    watched_year: int | None = None,
    include_user_id: bool = True,
) -> tuple[str, list[Any]]:
    where = ["uf.user_id = %s"] if include_user_id else []
    params: list[Any] = [user_id] if include_user_id else []

    if min_rating is not None:
        where.append("uf.rating >= %s")
        params.append(min_rating)
    if max_rating is not None:
        where.append("uf.rating <= %s")
        params.append(max_rating)
    if min_runtime is not None:
        where.append("f.runtime_min >= %s")
        params.append(min_runtime)
    if max_runtime is not None:
        where.append("f.runtime_min <= %s")
        params.append(max_runtime)
    if decade_start is not None:
        where.append("(f.year IS NOT NULL AND f.year BETWEEN %s AND %s)")
        params.extend([decade_start, decade_start + 9])

    director_like = _normalize_text_filter(director_name)
    if director_like:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM film_people fp
                JOIN people p ON p.id = fp.person_id
                WHERE fp.film_id = f.id
                  AND fp.role = 'director'
                  AND p.name ILIKE %s
            )
            """
        )
        params.append(director_like)

    actor_like = _normalize_text_filter(actor_name)
    if actor_like:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM film_people fp
                JOIN people p ON p.id = fp.person_id
                WHERE fp.film_id = f.id
                  AND fp.role = 'actor'
                  AND p.name ILIKE %s
            )
            """
        )
        params.append(actor_like)

    if country_code:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM film_countries fc
                WHERE fc.film_id = f.id
                  AND fc.country_code = %s
            )
            """
        )
        params.append(country_code.strip().upper())

    genre_like = _normalize_text_filter(genre_name)
    if genre_like:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM film_genres fg
                JOIN genres g ON g.id = fg.genre_id
                WHERE fg.film_id = f.id
                  AND g.name ILIKE %s
            )
            """
        )
        params.append(genre_like)

    if watched_month is not None:
        where.append("EXTRACT(MONTH FROM uf.watched_date) = %s")
        params.append(watched_month)
    if watched_year is not None:
        where.append("EXTRACT(YEAR FROM uf.watched_date) = %s")
        params.append(watched_year)

    return (" AND ".join(where) if where else "TRUE"), params


def get_user_id_by_username(username: str) -> str | None:
    with get_cursor() as cur:
        cur.execute("SELECT id::text FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return row[0] if row else None


def get_user_lookup(username: str) -> dict[str, Any] | None:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                u.username,
                (
                    SELECT COUNT(*)::INT
                    FROM (
                        SELECT DISTINCT ON (uf.film_id) uf.film_id
                        FROM user_films uf
                        WHERE uf.user_id = u.id
                        ORDER BY
                            uf.film_id,
                            COALESCE(uf.log_date, uf.watched_date) DESC NULLS LAST,
                            uf.watched_date DESC NULLS LAST,
                            uf.id DESC
                    ) latest_films
                ) AS total_filmes,
                (
                    SELECT COUNT(*)::INT
                    FROM watchlist w
                    WHERE w.user_id = u.id
                ) AS total_watchlist
            FROM users u
            WHERE u.username = %s
            """,
            (username,),
        )
        row = cur.fetchone()

    if not row:
        return None

    total_filmes = int(row[1] or 0)
    total_watchlist = int(row[2] or 0)
    return {
        "username": row[0],
        "has_data": (total_filmes + total_watchlist) > 0,
        "total_filmes": total_filmes,
        "total_watchlist": total_watchlist,
    }


def get_main_kpis(user_id: str) -> dict[str, float | int | None]:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                COUNT(*)::INT AS total_filmes,
                ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal,
                ROUND(SUM(COALESCE(f.runtime_min, 0)) / 60.0, 2) AS total_horas
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        return {"total_filmes": 0, "media_nota_pessoal": None, "total_horas": 0.0}
    return {
        "total_filmes": int(row[0] or 0),
        "media_nota_pessoal": _normalize_number(row[1]),
        "total_horas": _normalize_number(row[2]) or 0.0,
    }


def get_rating_gap_kpis(user_id: str) -> dict[str, float | None]:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                ROUND(AVG(uf.rating - f.letterboxd_avg_rating)::NUMERIC, 2) AS diferenca_media,
                ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_pessoal,
                ROUND(AVG(f.letterboxd_avg_rating)::NUMERIC, 2) AS media_letterboxd
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE uf.rating IS NOT NULL
              AND f.letterboxd_avg_rating IS NOT NULL
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        return {"diferenca_media": None, "media_pessoal": None, "media_letterboxd": None}
    return {
        "diferenca_media": _normalize_number(row[0]),
        "media_pessoal": _normalize_number(row[1]),
        "media_letterboxd": _normalize_number(row[2]),
    }


def get_logs_by_month(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                EXTRACT(MONTH FROM uf.watched_date)::INT AS mes,
                COUNT(*)::INT AS total
            FROM user_films uf
            WHERE uf.user_id = %s
              AND uf.watched_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    return [{"mes": int(row[0]), "total": int(row[1])} for row in rows]


def get_release_year_kpi(user_id: str) -> dict[str, float | None]:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT ROUND(AVG(f.year)::NUMERIC, 1) AS ano_medio_lancamento
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE f.year IS NOT NULL
            """,
            (user_id,),
        )
        row = cur.fetchone()
    return {"ano_medio_lancamento": _normalize_number(row[0]) if row else None}


def get_random_watchlist_film(user_id: str) -> dict[str, Any] | None:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                f.id AS film_id,
                f.title,
                f.year,
                f.runtime_min,
                luf.rating AS user_rating,
                f.letterboxd_avg_rating,
                luf.watched_date,
                f.tagline,
                f.letterboxd_url
            FROM watchlist w
            JOIN films f ON f.id = w.film_id
            LEFT JOIN latest_user_films luf ON luf.film_id = f.id
            WHERE w.user_id = %s
            ORDER BY random()
            LIMIT 1
            """,
            (user_id, user_id),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "film_id": int(row[0]),
        "title": row[1],
        "year": row[2],
        "runtime_min": row[3],
        "user_rating": _normalize_number(row[4]),
        "letterboxd_avg_rating": _normalize_number(row[5]),
        "watched_date": str(row[6]) if row[6] is not None else None,
        "tagline": row[7],
        "letterboxd_url": row[8],
    }


def get_logs_by_year(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                EXTRACT(YEAR FROM uf.watched_date)::INT AS ano,
                COUNT(*)::INT AS total
            FROM user_films uf
            WHERE uf.user_id = %s
              AND uf.watched_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    return [{"ano": int(row[0]), "total": int(row[1])} for row in rows]


def get_rating_distribution(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            WITH latest_rating_per_film AS (
                SELECT DISTINCT ON (uf.film_id)
                    uf.film_id,
                    uf.rating
                FROM user_films uf
                WHERE uf.user_id = %s
                  AND uf.rating IS NOT NULL
                ORDER BY
                    uf.film_id,
                    COALESCE(uf.log_date, uf.watched_date) DESC NULLS LAST,
                    uf.watched_date DESC NULLS LAST,
                    uf.id DESC
            )
            SELECT rating, COUNT(*)::INT AS total
            FROM latest_rating_per_film
            GROUP BY rating
            ORDER BY rating
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    return [{"rating": _normalize_number(row[0]), "total": int(row[1])} for row in rows]


def get_country_counts(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                fc.country_code,
                COUNT(DISTINCT uf.film_id)::INT AS total_filmes
            FROM user_films uf
            JOIN film_countries fc ON fc.film_id = uf.film_id
            WHERE uf.user_id = %s
            GROUP BY fc.country_code
            ORDER BY total_filmes DESC, fc.country_code
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    return [{"country_code": row[0], "total_filmes": int(row[1])} for row in rows]


def get_genre_counts(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                g.name AS genero,
                COUNT(DISTINCT uf.film_id)::INT AS total_filmes
            FROM user_films uf
            JOIN film_genres fg ON fg.film_id = uf.film_id
            JOIN genres g ON g.id = fg.genre_id
            WHERE uf.user_id = %s
            GROUP BY g.name
            ORDER BY total_filmes DESC, g.name
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    return [{"genero": row[0], "total_filmes": int(row[1])} for row in rows]


def get_people_rankings(
    user_id: str,
    *,
    role: str,
    min_films: int = 3,
    order_by: str = "most_watched",
) -> list[dict[str, Any]]:
    order_sql_map = {
        "most_watched": "filmes_assistidos DESC, media_nota_pessoal DESC, p.name",
        "best_rated": "media_nota_pessoal DESC, filmes_assistidos DESC, p.name",
    }
    if role not in {"director", "actor"}:
        raise ValueError(f"Papel de ranking nao suportado: {role}")
    order_sql = order_sql_map.get(order_by)
    if order_sql is None:
        raise ValueError(f"Ordenacao de ranking nao suportada: {order_by}")

    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                p.name AS nome,
                COUNT(DISTINCT uf.film_id)::INT AS filmes_assistidos,
                ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal
            FROM latest_user_films uf
            JOIN film_people fp ON fp.film_id = uf.film_id AND fp.role = %s
            JOIN people p ON p.id = fp.person_id
            WHERE uf.rating IS NOT NULL
            GROUP BY p.name
            HAVING COUNT(DISTINCT uf.film_id) >= %s
            ORDER BY {order_sql}
            """,
            (user_id, role, min_films),
        )
        rows = cur.fetchall()

    return [
        {
            "nome": row[0],
            "filmes_assistidos": int(row[1]),
            "media_nota_pessoal": _normalize_number(row[2]),
        }
        for row in rows
    ]


def _fetch_filtered_film_rows(
    user_id: str,
    *,
    random_order: bool = False,
    min_rating: float | None = None,
    max_rating: float | None = None,
    min_runtime: int | None = None,
    max_runtime: int | None = None,
    decade_start: int | None = None,
    director_name: str | None = None,
    actor_name: str | None = None,
    country_code: str | None = None,
    genre_name: str | None = None,
    watched_month: int | None = None,
    watched_year: int | None = None,
) -> list[tuple[Any, ...]]:
    where_sql, params = _build_filtered_clause(
        user_id,
        min_rating=min_rating,
        max_rating=max_rating,
        min_runtime=min_runtime,
        max_runtime=max_runtime,
        decade_start=decade_start,
        director_name=director_name,
        actor_name=actor_name,
        country_code=country_code,
        genre_name=genre_name,
        watched_month=watched_month,
        watched_year=watched_year,
        include_user_id=False,
    )
    order_sql = "random()" if random_order else "uf.watched_date DESC NULLS LAST, f.title"
    limit_sql = "LIMIT 1" if random_order else ""

    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                f.id AS film_id,
                f.title,
                f.year,
                f.runtime_min,
                uf.rating AS user_rating,
                f.letterboxd_avg_rating,
                uf.watched_date,
                f.tagline,
                f.letterboxd_url
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE {where_sql}
            ORDER BY {order_sql}
            {limit_sql}
            """,
            (user_id, *params),
        )
        return cur.fetchall()


def _serialize_filtered_films(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {
            "film_id": int(row[0]),
            "title": row[1],
            "year": row[2],
            "runtime_min": row[3],
            "user_rating": _normalize_number(row[4]),
            "letterboxd_avg_rating": _normalize_number(row[5]),
            "watched_date": str(row[6]) if row[6] is not None else None,
            "tagline": row[7],
            "letterboxd_url": row[8],
        }
        for row in rows
    ]


def get_filtered_films(
    user_id: str,
    *,
    min_rating: float | None = None,
    max_rating: float | None = None,
    min_runtime: int | None = None,
    max_runtime: int | None = None,
    decade_start: int | None = None,
    director_name: str | None = None,
    actor_name: str | None = None,
    country_code: str | None = None,
    genre_name: str | None = None,
    watched_month: int | None = None,
    watched_year: int | None = None,
) -> list[dict[str, Any]]:
    rows = _fetch_filtered_film_rows(
        user_id,
        min_rating=min_rating,
        max_rating=max_rating,
        min_runtime=min_runtime,
        max_runtime=max_runtime,
        decade_start=decade_start,
        director_name=director_name,
        actor_name=actor_name,
        country_code=country_code,
        genre_name=genre_name,
        watched_month=watched_month,
        watched_year=watched_year,
    )
    return _serialize_filtered_films(rows)


__all__ = [
    "fetch_existing_film_urls",
    "fetch_existing_film_keys",
    "load_all_to_db",
    "get_user_id_by_username",
    "get_user_lookup",
    "get_main_kpis",
    "get_rating_gap_kpis",
    "get_logs_by_month",
    "get_release_year_kpi",
    "get_random_watchlist_film",
    "get_logs_by_year",
    "get_rating_distribution",
    "get_country_counts",
    "get_genre_counts",
    "get_people_rankings",
    "get_filtered_films",
]
