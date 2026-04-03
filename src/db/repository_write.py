from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.db.connection import get_connection
from src.db.mappings import country_code, normalize_language
from src.db.repository_common import (
    _chunked,
    _db_null,
    _execute_many,
    _film_key,
    _normalize_url,
    _safe_bool,
    _safe_email,
    _safe_password_hash,
)
from src.ingestion.scraper import FilmScrapeResult

logger = logging.getLogger(__name__)


def fetch_existing_film_urls() -> set[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT letterboxd_url FROM films")
            rows = cur.fetchall()

    urls = {_normalize_url(row[0]) for row in rows}
    urls.discard(None)
    return urls


def fetch_existing_film_keys() -> set[tuple[str, int | None]]:
    with get_connection() as conn:
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
                normalize_language(item.original_language),
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


def _upsert_film_dimensions(cur: Any, results: list[FilmScrapeResult], film_id_by_url: dict[str, int]) -> None:
    genre_names: set[str] = set()
    people_names: set[str] = set()
    pending_film_genres: set[tuple[int, str]] = set()
    pending_directors: set[tuple[int, str]] = set()
    pending_countries: set[tuple[int, str]] = set()
    pending_actors: dict[tuple[int, str], int] = {}

    for item in results:
        url = _normalize_url(item.letterboxd_url)
        if not url or url not in film_id_by_url:
            continue
        film_id = film_id_by_url[url]

        for raw_genre in item.genres:
            genre = (raw_genre or "").strip()
            if not genre:
                continue
            genre_names.add(genre)
            pending_film_genres.add((film_id, genre))

        for raw_director in item.directors:
            director = (raw_director or "").strip()
            if not director:
                continue
            people_names.add(director)
            pending_directors.add((film_id, director))

        for order, raw_actor in enumerate(item.cast, start=1):
            actor = (raw_actor or "").strip()
            if not actor:
                continue
            people_names.add(actor)
            pending_actors.setdefault((film_id, actor), order)

        for raw_country in item.countries:
            code = country_code(raw_country)
            if code:
                pending_countries.add((film_id, code))

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

    for film_id, url, title, year in cur.fetchall():
        normalized = _normalize_url(url)
        if normalized:
            url_map[normalized] = film_id
        film_key = _film_key(title, year)
        if film_key is not None:
            key_map[film_key] = film_id

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
    with get_connection() as conn:
        with conn.cursor() as cur:
            user_id = _upsert_user(cur, parsed["user"])
            logger.info("DB: upsert de filmes...")
            film_id_by_url = _upsert_films(cur, scrape_results)
            logger.info("DB: upsert de dimensoes (genres/people/countries)...")
            _upsert_film_dimensions(cur, scrape_results, film_id_by_url)

            url_aliases: dict[str, str] = {}
            for item in scrape_results:
                requested = _normalize_url(item.requested_url)
                canonical = _normalize_url(item.letterboxd_url)
                if requested and canonical:
                    url_aliases[requested] = canonical

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
