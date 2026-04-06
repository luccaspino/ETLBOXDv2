from __future__ import annotations

from typing import Any

from src.db.connection import get_cursor
from src.db.mappings import country_name
from src.db.repository_common import (
    LATEST_USER_FILMS_CTE,
    _normalize_number,
    _normalize_text_filter,
)
from src.text_filters import is_show_all_placeholder, normalize_text_token


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
                f.poster_url,
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
        "poster_url": row[8],
        "letterboxd_url": row[9],
    }


def get_random_review(user_id: str) -> dict[str, Any] | None:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                f.id AS film_id,
                f.title,
                f.year,
                uf.watched_date,
                uf.review_text,
                f.letterboxd_url
            FROM user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE uf.user_id = %s
              AND uf.review_text IS NOT NULL
              AND BTRIM(uf.review_text) <> ''
            ORDER BY random()
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        return None
    return {
        "film_id": int(row[0]),
        "title": row[1],
        "year": row[2],
        "watched_date": str(row[3]) if row[3] is not None else None,
        "review_text": row[4],
        "letterboxd_url": row[5],
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
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                fc.country_code,
                COUNT(DISTINCT uf.film_id)::INT AS total_filmes
            FROM latest_user_films uf
            JOIN film_countries fc ON fc.film_id = uf.film_id
            GROUP BY fc.country_code
            ORDER BY total_filmes DESC, fc.country_code
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    return [{"country_name": country_name(row[0]) or row[0], "total_filmes": int(row[1])} for row in rows]


def get_genre_counts(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                g.name AS genero,
                COUNT(DISTINCT uf.film_id)::INT AS total_filmes
            FROM latest_user_films uf
            JOIN film_genres fg ON fg.film_id = uf.film_id
            JOIN genres g ON g.id = fg.genre_id
            GROUP BY g.name
            ORDER BY total_filmes DESC, g.name
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    return [{"genero": row[0], "total_filmes": int(row[1])} for row in rows]


def _get_category_rankings(
    user_id: str,
    *,
    category: str,
    order_by: str = "most_watched",
    min_films: int = 1,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    order_sql_map = {
        "most_watched": "filmes_assistidos DESC, media_nota_pessoal DESC, nome",
        "best_rated": "media_nota_pessoal DESC, filmes_assistidos DESC, nome",
    }
    config_map = {
        "genre": {
            "select_name": "g.name",
            "join_sql": "JOIN film_genres fg ON fg.film_id = uf.film_id JOIN genres g ON g.id = fg.genre_id",
        },
        "country": {
            "select_name": "fc.country_code",
            "join_sql": "JOIN film_countries fc ON fc.film_id = uf.film_id",
        },
    }
    config = config_map.get(category)
    if config is None:
        raise ValueError(f"Categoria de ranking nao suportada: {category}")

    order_sql = order_sql_map.get(order_by)
    if order_sql is None:
        raise ValueError(f"Ordenacao de ranking nao suportada: {order_by}")

    rating_filter = "WHERE uf.rating IS NOT NULL" if order_by == "best_rated" else ""
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = [user_id, min_films]
    if limit is not None:
        params.append(limit)

    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT
                {config['select_name']} AS nome,
                COUNT(DISTINCT uf.film_id)::INT AS filmes_assistidos,
                ROUND(AVG(uf.rating)::NUMERIC, 2) AS media_nota_pessoal
            FROM latest_user_films uf
            {config['join_sql']}
            {rating_filter}
            GROUP BY 1
            HAVING COUNT(DISTINCT uf.film_id) >= %s
            ORDER BY {order_sql}
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    output: list[dict[str, Any]] = []
    for nome, filmes_assistidos, media_nota_pessoal in rows:
        if category == "country":
            nome = country_name(nome) or nome
        output.append(
            {
                "nome": nome,
                "filmes_assistidos": int(filmes_assistidos),
                "media_nota_pessoal": _normalize_number(media_nota_pessoal),
            }
        )
    return output


def get_country_rankings(
    user_id: str,
    *,
    order_by: str = "most_watched",
    min_films: int = 1,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    return _get_category_rankings(
        user_id,
        category="country",
        order_by=order_by,
        min_films=min_films,
        limit=limit,
    )


def get_genre_rankings(
    user_id: str,
    *,
    order_by: str = "most_watched",
    min_films: int = 1,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    return _get_category_rankings(
        user_id,
        category="genre",
        order_by=order_by,
        min_films=min_films,
        limit=limit,
    )


def get_people_rankings(
    user_id: str,
    *,
    role: str,
    min_films: int = 3,
    order_by: str = "most_watched",
    limit: int | None = None,
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
    limit_sql = "LIMIT %s" if limit is not None else ""
    params: list[Any] = [user_id, role, min_films]
    if limit is not None:
        params.append(limit)

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
            {limit_sql}
            """,
            params,
        )
        rows = cur.fetchall()

    output: list[dict[str, Any]] = []
    for row in rows:
        person_name = normalize_text_token(row[0])
        if not person_name or is_show_all_placeholder(person_name):
            continue
        output.append(
            {
                "nome": person_name,
                "filmes_assistidos": int(row[1]),
                "media_nota_pessoal": _normalize_number(row[2]),
            }
        )
    return output


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
                f.poster_url,
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
            "poster_url": row[8],
            "letterboxd_url": row[9],
        }
        for row in rows
    ]


def _serialize_logged_films(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        genres = [str(item) for item in (row[10] or []) if item]
        countries = [country_name(item) or str(item) for item in (row[11] or []) if item]
        output.append(
            {
                "film_id": int(row[0]),
                "title": row[1],
                "year": row[2],
                "runtime_min": row[3],
                "user_rating": _normalize_number(row[4]),
                "letterboxd_avg_rating": _normalize_number(row[5]),
                "watched_date": str(row[6]) if row[6] is not None else None,
                "tagline": row[7],
                "poster_url": row[8],
                "letterboxd_url": row[9],
                "genres": genres,
                "countries": countries,
            }
        )
    return output


def get_watchlist_films(user_id: str) -> list[dict[str, Any]]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                f.id AS film_id,
                f.title,
                f.year,
                f.runtime_min,
                f.original_language,
                f.tagline,
                f.poster_url,
                f.letterboxd_url,
                f.letterboxd_avg_rating,
                directors.director,
                genres.genres,
                cast_top3.cast_top3,
                w.added_date
            FROM watchlist w
            JOIN films f ON f.id = w.film_id
            LEFT JOIN LATERAL (
                SELECT STRING_AGG(p.name, ', ' ORDER BY p.name) AS director
                FROM film_people fp
                JOIN people p ON p.id = fp.person_id
                WHERE fp.film_id = f.id
                  AND fp.role = 'director'
            ) directors ON TRUE
            LEFT JOIN LATERAL (
                SELECT STRING_AGG(g.name, ', ' ORDER BY g.name) AS genres
                FROM film_genres fg
                JOIN genres g ON g.id = fg.genre_id
                WHERE fg.film_id = f.id
            ) genres ON TRUE
            LEFT JOIN LATERAL (
                SELECT STRING_AGG(p.name, ' | ' ORDER BY fp.cast_order) AS cast_top3
                FROM film_people fp
                JOIN people p ON p.id = fp.person_id
                WHERE fp.film_id = f.id
                  AND fp.role = 'actor'
                  AND fp.cast_order BETWEEN 1 AND 3
            ) cast_top3 ON TRUE
            WHERE w.user_id = %s
            ORDER BY w.added_date DESC NULLS LAST, f.title
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    return [
        {
            "film_id": int(row[0]),
            "title": row[1],
            "year": row[2],
            "runtime_min": row[3],
            "original_language": row[4],
            "tagline": row[5],
            "poster_url": row[6],
            "letterboxd_url": row[7],
            "letterboxd_avg_rating": _normalize_number(row[8]),
            "director": row[9],
            "genres": row[10],
            "cast_top3": row[11],
            "added_date": str(row[12]) if row[12] is not None else None,
        }
        for row in rows
    ]


def get_filter_options(user_id: str) -> dict[str, Any]:
    with get_cursor() as cur:
        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT uf.rating
            FROM latest_user_films uf
            WHERE uf.rating IS NOT NULL
            ORDER BY uf.rating
            """,
            (user_id,),
        )
        personal_ratings = [_normalize_number(row[0]) for row in cur.fetchall()]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT f.letterboxd_avg_rating
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE f.letterboxd_avg_rating IS NOT NULL
            ORDER BY f.letterboxd_avg_rating
            """,
            (user_id,),
        )
        letterboxd_ratings = [_normalize_number(row[0]) for row in cur.fetchall()]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT f.year::INT AS release_year
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE f.year IS NOT NULL
            ORDER BY release_year DESC
            """,
            (user_id,),
        )
        release_years = [int(row[0]) for row in cur.fetchall() if row[0] is not None]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT ((f.year / 10) * 10)::INT AS release_decade
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE f.year IS NOT NULL
            ORDER BY release_decade
            """,
            (user_id,),
        )
        release_decades = [int(row[0]) for row in cur.fetchall() if row[0] is not None]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT EXTRACT(YEAR FROM uf.watched_date)::INT AS watched_year
            FROM latest_user_films uf
            WHERE uf.watched_date IS NOT NULL
            ORDER BY watched_year DESC
            """,
            (user_id,),
        )
        watched_years = [int(row[0]) for row in cur.fetchall() if row[0] is not None]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT EXTRACT(MONTH FROM uf.watched_date)::INT AS watched_month
            FROM latest_user_films uf
            WHERE uf.watched_date IS NOT NULL
            ORDER BY watched_month
            """,
            (user_id,),
        )
        watched_months = [int(row[0]) for row in cur.fetchall() if row[0] is not None]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT g.name
            FROM latest_user_films uf
            JOIN film_genres fg ON fg.film_id = uf.film_id
            JOIN genres g ON g.id = fg.genre_id
            ORDER BY g.name
            """,
            (user_id,),
        )
        genres = [str(row[0]) for row in cur.fetchall() if row[0]]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT fc.country_code
            FROM latest_user_films uf
            JOIN film_countries fc ON fc.film_id = uf.film_id
            ORDER BY fc.country_code
            """,
            (user_id,),
        )
        country_options = [
            {"code": str(row[0]), "name": country_name(row[0]) or str(row[0])}
            for row in cur.fetchall()
            if row[0]
        ]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT p.name
            FROM latest_user_films uf
            JOIN film_people fp ON fp.film_id = uf.film_id AND fp.role = 'director'
            JOIN people p ON p.id = fp.person_id
            ORDER BY p.name
            """,
            (user_id,),
        )
        directors = [
            name
            for row in cur.fetchall()
            if (name := normalize_text_token(row[0])) and not is_show_all_placeholder(name)
        ]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT DISTINCT p.name
            FROM latest_user_films uf
            JOIN film_people fp ON fp.film_id = uf.film_id AND fp.role = 'actor'
            JOIN people p ON p.id = fp.person_id
            ORDER BY p.name
            """,
            (user_id,),
        )
        actors = [
            name
            for row in cur.fetchall()
            if (name := normalize_text_token(row[0])) and not is_show_all_placeholder(name)
        ]

        cur.execute(
            f"""
            {LATEST_USER_FILMS_CTE}
            SELECT MIN(f.runtime_min)::INT, MAX(f.runtime_min)::INT
            FROM latest_user_films uf
            JOIN films f ON f.id = uf.film_id
            WHERE f.runtime_min IS NOT NULL
            """,
            (user_id,),
        )
        runtime_row = cur.fetchone()

    return {
        "personal_ratings": [value for value in personal_ratings if value is not None],
        "letterboxd_ratings": [value for value in letterboxd_ratings if value is not None],
        "watched_years": watched_years,
        "watched_months": watched_months,
        "release_years": release_years,
        "release_decades": release_decades,
        "genres": genres,
        "countries": [item["name"] for item in country_options],
        "country_options": country_options,
        "directors": directors,
        "actors": actors,
        "runtime": {
            "min": int(runtime_row[0]) if runtime_row and runtime_row[0] is not None else None,
            "max": int(runtime_row[1]) if runtime_row and runtime_row[1] is not None else None,
        },
    }


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


def get_logged_films(
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
        include_user_id=True,
    )

    with get_cursor() as cur:
        cur.execute(
            f"""
            SELECT
                f.id AS film_id,
                f.title,
                f.year,
                f.runtime_min,
                uf.rating AS user_rating,
                f.letterboxd_avg_rating,
                uf.watched_date,
                f.tagline,
                f.poster_url,
                f.letterboxd_url,
                genres.genres,
                countries.country_codes
            FROM user_films uf
            JOIN films f ON f.id = uf.film_id
            LEFT JOIN LATERAL (
                SELECT ARRAY_AGG(g.name ORDER BY g.name) AS genres
                FROM film_genres fg
                JOIN genres g ON g.id = fg.genre_id
                WHERE fg.film_id = f.id
            ) genres ON TRUE
            LEFT JOIN LATERAL (
                SELECT ARRAY_AGG(fc.country_code ORDER BY fc.country_code) AS country_codes
                FROM film_countries fc
                WHERE fc.film_id = f.id
            ) countries ON TRUE
            WHERE {where_sql}
              AND uf.watched_date IS NOT NULL
            ORDER BY
                uf.watched_date DESC,
                COALESCE(uf.log_date, uf.watched_date) DESC,
                f.title,
                uf.id DESC
            """,
            params,
        )
        rows = cur.fetchall()

    return _serialize_logged_films(rows)
