from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from src.config import get_env

try:
    import psycopg

    HAS_PSYCOPG3 = True
except Exception:
    psycopg = None
    HAS_PSYCOPG3 = False

try:
    import psycopg2

    HAS_PSYCOPG2 = True
except Exception:
    psycopg2 = None
    HAS_PSYCOPG2 = False

DATABASE_UNAVAILABLE_DETAIL = (
    "Banco temporariamente indisponivel. O Postgres pode estar acordando no Render; "
    "tente novamente em instantes."
)
DATABASE_RETRY_AFTER_SECONDS = 3


class DatabaseUnavailableError(Exception):
    pass


def _raise_database_unavailable(err: BaseException) -> None:
    raise DatabaseUnavailableError(DATABASE_UNAVAILABLE_DETAIL) from err


def get_connection(autocommit: bool = False) -> Any:
    database_url = get_env("DATABASE_URL")
    host = get_env("POSTGRES_HOST", "localhost")
    port = get_env("POSTGRES_PORT", "5432")
    db = get_env("POSTGRES_DB", "letterboxd")
    user = get_env("POSTGRES_USER", "letterboxd")
    password = get_env("POSTGRES_PASSWORD")
    sslmode = get_env("POSTGRES_SSLMODE", "prefer")

    if not database_url and not password:
        raise RuntimeError("POSTGRES_PASSWORD nao definido no ambiente (.env).")

    if HAS_PSYCOPG3:
        try:
            if database_url:
                return psycopg.connect(database_url, autocommit=autocommit)
            return psycopg.connect(
                host=host,
                port=port,
                dbname=db,
                user=user,
                password=password,
                sslmode=sslmode,
                autocommit=autocommit,
            )
        except (psycopg.InterfaceError, psycopg.OperationalError, OSError, TimeoutError) as err:
            _raise_database_unavailable(err)

    if HAS_PSYCOPG2:
        try:
            if database_url:
                conn = psycopg2.connect(database_url)
            else:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    dbname=db,
                    user=user,
                    password=password,
                    sslmode=sslmode,
                )
            conn.autocommit = autocommit
            return conn
        except (psycopg2.InterfaceError, psycopg2.OperationalError, OSError, TimeoutError) as err:
            _raise_database_unavailable(err)

    raise RuntimeError("Nenhum driver PostgreSQL encontrado. Instale `psycopg` ou `psycopg2-binary`.")


@contextmanager
def get_cursor() -> Iterator[Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            yield cur
