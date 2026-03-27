from __future__ import annotations

import os
from typing import Any
from pathlib import Path

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


def _read_dotenv(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return env
    for line in p.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        env[key.strip()] = value.strip().strip("\"").strip("'")
    return env


def get_connection(autocommit: bool = False) -> Any:
    dotenv = _read_dotenv(".env")

    host = os.getenv("POSTGRES_HOST") or dotenv.get("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT") or dotenv.get("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB") or dotenv.get("POSTGRES_DB", "letterboxd")
    user = os.getenv("POSTGRES_USER") or dotenv.get("POSTGRES_USER", "letterboxd")
    password = os.getenv("POSTGRES_PASSWORD") or dotenv.get("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD nao definido no ambiente (.env).")

    if HAS_PSYCOPG3:
        return psycopg.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            autocommit=autocommit,
        )
    if HAS_PSYCOPG2:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
        )
        conn.autocommit = autocommit
        return conn
    raise RuntimeError("Nenhum driver PostgreSQL encontrado. Instale `psycopg` ou `psycopg2-binary`.")
