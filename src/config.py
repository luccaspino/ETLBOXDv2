from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def _read_dotenv(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    dotenv_path = Path(path)
    if not dotenv_path.exists():
        return env

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")

    return env


def get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is not None:
        return value
    return _read_dotenv().get(name, default)


def get_int_env(name: str, default: int, *, min_value: int | None = None) -> int:
    raw = get_env(name)
    try:
        value = default if raw is None else int(str(raw).strip())
    except ValueError:
        value = default

    if min_value is not None:
        value = max(min_value, value)
    return value


__all__ = ["get_env", "get_int_env"]
