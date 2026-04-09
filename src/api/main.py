from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.routes.analytics import router as analytics_router
from src.api.routes.health import router as health_router
from src.api.routes.pipeline import router as pipeline_router
from src.api.routes.users import router as users_router
from src.config import get_env
from src.db.connection import (
    DATABASE_RETRY_AFTER_SECONDS,
    DatabaseUnavailableError,
)


class _SuppressHealthAccessLogs(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "GET /health " not in message and "GET /health/db " not in message


def _configure_access_logging() -> None:
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    uvicorn_access_logger.addFilter(_SuppressHealthAccessLogs())


def _cors_origins_from_env() -> list[str]:
    raw = get_env("API_CORS_ORIGINS", "") or ""
    origins = [item.strip() for item in raw.split(",") if item.strip()]
    return origins


app = FastAPI(title="Letterboxd Analytics API", version="1.1.2")
_configure_access_logging()

cors_origins = _cors_origins_from_env()
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(health_router)
app.include_router(pipeline_router)
app.include_router(users_router)
app.include_router(analytics_router)


@app.exception_handler(DatabaseUnavailableError)
def handle_database_unavailable(
    _request: Request,
    err: DatabaseUnavailableError,
) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "detail": str(err),
            "code": "database_unavailable",
        },
        headers={"Retry-After": str(DATABASE_RETRY_AFTER_SECONDS)},
    )


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Letterboxd Analytics API online", "docs": "/docs"}
