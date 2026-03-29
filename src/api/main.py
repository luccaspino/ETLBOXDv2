from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes.analytics import router as analytics_router
from src.api.routes.health import router as health_router
from src.api.routes.pipeline import router as pipeline_router
from src.api.routes.users import router as users_router


def _cors_origins_from_env() -> list[str]:
    raw = os.getenv("API_CORS_ORIGINS", "")
    origins = [item.strip() for item in raw.split(",") if item.strip()]
    return origins


app = FastAPI(title="Letterboxd Analytics API", version="1.0.0")

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


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Letterboxd Analytics API online", "docs": "/docs"}
