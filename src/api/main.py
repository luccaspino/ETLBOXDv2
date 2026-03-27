from __future__ import annotations

import logging

from fastapi import FastAPI

from src.api.routes.analytics import router as analytics_router
from src.api.routes.health import router as health_router
from src.api.routes.pipeline import router as pipeline_router

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

app = FastAPI(title="Letterboxd Analytics API", version="0.7.0")
app.include_router(health_router)
app.include_router(pipeline_router)
app.include_router(analytics_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "Letterboxd Analytics API online", "docs": "/docs"}
