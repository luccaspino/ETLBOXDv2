from __future__ import annotations

import logging
import threading
import time
from collections import deque
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status

from src.api.schemas import PipelineRunResponse
from src.api.validators.pipeline import (
    save_upload_to_temp,
    validate_request_size,
    validate_upload_metadata,
    validate_zip_contents,
)
from src.config import get_int_env
from src.pipeline.orchestrator import run

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_PIPELINE_RUN_LOCK = threading.Lock()
_PIPELINE_RATE_LIMIT_LOCK = threading.Lock()
_PIPELINE_REQUEST_HISTORY: dict[str, deque[float]] = {}


def _client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "").strip()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip() or "unknown"
    return request.client.host if request.client else "unknown"


def _enforce_rate_limit(request: Request) -> None:
    client_ip = _client_ip(request)
    now = time.time()
    window_seconds = get_int_env("PIPELINE_RATE_LIMIT_WINDOW_SECONDS", 900, min_value=1)
    max_requests = get_int_env("PIPELINE_RATE_LIMIT_MAX_REQUESTS", 3, min_value=1)

    with _PIPELINE_RATE_LIMIT_LOCK:
        history = _PIPELINE_REQUEST_HISTORY.setdefault(client_ip, deque())
        cutoff = now - window_seconds

        while history and history[0] <= cutoff:
            history.popleft()

        if len(history) >= max_requests:
            retry_after = max(1, int(history[0] + window_seconds - now))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    "Limite de requisicoes do pipeline excedido. "
                    f"Tente novamente em {retry_after} segundos."
                ),
                headers={"Retry-After": str(retry_after)},
            )

        history.append(now)


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(
    request: Request,
    file: UploadFile = File(..., description="ZIP exportado pelo Letterboxd"),
    workers: int = Form(12),
    timeout: int = Form(8),
    retries: int = Form(1),
    retry_backoff: float = Form(0.25),
    request_interval: float = Form(0.01),
    progress_every: int = Form(10),
    auto_retry_failed: bool = Form(True),
    retry_failed_passes: int = Form(6),
    allow_partial: bool = Form(False),
    max_failed_ratio: float = Form(0.0),
) -> PipelineRunResponse:
    temp_zip_path: Path | None = None
    _enforce_rate_limit(request)
    acquired = _PIPELINE_RUN_LOCK.acquire(blocking=False)
    if not acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ja existe um processamento de pipeline em andamento. Tente novamente em instantes.",
        )

    try:
        validate_request_size(request)
        validate_upload_metadata(file)
        temp_zip_path = save_upload_to_temp(file)
        validate_zip_contents(temp_zip_path)

        summary = run(
            zip_path=str(temp_zip_path),
            workers=max(1, min(32, workers)),
            timeout=max(1, min(60, timeout)),
            retries=max(0, min(5, retries)),
            retry_backoff=max(0.0, min(5.0, retry_backoff)),
            request_interval=max(0.0, min(2.0, request_interval)),
            progress_every=max(1, min(1000, progress_every)),
            errors_out=None,
            auto_retry_failed=auto_retry_failed,
            retry_failed_passes=max(0, min(10, retry_failed_passes)),
            require_complete_scrape=not allow_partial,
            max_failed_ratio=max(0.0, min(1.0, max_failed_ratio)),
        )
        return PipelineRunResponse(**summary)
    except HTTPException:
        raise
    except RuntimeError as err:
        logger.warning("Falha controlada na execucao do pipeline: %s", err)
        raise HTTPException(
            status_code=422,
            detail=str(err),
        ) from err
    except Exception as err:
        logger.exception("Erro interno ao executar pipeline")
        raise HTTPException(status_code=500, detail="Erro interno ao executar pipeline.") from err
    finally:
        try:
            file.file.close()
        except Exception:
            pass
        if temp_zip_path and temp_zip_path.exists():
            temp_zip_path.unlink(missing_ok=True)
        if acquired:
            _PIPELINE_RUN_LOCK.release()
