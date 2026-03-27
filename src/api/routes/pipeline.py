from __future__ import annotations

from fastapi import APIRouter, HTTPException

from src.api.schemas import PipelineRunRequest, PipelineRunResponse
from src.pipeline.orchestrator import run

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(payload: PipelineRunRequest) -> PipelineRunResponse:
    try:
        summary = run(
            zip_path=payload.zip_path,
            workers=payload.workers,
            timeout=payload.timeout,
            retries=payload.retries,
            retry_backoff=payload.retry_backoff,
            request_interval=payload.request_interval,
            progress_every=payload.progress_every,
            errors_out=payload.errors_out,
            auto_retry_failed=payload.auto_retry_failed,
            retry_failed_passes=payload.retry_failed_passes,
            require_complete_scrape=not payload.allow_partial,
        )
        return PipelineRunResponse(**summary)
    except FileNotFoundError as err:
        raise HTTPException(status_code=404, detail=str(err)) from err
    except RuntimeError as err:
        raise HTTPException(status_code=422, detail=str(err)) from err
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Erro ao executar pipeline: {err}") from err
