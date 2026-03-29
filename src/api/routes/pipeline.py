from __future__ import annotations

import logging
import os
import tempfile
import threading
import zipfile
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status

from src.api.schemas import PipelineRunResponse
from src.pipeline.orchestrator import run

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_ALLOWED_ZIP_CONTENT_TYPES = {
    "application/zip",
    "application/x-zip-compressed",
    "application/octet-stream",
}
_REQUIRED_ZIP_FILES = {
    "profile.csv",
    "diary.csv",
    "ratings.csv",
    "reviews.csv",
    "watchlist.csv",
}
_MAX_ARCHIVE_FILE_COUNT = 200
_MAX_ARCHIVE_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
_PIPELINE_RUN_LOCK = threading.Lock()


def _max_zip_bytes() -> int:
    raw = os.getenv("PIPELINE_MAX_ZIP_MB", "25").strip()
    try:
        mb = max(1, int(raw))
    except ValueError:
        mb = 25
    return mb * 1024 * 1024


def _validate_request_size(request: Request) -> None:
    raw = request.headers.get("content-length", "").strip()
    if not raw:
        return
    try:
        content_length = int(raw)
    except ValueError as err:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cabecalho Content-Length invalido.",
        ) from err

    if content_length > _max_zip_bytes():
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Arquivo excede o limite de {_max_zip_bytes() // (1024 * 1024)} MB.",
        )


def _validate_upload_metadata(file: UploadFile) -> None:
    filename = (file.filename or "").strip()
    if not filename.lower().endswith(".zip"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Envie um arquivo .zip exportado pelo Letterboxd.",
        )
    if file.content_type and file.content_type not in _ALLOWED_ZIP_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tipo de arquivo invalido. Envie um arquivo ZIP.",
        )


def _save_upload_to_temp(file: UploadFile) -> Path:
    max_bytes = _max_zip_bytes()
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    temp_path = Path(temp_file.name)
    written = 0

    try:
        while True:
            chunk = file.file.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"Arquivo excede o limite de {max_bytes // (1024 * 1024)} MB.",
                )
            temp_file.write(chunk)
    except Exception:
        temp_file.close()
        if temp_path.exists():
            temp_path.unlink()
        raise

    temp_file.close()
    return temp_path


def _validate_zip_contents(zip_path: Path) -> None:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            infos = zf.infolist()
            names: set[str] = set()
            for info in infos:
                name = info.filename.replace("\\", "/").strip()
                if not name or name.endswith("/"):
                    continue
                entry_path = Path(name)
                if entry_path.is_absolute() or ".." in entry_path.parts:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail="ZIP invalido. Estrutura interna nao permitida.",
                    )
                names.add(entry_path.name)

            missing = sorted(_REQUIRED_ZIP_FILES - names)
            if missing:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"ZIP invalido. Arquivos obrigatorios ausentes: {', '.join(missing)}.",
                )

            unsupported = sorted(name for name in names if not name.lower().endswith(".csv"))
            if unsupported:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"ZIP invalido. Arquivos nao suportados: {', '.join(unsupported)}.",
                )

            if len(infos) > _MAX_ARCHIVE_FILE_COUNT:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="ZIP invalido. Quantidade de arquivos acima do permitido.",
                )

            uncompressed_total = sum(max(info.file_size, 0) for info in infos)
            if uncompressed_total > _MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="ZIP invalido. Conteudo descompactado acima do permitido.",
                )
    except HTTPException:
        raise
    except zipfile.BadZipFile as err:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Arquivo ZIP invalido ou corrompido.",
        ) from err


@router.post("/run", response_model=PipelineRunResponse)
def run_pipeline(
    request: Request,
    file: UploadFile = File(..., description="ZIP exportado pelo Letterboxd"),
    workers: int = Form(12),
    timeout: int = Form(8),
    retries: int = Form(1),
    retry_backoff: float = Form(0.25),
    request_interval: float = Form(0.01),
    progress_every: int = Form(100),
    auto_retry_failed: bool = Form(True),
    retry_failed_passes: int = Form(6),
    allow_partial: bool = Form(False),
    max_failed_ratio: float = Form(0.0),
) -> PipelineRunResponse:
    temp_zip_path: Path | None = None
    acquired = _PIPELINE_RUN_LOCK.acquire(blocking=False)
    if not acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ja existe um processamento de pipeline em andamento. Tente novamente em instantes.",
        )

    try:
        _validate_request_size(request)
        _validate_upload_metadata(file)
        temp_zip_path = _save_upload_to_temp(file)
        _validate_zip_contents(temp_zip_path)

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
            detail="Nao foi possivel concluir o pipeline com os parametros informados.",
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
