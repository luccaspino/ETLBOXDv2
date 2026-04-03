from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

from fastapi import HTTPException, Request, UploadFile, status

from src.config import get_int_env

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


def get_max_zip_bytes() -> int:
    return get_int_env("PIPELINE_MAX_ZIP_MB", 25, min_value=1) * 1024 * 1024


def validate_request_size(request: Request) -> None:
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

    if content_length > get_max_zip_bytes():
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Arquivo excede o limite de {get_max_zip_bytes() // (1024 * 1024)} MB.",
        )


def validate_upload_metadata(file: UploadFile) -> None:
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


def save_upload_to_temp(file: UploadFile) -> Path:
    max_bytes = get_max_zip_bytes()
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


def validate_zip_contents(zip_path: Path) -> None:
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
