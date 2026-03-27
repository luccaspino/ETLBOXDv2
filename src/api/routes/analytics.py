from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import MainKpisResponse, MonthlyLogItem, RatingGapResponse
from src.db.repository import (
    get_logs_by_month,
    get_main_kpis,
    get_rating_gap_kpis,
    get_user_id_by_username,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _require_user_id(username: str) -> str:
    user_id = get_user_id_by_username(username)
    if not user_id:
        raise HTTPException(status_code=404, detail=f"Usuario '{username}' nao encontrado.")
    return user_id


@router.get("/kpis/main", response_model=MainKpisResponse)
def get_kpis_main(username: str = Query(..., description="Username da tabela users")) -> MainKpisResponse:
    user_id = _require_user_id(username)
    return MainKpisResponse(**get_main_kpis(user_id))


@router.get("/kpis/rating-gap", response_model=RatingGapResponse)
def get_kpis_rating_gap(username: str = Query(..., description="Username da tabela users")) -> RatingGapResponse:
    user_id = _require_user_id(username)
    return RatingGapResponse(**get_rating_gap_kpis(user_id))


@router.get("/logs/monthly", response_model=list[MonthlyLogItem])
def get_monthly_logs(username: str = Query(..., description="Username da tabela users")) -> list[MonthlyLogItem]:
    user_id = _require_user_id(username)
    return [MonthlyLogItem(**row) for row in get_logs_by_month(user_id)]
