from __future__ import annotations

from collections.abc import Sequence
from html import escape

import streamlit as st

MONTH_LABELS_PT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


def month_label(month: int | None) -> str:
    if month is None:
        return "-"
    return MONTH_LABELS_PT.get(month, str(month))


def extract_month_from_date(date_text: str | None) -> int | None:
    if not date_text or len(date_text) < 7:
        return None
    try:
        return int(date_text[5:7])
    except ValueError:
        return None


def _format_user_rating(value: object) -> str:
    if value is None or value == "":
        return "Sem nota"
    try:
        numeric = round(float(value) * 2) / 2
    except (TypeError, ValueError):
        return "Sem nota"

    full_stars = int(numeric)
    has_half_star = (numeric - full_stars) >= 0.5
    return ("&#9733;" * full_stars) + ("&frac12;" if has_half_star else "")


def _build_card_html(film: dict) -> str:
    poster_url = str(film.get("poster_url") or "").strip()
    title = str(film.get("title") or "Sem título")
    year = film.get("year")
    letterboxd_url = str(film.get("letterboxd_url") or "").strip()
    rating_stars = _format_user_rating(film.get("user_rating"))

    title_label = f"{title} ({year})" if year else title
    title_html = escape(title_label)

    if poster_url:
        media_html = f'<img src="{escape(poster_url, quote=True)}" alt="{title_html}" loading="lazy" />'
    else:
        media_html = '<div class="month-collage-placeholder">Pôster indisponível</div>'

    meta_html = ""
    if rating_stars != "Sem nota":
        meta_html = f'<div class="month-collage-stars">{rating_stars}</div>'

    card_html = f"""
    <div class="month-collage-card">
      {media_html}
      <div class="month-collage-overlay">
        <div class="month-collage-title">{title_html}</div>
        {meta_html}
      </div>
    </div>
    """

    if not letterboxd_url:
        return card_html

    safe_url = escape(letterboxd_url, quote=True)
    return (
        f'<a class="month-collage-link" href="{safe_url}" target="_blank" '
        f'rel="noopener noreferrer">{card_html}</a>'
    )


def render_month_collage(films: Sequence[dict]) -> None:
    st.markdown(
        """
        <style>
        .month-collage-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, 200px);
            gap: 10px;
            justify-content: start;
        }
        .month-collage-link {
            text-decoration: none;
            color: inherit;
        }
        .month-collage-card {
            position: relative;
            width: 200px;
            height: 300px;
            overflow: hidden;
            border-radius: 10px;
            background: #161616;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.22);
        }
        .month-collage-card img,
        .month-collage-placeholder {
            width: 200px;
            height: 300px;
            display: block;
            object-fit: cover;
        }
        .month-collage-placeholder {
            align-items: center;
            color: rgba(255, 255, 255, 0.8);
            display: flex;
            font-size: 0.85rem;
            justify-content: center;
            padding: 1rem;
            text-align: center;
        }
        .month-collage-overlay {
            position: absolute;
            inset: auto 0 0 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 4px;
            padding: 32px 12px 12px;
            color: #fff;
            text-align: center;
            background: linear-gradient(
                180deg,
                rgba(0, 0, 0, 0.0) 0%,
                rgba(0, 0, 0, 0.45) 38%,
                rgba(0, 0, 0, 0.86) 100%
            );
        }
        .month-collage-title {
            width: 100%;
            min-height: 2.5em;
            display: -webkit-box;
            -webkit-box-orient: vertical;
            -webkit-line-clamp: 2;
            overflow: hidden;
            font-size: 1rem;
            font-weight: 700;
            line-height: 1.25;
            margin: 0;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.65);
        }
        .month-collage-stars {
            color: #9fe870;
            font-size: 0.95rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            line-height: 1.2;
            margin: 0;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.65);
        }
        .month-collage-link:hover .month-collage-card {
            transform: translateY(-2px);
            transition: transform 120ms ease;
        }
        @media (max-width: 640px) {
            .month-collage-grid {
                grid-template-columns: repeat(auto-fill, 160px);
            }
            .month-collage-card,
            .month-collage-card img,
            .month-collage-placeholder {
                width: 160px;
                height: 240px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    cards_html = "".join(_build_card_html(film) for film in films)
    st.markdown(f'<div class="month-collage-grid">{cards_html}</div>', unsafe_allow_html=True)


def render_film_grid(films: Sequence[dict]) -> None:
    render_month_collage(films)
