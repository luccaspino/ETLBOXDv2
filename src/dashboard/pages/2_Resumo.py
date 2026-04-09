from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pandas as pd
import streamlit as st

from src.dashboard.api_client import (
    ApiClientError,
    get_filtered_films,
    get_summary_bundle,
)
from src.dashboard.branding import configure_page, render_sidebar_nav
from src.dashboard.components.collages import (
    extract_month_from_date,
    month_label,
    render_month_collage,
)
from src.dashboard.components.messages import render_api_error, render_empty_state
from src.dashboard.components.summary import (
    aggregate_logs_by_month,
    aggregate_logs_by_year,
    aggregate_name_counts,
    aggregate_rating_distribution,
    build_logged_films_dataframe,
    build_month_selection_chart,
    build_rating_selection_chart,
    build_year_selection_chart,
    compute_summary_metrics,
    extract_selected_month,
    extract_selected_rating,
    extract_selected_year,
    filter_logged_films,
    format_rating_label,
)
from src.dashboard.components.tables import render_records_table
from src.dashboard.state import get_active_username, initialize_state


def _coerce_selected_int(value: object, *, minimum: int, maximum: int) -> int | None:
    try:
        integer_value = int(value)
    except (TypeError, ValueError):
        return None
    if minimum <= integer_value <= maximum:
        return integer_value
    return None


def _coerce_selected_rating(value: object) -> float | None:
    try:
        rating_value = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    if 0.5 <= rating_value <= 5.0:
        return rating_value
    return None


def _describe_active_filters(
    selected_month: int | None,
    selected_year: int | None,
    selected_rating: float | None,
) -> str:
    active_filters: list[str] = []
    if selected_month is not None:
        active_filters.append(f"mes {month_label(selected_month)}")
    if selected_year is not None:
        active_filters.append(f"ano {selected_year}")
    if selected_rating is not None:
        active_filters.append(f"nota {format_rating_label(selected_rating)}")
    return ", ".join(active_filters)


def _dataframe_to_records(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []

    normalized = df.copy()
    if "watched_date" in normalized.columns:
        normalized["watched_date"] = normalized["watched_date"].dt.strftime("%Y-%m-%d")
    normalized = normalized.where(pd.notna(normalized), None)
    return normalized.to_dict("records")


configure_page("ETLboxd | Resumo")
initialize_state()

username = get_active_username()

st.title("Resumo")
st.caption("KPIs principais, histórico de atividade e distribuições do usuário ativo.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Selecione um usuario",
        "Abra o Menu e escolha um usuario antes de consultar o resumo.",
    )
    st.stop()

st.session_state.setdefault("summary_month_filter", None)
st.session_state.setdefault("summary_year_filter", None)
st.session_state.setdefault("summary_rating_filter", None)
st.session_state.setdefault("summary_crossfilter_chart_nonce", 0)

selected_month = _coerce_selected_int(
    st.session_state.get("summary_month_filter"),
    minimum=1,
    maximum=12,
)
selected_year = _coerce_selected_int(
    st.session_state.get("summary_year_filter"),
    minimum=1880,
    maximum=2100,
)
selected_rating = _coerce_selected_rating(st.session_state.get("summary_rating_filter"))

try:
    with st.spinner("Carregando resumo analitico..."):
        summary_bundle = get_summary_bundle(username)
        main_kpis = summary_bundle["main_kpis"]
        rating_gap = summary_bundle["rating_gap"]
        release_year = summary_bundle["release_year"]
        monthly_logs = summary_bundle["monthly_logs"]
        yearly_logs = summary_bundle["yearly_logs"]
        rating_distribution_rows = summary_bundle["rating_distribution_rows"]
        country_distribution = summary_bundle["country_distribution"]
        genre_distribution = summary_bundle["genre_distribution"]
        logged_film_rows = summary_bundle["logged_film_rows"]
        logged_films_supported = bool(summary_bundle["logged_films_supported"])
except ApiClientError as err:
    render_api_error(err)
    st.stop()

if not logged_films_supported and any(
    value is not None for value in (selected_month, selected_year, selected_rating)
):
    st.session_state["summary_month_filter"] = None
    st.session_state["summary_year_filter"] = None
    st.session_state["summary_rating_filter"] = None
    selected_month = None
    selected_year = None
    selected_rating = None

active_filters_label = _describe_active_filters(selected_month, selected_year, selected_rating)

if logged_films_supported and active_filters_label:
    filter_col, action_col = st.columns([4, 1])
    with filter_col:
        st.info(
            f"Filtros cruzados ativos: {active_filters_label}. "
            "Os indicadores e distribuicoes abaixo usam a intersecao desses recortes."
        )
    with action_col:
        if st.button("Limpar filtros", width="stretch"):
            st.session_state["summary_month_filter"] = None
            st.session_state["summary_year_filter"] = None
            st.session_state["summary_rating_filter"] = None
            st.session_state["summary_crossfilter_chart_nonce"] += 1
            st.rerun()
elif not logged_films_supported:
    st.info(
        "Modo de compatibilidade ativo: o backend atual ainda nao publicou os logs detalhados, "
        "entao os totais continuam corretos e a colagem usa o comportamento anterior. "
        "O filtro cruzado por clique nos graficos sera habilitado quando a rota nova estiver disponivel."
    )

logged_films_df = build_logged_films_dataframe(logged_film_rows) if logged_films_supported else pd.DataFrame()

if logged_films_supported:
    filtered_logs_df = filter_logged_films(
        logged_films_df,
        month=selected_month,
        year=selected_year,
        rating=selected_rating,
    )
    metrics = compute_summary_metrics(filtered_logs_df)
    monthly_df = aggregate_logs_by_month(
        filter_logged_films(
            logged_films_df,
            month=selected_month,
            year=selected_year,
            rating=selected_rating,
            exclude={"month"},
        )
    )
    yearly_df = aggregate_logs_by_year(
        filter_logged_films(
            logged_films_df,
            month=selected_month,
            year=selected_year,
            rating=selected_rating,
            exclude={"year"},
        )
    )
    rating_df = aggregate_rating_distribution(
        filter_logged_films(
            logged_films_df,
            month=selected_month,
            year=selected_year,
            rating=selected_rating,
            exclude={"rating"},
        )
    )
    country_distribution = aggregate_name_counts(
        filtered_logs_df,
        list_column="countries_list",
        output_label="country_name",
    )
    genre_distribution = aggregate_name_counts(
        filtered_logs_df,
        list_column="genres_list",
        output_label="genero",
    )
else:
    metrics = {
        "total_filmes": main_kpis.get("total_filmes", 0),
        "media_nota_pessoal": main_kpis.get("media_nota_pessoal"),
        "total_horas": main_kpis.get("total_horas", 0.0),
        "diferenca_media": rating_gap.get("diferenca_media"),
        "media_letterboxd": rating_gap.get("media_letterboxd"),
        "ano_medio_lancamento": release_year.get("ano_medio_lancamento"),
    }
    yearly_df = pd.DataFrame(yearly_logs).sort_values("ano") if yearly_logs else pd.DataFrame()
    rating_df = pd.DataFrame(rating_distribution_rows)
    if not rating_df.empty:
        rating_df = rating_df.sort_values("rating").reset_index(drop=True)

metrics_row = st.columns(6)
metrics_row[0].metric("Total de filmes", metrics.get("total_filmes", 0))
metrics_row[1].metric("Media pessoal", metrics.get("media_nota_pessoal"))
metrics_row[2].metric("Total de horas", metrics.get("total_horas", 0.0))
metrics_row[3].metric("Diferenca media", metrics.get("diferenca_media"))
metrics_row[4].metric("Media Letterboxd", metrics.get("media_letterboxd"))
metrics_row[5].metric("Ano medio", metrics.get("ano_medio_lancamento"))

charts_col1, charts_col2 = st.columns(2)

with charts_col1:
    st.subheader("Logs por mes")
    if logged_films_supported:
        monthly_chart = build_month_selection_chart(monthly_df, selected_month)
        month_chart_event = st.altair_chart(
            monthly_chart,
            width="stretch",
            key=f"summary-month-chart:{username}:{st.session_state['summary_crossfilter_chart_nonce']}",
            on_select="rerun",
            selection_mode="month_select",
        )
        event_month = extract_selected_month(month_chart_event)
        if event_month != selected_month:
            st.session_state["summary_month_filter"] = event_month
            st.rerun()

        if selected_month is None and not active_filters_label:
            st.caption("Clique nas barras de mes, ano ou nota para aplicar o filtro cruzado na pagina.")
        elif selected_month is None:
            st.caption("Clique em um mes para adicionar esse recorte ao filtro cruzado atual.")
        else:
            st.caption("Duplo clique neste grafico remove apenas o filtro de mes.")
    else:
        monthly_df = pd.DataFrame(monthly_logs)
        if monthly_df.empty:
            render_empty_state("Sem dados mensais", "Nenhum log mensal foi encontrado para este usuario.")
        else:
            monthly_df = monthly_df.sort_values("mes")
            st.bar_chart(monthly_df.set_index("mes"))

with charts_col2:
    st.subheader("Logs por ano")
    if yearly_df.empty:
        render_empty_state("Sem dados anuais", "Nenhum log anual foi encontrado para este usuario.")
    elif logged_films_supported:
        yearly_chart = build_year_selection_chart(yearly_df, selected_year)
        year_chart_event = st.altair_chart(
            yearly_chart,
            width="stretch",
            key=f"summary-year-chart:{username}:{st.session_state['summary_crossfilter_chart_nonce']}",
            on_select="rerun",
            selection_mode="year_select",
        )
        event_year = extract_selected_year(year_chart_event)
        if event_year != selected_year:
            st.session_state["summary_year_filter"] = event_year
            st.rerun()

        if selected_year is None:
            st.caption("Clique em um ano para adicionar esse recorte ao filtro cruzado atual.")
        else:
            st.caption("Duplo clique neste grafico remove apenas o filtro de ano.")
    else:
        st.bar_chart(yearly_df.set_index("ano"))

distribution_row = st.columns(3)

with distribution_row[0]:
    st.subheader("Distribuicao de notas")
    if rating_df.empty:
        render_empty_state("Sem distribuicao", "Nao ha distribuicao de notas disponivel para este recorte.")
    else:
        if logged_films_supported:
            rating_chart = build_rating_selection_chart(rating_df, selected_rating)
            rating_chart_event = st.altair_chart(
                rating_chart,
                width="stretch",
                key=f"summary-rating-chart:{username}:{st.session_state['summary_crossfilter_chart_nonce']}",
                on_select="rerun",
                selection_mode="rating_select",
            )
            event_rating = extract_selected_rating(rating_chart_event)
            if event_rating != selected_rating:
                st.session_state["summary_rating_filter"] = event_rating
                st.rerun()

            if selected_rating is None:
                st.caption("Clique em uma nota para adicionar esse recorte ao filtro cruzado atual.")
            else:
                st.caption("Duplo clique neste grafico remove apenas o filtro de nota.")
        else:
            st.bar_chart(rating_df.set_index("rating"))
        render_records_table(rating_df.to_dict("records"))

with distribution_row[1]:
    st.subheader("Top paises")
    if not country_distribution:
        render_empty_state("Sem paises", "Nao ha paises disponiveis para este recorte.")
    else:
        render_records_table(country_distribution[:10])

with distribution_row[2]:
    st.subheader("Top generos")
    if not genre_distribution:
        render_empty_state("Sem generos", "Nao ha generos disponiveis para este recorte.")
    else:
        render_records_table(genre_distribution[:10])

st.markdown("---")
st.subheader("Colagem mensal")
st.caption("Posteres com tamanho fixo e legenda sobreposta para um mes assistido.")

if logged_films_supported:
    collage_source_df = filter_logged_films(logged_films_df, rating=selected_rating)
    available_years = sorted(
        [int(item) for item in collage_source_df["watched_year"].dropna().astype(int).unique().tolist()],
        reverse=True,
    )
else:
    available_years = sorted(
        [int(item["ano"]) for item in yearly_logs if item.get("ano") is not None],
        reverse=True,
    )

if not available_years:
    render_empty_state("Sem meses disponiveis", "Nao ha filmes com data assistida para montar a colagem.")
else:
    collage_control_col1, collage_control_col2 = st.columns([1, 1])
    with collage_control_col1:
        if logged_films_supported and selected_year is not None:
            selected_collage_year = selected_year
            st.metric("Ano da colagem", selected_collage_year)
        else:
            selected_collage_year = st.selectbox(
                "Ano da colagem",
                options=available_years,
                key="month-collage-year",
            )

    if logged_films_supported:
        year_film_df = collage_source_df[collage_source_df["watched_year"] == selected_collage_year].copy()
    else:
        try:
            with st.spinner("Buscando filmes do ano selecionado..."):
                year_film_rows = get_filtered_films(username, watched_year=selected_collage_year)
        except ApiClientError as err:
            render_api_error(err, message="Ocorreu um erro ao carregar a colagem mensal.")
            year_film_rows = None

    if logged_films_supported:
        available_months = sorted(
            [int(item) for item in year_film_df["watched_month"].dropna().astype(int).unique().tolist()]
        )
    else:
        available_months = (
            sorted(
                {
                    month_number
                    for row in year_film_rows
                    if (month_number := extract_month_from_date(row.get("watched_date"))) is not None
                }
            )
            if year_film_rows is not None
            else []
        )

    if not available_months:
        render_empty_state(
            "Sem filmes nesse ano",
            "Nenhum filme com data assistida valida foi encontrado para o ano selecionado.",
        )
    else:
        if logged_films_supported and selected_month is not None:
            selected_collage_month = selected_month
            with collage_control_col2:
                st.metric("Mes da colagem", month_label(selected_collage_month))
        else:
            with collage_control_col2:
                selected_collage_month = st.selectbox(
                    "Mes da colagem",
                    options=available_months,
                    format_func=month_label,
                    key=f"month-collage-month-{selected_collage_year}",
                )

        if logged_films_supported:
            month_collage_rows = _dataframe_to_records(
                year_film_df[year_film_df["watched_month"] == selected_collage_month].copy()
            )
            if active_filters_label:
                st.caption(
                    "A colagem acompanha os filtros cruzados ativos; os seletores acima controlam apenas "
                    "dimensoes ainda nao fixadas por clique."
                )
        else:
            month_collage_rows = [
                row
                for row in year_film_rows
                if extract_month_from_date(row.get("watched_date")) == selected_collage_month
            ]

        if not month_collage_rows:
            if logged_films_supported and selected_month is not None:
                render_empty_state(
                    "Sem filmes nesse mes",
                    f"Nao ha posteres para {month_label(selected_month)} em {selected_collage_year}.",
                )
            else:
                render_empty_state(
                    "Sem filmes nesse mes",
                    "Nao ha posteres disponiveis para o mes selecionado.",
                )
        else:
            st.caption(
                f"{len(month_collage_rows)} filmes em {month_label(selected_collage_month)} de {selected_collage_year}."
            )
            render_month_collage(month_collage_rows)
