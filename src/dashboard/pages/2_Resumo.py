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
    get_country_distribution,
    get_filtered_films,
    get_genre_distribution,
    get_logged_films,
    get_main_kpis,
    get_monthly_logs,
    get_rating_distribution,
    get_rating_gap_kpis,
    get_release_year_kpi,
    get_yearly_logs,
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
    compute_summary_metrics,
    extract_selected_month,
)
from src.dashboard.components.tables import render_records_table
from src.dashboard.state import get_active_username, initialize_state


def _extract_year_from_date(date_text: str | None) -> int | None:
    if not date_text or len(date_text) < 4:
        return None
    try:
        return int(date_text[:4])
    except ValueError:
        return None


configure_page("ETLboxd | Resumo")
initialize_state()

username = get_active_username()

st.title("Resumo")
st.caption("KPIs principais, historico de atividade e distribuicoes do usuario ativo.")

with st.sidebar:
    render_sidebar_nav(username)

if not username:
    render_empty_state(
        "Selecione um usuario",
        "Abra o Menu e escolha um usuario antes de consultar o resumo.",
    )
    st.stop()

st.session_state.setdefault("summary_month_filter", None)
st.session_state.setdefault("summary_month_chart_nonce", 0)

selected_month = st.session_state.get("summary_month_filter")
if not isinstance(selected_month, int) or not (1 <= selected_month <= 12):
    selected_month = None

try:
    with st.spinner("Carregando resumo analitico..."):
        main_kpis = get_main_kpis(username)
        rating_gap = get_rating_gap_kpis(username)
        release_year = get_release_year_kpi(username)
        monthly_logs = get_monthly_logs(username)
        yearly_logs = get_yearly_logs(username)
        rating_distribution_rows = get_rating_distribution(username)
        country_distribution = get_country_distribution(username)
        genre_distribution = get_genre_distribution(username)

        try:
            logged_film_rows = get_logged_films(username, allow_legacy_fallback=False)
            logged_films_supported = True
        except ApiClientError as err:
            if err.status_code == 404:
                logged_film_rows = []
                logged_films_supported = False
            else:
                raise
except ApiClientError as err:
    render_api_error(err)
    st.stop()

if not logged_films_supported and selected_month is not None:
    st.session_state["summary_month_filter"] = None
    selected_month = None

if logged_films_supported and selected_month is not None:
    filter_col, action_col = st.columns([4, 1])
    with filter_col:
        st.info(
            f"Filtro cruzado ativo: somente logs de {month_label(selected_month)} alimentam os indicadores abaixo."
        )
    with action_col:
        if st.button("Limpar filtro", width="stretch"):
            st.session_state["summary_month_filter"] = None
            st.session_state["summary_month_chart_nonce"] += 1
            st.rerun()
elif not logged_films_supported:
    st.info(
        "Modo de compatibilidade ativo: o backend atual ainda nao publicou os logs detalhados, "
        "entao os totais continuam corretos e a colagem usa o comportamento anterior. "
        "O filtro cruzado por clique no grafico sera habilitado quando a rota nova estiver disponivel."
    )

logged_films_df = build_logged_films_dataframe(logged_film_rows) if logged_films_supported else pd.DataFrame()

if logged_films_supported and selected_month is not None:
    filtered_logs_df = logged_films_df[logged_films_df["watched_month"] == selected_month].copy()
    metrics = compute_summary_metrics(filtered_logs_df)
    yearly_df = aggregate_logs_by_year(filtered_logs_df)
    rating_df = aggregate_rating_distribution(filtered_logs_df)
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
        monthly_df = aggregate_logs_by_month(logged_films_df)
        monthly_chart = build_month_selection_chart(monthly_df, selected_month)
        month_chart_event = st.altair_chart(
            monthly_chart,
            width="stretch",
            key=f"summary-month-chart:{username}:{st.session_state['summary_month_chart_nonce']}",
            on_select="rerun",
            selection_mode="month_select",
        )
        event_month = extract_selected_month(month_chart_event)
        if event_month != selected_month:
            st.session_state["summary_month_filter"] = event_month
            st.rerun()

        if selected_month is None:
            st.caption("Clique em um mes para aplicar o filtro cruzado no restante da pagina.")
        else:
            st.caption("Duplo clique no grafico ou use 'Limpar filtro' para voltar ao total.")
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
    else:
        st.bar_chart(yearly_df.set_index("ano"))
        if logged_films_supported and selected_month is not None:
            st.caption(f"Contagens anuais considerando apenas {month_label(selected_month)}.")

distribution_row = st.columns(3)

with distribution_row[0]:
    st.subheader("Distribuicao de notas")
    if rating_df.empty:
        render_empty_state("Sem distribuicao", "Nao ha distribuicao de notas disponivel para este recorte.")
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
    available_years = sorted(
        [int(item) for item in logged_films_df["watched_year"].dropna().astype(int).unique().tolist()],
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
        selected_collage_year = st.selectbox(
            "Ano da colagem",
            options=available_years,
            key="month-collage-year",
        )

    if logged_films_supported:
        year_film_rows = [
            row
            for row in logged_film_rows
            if _extract_year_from_date(row.get("watched_date")) == selected_collage_year
        ]
    else:
        try:
            with st.spinner("Buscando filmes do ano selecionado..."):
                year_film_rows = get_filtered_films(username, watched_year=selected_collage_year)
        except ApiClientError as err:
            render_api_error(err, message="Ocorreu um erro ao carregar a colagem mensal.")
            year_film_rows = None

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
            st.caption("A colagem esta sincronizada com o filtro cruzado aplicado no grafico mensal.")
        else:
            with collage_control_col2:
                selected_collage_month = st.selectbox(
                    "Mes da colagem",
                    options=available_months,
                    format_func=month_label,
                    key=f"month-collage-month-{selected_collage_year}",
                )

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
