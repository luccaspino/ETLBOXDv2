from __future__ import annotations

try:
    import streamlit as st
except Exception as err:  # pragma: no cover
    raise RuntimeError(
        "Streamlit nao instalado. Adicione `streamlit` ao requirements para usar o dashboard."
    ) from err


st.set_page_config(page_title="Letterboxd Analytics", layout="wide")
st.title("Letterboxd Analytics Dashboard")
st.caption("Estrutura inicial do dashboard pronta. Proximo passo: conectar queries analiticas.")

st.info("Use `streamlit run src/dashboard/app.py` para abrir localmente.")
