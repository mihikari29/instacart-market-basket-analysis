"""Streamlit entry point for the Module 4D dashboard MVP."""

from __future__ import annotations

import streamlit as st

from src.module4.dashboard.ui.components import render_error
from src.module4.dashboard.ui.pages.analytics import render_analytics
from src.module4.dashboard.ui.pages.overview import render_overview
from src.module4.dashboard.ui.pages.recommendations import render_recommendations
from src.module4.dashboard.ui.state import clear_response_cache, consume_refresh_request, get_api_client


PAGES = ("Overview", "Realtime & Batch Analytics", "Hybrid Recommendations")


def main() -> None:
    st.set_page_config(
        page_title="Module 4 | Market Basket Dashboard",
        page_icon="🛒",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.sidebar.title("Module 4 Dashboard")
    st.sidebar.caption("Read-only views through the Module 4D API")
    if st.sidebar.button("Refresh dashboard data", key="refresh_dashboard_data"):
        clear_response_cache(st.session_state)
        st.rerun()
    page = st.sidebar.radio("Dashboard page", PAGES, key="ui_page")

    client, config_error = get_api_client(st.session_state)
    refresh_requested = consume_refresh_request(st.session_state)
    if config_error is not None:
        st.title(page)
        render_error(st, config_error, label="API configuration")
        st.caption("Set MODULE4D_API_BASE_URL to a localhost URL or the internal module4d-api service URL.")
        return

    if page == "Overview":
        render_overview(st, client)
    elif page == "Realtime & Batch Analytics":
        render_analytics(st, client)
    else:
        render_recommendations(st, client, refresh_requested=refresh_requested)


main()
