"""Per-user verified Hybrid recommendation view."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.module4.dashboard.ui.api_client import ApiClientError, validate_user_id
from src.module4.dashboard.ui.components import display_time, render_error, show_cached
from src.module4.dashboard.ui.state import fetch


ACTIVE_USER_KEY = "_module4d_active_recommendation_user"
LAST_ERROR_KEY = "_module4d_last_recommendation_error"


def _load(st: Any, client: Any, user_id: int, *, refresh: bool):
    with st.spinner(f"Reading verified Hybrid recommendations for user {user_id}…"):
        return fetch(st.session_state, client, "get_recommendations", user_id, refresh=refresh)


def render_recommendations(st: Any, client: Any, *, refresh_requested: bool = False) -> None:
    st.title("Hybrid Recommendations")
    st.caption("Verified per-user Hybrid snapshot from the serving API. The dashboard does not recompute scores or publish recommendations.")

    active_user = st.session_state.get(ACTIVE_USER_KEY)
    initial_value = str(active_user) if isinstance(active_user, int) else "1"
    with st.form("recommendation_request"):
        raw_user_id = st.text_input(
            "Positive integer user ID",
            value=initial_value,
            help="Enter a positive signed 64-bit integer. The API decides whether a published snapshot exists.",
            key="recommendation_user_id",
        )
        submitted = st.form_submit_button(
            "Load / refresh recommendations", type="primary", key="recommendation_submit"
        )

    payload = None
    error: ApiClientError | None = None
    cached = False
    user_id: int | None = None
    if submitted:
        try:
            user_id = validate_user_id(raw_user_id)
        except ApiClientError as validation_error:
            error = validation_error
        else:
            payload, error, cached = _load(st, client, user_id, refresh=True)
            if error is None:
                st.session_state[ACTIVE_USER_KEY] = user_id
                st.session_state.pop(LAST_ERROR_KEY, None)
            else:
                st.session_state[LAST_ERROR_KEY] = {
                    "user_id": user_id,
                    "code": error.code,
                    "status_code": error.status_code,
                    "request_id": error.request_id,
                    "message": error.message,
                }
    elif st.session_state.get(LAST_ERROR_KEY):
        last_error = st.session_state[LAST_ERROR_KEY]
        try:
            current_user_id = validate_user_id(raw_user_id)
        except ApiClientError:
            current_user_id = None
        if last_error.get("user_id") == current_user_id:
            error = ApiClientError(
                last_error["message"],
                status_code=last_error["status_code"],
                code=last_error["code"],
                request_id=last_error["request_id"],
            )
    if not submitted and error is None and isinstance(active_user, int):
        user_id = active_user
        payload, error, cached = _load(st, client, active_user, refresh=refresh_requested)

    if error:
        render_error(st, error, label="Recommendation snapshot")
        if error.code == "snapshot_changed":
            retry_col, note_col = st.columns([1, 3])
            with retry_col:
                retry = st.button("Retry this read", key="retry_recommendation_read")
            with note_col:
                st.caption("This retries the same user ID once after a new explicit action.")
            if retry:
                retry_user = user_id or error_user_id(st.session_state)
                if retry_user is not None:
                    payload, error, cached = _load(st, client, retry_user, refresh=True)
                    if error is None:
                        st.session_state[ACTIVE_USER_KEY] = retry_user
                        st.session_state.pop(LAST_ERROR_KEY, None)
                        user_id = retry_user
    elif payload is None:
        st.info("Enter a user ID and submit to query the recommendation API.")

    if error:
        return
    if payload is None:
        return

    source = payload["source"]
    config = payload["ranking_config"]
    st.caption(f"Recommendation snapshot user ID: {payload['user_id']}")
    metrics = st.columns(4, gap="large")
    metrics[0].metric("Serving version", payload["serving_version"])
    metrics[1].metric("Recommendation count", payload["recommendation_count"])
    metrics[2].metric("Mode", payload["mode"])
    metrics[3].metric("Lambda requested / effective", f"{config['lambda_requested']} / {config['lambda_effective']}")
    if payload["reason"]:
        st.info(f"Serving reason: {payload['reason']}")

    st.subheader("Snapshot provenance")
    provenance_cols = st.columns(3, gap="large")
    with provenance_cols[0]:
        st.markdown("**Serving sources**")
        st.write(f"Source run: {source['source_run_id']}")
        st.write(f"Model version: {source['model_version']}")
        st.write(f"Graph version: {source['graph_version']}")
    with provenance_cols[1]:
        st.markdown("**Trend input**")
        st.write(f"Namespace: {source['trend_source_namespace']}")
        st.write(f"Scope: {source['trend_source_scope'] or 'not recorded'}")
        st.write(f"Window: {display_time(source['trend_window_start'])} – {display_time(source['trend_window_end'])}")
        st.write(f"As-of event time: {display_time(source['as_of_event_time'])}")
    with provenance_cols[2]:
        st.markdown("**Snapshot times and ranking**")
        st.write(f"Generated: {display_time(payload['generated_at'])}")
        st.write(f"Pointer updated: {display_time(source['pointer_updated_at'])}")
        st.write(f"API observed: {display_time(payload['observed_at'])}")
        st.write(f"Lambda requested: {config['lambda_requested']} · effective: {config['lambda_effective']}")

    recommendations = payload["recommendations"]
    if not recommendations:
        st.info("This is a ready published snapshot with zero recommendations.")
    else:
        chart = pd.DataFrame(
            [{"Rank / product": f"#{row['rank']} · {row['product_name']} ({row['product_id']})", "Final score (0–1)": row["final_score"]} for row in recommendations]
        )
        st.bar_chart(
            chart,
            x="Rank / product",
            y="Final score (0–1)",
            horizontal=True,
            sort=False,
            width="stretch",
            alt="Persisted final Hybrid recommendation score by rank; scores range from zero to one.",
        )
        st.dataframe(
            [
                {
                    "Rank": row["rank"],
                    "Product": row["product_name"],
                    "Product ID": row["product_id"],
                    "Raw ALS score": row["als_score"],
                    "Raw trend score": row["trend_score"],
                    "Normalized ALS score": row["normalized_als_score"],
                    "Normalized trend score": row["normalized_trend_score"],
                    "Final score": row["final_score"],
                    "From ALS": row["from_als"],
                    "From trend": row["from_trend"],
                }
                for row in recommendations
            ],
            hide_index=True,
            width="stretch",
        )
        st.caption(
            f"Chart shows only persisted final_score (0–1). Raw ALS/trend values and normalized components are separate table columns; no score is recomputed. "
            f"Source: final_recommendations · Serving version: {payload['serving_version']} · Snapshot observed: {display_time(payload['observed_at'])}"
        )
    show_cached(st, cached)
    st.info(
        "Direct ALS-versus-Hybrid comparison is unavailable: the current API exposes verified Hybrid snapshots but no independently verified per-user ALS comparison endpoint. "
        "A separate backend endpoint and milestone are required."
    )


def error_user_id(session_state: Any) -> int | None:
    value = session_state.get(LAST_ERROR_KEY, {}).get("user_id")
    return value if isinstance(value, int) else None
