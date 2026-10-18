"""Per-user read-only ALS, trend, and Hybrid recommendation comparison."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.module4.dashboard.ui.api_client import ApiClientError, validate_user_id
from src.module4.dashboard.ui.components import display_time, render_error, show_cached
from src.module4.dashboard.ui.state import fetch


ACTIVE_USER_KEY = "_module4d_active_recommendation_user"
_RETRY_BOTH_READS_KEY = "_module4d_retry_both_recommendation_reads"


def _load_hybrid(st: Any, client: Any, user_id: int, *, refresh: bool):
    with st.spinner(f"Reading verified Hybrid recommendations for user {user_id}…"):
        return fetch(st.session_state, client, "get_recommendations", user_id, refresh=refresh)


def _load_als(st: Any, client: Any, user_id: int, model_version: str | None, *, refresh: bool):
    with st.spinner(f"Reading stored ALS recommendations for user {user_id}…"):
        return fetch(
            st.session_state,
            client,
            "get_als_recommendations",
            user_id,
            model_version,
            20,
            refresh=refresh,
        )


def comparison_rows(als_payload: dict[str, Any] | None, hybrid_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Align persisted rows by product ID without filling unavailable scores."""
    by_product: dict[int, dict[str, Any]] = {}
    for row in (als_payload or {}).get("recommendations", []):
        by_product[row["product_id"]] = {
            "Product ID": row["product_id"],
            "Product": row["product_name"] or f"Product {row['product_id']}",
            "ALS rank": row["als_rank"],
            "ALS score": row["als_score"],
            "Trend score": None,
            "Final rank": None,
            "Final score": None,
        }
    for row in (hybrid_payload or {}).get("recommendations", []):
        item = by_product.setdefault(
            row["product_id"],
            {
                "Product ID": row["product_id"],
                "Product": row["product_name"],
                "ALS rank": None,
                "ALS score": None,
                "Trend score": None,
                "Final rank": None,
                "Final score": None,
            },
        )
        if row.get("product_name"):
            item["Product"] = row["product_name"]
        if row.get("from_trend") is True and row.get("trend_score") is not None:
            item["Trend score"] = row["trend_score"]
        item["Final rank"] = row["rank"]
        item["Final score"] = row["final_score"]
    return sorted(
        by_product.values(),
        key=lambda row: (
            row["Final rank"] if row["Final rank"] is not None else 21,
            row["ALS rank"] if row["ALS rank"] is not None else 21,
            row["Product ID"],
        ),
    )


def render_recommendations(st: Any, client: Any, *, refresh_requested: bool = False) -> None:
    st.title("ALS and Hybrid Recommendations")
    st.caption(
        "Reads the active versioned ALS snapshot and the published per-user Hybrid snapshot. "
        "The dashboard does not compute scores or publish recommendations."
    )

    retry_requested = bool(st.session_state.pop(_RETRY_BOTH_READS_KEY, False))
    active_user = st.session_state.get(ACTIVE_USER_KEY)
    initial_value = str(active_user) if isinstance(active_user, int) else "1"
    with st.form("recommendation_request"):
        raw_user_id = st.text_input(
            "Positive integer user ID",
            value=initial_value,
            help="Enter a positive signed 64-bit integer.",
            key="recommendation_user_id",
        )
        raw_model_version = st.text_input(
            "ALS model version (blank uses the active published model)",
            value="",
            key="recommendation_als_model_version",
        )
        submitted = st.form_submit_button(
            "Load / refresh ALS and Hybrid", type="primary", key="recommendation_submit"
        )

    user_id: int | None = None
    hybrid_payload = als_payload = None
    hybrid_error: ApiClientError | None = None
    als_error: ApiClientError | None = None
    hybrid_cached = als_cached = False
    model_version = raw_model_version.strip() or None
    if submitted or isinstance(active_user, int):
        try:
            user_id = validate_user_id(raw_user_id if submitted else active_user)
        except ApiClientError as validation_error:
            hybrid_error = validation_error
        if user_id is not None:
            st.session_state[ACTIVE_USER_KEY] = user_id
            hybrid_payload, hybrid_error, hybrid_cached = _load_hybrid(
                st, client, user_id, refresh=submitted or refresh_requested or retry_requested
            )
            als_payload, als_error, als_cached = _load_als(
                st, client, user_id, model_version, refresh=submitted or refresh_requested or retry_requested
            )
    else:
        st.info("Enter a user ID and submit to query the recommendation API.")
        return

    if hybrid_error:
        render_error(st, hybrid_error, label="Hybrid snapshot")
        if hybrid_error.code == "snapshot_changed":
            def request_retry_both_reads() -> None:
                st.session_state[_RETRY_BOTH_READS_KEY] = True

            st.button(
                "Retry both reads",
                key="retry_recommendation_read",
                on_click=request_retry_both_reads,
            )
    if als_error:
        render_error(st, als_error, label="ALS snapshot")

    if als_payload is not None:
        st.subheader("Stored ALS snapshot")
        st.caption(
            f"Model version: {als_payload['model_version']} · run: {als_payload['source']['run_id']} · "
            f"graph metadata version: {als_payload['source']['graph_version']}"
        )
        if als_payload["status"] == "empty":
            st.info(
                "No ALS recommendation rows exist for this user in the selected model. "
                "This does not establish whether the user exists in the source dataset."
            )
        elif als_payload["recommendations"]:
            st.dataframe(
                [
                    {
                        "ALS rank": row["als_rank"],
                        "Product": row["product_name"] or f"Product {row['product_id']}",
                        "Product ID": row["product_id"],
                        "ALS score": row["als_score"],
                    }
                    for row in als_payload["recommendations"]
                ],
                hide_index=True,
                width="stretch",
            )

    if hybrid_payload is not None:
        source = hybrid_payload["source"]
        config = hybrid_payload["ranking_config"]
        st.subheader("Published Hybrid snapshot")
        st.caption(f"Recommendation snapshot user ID: {hybrid_payload['user_id']}")
        metrics = st.columns(4, gap="large")
        metrics[0].metric("Serving version", hybrid_payload["serving_version"])
        metrics[1].metric("Recommendation count", hybrid_payload["recommendation_count"])
        metrics[2].metric("Mode", hybrid_payload["mode"])
        metrics[3].metric(
            "Lambda requested / effective",
            f"{config['lambda_requested']} / {config['lambda_effective']}",
        )
        if hybrid_payload["reason"]:
            st.info(f"Serving reason: {hybrid_payload['reason']}")

        st.markdown("**Snapshot provenance**")
        provenance_cols = st.columns(3, gap="large")
        with provenance_cols[0]:
            st.write(f"Source run: {source['source_run_id']}")
            st.write(f"Model version: {source['model_version']}")
            st.write(f"Graph version: {source['graph_version']}")
        with provenance_cols[1]:
            st.write(f"Trend namespace: {source['trend_source_namespace']}")
            st.write(f"Trend scope: {source['trend_source_scope'] or 'not recorded'}")
            st.write(
                f"Trend window: {display_time(source['trend_window_start'])} – "
                f"{display_time(source['trend_window_end'])}"
            )
        with provenance_cols[2]:
            st.write(f"Generated: {display_time(hybrid_payload['generated_at'])}")
            st.write(f"Pointer updated: {display_time(source['pointer_updated_at'])}")
            st.write(f"API observed: {display_time(hybrid_payload['observed_at'])}")

        if not hybrid_payload["recommendations"]:
            st.info("This is a ready published Hybrid snapshot with zero recommendations.")
        else:
            st.bar_chart(
                pd.DataFrame(
                    [
                        {
                            "Rank / product": f"#{row['rank']} · {row['product_name']} ({row['product_id']})",
                            "Final score (0–1)": row["final_score"],
                        }
                        for row in hybrid_payload["recommendations"]
                    ]
                ),
                x="Rank / product",
                y="Final score (0–1)",
                horizontal=True,
                sort=False,
                width="stretch",
                alt="Persisted final Hybrid recommendation score by rank.",
            )

    if als_payload is not None or hybrid_payload is not None:
        if als_payload and hybrid_payload:
            same_generation = (
                als_payload["source"]["model_version"] == hybrid_payload["source"]["model_version"]
                and als_payload["source"]["run_id"] == hybrid_payload["source"]["source_run_id"]
            )
            if not same_generation:
                st.warning("The ALS and Hybrid snapshots refer to different model generations.")
        st.subheader("ALS, trend, and final score comparison")
        st.dataframe(
            comparison_rows(als_payload, hybrid_payload),
            hide_index=True,
            width="stretch",
        )
        st.caption(
            "ALS score comes from user_recommendations. Trend and final scores come from the stored "
            "Hybrid snapshot and are blank when that source row is absent. No score is recomputed."
        )
    show_cached(st, hybrid_cached and als_cached)
