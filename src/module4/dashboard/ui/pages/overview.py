"""Overview of API liveness and independently observed data sources."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.module4.dashboard.ui.components import display_time, render_error, render_source_status, show_cached
from src.module4.dashboard.ui.state import configured_trend_namespaces, fetch


def _load(st: Any, client: Any, method: str, *args: Any, **kwargs: Any):
    with st.spinner("Reading the configured API source…"):
        return fetch(st.session_state, client, method, *args, **kwargs)


def _source_detail(source: dict[str, Any]) -> str:
    run_id = source.get("run_id") or "not recorded"
    published = display_time(source.get("last_batch_time"))
    return f"Run: {run_id} · Last batch publication: {published}"


def render_overview(st: Any, client: Any) -> None:
    st.title("Overview")
    st.caption("Independent source checks through the read-only Module 4D API. Each observation belongs to its own request.")

    architecture = st.columns(4, gap="large")
    cards = (
        ("Batch layer", "Prior-only product and department metrics."),
        ("Speed layer", "A selected finalized event-time trend window."),
        ("Hybrid serving", "Per-user verified recommendation snapshots."),
        ("Visualization", "Read-only API requests; no publisher calls."),
    )
    for column, (title, detail) in zip(architecture, cards, strict=True):
        with column:
            st.markdown(f"**{title}**")
            st.caption(detail)

    now = datetime.now(timezone.utc)
    health, health_error, health_cached = _load(st, client, "get_health")
    products, products_error, products_cached = _load(st, client, "get_batch_products", "purchase_count", 1)
    departments, departments_error, departments_cached = _load(st, client, "get_batch_departments")
    namespace = _overview_namespace()
    trends, trends_error, trends_cached = _load(
        st,
        client,
        "get_trends",
        namespace,
        now,
    ) if namespace else (None, None, False)
    evidence, evidence_error, evidence_cached = _load(st, client, "get_evidence_registry")

    st.subheader("API connectivity")
    if health_error:
        render_source_status(st, "FastAPI process liveness", state="error", details="This check does not test MongoDB or source availability.")
        render_error(st, health_error, label="API liveness")
    elif health and health.get("status") == "ok":
        render_source_status(st, "FastAPI process liveness", state="available", details="/health reports process liveness only; dependent data sources are checked separately.")
        show_cached(st, health_cached)
    else:
        render_source_status(st, "FastAPI process liveness", state="unavailable", details="No successful liveness response was received.")

    st.subheader("Data availability")
    product_col, department_col, trend_col = st.columns(3, gap="large")
    with product_col:
        if products_error:
            render_source_status(st, "Batch products", state="error", details="Source: batch_product_metrics · prior-only item purchases")
            render_error(st, products_error, label="Batch products")
        elif products:
            source = products["source"]
            state = "available" if products["items"] else "empty"
            render_source_status(st, "Batch products", state=state, details=_source_detail(source))
            st.caption(f"Source: {source['collection']} · Scope: {source['scope']} · Observed: {display_time(products['observed_at'])}")
            show_cached(st, products_cached)
    with department_col:
        if departments_error:
            render_source_status(st, "Batch departments", state="error", details="Source: department_metrics · prior-only item purchases")
            render_error(st, departments_error, label="Batch departments")
        elif departments:
            source = departments["source"]
            state = "available" if departments["items"] else "empty"
            render_source_status(st, "Batch departments", state=state, details=_source_detail(source))
            st.caption(f"Source: {source['collection']} · Scope: {source['scope']} · Observed: {display_time(departments['observed_at'])}")
            show_cached(st, departments_cached)
    with trend_col:
        if not namespace:
            render_source_status(st, "Finalized trends", state="unavailable", details="Set MODULE4D_TREND_SOURCE_NAMESPACES in the UI environment to a backend-configured namespace.")
        elif trends_error:
            render_source_status(st, "Finalized trends", state="error", details="Source: realtime_trending_windows · one finalized event-time window")
            render_error(st, trends_error, label="Finalized trends")
        elif trends:
            state = "available" if trends["available"] else "unavailable"
            render_source_status(
                st,
                "Finalized trends",
                state=state,
                details=f"Status: {trends['status']} · Namespace: {trends['source_namespace']} · Scope: {trends['source_scope'] or 'not recorded'}",
            )
            st.caption(
                f"Window: {display_time(trends['window_start'])} – {display_time(trends['window_end'])} · "
                f"Published: {display_time(trends['published_at'])} · Observed: {display_time(trends['observed_at'])}"
            )
            if trends.get("reason"):
                st.caption(f"Unavailable reason: {trends['reason']}")
            show_cached(st, trends_cached)

    st.subheader("Historical evidence registry")
    if evidence_error:
        render_error(st, evidence_error, label="Evidence registry")
    elif evidence:
        rows = []
        for item in evidence["items"]:
            rows.append(
                {
                    "Evidence": item["title"],
                    "Availability": item["status"],
                    "Classification": item["classification"],
                    "Source": item["source_label"],
                    "SHA-256": item["sha256"] or "—",
                }
            )
        if rows:
            st.dataframe(rows, hide_index=True, width="stretch")
        st.caption(f"Static registry response observed at {display_time(evidence['observed_at'])}. Archived reports are historical evidence; C8B is not live Kafka ingestion.")
        show_cached(st, evidence_cached)


def _overview_namespace() -> str:
    values = configured_trend_namespaces()
    return values[0] if values else ""
