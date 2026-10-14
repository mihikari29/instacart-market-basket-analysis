"""Finalized trend, batch metric, and archived order distribution views."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.module4.dashboard.ui.api_client import ApiClientError, validate_as_of_event_time, validate_trend_namespace
from src.module4.dashboard.ui.components import display_time, render_error, show_cached
from src.module4.dashboard.ui.state import configured_trend_namespaces, fetch


def _error_record(error: ApiClientError | None) -> dict[str, Any] | None:
    if error is None:
        return None
    return {
        "message": error.message,
        "status_code": error.status_code,
        "code": error.code,
        "request_id": error.request_id,
    }


def _restore_error(value: dict[str, Any] | None) -> ApiClientError | None:
    if not isinstance(value, dict):
        return None
    return ApiClientError(
        value["message"],
        status_code=value["status_code"],
        code=value["code"],
        request_id=value["request_id"],
    )


def render_analytics(st: Any, client: Any) -> None:
    st.title("Realtime & Batch Analytics")
    st.caption("Finalized trends and prior-only purchase metrics are separate sources. Order distributions count all orders.")

    namespaces = configured_trend_namespaces()
    default_now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    with st.form("analytics_request"):
        if namespaces:
            namespace = st.selectbox("Configured trend source namespace", namespaces, key="analytics_namespace")
        else:
            namespace = ""
            st.warning("No trend namespace is configured in MODULE4D_TREND_SOURCE_NAMESPACES.")
        input_col, age_col = st.columns(2)
        with input_col:
            as_of_day = st.date_input("As-of event-time date (UTC)", value=default_now.date(), key="analytics_as_of_day")
            as_of_clock = st.time_input("As-of event-time (UTC)", value=default_now.time(), key="analytics_as_of_clock")
        with age_col:
            enforce_age = st.checkbox("Apply a maximum event age", value=False, key="analytics_enforce_age")
            max_age = st.number_input(
                "Maximum event age (seconds)",
                min_value=0,
                max_value=31_536_000,
                value=86_400,
                step=300,
                disabled=not enforce_age,
                key="analytics_max_age",
            )
        product_col, limit_col = st.columns(2)
        with product_col:
            order = st.selectbox("Batch product ranking", ("purchase_count", "reorder_rate"), key="analytics_order")
        with limit_col:
            limit = st.number_input("Product rows", min_value=1, max_value=50, value=20, step=1, key="analytics_limit")
        submitted = st.form_submit_button("Load / refresh analytics", type="primary", key="analytics_submit")

    payloads: dict[str, dict[str, Any] | None] = {"trends": None, "products": None, "departments": None, "orders": None}
    errors: dict[str, ApiClientError | None] = {key: None for key in payloads}
    cached: dict[str, bool] = {key: False for key in payloads}
    if submitted:
        as_of = datetime.combine(as_of_day, as_of_clock, tzinfo=timezone.utc)
        as_of_event_time = validate_as_of_event_time(as_of)
        if namespace:
            try:
                validate_trend_namespace(namespace, namespaces)
            except ApiClientError as error:
                errors["trends"] = error
            else:
                trend_args: tuple[Any, ...] = (namespace, as_of, int(max_age) if enforce_age else None)
                with st.spinner("Reading finalized trend window…"):
                    payloads["trends"], errors["trends"], cached["trends"] = fetch(
                        st.session_state, client, "get_trends", *trend_args, refresh=True
                    )
        else:
            errors["trends"] = ApiClientError(
                "Configure a source namespace that is allowlisted by the API.",
                status_code=422,
                code="invalid_namespace",
            )
        with st.spinner("Reading batch product metrics…"):
            payloads["products"], errors["products"], cached["products"] = fetch(
                st.session_state, client, "get_batch_products", order, int(limit), refresh=True
            )
        with st.spinner("Reading batch department metrics…"):
            payloads["departments"], errors["departments"], cached["departments"] = fetch(
                st.session_state, client, "get_batch_departments", refresh=True
            )
        with st.spinner("Reading archived order distributions…"):
            payloads["orders"], errors["orders"], cached["orders"] = fetch(
                st.session_state, client, "get_orders_distribution", refresh=True
            )
        st.session_state["_module4d_analytics_last_run"] = {
            "request": {
                "namespace": namespace,
                "as_of_event_time": as_of_event_time,
                "max_event_age_seconds": int(max_age) if enforce_age else None,
                "order": order,
                "limit": int(limit),
            },
            "payloads": payloads,
            "errors": {key: _error_record(error) for key, error in errors.items()},
        }
    else:
        previous = st.session_state.get("_module4d_analytics_last_run")
        if isinstance(previous, dict) and isinstance(previous.get("payloads"), dict):
            payloads = previous["payloads"]
            errors = {key: _restore_error((previous.get("errors") or {}).get(key)) for key in payloads}
            cached = {key: payloads.get(key) is not None for key in payloads}
        else:
            st.info("Choose filters and submit to read the selected sources. No trend history or automatic polling is requested.")

    last_run = st.session_state.get("_module4d_analytics_last_run")
    request_meta = last_run.get("request", {}) if isinstance(last_run, dict) else {}
    if request_meta:
        st.caption(
            f"Last submitted filters: namespace {request_meta.get('namespace') or 'not configured'} · "
            f"as-of event-time {display_time(request_meta.get('as_of_event_time'))} · "
            f"product ranking {request_meta.get('order')} · limit {request_meta.get('limit')} · "
            f"maximum event age {request_meta.get('max_event_age_seconds') if request_meta.get('max_event_age_seconds') is not None else 'not applied'} seconds"
        )

    st.subheader("Finalized trend ranking")
    trend = payloads["trends"]
    if errors["trends"]:
        render_error(st, errors["trends"], label="Finalized trends")
    elif trend:
        if not trend["available"]:
            st.warning(f"Trend source unavailable: {trend['status']} · {trend['reason'] or 'No availability reason supplied.'}")
        elif not trend["items"]:
            st.info("The selected finalized snapshot contains no trend rows.")
        else:
            trend_rows = sorted(trend["items"], key=lambda row: row["trend_rank"])
            chart = pd.DataFrame(
                [{"Product ID": row["product_id"], "Trend score (0–1)": row["trend_score"]} for row in trend_rows]
            )
            st.bar_chart(
                chart,
                x="Product ID",
                y="Trend score (0–1)",
                horizontal=True,
                sort=False,
                width="stretch",
                alt="Trend scores from the selected finalized event-time window, ranked by trend rank.",
            )
            st.dataframe(
                [{"Trend rank": row["trend_rank"], "Product ID": row["product_id"], "Trend score": row["trend_score"]} for row in trend_rows],
                hide_index=True,
                width="stretch",
            )
        st.caption(
            f"Source: realtime_trending_windows · Namespace: {trend['source_namespace']} · Scope: {trend['source_scope'] or 'not recorded'} · "
            f"Finalized window: {display_time(trend['window_start'])} – {display_time(trend['window_end'])} · "
            f"Query cutoff: {display_time(request_meta.get('as_of_event_time'))} · "
            f"Published: {display_time(trend['published_at'])} · Observed: {display_time(trend['observed_at'])}"
        )
        if trend.get("reason"):
            st.caption(f"Unavailable reason: {trend['reason']}")
        show_cached(st, cached["trends"])
    else:
        st.caption("No trend result loaded yet.")

    st.subheader("Batch products")
    products = payloads["products"]
    if errors["products"]:
        render_error(st, errors["products"], label="Batch products")
    elif products:
        rows = products["items"]
        if rows:
            metric = products["order"]
            chart = pd.DataFrame(
                [{"Product ID": row["product_id"], metric: row[metric]} for row in rows]
            )
            st.bar_chart(
                chart,
                x="Product ID",
                y=metric,
                horizontal=True,
                sort=f"-{metric}",
                width="stretch",
                alt=f"Products ranked by {metric} from prior-only item purchases.",
            )
            st.dataframe(
                [
                    {
                        "Product ID": row["product_id"],
                        "Purchase count": row["purchase_count"],
                        "Unique users": row["unique_users"],
                        "Reorder count": row["reorder_count"],
                        "Reorder rate": row["reorder_rate"],
                        "Run ID": row["run_id"],
                        "Last batch time": display_time(row["last_batch_time"]),
                    }
                    for row in rows
                ],
                hide_index=True,
                width="stretch",
            )
        else:
            st.info("The batch product source returned a valid empty result.")
        source = products["source"]
        st.caption(
            f"Source: {source['collection']} · Scope: {source['scope']} · Run: {source['run_id'] or 'not recorded'} · "
            f"Last batch: {display_time(source['last_batch_time'])} · Observed: {display_time(products['observed_at'])} · "
            f"Metric: {products['metric_definition'].get(products['order'], 'API-defined metric')}"
        )
        show_cached(st, cached["products"])
    else:
        st.caption("No batch product result loaded yet.")

    st.subheader("Departments")
    departments = payloads["departments"]
    if errors["departments"]:
        render_error(st, errors["departments"], label="Batch departments")
    elif departments:
        rows = departments["items"]
        if rows:
            chart = pd.DataFrame(
                [{"Department ID": row["department_id"], "Prior purchase events": row["purchase_count"]} for row in rows]
            )
            st.bar_chart(
                chart,
                x="Department ID",
                y="Prior purchase events",
                horizontal=True,
                sort="-Prior purchase events",
                width="stretch",
                alt="Department purchase counts from prior-only item purchases; labels are IDs only.",
            )
            st.dataframe(
                [
                    {
                        "Department ID": row["department_id"],
                        "Purchase count": row["purchase_count"],
                        "Unique users": row["unique_users"],
                        "Reorder rate": row["reorder_rate"],
                        "Run ID": row["run_id"],
                        "Last batch time": display_time(row["last_batch_time"]),
                    }
                    for row in rows
                ],
                hide_index=True,
                width="stretch",
            )
        else:
            st.info("The batch department source returned a valid empty result.")
        source = departments["source"]
        st.caption(
            f"Source: {source['collection']} · Scope: {source['scope']} · Run: {source['run_id'] or 'not recorded'} · "
            f"Last batch: {display_time(source['last_batch_time'])} · Observed: {display_time(departments['observed_at'])}"
        )
        show_cached(st, cached["departments"])
    else:
        st.caption("No department result loaded yet.")

    st.subheader("Order distributions")
    orders = payloads["orders"]
    if errors["orders"]:
        render_error(st, errors["orders"], label="Order distributions")
    elif orders:
        day_col, hour_col = st.columns(2, gap="large")
        with day_col:
            st.markdown("**Orders by source day-of-week bin (0–6)**")
            st.bar_chart(
                pd.DataFrame(
                    [{"Source day-of-week bin": row["order_dow"], "Orders": row["order_count"]} for row in orders["orders_by_day_of_week"]]
                ),
                x="Source day-of-week bin",
                y="Orders",
                sort=False,
                width="stretch",
                alt="All-order counts ordered by the source day-of-week bins 0 through 6.",
            )
        with hour_col:
            st.markdown("**Orders by hour of day (0–23)**")
            st.bar_chart(
                pd.DataFrame(
                    [{"Hour of day": row["order_hour_of_day"], "Orders": row["order_count"]} for row in orders["orders_by_hour"]]
                ),
                x="Hour of day",
                y="Orders",
                sort=False,
                width="stretch",
                alt="All-order counts ordered by hour bins 0 through 23.",
            )
        provenance = orders["provenance"]
        st.caption(
            f"Source: {provenance['source_label']} · Classification: {provenance['classification']} · "
            f"Archived: {provenance['archived']} · Measure: {orders['measure']} · Total orders: {orders['total_orders']:,} · "
            f"Observed: {display_time(orders['observed_at'])}"
        )
        show_cached(st, cached["orders"])
    else:
        st.caption("No order distribution result loaded yet.")
