"""Archived ALS, Spark benchmark, and Module 4C evidence views."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.module4.dashboard.ui.components import display_time, render_error, show_cached
from src.module4.dashboard.ui.api_client import ApiClientError
from src.module4.dashboard.ui.state import fetch


EVIDENCE_REQUESTS = (
    ("benchmarks", "get_evidence", "module2-full-hardening-benchmarks"),
    ("c8a", "get_evidence", "module4c-c8a-responsiveness"),
    ("c8b", "get_evidence", "module4c-c8b-streaming-demo"),
)
MODEL_LABELS = {
    "als": "ALS",
    "global_popularity": "Global popularity",
    "personal_frequency": "Personal frequency",
}
COHORT_LABELS = {
    "all_official_train_users": "All official-train users",
    "warm_users_and_all_truth_items": "Warm users and all truth items",
}
METRIC_LABELS = {
    "precision_at_k": "Precision@K",
    "recall_at_k": "Recall@K",
    "map_at_k": "MAP@K",
    "ndcg_at_k": "NDCG@K",
}


def _read(st: Any, client: Any, method: str, *args: Any, refresh: bool):
    return fetch(st.session_state, client, method, *args, refresh=refresh)


def _cached_caption(st: Any, cached: bool, payload: dict[str, Any]) -> None:
    show_cached(st, cached)
    observed = payload.get("observed_at")
    st.caption(f"API observation: {display_time(observed)} · Archived source; this is not live telemetry.")


def _load_page_sources(st: Any, client: Any, refresh: bool) -> dict[str, tuple[dict[str, Any] | None, Any, bool]]:
    requests = [
        ("registry", "get_evidence_registry", ()),
        ("als", "get_als_evaluation", ()),
        *[(key, method, (evidence_id,)) for key, method, evidence_id in EVIDENCE_REQUESTS],
    ]
    results = {}
    for key, method, args in requests:
        results[key] = _read(st, client, method, *args, refresh=refresh)
    return results


def render_evaluation(st: Any, client: Any) -> None:
    st.title("Evaluation & Performance")
    st.caption(
        "Recorded, read-only evidence from the Module 4D API. These archived experiments do not run evaluation, "
        "benchmarks, Spark jobs, or publishers when this page loads."
    )
    refresh = st.button("Refresh evaluation and evidence", key="evaluation_refresh")
    sources = _load_page_sources(st, client, refresh)

    _render_guarded(st, "ALS evaluation", _render_als, *sources["als"])
    _render_guarded(st, "Module 2 benchmarks", _render_benchmarks, *sources["benchmarks"])
    _render_guarded(st, "C8A responsiveness evidence", _render_c8a, *sources["c8a"])
    _render_guarded(st, "C8B streaming evidence", _render_c8b, *sources["c8b"])
    _render_guarded(st, "Evidence registry", _render_registry, *sources["registry"])


def _render_guarded(st: Any, label: str, renderer: Any, *args: Any) -> None:
    """Keep a malformed section from hiding other independent evidence panels."""
    try:
        renderer(st, *args)
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        render_error(
            st,
            ApiClientError(
                "The dashboard API returned an invalid response.",
                status_code=502,
                code="invalid_response",
            ),
            label=label,
        )


def _render_als(st: Any, payload: dict[str, Any] | None, error: Any, cached: bool) -> None:
    st.header("ALS evaluation and recorded baselines")
    if error:
        render_error(st, error, label="ALS evaluation")
        return
    if payload is None:
        st.info("No successful ALS evaluation response is available.")
        return

    cohort_id = st.selectbox(
        "Primary evaluation cohort",
        tuple(payload["cohorts"]),
        format_func=lambda value: COHORT_LABELS.get(value, value),
        key="evaluation_primary_cohort",
    )
    selected = payload["cohorts"][cohort_id]
    k = st.selectbox("K", payload["ks"], index=1, key="evaluation_primary_k")
    metric_id = st.selectbox(
        "Metric",
        tuple(METRIC_LABELS),
        format_func=METRIC_LABELS.get,
        key="evaluation_primary_metric",
    )
    method_id = st.selectbox(
        "Recorded method",
        tuple(MODEL_LABELS),
        format_func=MODEL_LABELS.get,
        key="evaluation_primary_method",
    )

    cohort_metrics = selected["metrics"]
    chart_rows = [
        {
            "Recorded method": MODEL_LABELS[model_id],
            METRIC_LABELS[metric_id]: cohort_metrics[model_id][str(k)][metric_id],
        }
        for model_id in MODEL_LABELS
    ]
    st.subheader(f"{METRIC_LABELS[metric_id]} at K={k}")
    st.bar_chart(
        pd.DataFrame(chart_rows),
        x="Recorded method",
        y=METRIC_LABELS[metric_id],
        horizontal=True,
        sort=False,
        width="stretch",
        alt=f"{METRIC_LABELS[metric_id]} comparison for {COHORT_LABELS.get(cohort_id, cohort_id)} at K={k}.",
    )
    st.caption(
        f"Source: {payload['title']} · Archived: {payload['provenance']['archived']} · "
        f"Cohort: {cohort_id} · K={k} · Scores are recorded values, not recalculated."
    )
    selected_score = cohort_metrics[method_id][str(k)][metric_id]
    st.metric(f"{MODEL_LABELS[method_id]} · {METRIC_LABELS[metric_id]}@{k}", f"{selected_score:.6f}")

    all_metrics = []
    coverage_rows = []
    for model_id, model_label in MODEL_LABELS.items():
        row = cohort_metrics[model_id][str(k)]
        all_metrics.append(
            {
                "Recorded method": model_label,
                "Precision@K": row["precision_at_k"],
                "Recall@K": row["recall_at_k"],
                "MAP@K": row["map_at_k"],
                "NDCG@K": row["ndcg_at_k"],
            }
        )
        coverage_rows.append(
            {
                "Recorded method": model_label,
                "Evaluated users": row["evaluated_users"],
                "Cohort users": selected["users"],
                "User coverage": _available_rate(row.get("user_coverage")),
                "Catalog coverage rate": _available_rate(row.get("catalog_coverage_rate")),
                "Catalog products": _available_count(row.get("catalog_coverage")),
            }
        )
    st.subheader(f"All four metrics · {COHORT_LABELS.get(cohort_id, cohort_id)} · K={k}")
    st.dataframe(all_metrics, hide_index=True, width="stretch")
    st.subheader("Evaluation counts and coverage")
    st.dataframe(coverage_rows, hide_index=True, width="stretch")
    st.caption(
        "Coverage rates are source fractions on a 0–1 scale. Coverage fields marked unavailable were not "
        "present in the recorded response."
    )

    if cohort_id == "all_official_train_users" and k == 10:
        als_recall = cohort_metrics["als"]["10"]["recall_at_k"]
        personal_recall = cohort_metrics["personal_frequency"]["10"]["recall_at_k"]
        st.info(
            f"On this all-user official-train Recall@10 comparison, ALS is {als_recall:.6f} and personal "
            f"frequency is {personal_recall:.6f}; ALS did not outperform personal frequency on this metric. "
            "This single metric does not establish global model superiority."
        )

    definition = selected.get("definition") or "No cohort definition was recorded."
    st.markdown(f"**Recorded cohort definition:** {definition}")
    with st.expander("Evaluation methodology, truth summary, and provenance"):
        st.markdown(f"**Algorithm scope:** {payload['algorithm_scope']}")
        st.markdown(f"**Evaluation method:** {payload['evaluation_method'].get('ground_truth', 'Not recorded')}.")
        st.markdown(f"**Cohort exclusion:** {payload['evaluation_method'].get('cohort_exclusion', 'Not recorded')}.")
        st.markdown(f"**Fallback:** {payload['evaluation_method'].get('fallback', 'Not recorded')}.")
        st.markdown(f"**ALS training method:** {payload['evaluation_method'].get('als_training_method', 'Not recorded')}.")
        st.markdown(
            "Official test-set truth available: **Yes**" if payload["official_test_truth_available"]
            else "Official test-set truth available: **No**. These are official-train evaluation results."
        )
        st.dataframe(
            [{"Truth summary field": name, "Count": count} for name, count in payload["truth_summary"].items()],
            hide_index=True,
            width="stretch",
        )
        st.caption(
            f"Evidence ID: {payload['evidence_id']} · Classification: {payload['provenance']['classification']} · "
            f"SHA-256: {payload['provenance']['sha256']}"
        )

    with st.expander("History subgroups · shown separately from primary cohorts"):
        history_k = st.selectbox("History subgroup K", payload["ks"], index=1, key="evaluation_history_k")
        history_metric = st.selectbox(
            "History subgroup metric",
            tuple(METRIC_LABELS),
            index=1,
            format_func=METRIC_LABELS.get,
            key="evaluation_history_metric",
        )
        history_rows = []
        for subgroup_id, subgroup in payload["history_subgroups"].items():
            for model_id, model_label in MODEL_LABELS.items():
                subgroup_metric = subgroup["metrics"][model_id][str(history_k)]
                history_rows.append(
                    {
                        "Secondary history subgroup": subgroup_id,
                        "Recorded method": model_label,
                        "Evaluated users": subgroup_metric["evaluated_users"],
                        METRIC_LABELS[history_metric]: subgroup_metric[history_metric],
                    }
                )
        st.dataframe(history_rows, hide_index=True, width="stretch")
        st.caption("These history slices are not combined with either primary evaluation cohort.")
    _cached_caption(st, cached, payload)


def _available_rate(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value:.6f}"
    return "Unavailable"


def _available_count(value: Any) -> int | str:
    return value if type(value) is int else "Unavailable"


def _render_benchmarks(st: Any, payload: dict[str, Any] | None, error: Any, cached: bool) -> None:
    st.header("Spark batch performance")
    if error:
        render_error(st, error, label="Module 2 benchmarks")
        return
    if payload is None:
        st.info("No successful benchmark evidence response is available.")
        return

    data = payload["data"]
    metrics = data["metrics"]
    run = data["run"]
    st.caption(
        f"Source: {payload['title']} · Run: {run['run_id']} · Date: {run['date_utc']} · "
        f"Classification: {run['dataset_classification']} · Archived: {payload['provenance']['archived']}"
    )
    st.markdown("**A. Join strategy median times (seconds)**")
    join = metrics["join_strategy_medians_seconds"]
    st.bar_chart(
        pd.DataFrame(
            [
                {"Join strategy": "Broadcast Hash Join", "Median time (seconds)": join["broadcast_hash_join"]},
                {"Join strategy": "Sort-Merge Join", "Median time (seconds)": join["sort_merge_join"]},
            ]
        ),
        x="Join strategy",
        y="Median time (seconds)",
        horizontal=True,
        sort=False,
        width="stretch",
        alt="Recorded median Spark join times in seconds for one isolated benchmark run.",
    )
    st.caption("Join strategies: Broadcast Hash Join and Sort-Merge Join · Median time (seconds).")

    st.markdown("**B. Partition scans (median seconds)**")
    scans = metrics["partition_scan_medians_seconds"]
    st.bar_chart(
        pd.DataFrame(
            [
                {"Scan arm": "Full scan", "Median time (seconds)": scans["full_scan"]},
                {"Scan arm": "Date-column filter", "Median time (seconds)": scans["date_filter"]},
                {"Scan arm": "Partition-column filter", "Median time (seconds)": scans["partition_filter"]},
            ]
        ),
        x="Scan arm",
        y="Median time (seconds)",
        horizontal=True,
        sort=False,
        width="stretch",
        alt="Recorded scan timings; the full scan has a different scope than filtered scans.",
    )
    st.warning(
        data["method_notes"].get(
            "partition_pruning",
            "The full scan covers a different scope; do not treat it as an apples-to-apples speedup comparison.",
        )
    )
    st.caption("Scan arms: Full scan, Date-column filter, and Partition-column filter · Median time (seconds).")
    st.caption(f"Partition-filter rows recorded: {scans['partition_filter_rows']:,}.")

    st.markdown("**C. Cache timing (median seconds)**")
    cache = metrics["cache_medians_seconds"]
    st.bar_chart(
        pd.DataFrame(
            [
                {"Cache phase": "Uncached aggregate", "Median time (seconds)": cache["uncached"]},
                {"Cache phase": "Cache materialization", "Median time (seconds)": cache["cache_materialization"]},
                {"Cache phase": "Cached reuse", "Median time (seconds)": cache["cached_reuse"]},
            ]
        ),
        x="Cache phase",
        y="Median time (seconds)",
        horizontal=True,
        sort=False,
        width="stretch",
        alt="Recorded cache timing phases with cache materialization shown separately.",
    )
    st.caption(data["method_notes"].get("cache", "Cache materialization and reuse are separate recorded values."))
    st.caption("Cache phases: Uncached aggregate, Cache materialization, and Cached reuse · Median time (seconds).")

    shuffle = metrics["shuffle_metrics"]
    if not shuffle["available"]:
        st.warning(f"Structured shuffle metrics unavailable: {shuffle['reason']}")
    else:
        st.info("The API marked structured shuffle metrics available but returned no series in this curated report.")

    with st.expander("Benchmark provenance and environment"):
        st.markdown(f"**Run ID:** {run['run_id']}")
        st.markdown(f"**UTC date and timestamp:** {run['date_utc']} · {run['timestamp_utc']}")
        environment = data["environment"]
        st.markdown(
            f"**Environment:** {environment['platform']} · {environment['resources']} · "
            f"Spark {environment['spark']} · Java {environment['java']} · Python {environment['python']}"
        )
        st.markdown(
            f"**Warmups:** {environment['warmups']} · **Measured repetitions:** "
            f"{environment['measured_repetitions']} · **Shuffle partitions:** {environment['shuffle_partitions']}"
        )
        st.caption("These are timing observations from the recorded isolated experiment, not production throughput or cluster-scale scalability.")
        st.caption(f"Evidence SHA-256: {payload['provenance']['sha256']}")
    _cached_caption(st, cached, payload)


def _render_c8a(st: Any, payload: dict[str, Any] | None, error: Any, cached: bool) -> None:
    st.header("Hybrid responsiveness · C8A")
    if error:
        render_error(st, error, label="C8A responsiveness evidence")
        return
    if payload is None:
        st.info("No successful C8A evidence response is available.")
        return

    data = payload["data"]
    st.warning(f"Synthetic experimental scope · {data['classification']} · Experiment: {data['experiment_id']}")
    ranking = data["ranking_response"]
    rows = [
        {
            "Lambda": row["lambda"],
            "Baseline Top-1": row["baseline_top1"],
            "Changed Top-1": row["changed_top1"],
            "Products with changed rank": row["rank_changed_products"],
            "Mean absolute rank displacement": row["mean_absolute_rank_displacement"],
            "Top-20 overlap": data["top20_overlap"],
        }
        for row in ranking
    ]
    st.dataframe(rows, hide_index=True, width="stretch")
    st.caption(
        f"Source: {payload['title']} · Archived: {payload['provenance']['archived']} · "
        "Top-20 overlap is the recorded report-level summary."
    )
    default_row = next((row for row in ranking if row["lambda"] == 0.7), None)
    changed = [row["lambda"] for row in ranking if row["baseline_top1"] != row["changed_top1"]]
    if default_row and default_row["baseline_top1"] == default_row["changed_top1"]:
        changed_text = ", ".join(f"λ={value:.1f}" for value in changed) or "no tested lambda"
        st.info(
            f"For this selected synthetic trend shift, Top-1 changed at {changed_text}; it did not change "
            "at the default λ=0.7. This describes ranking responsiveness in this experiment only."
        )
    else:
        st.info("The recorded response does not show a Top-1 change at the default λ=0.7.")

    timing = data["first_publication_cli_wall_ms_at_lambda_0_5"]
    st.subheader("Lambda 0.5 first-publication timing")
    st.markdown(
        "**CLI first-publication wall time, including process startup and MongoDB serving, excluding fixture setup.**"
    )
    baseline_col, changed_col = st.columns(2)
    with baseline_col:
        st.metric("Baseline median (ms)", f"{timing['baseline']['median_ms']:.3f}")
        st.caption(f"{timing['baseline']['repetitions']} recorded repetitions")
    with changed_col:
        st.metric("Changed-window median (ms)", f"{timing['changed']['median_ms']:.3f}")
        st.caption(f"{timing['changed']['repetitions']} recorded repetitions")
    st.caption(f"Warmups: {timing['warmups']} · {data['interpretation'] if 'interpretation' in data else 'Synthetic CLI wall-time measurement.'}")
    if not data["recommendation_accuracy_measured"]:
        st.info("Recommendation accuracy was not measured in C8A.")
    with st.expander("C8A limitations and provenance"):
        for limitation in data["limitations"]:
            st.markdown(f"- {limitation}")
        st.caption(f"Evidence SHA-256: {payload['provenance']['sha256']}")
    _cached_caption(st, cached, payload)


def _render_c8b(st: Any, payload: dict[str, Any] | None, error: Any, cached: bool) -> None:
    st.header("Kafka/Spark integration evidence · C8B")
    if error:
        render_error(st, error, label="C8B streaming evidence")
        return
    if payload is None:
        st.info("No successful C8B evidence response is available.")
        return

    data = payload["data"]
    st.warning(f"Synthetic bounded experiment · {data['classification']} · Experiment: {data['experiment_id']}")
    st.subheader("Historical event reconciliation")
    events = data["events"]
    event_columns = st.columns(5)
    event_items = (
        ("Planned events", "planned"),
        ("Acknowledged events", "acknowledged"),
        ("Consumed events", "consumed"),
        ("Invalid records", "invalid"),
        ("Finalized bundle documents", "finalized_bundle_documents"),
    )
    for column, (label, key) in zip(event_columns, event_items, strict=True):
        with column:
            st.metric(label, events[key])
    st.caption("These are historical evidence values from the archived receipt, not live system counters.")

    st.subheader("Finalized windows")
    window_columns = st.columns(2)
    for column, window in zip(window_columns, data["windows"], strict=True):
        with column:
            st.markdown(f"**Window {window['name']}**")
            st.markdown(f"Event-time: {window['start_utc']} → {window['end_utc']}")
            st.markdown(f"Watermark observation: {window['watermark_utc']}")
            st.markdown(f"Microbatch ID: {window['microbatch_id']} · Published rows: {window['row_count']}")
            st.markdown(f"Top-1 product ID: {window['top1_product_id']}")
            st.markdown(f"Bundle: `{_short_identity(window['bundle_id'])}`")
    with st.expander("Full C8B bundle identities and content digests"):
        st.dataframe(
            [
                {
                    "Window": window["name"],
                    "Full bundle identity": window["bundle_id"],
                    "Content SHA-256": window["content_sha256"],
                }
                for window in data["windows"]
            ],
            hide_index=True,
            width="stretch",
        )

    st.subheader("Recorded Spark batch durations (milliseconds)")
    durations = data["spark_batch_durations_ms"]
    if durations["add_batch"] or durations["trigger_execution"]:
        chart_data = pd.DataFrame(
            {
                "add_batch (ms)": pd.Series(durations["add_batch"], dtype="float64"),
                "trigger_execution (ms)": pd.Series(durations["trigger_execution"], dtype="float64"),
            }
        )
        chart_data.index.name = "Recorded duration sequence index"
        st.line_chart(
            chart_data,
            x_label="Recorded duration sequence index",
            y_label="Duration (milliseconds)",
            width="stretch",
            alt="Recorded add_batch and trigger_execution durations from the bounded synthetic C8B experiment.",
        )
        st.dataframe(chart_data.reset_index(), hide_index=True, width="stretch")
    else:
        st.info("No Spark duration samples were recorded in this evidence response.")
    st.caption("These bounded observations do not define production streaming latency distributions.")

    st.subheader("Recorded serving assertions")
    serving = data["serving"]
    serving_columns = st.columns(3)
    assertions = (
        ("Pointer advanced to window B", serving["pointer_advanced_to_window_b"]),
        ("Previous-version rows remained immutable", serving["previous_version_rows_immutable"]),
        ("Repeated publish kept the same version and rows", serving["repeat_b_same_version_and_rows"]),
    )
    for column, (label, passed) in zip(serving_columns, assertions, strict=True):
        with column:
            st.metric(label, "Yes" if passed else "No")
    st.caption(f"Repeated publish status: {serving['repeat_b_status']} · Rows per version: {serving['complete_rows_per_version']}")
    with st.expander("C8B runtime, limitations, and provenance"):
        versions = data["runtime_versions"]
        isolation = data["runtime_isolation"]
        st.markdown(
            f"**Runtime versions:** Kafka {versions['kafka']} · Spark {versions['spark']} · MongoDB {versions['mongodb']}"
        )
        st.markdown(
            f"**Isolation:** host ports published={isolation['host_ports_published']} · "
            f"persistent volumes={isolation['persistent_volumes']} · Spark {isolation['spark_mode']} · "
            f"shuffle partitions={isolation['spark_shuffle_partitions']}"
        )
        for limitation in data["limitations"]:
            st.markdown(f"- {limitation}")
        st.caption(data["interpretation"])
        st.caption(f"Evidence SHA-256: {payload['provenance']['sha256']}")
    _cached_caption(st, cached, payload)


def _short_identity(value: str) -> str:
    return value if len(value) <= 56 else f"{value[:40]}…{value[-12:]}"


def _render_registry(st: Any, payload: dict[str, Any] | None, error: Any, cached: bool) -> None:
    st.header("Evidence registry status")
    if error:
        render_error(st, error, label="Evidence registry")
        return
    if payload is None:
        st.info("The evidence registry did not return a successful response.")
        return
    expected_ids = {"module2-full-hardening-benchmarks", "module4c-c8a-responsiveness", "module4c-c8b-streaming-demo"}
    rows = [
        {
            "Evidence ID": item["evidence_id"],
            "Status": item["status"],
            "Classification": item["classification"],
            "Archived source": item["source_label"],
            "SHA-256": item["sha256"] or "Unavailable",
        }
        for item in payload["items"]
        if item["evidence_id"] in expected_ids
    ]
    if rows:
        st.dataframe(rows, hide_index=True, width="stretch")
    else:
        st.info("The registry returned no matching performance evidence entries.")
    _cached_caption(st, cached, payload)
