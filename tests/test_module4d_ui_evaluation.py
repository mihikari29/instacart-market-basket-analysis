"""AppTest and fixed-client contract tests for the D2B evidence page."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.module4.dashboard.ui import app as dashboard_app
from src.module4.dashboard.ui.api_client import ApiClient, ApiClientError
from src.module4.dashboard.ui.pages.evaluation import _available_rate
from src.module4.dashboard.ui.state import fetch


ROOT = Path(__file__).resolve().parents[1]
APP_FILE = ROOT / "src/module4/dashboard/ui/app.py"
OBSERVED_AT = "2026-10-14T10:00:00Z"
SHA = "a" * 64


def _metric(k: int, *, precision: float, recall: float, map_value: float, ndcg: float) -> dict:
    return {
        "k": k,
        "status": "evaluated",
        "precision_at_k": precision,
        "recall_at_k": recall,
        "map_at_k": map_value,
        "ndcg_at_k": ndcg,
        "evaluated_users": 131209,
        "user_coverage": 1.0,
        "catalog_coverage_rate": 0.047939,
        "catalog_coverage": 2382,
        "users_with_no_recommendations": 0,
    }


def _evaluation_response() -> dict:
    score_overrides = {
        ("als", 10): _metric(10, precision=0.09578611223315568, recall=0.12147208248298487, map_value=0.06988428671987636, ndcg=0.13710192304026791),
        ("global_popularity", 10): _metric(10, precision=0.07252246416023238, recall=0.06984315126267848, map_value=0.04400797674774381, ndcg=0.09788083813742669),
        ("personal_frequency", 10): _metric(10, precision=0.26692833570868063, recall=0.31923221187877215, map_value=0.2616500461164106, ndcg=0.38676340726814845),
    }
    methods = ("als", "global_popularity", "personal_frequency")

    def metrics(users: int) -> dict:
        output = {}
        for method_index, method in enumerate(methods):
            output[method] = {}
            for k in (5, 10, 20):
                row = score_overrides.get((method, k)) or _metric(
                    k,
                    precision=0.03 + method_index * 0.01,
                    recall=0.05 + method_index * 0.02,
                    map_value=0.02 + method_index * 0.01,
                    ndcg=0.04 + method_index * 0.02,
                )
                row = dict(row)
                row["evaluated_users"] = users
                output[method][str(k)] = row
        return output

    def cohort(users: int, definition: str | None) -> dict:
        return {"definition": definition, "users": users, "metrics": metrics(users)}

    primary = {
        "all_official_train_users": cohort(
            131209, "all users with non-empty official train truth; cold items remain in denominator"
        ),
        "warm_users_and_all_truth_items": cohort(
            131200, "user factor exists and every official train truth product has an item factor"
        ),
    }
    history = {
        "new_to_user": cohort(107008, None),
        "reordered": cohort(122607, None),
    }
    deltas = {
        cohort_id: {
            model: {str(k): {name: 0.01 for name in ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k")} for k in (5, 10, 20)}
            for model in ("global_popularity", "personal_frequency")
        }
        for cohort_id in primary
    }
    return {
        "evidence_id": "module4a-full-als-evaluation",
        "title": "Module 4A full-data ALS and baseline evaluation",
        "provenance": {
            "artifact_key": "als_evaluation",
            "source_label": "Module 4A archived official-train evaluation",
            "classification": "official_train_evaluation",
            "archived": True,
            "sha256": SHA,
        },
        "algorithm_scope": "ALS and recorded baselines",
        "evaluation_method": {
            "ground_truth": "official order_products_train, distinct product IDs per user",
            "cohort_exclusion": "users with empty official train truth are excluded and counted in data report",
            "fallback": "global prior popularity for users without ALS user factors in all-user evaluation only",
            "als_training_method": "not recorded in this allowlisted evaluation artifact",
        },
        "ks": [5, 10, 20],
        "cohorts": primary,
        "history_subgroups": history,
        "als_vs_baseline_deltas": deltas,
        "official_test_truth_available": False,
        "truth_summary": {"users": 131209, "truth_products": 39123, "cold_truth_products": 8},
        "observed_at": OBSERVED_AT,
    }


def _provenance(classification: str) -> dict:
    return {
        "artifact_key": "archived_evidence",
        "source_label": "Archived test evidence",
        "classification": classification,
        "archived": True,
        "sha256": SHA,
    }


def _benchmarks_response() -> dict:
    return {
        "evidence_id": "module2-full-hardening-benchmarks",
        "title": "Module 2 full-data hardening benchmark",
        "kind": "benchmarks",
        "provenance": _provenance("full_public_dataset_isolated_runtime_validation"),
        "data": {
            "run": {
                "run_id": "20261003T080535_74b9d443",
                "timestamp_utc": "2026-10-03T08:05:35.041350+00:00",
                "dataset_classification": "FULL PUBLIC DATASET / ISOLATED RUNTIME VALIDATION",
                "date_utc": "2026-10-03",
            },
            "environment": {
                "platform": "Isolated Ubuntu 24.04 environment",
                "resources": "approximately 16 GiB RAM; standalone Spark with two cores and one 1 GiB executor",
                "spark": "3.5.5",
                "java": "17.0.14",
                "python": "3.10.12",
                "shuffle_partitions": 32,
                "warmups": 1,
                "measured_repetitions": 3,
            },
            "metrics": {
                "join_strategy_medians_seconds": {"broadcast_hash_join": 2.304045968, "sort_merge_join": 17.581933945},
                "partition_scan_medians_seconds": {
                    "full_scan": 3.497363414,
                    "date_filter": 1.666807938,
                    "partition_filter": 0.074905592,
                    "partition_filter_rows": 177739,
                },
                "cache_medians_seconds": {
                    "uncached": 2.110283542,
                    "cache_materialization": 2.175583285,
                    "cached_reuse": 0.08808505,
                },
                "shuffle_metrics": {"available": False, "reason": "No structured shuffle metrics are recorded in this selected artifact."},
            },
            "method_notes": {
                "partition_pruning": "Full scan has a different scope; date and partition filters are paired benchmark arms.",
                "cache": "Materialization cost and cached reuse are separate values.",
                "runs_are_comparable": True,
            },
            "module2_validation": {
                "prior_only_analytics": True,
                "mongo_published": True,
                "published_product_documents": 49677,
                "published_department_documents": 21,
            },
        },
        "observed_at": OBSERVED_AT,
    }


def _c8a_response() -> dict:
    return {
        "evidence_id": "module4c-c8a-responsiveness",
        "title": "Module 4C C8A controlled responsiveness receipt",
        "kind": "synthetic_responsiveness",
        "provenance": _provenance("controlled_synthetic_experiment"),
        "data": {
            "experiment_id": "c8a-20261013T144721Z-37027014",
            "status": "passed",
            "classification": "controlled synthetic responsiveness; not recommendation-quality evidence",
            "top20_overlap": "20/20 for each lambda",
            "ranking_response": [
                {"lambda": 0.0, "baseline_top1": "P1", "changed_top1": "P2", "rank_changed_products": 2, "mean_absolute_rank_displacement": 0.1},
                {"lambda": 0.5, "baseline_top1": "P1", "changed_top1": "P2", "rank_changed_products": 2, "mean_absolute_rank_displacement": 0.1},
                {"lambda": 0.7, "baseline_top1": "P1", "changed_top1": "P1", "rank_changed_products": 0, "mean_absolute_rank_displacement": 0.0},
                {"lambda": 0.9, "baseline_top1": "P1", "changed_top1": "P1", "rank_changed_products": 0, "mean_absolute_rank_displacement": 0.0},
                {"lambda": 1.0, "baseline_top1": "P1", "changed_top1": "P1", "rank_changed_products": 0, "mean_absolute_rank_displacement": 0.0},
            ],
            "first_publication_cli_wall_ms_at_lambda_0_5": {
                "baseline": {"repetitions": 3, "median_ms": 790.659529, "min_ms": 777.895784, "max_ms": 794.416567},
                "changed": {"repetitions": 3, "median_ms": 803.199924, "min_ms": 777.640903, "max_ms": 807.05074},
                "warmups": 1,
                "includes_process_startup_and_mongodb_serving": True,
                "excludes_fixture_and_bundle_setup": True,
                "not_kafka_to_serving_latency": True,
            },
            "recommendation_accuracy_measured": False,
            "limitations": ["Synthetic controlled counts; no recommendation accuracy measured."],
        },
        "observed_at": OBSERVED_AT,
    }


def _c8b_response() -> dict:
    return {
        "evidence_id": "module4c-c8b-streaming-demo",
        "title": "Module 4C C8B bounded streaming demonstration receipt",
        "kind": "synthetic_streaming_demo",
        "provenance": _provenance("bounded_synthetic_kafka_spark_demonstration"),
        "data": {
            "experiment_id": "c8b-20261013t153557z-11f0a25f",
            "status": "passed",
            "classification": "bounded synthetic Kafka/Spark demonstration; archived evidence, not live streaming",
            "runtime_versions": {"kafka": "3.7.0", "spark": "3.5.5", "mongodb": "7.0.16"},
            "runtime_isolation": {"host_ports_published": False, "persistent_volumes": False, "spark_mode": "local[2]", "spark_shuffle_partitions": 2},
            "events": {"planned": 48, "acknowledged": 48, "consumed": 48, "invalid": 0, "finalized_bundle_documents": 36},
            "windows": [
                {"name": "A", "start_utc": "2024-01-01T00:00:00.000Z", "end_utc": "2024-01-01T02:00:00.000Z", "watermark_utc": "2024-01-01T02:15:00.000Z", "microbatch_id": 3, "row_count": 20, "top1_product_id": 1, "bundle_id": "trend-window:bundle-A", "content_sha256": "b" * 64, "last_watermark_ack_to_bundle_visible_ms": 12291.324},
                {"name": "B", "start_utc": "2024-01-01T03:00:00.000Z", "end_utc": "2024-01-01T05:00:00.000Z", "watermark_utc": "2024-01-01T05:15:00.000Z", "microbatch_id": 7, "row_count": 20, "top1_product_id": 2, "bundle_id": "trend-window:bundle-B", "content_sha256": "c" * 64, "last_watermark_ack_to_bundle_visible_ms": 7329.23},
            ],
            "spark_batch_durations_ms": {"add_batch": [13216, 3765, 3255, 3135, 2541, 2132], "trigger_execution": [14723, 4092, 3353, 3238, 2638, 2222]},
            "serving": {
                "complete_rows_per_version": 20,
                "pointer_advanced_to_window_b": True,
                "previous_version_rows_immutable": True,
                "repeat_b_status": "already_active",
                "repeat_b_same_version_and_rows": True,
                "repeat_b_cli_wall_ms": 894.717,
            },
            "limitations": ["No recommendation accuracy measured.", "Not a production or capacity benchmark.", "Spark checkpoint restart not exercised."],
            "interpretation": "The named acknowledgement-to-bundle-visible interval is a bounded synthetic boundary measurement, not Kafka lag or generic end-to-end latency.",
        },
        "observed_at": OBSERVED_AT,
    }


def _registry_response() -> dict:
    evidence = (
        ("module2-full-hardening-benchmarks", "benchmarks", "full_public_dataset_isolated_runtime_validation"),
        ("module4c-c8a-responsiveness", "synthetic_responsiveness", "controlled_synthetic_experiment"),
        ("module4c-c8b-streaming-demo", "synthetic_streaming_demo", "bounded_synthetic_kafka_spark_demonstration"),
    )
    return {
        "items": [
            {
                "evidence_id": evidence_id,
                "title": f"Archived {evidence_id}",
                "kind": kind,
                "classification": classification,
                "source_label": f"Archived source {evidence_id}",
                "available": True,
                "status": "available",
                "sha256": SHA,
            }
            for evidence_id, kind, classification in evidence
        ],
        "observed_at": OBSERVED_AT,
    }


class FakeEvidenceApi:
    base_url = "http://127.0.0.1:8000"

    def __init__(self):
        self.calls = []
        self.errors = {}
        self.evaluation = _evaluation_response()
        self.payloads = {
            "module2-full-hardening-benchmarks": _benchmarks_response(),
            "module4c-c8a-responsiveness": _c8a_response(),
            "module4c-c8b-streaming-demo": _c8b_response(),
        }

    def get_evidence_registry(self):
        self.calls.append(("registry", ()))
        return _registry_response()

    def get_als_evaluation(self):
        self.calls.append(("als", ()))
        return deepcopy(self.evaluation)

    def get_evidence(self, evidence_id):
        self.calls.append(("evidence", (evidence_id,)))
        if evidence_id in self.errors:
            raise self.errors[evidence_id]
        return deepcopy(self.payloads[evidence_id])


def _app(monkeypatch, fake: FakeEvidenceApi | None = None):
    api = fake or FakeEvidenceApi()
    app = AppTest.from_file(str(APP_FILE))
    app.session_state["_module4d_api_client"] = api
    app.session_state["ui_page"] = "Evaluation & Performance"
    app.run()
    return app, api


def _rendered_text(app) -> str:
    element_types = (
        app.title,
        app.header,
        app.subheader,
        app.caption,
        app.markdown,
        app.info,
        app.warning,
        app.error,
        app.metric,
        app.selectbox,
        app.expander,
        app.dataframe,
    )
    values = []
    for group in element_types:
        for item in group:
            for field in ("label", "value", "options"):
                value = getattr(item, field, None)
                if value is not None:
                    values.append(value.to_string() if hasattr(value, "to_string") else str(value))
    return "\n".join(values)


def test_navigation_and_evaluation_page_load_all_fixed_evidence_requests(monkeypatch):
    app, api = _app(monkeypatch)
    assert not app.exception
    assert app.radio(key="ui_page").options == list(dashboard_app.PAGES)
    assert "Evaluation & Performance" in _rendered_text(app)
    assert {call for call in api.calls} == {
        ("registry", ()),
        ("als", ()),
        ("evidence", ("module2-full-hardening-benchmarks",)),
        ("evidence", ("module4c-c8a-responsiveness",)),
        ("evidence", ("module4c-c8b-streaming-demo",)),
    }


def test_primary_cohort_k_metric_and_method_controls_preserve_model_identity(monkeypatch):
    app, _ = _app(monkeypatch)
    assert app.selectbox(key="evaluation_primary_cohort").options == [
        "All official-train users",
        "Warm users and all truth items",
    ]
    assert app.selectbox(key="evaluation_primary_k").options == ["5", "10", "20"]
    app.selectbox(key="evaluation_primary_cohort").set_value("warm_users_and_all_truth_items")
    app.selectbox(key="evaluation_primary_k").set_value(20)
    app.selectbox(key="evaluation_primary_metric").set_value("ndcg_at_k")
    app.selectbox(key="evaluation_primary_method").set_value("personal_frequency")
    app.run()
    assert not app.exception
    text = _rendered_text(app)
    assert "Personal frequency" in text
    assert "NDCG@K" in text
    assert "K=20" in text
    assert "Warm users and all truth items" in text


def test_primary_all_metrics_and_accepted_recall_finding_are_retained(monkeypatch):
    app, _ = _app(monkeypatch)
    text = _rendered_text(app)
    for label in ("Precision@K", "Recall@K", "MAP@K", "NDCG@K", "ALS", "Global popularity", "Personal frequency"):
        assert label in text
    assert "0.121472" in text
    assert "0.319232" in text
    assert "ALS did not outperform personal frequency on this metric" in text
    assert "single metric does not establish global model superiority" in text
    assert "Official test-set truth available: **No**" in text
    assert "Hybrid evaluation" not in text


def test_history_subgroups_have_independent_controls_and_are_explicitly_separate(monkeypatch):
    app, _ = _app(monkeypatch)
    assert app.selectbox(key="evaluation_history_k").value == 10
    assert app.selectbox(key="evaluation_history_metric").value == "recall_at_k"
    app.selectbox(key="evaluation_primary_k").set_value(5)
    app.selectbox(key="evaluation_history_k").set_value(20)
    app.selectbox(key="evaluation_history_metric").set_value("map_at_k")
    app.run()
    text = _rendered_text(app)
    assert "History subgroups · shown separately from primary cohorts" in text
    assert "not combined with either primary evaluation cohort" in text
    assert "History subgroup K" in text
    assert not app.exception


def test_optional_coverage_metrics_render_as_unavailable(monkeypatch):
    api = FakeEvidenceApi()
    evaluation = _evaluation_response()
    evaluation["cohorts"]["all_official_train_users"]["metrics"]["als"]["10"]["user_coverage"] = None
    evaluation["cohorts"]["all_official_train_users"]["metrics"]["als"]["10"]["catalog_coverage_rate"] = None
    evaluation["cohorts"]["all_official_train_users"]["metrics"]["als"]["10"].pop("catalog_coverage")
    api.evaluation = evaluation
    app = AppTest.from_file(str(APP_FILE))
    app.session_state["_module4d_api_client"] = api
    app.session_state["ui_page"] = "Evaluation & Performance"
    app.run()
    assert not app.exception
    assert _available_rate(None) == "Unavailable"
    text = _rendered_text(app)
    assert "Coverage fields marked unavailable" in text
    assert "Coverage rates are source fractions on a 0–1 scale" in text
    assert "Unavailable" in text


def test_benchmark_units_scope_cache_cost_shuffle_state_and_provenance(monkeypatch):
    app, _ = _app(monkeypatch)
    text = _rendered_text(app)
    for expected in (
        "Join strategy median times (seconds)",
        "Broadcast Hash Join",
        "Sort-Merge Join",
        "Date-column filter",
        "Partition-column filter",
        "Full scan has a different scope",
        "Cache materialization",
        "Uncached aggregate",
        "Cached reuse",
        "Structured shuffle metrics unavailable",
        "20261003T080535_74b9d443",
        "Spark 3.5.5",
        "Warmups:",
        "not production throughput or cluster-scale scalability",
    ):
        assert expected in text


def test_c8a_lambda_order_default_interpretation_accuracy_and_timing_label(monkeypatch):
    app, _ = _app(monkeypatch)
    table_frames = [element.value for element in app.dataframe]
    response = _c8a_response()["data"]["ranking_response"]
    assert [row["lambda"] for row in response] == [0.0, 0.5, 0.7, 0.9, 1.0]
    chart_table = table_frames[4]
    assert list(chart_table["Lambda"]) == [0.0, 0.5, 0.7, 0.9, 1.0]
    assert list(chart_table["Baseline Top-1"]) == ["P1"] * 5
    assert list(chart_table["Changed Top-1"]) == ["P2", "P2", "P1", "P1", "P1"]
    assert list(chart_table["Products with changed rank"]) == [2, 2, 0, 0, 0]
    assert list(chart_table["Mean absolute rank displacement"]) == [0.1, 0.1, 0.0, 0.0, 0.0]
    assert list(chart_table["Top-20 overlap"]) == ["20/20 for each lambda"] * 5
    text = _rendered_text(app)
    assert "Top-1 changed at λ=0.0, λ=0.5; it did not change at the default λ=0.7" in text
    assert "recommendation-quality evidence" in text
    assert "Recommendation accuracy was not measured in C8A" in text
    assert "CLI first-publication wall time, including process startup and MongoDB serving, excluding fixture setup." in text
    assert "Baseline median (ms)" in text and "Changed-window median (ms)" in text
    assert "790.660" in text and "803.200" in text
    assert "Kafka-to-serving latency" not in text
    assert "Production response time" not in text
    assert table_frames


def test_c8b_reconciliation_windows_durations_serving_and_limits(monkeypatch):
    app, _ = _app(monkeypatch)
    tables = [element.value for element in app.dataframe]
    assert list(tables[5]["Window"]) == ["A", "B"]
    assert list(tables[5]["Full bundle identity"]) == ["trend-window:bundle-A", "trend-window:bundle-B"]
    assert list(tables[5]["Content SHA-256"]) == ["b" * 64, "c" * 64]
    assert list(tables[6]["add_batch (ms)"]) == [13216.0, 3765.0, 3255.0, 3135.0, 2541.0, 2132.0]
    assert list(tables[6]["trigger_execution (ms)"]) == [14723.0, 4092.0, 3353.0, 3238.0, 2638.0, 2222.0]
    assert [metric.value for metric in app.metric if metric.label in {"Planned events", "Acknowledged events", "Consumed events", "Invalid records", "Finalized bundle documents"}] == ["48", "48", "48", "0", "36"]
    assert [metric.value for metric in app.metric if metric.label in {"Pointer advanced to window B", "Previous-version rows remained immutable", "Repeated publish kept the same version and rows"}] == ["Yes", "Yes", "Yes"]
    text = _rendered_text(app)
    for expected in (
        "Planned events",
        "Acknowledged events",
        "Consumed events",
        "Invalid records",
        "Finalized bundle documents",
        "Window A",
        "Window B",
        "Watermark observation",
        "Microbatch ID",
        "Full C8B bundle identities and content digests",
        "Recorded Spark batch durations (milliseconds)",
        "add_batch (ms)",
        "trigger_execution (ms)",
        "Pointer advanced to window B",
        "Previous-version rows remained immutable",
        "Repeated publish kept the same version and rows",
        "Spark checkpoint restart not exercised.",
        "not live streaming",
        "not Kafka lag or generic end-to-end latency",
        "2024-01-01T00:00:00.000Z",
        "2024-01-01T02:00:00.000Z",
        "2024-01-01T02:15:00.000Z",
        "Published rows: 20",
        "Top-1 product ID: 2",
    ):
        assert expected in text
    assert "historical evidence values" in text.lower()
    assert not app.exception


def test_section_failures_are_isolated_and_successful_sibling_panels_render(monkeypatch):
    api = FakeEvidenceApi()
    api.errors["module4c-c8a-responsiveness"] = ApiClientError(
        "Registered report is missing.", status_code=503, code="evidence_unavailable", request_id="req-c8a"
    )
    app, _ = _app(monkeypatch, api)
    text = _rendered_text(app)
    assert "C8A responsiveness evidence" in text and "HTTP 503" in text
    assert "Spark batch performance" in text
    assert "Kafka/Spark integration evidence · C8B" in text
    assert not app.exception


def test_missing_and_corrupt_reports_fail_without_replacing_evidence(monkeypatch):
    api = FakeEvidenceApi()
    api.errors["module2-full-hardening-benchmarks"] = ApiClientError(
        "Registered evidence is missing from this API runtime.", status_code=503, code="evidence_unavailable"
    )
    api.payloads["module4c-c8b-streaming-demo"]["data"]["windows"] = []
    app, _ = _app(monkeypatch, api)
    text = _rendered_text(app)
    assert "HTTP 503" in text
    assert "No successful benchmark evidence response" not in text
    assert "C8B streaming evidence" in text and "invalid_response" in text
    assert not app.exception


def test_explicit_refresh_bypasses_success_cache_and_normal_rerun_reuses_cache(monkeypatch):
    app, api = _app(monkeypatch)
    first_count = len(api.calls)
    app.run()
    assert len(api.calls) == first_count
    app.button(key="evaluation_refresh").click()
    app.run()
    assert len(api.calls) == first_count * 2


def test_page_shows_archive_provenance_and_not_raw_environment_secrets(monkeypatch):
    app, _ = _app(monkeypatch)
    text = _rendered_text(app)
    assert "Archived source" in text
    assert "SHA-256" in text
    assert "API observation" in text
    assert "MODULE4D_API_BASE_URL" not in text
    assert "mongodb://" not in text
    assert "password" not in text.lower()


def test_frontend_remains_api_only_and_does_not_trigger_publication():
    source_root = ROOT / "src/module4/dashboard/ui"
    source = "\n".join(path.read_text() for path in source_root.rglob("*.py"))
    for forbidden in (
        "pymongo", "MongoClient", "MONGODB_URI", "mongodb://", "KafkaConsumer", "SparkSession", "pyspark",
        "HDFS", "serve_hybrid_user", "publish_hybrid", "publish_recommendations", "subprocess.Popen", "spark-submit",
        "Path(", ".open(", "read_text(", "read_evidence(", "src.module4.dashboard.evidence",
    ):
        assert forbidden not in source


class _Response:
    def __init__(self, body: dict, status: int = 200):
        import json

        self.status_code = status
        self.headers = {"Content-Type": "application/json"}
        self.raw = json.dumps(body).encode()
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.raw

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.trust_env = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def test_client_uses_only_the_five_fixed_evidence_routes_and_validates_payloads():
    session = _Session(
        [
            _Response(_registry_response()),
            _Response(_evaluation_response()),
            _Response(_benchmarks_response()),
            _Response(_c8a_response()),
            _Response(_c8b_response()),
        ]
    )
    client = ApiClient("http://127.0.0.1:8000", session=session)
    client.get_evidence_registry()
    client.get_als_evaluation()
    for evidence_id in (
        "module2-full-hardening-benchmarks",
        "module4c-c8a-responsiveness",
        "module4c-c8b-streaming-demo",
    ):
        client.get_evidence(evidence_id)
    assert [call[0] for call in session.calls] == [
        "http://127.0.0.1:8000/api/v1/evidence",
        "http://127.0.0.1:8000/api/v1/evaluation/als",
        "http://127.0.0.1:8000/api/v1/evidence/module2-full-hardening-benchmarks",
        "http://127.0.0.1:8000/api/v1/evidence/module4c-c8a-responsiveness",
        "http://127.0.0.1:8000/api/v1/evidence/module4c-c8b-streaming-demo",
    ]
    with pytest.raises(ApiClientError, match="registered evaluation or performance"):
        client.get_evidence("../secret.json")
    assert len(session.calls) == 5


@pytest.mark.parametrize(
    "body,endpoint",
    [
        ({**_evaluation_response(), "ks": [5, 7, 20]}, "als"),
        ({**_c8a_response(), "data": {**_c8a_response()["data"], "ranking_response": []}}, "c8a"),
        ({**_c8b_response(), "data": {**_c8b_response()["data"], "windows": []}}, "c8b"),
    ],
)
def test_malformed_response_contract_fails_closed(body, endpoint):
    evidence_id = {
        "als": None,
        "c8a": "module4c-c8a-responsiveness",
        "c8b": "module4c-c8b-streaming-demo",
    }[endpoint]
    session = _Session([_Response(body)])
    client = ApiClient("http://127.0.0.1:8000", session=session)
    with pytest.raises(ApiClientError) as caught:
        client.get_als_evaluation() if endpoint == "als" else client.get_evidence(evidence_id)
    assert caught.value.code == "invalid_response"
    assert caught.value.status_code == 200


def test_c8b_checkpoint_restart_limit_cannot_disappear_from_the_api_response():
    payload = _c8b_response()
    payload["data"]["limitations"] = ["Not a production or capacity benchmark."]
    client = ApiClient("http://127.0.0.1:8000", session=_Session([_Response(payload)]))
    with pytest.raises(ApiClientError) as caught:
        client.get_evidence("module4c-c8b-streaming-demo")
    assert caught.value.code == "invalid_response"


def test_backend_missing_report_status_and_request_id_are_sanitized_and_not_cached():
    body = {"error": {"code": "evidence_unavailable", "message": "Registered report missing.", "request_id": "req-missing"}}
    session = _Session([_Response(body, status=503)])
    client = ApiClient("http://127.0.0.1:8000", session=session)
    with pytest.raises(ApiClientError) as caught:
        client.get_evidence("module2-full-hardening-benchmarks")
    assert caught.value.status_code == 503
    assert caught.value.request_id == "req-missing"
    state = {}
    fake = FakeEvidenceApi()
    fake.errors["module2-full-hardening-benchmarks"] = caught.value
    first, error, _ = fetch(state, fake, "get_evidence", "module2-full-hardening-benchmarks")
    assert first is None and error is caught.value
    assert not state["_module4d_response_cache"]


@pytest.mark.parametrize(
    "status,code",
    [(404, "unknown_evidence_id"), (502, "invalid_evidence"), (503, "evidence_unavailable")],
)
def test_evidence_http_error_statuses_are_preserved(status, code):
    body = {"error": {"code": code, "message": "Evidence request failed safely.", "request_id": f"req-{status}"}}
    client = ApiClient("http://127.0.0.1:8000", session=_Session([_Response(body, status=status)]))
    with pytest.raises(ApiClientError) as caught:
        client.get_evidence("module2-full-hardening-benchmarks")
    assert caught.value.status_code == status
    assert caught.value.code == code
    assert caught.value.request_id == f"req-{status}"
