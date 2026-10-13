"""Pure contract tests for the bounded C8B streaming demonstration."""

from __future__ import annotations

import math

import pytest

from scripts.module4c_streaming_demo import (
    FIXTURE_INGESTION_MS,
    REQUIRED_EVENT_FIELDS,
    SOURCE_SCOPE,
    TARGET_A_END,
    TARGET_A_START,
    TARGET_B_END,
    TARGET_B_START,
    WATERMARK_DELAY,
    build_event_fixture,
    expected_source_provenance,
    independent_hybrid_oracle,
    independent_trend_oracle,
    parse_timestamp,
    redact_secrets,
    resource_names,
    target_window_events,
    timestamp,
    validate_disposable_database_name,
    validate_event,
    validate_fixture,
    validate_mongodb_target,
    validate_report,
    validate_run_id,
    wait_bounded,
)
from src.module3.schemas import KAFKA_EVENT_SCHEMA


RUN_ID = "c8b-20261013t151900z-abc123ef"


def _report() -> dict:
    return {
        "experiment_id": RUN_ID,
        "status": "passed",
        "git_commit": "a" * 40,
        "git_dirty": True,
        "source_scope": SOURCE_SCOPE,
        "source_namespace": "c8b:controlled-synthetic:test",
        "dataset_type": "controlled synthetic Kafka fixture",
        "recommendation_accuracy_measured": False,
        "production_latency_benchmark": False,
        "kafka_topic": "i3-c8b-test",
        "event_counts": {"planned": 48, "acknowledged": 48, "consumed": 48},
        "spark_version": "3.5.5",
        "kafka_version": "3.7.0",
        "mongodb_version": "7.0.16",
        "window_configuration": {"window_long": "120 minutes", "slide": "5 minutes"},
        "watermark_configuration": {"delay": "10 minutes"},
        "checkpoint_identity": RUN_ID,
        "target_windows": {},
        "spark_progress_summary": {},
        "bundle_identities": [],
        "bundle_content_hashes": [],
        "serving_requests": [],
        "serving_versions": [],
        "ranking_before": {},
        "ranking_after": {},
        "readiness_checks": {},
        "retry_checks": {},
        "timing_observations": {
            "kafka_producer_ack_wall_ms": [],
            "spark_microbatch_duration_ms": {},
            "processing_time_to_finalized_publication_ms": {},
            "module4c_cli_wall_ms": [],
        },
        "cleanup_status": {},
        "limitations": [],
    }


def test_fixture_generation_is_deterministic_and_has_48_unique_events():
    first = build_event_fixture(RUN_ID)
    second = build_event_fixture(RUN_ID)
    assert first == second
    assert sum(map(len, first.values())) == 48
    all_events = [event for phase in first.values() for event in phase]
    assert len({event["event_id"] for event in all_events}) == 48
    assert len({event["order_id"] for event in all_events}) == 48
    assert all(event["ingestion_time_epoch_ms"] == FIXTURE_INGESTION_MS for event in all_events)


def test_fixture_schema_matches_module3_and_each_event_is_valid():
    phases = build_event_fixture(RUN_ID)
    assert tuple(KAFKA_EVENT_SCHEMA.fieldNames()) == REQUIRED_EVENT_FIELDS
    for phase in phases.values():
        for event in phase:
            validate_event(event)
            assert set(event) == set(KAFKA_EVENT_SCHEMA.fieldNames())
            assert timestamp(parse_timestamp(event["event_time_iso"])) == event["event_time_iso"]


def test_fixture_phases_and_event_times_are_chronological():
    phases = build_event_fixture(RUN_ID)
    assert tuple(phases) == ("window_a", "advance_a", "window_b", "advance_b")
    maxima = []
    for phase in phases.values():
        event_times = [event["event_time_epoch_ms"] for event in phase]
        assert event_times == sorted(event_times)
        maxima.append(max(event_times))
    assert maxima == sorted(maxima)
    validate_fixture(phases)


@pytest.mark.parametrize(
    ("start", "end", "phase", "favored"),
    [
        (TARGET_A_START, TARGET_A_END, "window_a", 1),
        (TARGET_B_START, TARGET_B_END, "window_b", 2),
    ],
)
def test_target_events_have_expected_window_membership_and_favored_counts(start, end, phase, favored):
    phases = build_event_fixture(RUN_ID)
    events = target_window_events(phases, start, end)
    assert len(events) == 22
    assert all(start <= parse_timestamp(event["event_time_iso"]) < end for event in events)
    counts = {product_id: 0 for product_id in range(1, 21)}
    for event in events:
        counts[event["product_id"]] += 1
    assert counts[favored] == 3
    assert all(count == 1 for product_id, count in counts.items() if product_id != favored)
    assert all(event in phases[phase] for event in events)


def test_watermark_advancement_requires_strictly_passing_target_end():
    phases = build_event_fixture(RUN_ID)
    max_a = max(parse_timestamp(event["event_time_iso"]) for event in phases["advance_a"])
    max_b = max(parse_timestamp(event["event_time_iso"]) for event in phases["advance_b"])
    assert max_a - WATERMARK_DELAY > TARGET_A_END
    assert max_b - WATERMARK_DELAY > TARGET_B_END

    invalid = {name: [dict(event) for event in events] for name, events in phases.items()}
    exact_threshold = TARGET_A_END + WATERMARK_DELAY
    for candidate in invalid["advance_a"]:
        candidate["event_time_epoch_ms"] = int(exact_threshold.timestamp() * 1000)
        candidate["event_time_iso"] = timestamp(exact_threshold)
    with pytest.raises(ValueError, match="strictly beyond"):
        validate_fixture(invalid)


def test_independent_module3_oracle_ranks_p1_then_p2_with_exact_counts():
    phases = build_event_fixture(RUN_ID)
    a = independent_trend_oracle(
        target_window_events(phases, TARGET_A_START, TARGET_A_END),
        start=TARGET_A_START,
        end=TARGET_A_END,
    )
    b = independent_trend_oracle(
        target_window_events(phases, TARGET_B_START, TARGET_B_END),
        start=TARGET_B_START,
        end=TARGET_B_END,
    )
    assert len(a) == len(b) == 20
    assert a[0]["product_id"] == 1
    assert b[0]["product_id"] == 2
    assert a[0]["purchase_count_30m"] == a[0]["purchase_count_120m"] == 3
    assert b[1]["product_id"] == 1
    assert all(row["trend_score"] == 0.0 for row in a[1:])
    assert all(row["trend_score"] == 0.0 for row in b if row["product_id"] != 2)
    assert a[0]["trend_score"] == b[0]["trend_score"] == 1.0


def test_independent_hybrid_oracle_changes_top1_at_lambda_half():
    phases = build_event_fixture(RUN_ID)
    a_trend = independent_trend_oracle(
        target_window_events(phases, TARGET_A_START, TARGET_A_END),
        start=TARGET_A_START,
        end=TARGET_A_END,
    )
    b_trend = independent_trend_oracle(
        target_window_events(phases, TARGET_B_START, TARGET_B_END),
        start=TARGET_B_START,
        end=TARGET_B_END,
    )
    a = independent_hybrid_oracle(a_trend, lambda_weight=0.5)
    b = independent_hybrid_oracle(b_trend, lambda_weight=0.5)
    assert a[0]["product_id"] == 1
    assert b[0]["product_id"] == 2
    assert math.isclose(a[0]["final_score"], 1.0, abs_tol=1e-12)
    assert math.isclose(b[0]["final_score"], 0.75, abs_tol=1e-12)


def test_fixture_uses_explicit_controlled_synthetic_scope():
    assert SOURCE_SCOPE == "controlled_synthetic"
    report = _report()
    validate_report(report)
    report["source_scope"] = "prior_only"
    with pytest.raises(ValueError, match="controlled_synthetic"):
        validate_report(report)


def test_readiness_provenance_maps_to_cli_source_fields():
    readiness = {
        "run_id": "c8b-source-run",
        "model_version": "c8b-als-model",
        "graph_version": "c8b-graph",
    }
    assert expected_source_provenance(readiness) == {
        "source_run_id": "c8b-source-run",
        "model_version": "c8b-als-model",
        "graph_version": "c8b-graph",
    }


def test_evidence_must_disclaim_accuracy_and_production_latency_claims():
    report = _report()
    report["recommendation_accuracy_measured"] = True
    with pytest.raises(ValueError, match="accuracy was not measured"):
        validate_report(report)
    report["recommendation_accuracy_measured"] = False
    report["production_latency_benchmark"] = True
    with pytest.raises(ValueError, match="production latency benchmark"):
        validate_report(report)


def test_evidence_schema_requires_distinct_timing_categories_and_finite_values():
    report = _report()
    validate_report(report)
    report["timing_observations"]["module4c_cli_wall_ms"] = [math.inf]
    with pytest.raises(ValueError, match="non-finite"):
        validate_report(report)


def test_evidence_rejects_fabricated_event_to_serving_latency():
    report = _report()
    report["timing_observations"]["event_to_serving_latency_ms"] = 3600000
    with pytest.raises(ValueError, match="event-to-serving latency"):
        validate_report(report)


def test_timeout_is_bounded_and_includes_last_observation():
    now = [0.0]
    observations = iter((None, None))

    def clock():
        return now[0]

    def sleep(duration):
        now[0] += duration

    with pytest.raises(TimeoutError, match="last=None"):
        wait_bounded(
            lambda: next(observations),
            description="a final bundle",
            timeout_seconds=0.5,
            poll_seconds=0.25,
            monotonic=clock,
            sleep=sleep,
        )
    assert now[0] == 0.5


def test_wait_returns_observation_without_extra_polling():
    calls = []
    result = wait_bounded(
        lambda: calls.append(1) or {"ready": True},
        description="ready document",
        timeout_seconds=1,
        sleep=lambda _duration: pytest.fail("ready predicate must not sleep"),
    )
    assert result == {"ready": True}
    assert len(calls) == 1


def test_resource_names_are_isolated_by_validated_run_id():
    first = resource_names(RUN_ID)
    second = resource_names("c8b-20261013t151901z-abc123ef")
    assert first["network"] != second["network"]
    assert first["kafka_container"] != second["kafka_container"]
    assert first["mongo_container"] != second["mongo_container"]
    assert first["spark_container"] != second["spark_container"]
    assert first["topic"] != second["topic"]
    assert first["database"] != second["database"]
    assert first["checkpoint_id"] == RUN_ID
    validate_disposable_database_name(first["database"])
    with pytest.raises(ValueError, match="run ID"):
        validate_run_id("c8b-../../instacart")


@pytest.mark.parametrize("database", ["instacart", "admin", "config", "local", "i3_c8b_bad"])
def test_database_guard_refuses_production_or_malformed_database(database):
    with pytest.raises(ValueError, match="disposable"):
        validate_disposable_database_name(database)


@pytest.mark.parametrize(
    "uri",
    [
        "mongodb://localhost:27017",
        "mongodb://127.0.0.1:27017",
        "mongodb://c8b-mongo:27017/instacart",
        "mongodb://user:secret@c8b-mongo:27017",
        "mongodb://c8b-mongo:27017/?authSource=admin",
        "mongodb://c8b-mongo:27017,mongodb:27017",
        "not-a-mongodb-uri",
    ],
)
def test_mongodb_guard_refuses_external_production_or_credentialed_targets(uri):
    with pytest.raises(ValueError, match="MongoDB URI"):
        validate_mongodb_target(uri)


def test_mongodb_guard_allows_only_dedicated_internal_service():
    validate_mongodb_target("mongodb://c8b-mongo:27017")


def test_credential_redaction_covers_uri_and_inline_secrets():
    result = redact_secrets(
        "failed mongodb://fixture:topsecret@c8b-mongo:27017 password=anothersecret"
    )
    assert "topsecret" not in result
    assert "anothersecret" not in result
    assert "mongodb://<redacted>" in result
    assert "password=<redacted>" in result


def test_event_validation_rejects_timestamp_mismatch_and_extra_fields():
    event = build_event_fixture(RUN_ID)["window_a"][0]
    invalid_time = dict(event, event_time_iso="2024-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="does not agree"):
        validate_event(invalid_time)
    with pytest.raises(ValueError, match="event schema"):
        validate_event(dict(event, hidden_ground_truth=True))


def test_event_fixture_covers_only_the_seeded_twenty_product_catalog():
    events = [event for phase in build_event_fixture(RUN_ID).values() for event in phase]
    assert {event["product_id"] for event in events} == set(range(1, 21))
    assert {event["user_id"] for event in events} == {9401}
    assert all("label" not in event and "ground_truth" not in event for event in events)
