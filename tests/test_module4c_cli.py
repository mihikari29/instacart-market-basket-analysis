"""Argument, lifecycle and JSON contract tests for the Module 4C CLI."""

from __future__ import annotations

import builtins
from datetime import datetime, timedelta, timezone
import json

import pytest

from src.module4c import cli
from src.module4c.config import HybridRankingConfig
from src.module4c.data import ReadinessContractError
from src.module4c.service import HybridServingPublicationError, HybridServingResult
from src.module4c.trend_reader import TrendSnapshotIntegrityError


AS_OF = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
WINDOW_END = datetime(2024, 1, 1, 2, 0, tzinfo=timezone.utc)
GENERATED_AT = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)
URI = "mongodb://i3-mongo:27017"
DATABASE = "i3_module4c_cli_unit"


def _result(
    *,
    mode="hybrid",
    reason=None,
    trend_status="available",
    trend_window_end=WINDOW_END,
    trend_digest="a" * 64,
    count=3,
    user_id=42,
    version="hybrid:" + "b" * 64,
    publication_status="published",
    idempotent=False,
):
    return HybridServingResult(
        user_id=user_id,
        as_of_event_time=AS_OF,
        serving_version=version,
        recommendation_count=count,
        mode=mode,
        reason=reason,
        source_run_id="cli-run-001",
        model_version="cli-als-v1",
        graph_version="cli-graph-v1",
        trend_available=trend_status == "available",
        trend_status=trend_status,
        trend_reason=reason if trend_status != "available" else None,
        trend_source_namespace="demo-prior-fixture",
        trend_source_scope="historical_fixture",
        trend_window_start=(WINDOW_END - timedelta(hours=1)) if trend_window_end else None,
        trend_window_end=trend_window_end,
        trend_published_at=WINDOW_END + timedelta(minutes=1) if trend_window_end else None,
        trend_expires_at=None,
        trend_schema_version=1 if trend_window_end else None,
        trend_config_hash="c" * 64 if trend_window_end else None,
        trend_content_sha256=trend_digest if trend_window_end else None,
        candidate_count=count,
        excluded_trend_product_ids=(),
        publication_status=publication_status,
        readiness_pointer_id=f"hybrid:{user_id}",
        readiness_confirmed=True,
        generated_at=GENERATED_AT,
        idempotent=idempotent,
    )


class FakeClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _argv(**overrides):
    values = {
        "user_id": "42",
        "source_namespace": "demo-prior-fixture",
        "as_of_event_time": "2024-01-01T03:00:00Z",
        "mongodb_uri": URI,
        "mongo_database": DATABASE,
    }
    values.update(overrides)
    args = ["serve-user"]
    for name, value in values.items():
        if value is None:
            continue
        args.extend((f"--{name.replace('_', '-')}", value))
    return args


def _install(monkeypatch, *, result=None, error=None):
    client = FakeClient()
    constructed = []
    service_calls = []

    def make_client(uri, **kwargs):
        constructed.append((uri, kwargs))
        return client

    def serve(config, **kwargs):
        service_calls.append((config, kwargs))
        if error is not None:
            raise error
        return result or _result()

    monkeypatch.setattr(cli, "MongoClient", make_client)
    monkeypatch.setattr(cli, "serve_hybrid_user", serve)
    return client, constructed, service_calls


def test_required_arguments_are_enforced_before_client_creation(monkeypatch, capsys):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(["serve-user"])

    assert error.value.code == 2
    assert constructed == []
    assert "required" in capsys.readouterr().err


@pytest.mark.parametrize("user_id", ["0", "-4", "true", "42.0", "+42"])
def test_invalid_user_identifiers_are_rejected_by_argument_parser(monkeypatch, user_id):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(user_id=user_id))

    assert error.value.code == 2
    assert constructed == []


def test_empty_namespace_is_rejected_before_client_creation(monkeypatch):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(source_namespace=""))

    assert error.value.code == 2
    assert constructed == []


@pytest.mark.parametrize(
    "missing",
    [
        {"mongodb_uri": None},
        {"mongo_database": None},
        {"mongodb_uri": None, "mongo_database": None},
    ],
)
def test_mongodb_uri_and_database_are_required(monkeypatch, missing):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(**missing))

    assert error.value.code == 2
    assert constructed == []


def test_valid_timezone_asof_is_normalized_and_serialized_as_utc(monkeypatch, capsys):
    client, constructed, calls = _install(monkeypatch, result=_result())

    exit_code = cli.main(_argv(as_of_event_time="2024-01-01T06:00:00+03:00"))

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert constructed == [(URI, {"serverSelectionTimeoutMS": 10000})]
    assert calls[0][1]["as_of_event_time"] == AS_OF
    assert output["as_of_event_time"] == "2024-01-01T03:00:00.000Z"
    assert client.closed


@pytest.mark.parametrize(
    "timestamp",
    [
        "2024-01-01T03:00:00",
        "not-a-timestamp",
        "2024-01-01T03:00:00.000001Z",
    ],
)
def test_naive_malformed_or_submillisecond_asof_is_rejected(monkeypatch, timestamp):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(as_of_event_time=timestamp))

    assert error.value.code == 2
    assert constructed == []


@pytest.mark.parametrize("weight", ["-0.1", "1.01", "nan", "inf", "nope"])
def test_invalid_lambda_is_rejected_before_client_creation(monkeypatch, weight):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(lambda_weight=weight))

    assert error.value.code == 2
    assert constructed == []


def test_valid_lambda_override_and_defaults_construct_accepted_configs(monkeypatch, capsys):
    client, _constructed, calls = _install(monkeypatch)

    assert cli.main(_argv(lambda_weight="0.4")) == 0

    config, service_arguments = calls[0]
    assert config.mongodb_uri == URI
    assert config.mongo_database == DATABASE
    assert config.recommendation_k == 20
    assert config.mongo_recommendation_collection == "user_recommendations"
    assert service_arguments["ranking_config"] == HybridRankingConfig(lambda_weight=0.4)
    assert service_arguments["max_event_age"] is None
    assert json.loads(capsys.readouterr().out)["ranking_mode"] == "hybrid"
    assert client.closed


def test_lambda_defaults_to_existing_hybrid_config_value(monkeypatch, capsys):
    _client, _constructed, calls = _install(monkeypatch)

    assert cli.main(_argv()) == 0

    assert calls[0][1]["ranking_config"].lambda_weight == 0.7
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("seconds", ["-1", "nan", "inf", "invalid"])
def test_invalid_event_age_threshold_is_rejected(monkeypatch, seconds):
    _client, constructed, _calls = _install(monkeypatch)

    with pytest.raises(SystemExit) as error:
        cli.main(_argv(max_event_age_seconds=seconds))

    assert error.value.code == 2
    assert constructed == []


def test_explicit_maximum_event_age_is_propagated_as_timedelta(monkeypatch):
    client, _constructed, calls = _install(monkeypatch)

    assert cli.main(_argv(max_event_age_seconds="86400")) == 0

    assert calls[0][1]["max_event_age"] == timedelta(days=1)
    assert client.closed


def test_success_json_matches_required_machine_readable_contract(monkeypatch, capsys):
    result = _result()
    client, _constructed, _calls = _install(monkeypatch, result=result)

    assert cli.main(_argv()) == 0

    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.err == ""
    assert document == {
        "status": "published",
        "user_id": 42,
        "serving_version": "hybrid:" + "b" * 64,
        "published_rows": 3,
        "ranking_mode": "hybrid",
        "fallback_reason": None,
        "source_run_id": "cli-run-001",
        "model_version": "cli-als-v1",
        "graph_version": "cli-graph-v1",
        "trend_source_namespace": "demo-prior-fixture",
        "trend_window_end": "2024-01-01T02:00:00.000Z",
        "trend_content_sha256": "a" * 64,
        "trend_availability_status": "available",
        "readiness_pointer_id": "hybrid:42",
        "as_of_event_time": "2024-01-01T03:00:00.000Z",
    }
    assert client.closed


def test_cold_and_als_fallback_results_have_clear_json_modes(monkeypatch, capsys):
    _install(
        monkeypatch,
        result=_result(
            mode="cold_user_trend_only",
            reason="user_has_no_als_recommendations",
            count=2,
        ),
    )
    assert cli.main(_argv()) == 0
    cold = json.loads(capsys.readouterr().out)
    assert cold["ranking_mode"] == "cold_user_trend_only"
    assert cold["fallback_reason"] is None

    _install(
        monkeypatch,
        result=_result(
            mode="als_fallback_trend_unavailable",
            reason="no_matching_snapshot",
            trend_status="missing",
            trend_window_end=None,
            trend_digest=None,
            count=2,
        ),
    )
    assert cli.main(_argv()) == 0
    fallback = json.loads(capsys.readouterr().out)
    assert fallback["ranking_mode"] == "als_fallback_trend_unavailable"
    assert fallback["fallback_reason"] == "no_matching_snapshot"
    assert fallback["trend_availability_status"] == "missing"
    assert fallback["trend_window_end"] is None
    assert fallback["trend_content_sha256"] is None


@pytest.mark.parametrize(
    "failure",
    [
        TrendSnapshotIntegrityError("selected trend bundle is corrupt"),
        ReadinessContractError("active A/B readiness is not ready"),
        HybridServingPublicationError("C6 pointer promotion failed"),
    ],
)
def test_service_failures_exit_nonzero_without_success_json(monkeypatch, capsys, failure):
    client, _constructed, _calls = _install(monkeypatch, error=failure)

    exit_code = cli.main(_argv())

    captured = capsys.readouterr()
    assert exit_code != 0
    assert captured.out == ""
    assert "module4c serve-user failed" in captured.err
    assert str(failure) in captured.err
    assert client.closed


def test_error_output_redacts_mongodb_credentials_and_connection_string(monkeypatch, capsys):
    secret_uri = "mongodb://fixture_user:sup3rsecret@i3-mongo:27017/private"
    client, _constructed, _calls = _install(
        monkeypatch,
        error=RuntimeError(f"connection failed for {secret_uri}"),
    )

    exit_code = cli.main(_argv(mongodb_uri=secret_uri))

    captured = capsys.readouterr()
    assert exit_code != 0
    assert captured.out == ""
    assert "sup3rsecret" not in captured.err
    assert secret_uri not in captured.err
    assert "mongodb://<redacted>" in captured.err
    assert client.closed


def test_client_is_closed_when_service_fails(monkeypatch):
    client, _constructed, _calls = _install(monkeypatch, error=RuntimeError("query failed"))

    assert cli.main(_argv()) == 1

    assert client.closed


def test_cli_does_not_attempt_spark_kafka_or_hdfs_imports(monkeypatch):
    client, _constructed, calls = _install(monkeypatch)
    original_import = builtins.__import__
    forbidden = {"pyspark", "kafka", "hdfs"}

    def guarded_import(name, *args, **kwargs):
        if name.split(".", maxsplit=1)[0] in forbidden:
            raise AssertionError(f"unexpected runtime import: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    assert cli.main(_argv()) == 0

    assert calls
    assert client.closed
