"""No-service coverage of Spark progress pairing and the telemetry API contract."""

from threading import Event, Thread
import time
from types import SimpleNamespace

from bson import BSON
from bson.int64 import Int64
from fastapi.testclient import TestClient
import pytest

from src.module3.latency import summarize_ingestion_to_output
from src.module3.metrics import build_snapshot, publish_snapshot, validate_snapshot
from src.module3.progress_log import CompletedBatchPublisher, progress_logger
from src.module4.dashboard.api import create_app
from src.module4.dashboard.config import ApiConfig
from src.module4.dashboard.streaming_reader import read_streaming_metrics
from src.module4.dashboard.ui.api_client import _validate_response


def snapshot():
    document = build_snapshot(
        {"id": "query-1", "batchId": 3, "numInputRows": 10, "inputRowsPerSecond": 5., "processedRowsPerSecond": 7.},
        {"batch_id": 3, "ingestion_to_output_latency": summarize_ingestion_to_output(
            {"count": 2, "sum": 2200, "min": 1000, "max": 1200}, 1100)},
        source_namespace="test/replay", source_scope="fixture", run_id="run-1",
    )
    return document


def executed_progress(batch_id, *, rows=10):
    return {
        "id": "query-1",
        "batchId": batch_id,
        "numInputRows": rows,
        "inputRowsPerSecond": 5.,
        "processedRowsPerSecond": 7.,
        "durationMs": {"triggerExecution": 5, "addBatch": 2},
    }


def idle_progress(batch_id):
    return {
        "id": "query-1",
        "batchId": batch_id,
        "numInputRows": 0,
        "inputRowsPerSecond": 0.,
        "processedRowsPerSecond": 0.,
        "durationMs": {"triggerExecution": 1},
    }


def run_progress_logger(tmp_path, query, publisher, *, on_error=None, poll_interval=0.01):
    stop = Event()
    stop.set()
    errors = []
    thread = progress_logger(
        query,
        tmp_path,
        stop,
        publisher,
        errors,
        on_error=on_error,
        poll_interval=poll_interval,
    )
    thread.start()
    thread.join(2)
    assert not thread.is_alive()
    return errors


def receipt(batch_id, marker):
    return {"batch_id": batch_id, "marker": marker}


def test_metrics_pair_same_batch_and_preserve_clock_skew():
    doc = snapshot()
    assert doc["ingestion_to_output_latency"]["clock_skew_detected"] is True
    assert doc["ingestion_to_output_latency"]["latency_ms"] == {
        "minimum": -100,
        "mean": 0,
        "maximum": 100,
    }
    with pytest.raises(ValueError, match="batch identities"):
        build_snapshot(
            {"batchId": 2},
            {"batch_id": 3},
            source_namespace="test",
            source_scope="fixture",
            run_id="run",
        )


def test_initial_idle_observation_does_not_consume_batch_identity(tmp_path):
    publications = []
    publisher = CompletedBatchPublisher(lambda progress, result: publications.append((progress, result)))
    errors = run_progress_logger(
        tmp_path, SimpleNamespace(recentProgress=[idle_progress(0)], isActive=False), publisher,
        on_error=publisher.fail,
    )

    assert errors == []
    assert publications == []
    # The same ID remains available for the subsequently executed batch.
    publisher.record_sink_result(0, receipt(0, "same-id"))
    assert publisher(executed_progress(0)) is True
    assert publications == [(executed_progress(0), receipt(0, "same-id"))]


def test_idle_then_executed_batch_reusing_id_pairs_its_own_receipt(tmp_path):
    publications = []
    publisher = CompletedBatchPublisher(lambda progress, result: publications.append((progress, result)))
    publisher.record_sink_result(0, receipt(0, "batch-zero"))
    query = SimpleNamespace(recentProgress=[idle_progress(0), executed_progress(0)], isActive=False)

    errors = run_progress_logger(tmp_path, query, publisher, on_error=publisher.fail)

    assert errors == []
    assert publications == [(executed_progress(0), receipt(0, "batch-zero"))]


def test_executed_zero_input_state_cleanup_is_not_idle(tmp_path):
    publications = []
    publisher = CompletedBatchPublisher(lambda progress, result: publications.append((progress, result)))
    progress = executed_progress(4, rows=0)
    publisher.record_sink_result(4, receipt(4, "cleanup"))

    errors = run_progress_logger(
        tmp_path, SimpleNamespace(recentProgress=[progress], isActive=False), publisher,
        on_error=publisher.fail,
    )

    assert errors == []
    assert publications == [(progress, receipt(4, "cleanup"))]


def test_executed_batch_without_sink_receipt_fails_clearly(tmp_path):
    publisher = CompletedBatchPublisher(lambda _progress, _result: pytest.fail("must not publish"))
    errors = run_progress_logger(
        tmp_path,
        SimpleNamespace(recentProgress=[executed_progress(0)], isActive=False),
        publisher,
        on_error=publisher.fail,
    )

    assert errors == ["completed progress has no matching bounded sink receipt"]
    assert "completed progress has no matching bounded sink receipt" in publisher._failure


def test_background_publication_failure_is_sanitized(tmp_path):
    def fail(_progress, _result):
        raise RuntimeError("mongodb://user:secret@host")

    publisher = CompletedBatchPublisher(fail)
    publisher.record_sink_result(0, receipt(0, "zero"))
    errors = run_progress_logger(
        tmp_path,
        SimpleNamespace(recentProgress=[executed_progress(0)], isActive=False),
        publisher,
        on_error=publisher.fail,
    )

    assert errors == ["stream metrics publication failed"]
    assert "secret" not in str(errors)


def test_bounded_receipt_storage_applies_backpressure_instead_of_evicting():
    publications = []
    publisher = CompletedBatchPublisher(
        lambda progress, result: publications.append((progress["batchId"], result["marker"])),
        max_pending=1,
    )
    publisher.record_sink_result(0, receipt(0, "zero"))
    started = Event()

    def add_second_receipt():
        started.set()
        publisher.record_sink_result(1, receipt(1, "one"))

    thread = Thread(target=add_second_receipt)
    thread.start()
    assert started.wait(1)
    thread.join(0.02)
    assert thread.is_alive()

    assert publisher(executed_progress(0)) is True
    thread.join(1)
    assert not thread.is_alive()
    assert publisher(executed_progress(1)) is True
    assert publications == [(0, "zero"), (1, "one")]


def test_all_new_executed_batches_are_published_once_in_order(tmp_path):
    publications = []
    publisher = CompletedBatchPublisher(lambda progress, result: publications.append((progress["batchId"], result["marker"])))
    progress = [
        executed_progress(0),
        executed_progress(1, rows=0),
        idle_progress(2),
        executed_progress(2),
    ]
    for batch_id in range(3):
        publisher.record_sink_result(batch_id, receipt(batch_id, f"receipt-{batch_id}"))

    errors = run_progress_logger(
        tmp_path, SimpleNamespace(recentProgress=progress), publisher, on_error=publisher.fail
    )

    assert errors == []
    assert publications == [(0, "receipt-0"), (1, "receipt-1"), (2, "receipt-2")]


def test_repeated_historical_progress_is_idempotent_across_polls_and_final_drain(tmp_path):
    progress = [executed_progress(0), executed_progress(1)]

    class Query:
        def __init__(self):
            self.isActive = True
            self.read = Event()
            self.read_count = 0

        @property
        def recentProgress(self):
            self.read_count += 1
            self.read.set()
            return progress

    publications = []
    publisher = CompletedBatchPublisher(lambda record, result: publications.append(record["batchId"]))
    publisher.record_sink_result(0, receipt(0, "zero"))
    publisher.record_sink_result(1, receipt(1, "one"))
    query = Query()
    stop = Event()
    errors = []
    thread = progress_logger(query, tmp_path, stop, publisher, errors, on_error=publisher.fail, poll_interval=0.01)
    thread.start()
    assert query.read.wait(1)
    deadline = time.monotonic() + 1
    while len(publications) < 2 and time.monotonic() < deadline:
        time.sleep(0.001)
    query.isActive = False
    stop.set()
    thread.join(2)

    assert not thread.is_alive()
    assert errors == []
    assert query.read_count >= 2
    assert publications == [0, 1]


def test_shutdown_final_drain_publishes_last_completed_batch_without_thread_leak(tmp_path):
    progress = []

    class Query:
        def __init__(self):
            self.isActive = True
            self.read = Event()

        @property
        def recentProgress(self):
            self.read.set()
            return list(progress)

    publications = []
    publisher = CompletedBatchPublisher(lambda record, result: publications.append(record["batchId"]))
    publisher.record_sink_result(0, receipt(0, "final"))
    query = Query()
    stop = Event()
    errors = []
    thread = progress_logger(query, tmp_path, stop, publisher, errors, on_error=publisher.fail, poll_interval=10)
    thread.start()
    assert query.read.wait(1)
    progress.append(executed_progress(0))
    query.isActive = False
    stop.set()
    thread.join(2)

    assert not thread.is_alive()
    assert errors == []
    assert publications == [0]


@pytest.mark.parametrize("change", [
    {"input_rows_per_second": float("inf")}, {"num_input_rows": True}, {"batch_id": -1},
    {"schema_version": 2}, {"captured_at": "2026-10-18T10:00:00"}, {"extra": "unbounded"},
])
def test_metrics_reject_invalid_documents(change):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        validate_snapshot({**snapshot(), **change})


def test_publication_is_one_namespace_upsert_with_bounded_client(monkeypatch):
    calls = []

    class Client:
        def __init__(self, uri, **kwargs):
            assert kwargs["timeoutMS"] == 10_000

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __getitem__(self, key):
            return self

        def replace_one(self, query, document, **kwargs):
            calls.append((query, document, kwargs))

    monkeypatch.setattr("pymongo.MongoClient", Client)
    publish_snapshot(SimpleNamespace(mongodb_uri="mongodb://invalid", mongo_database="test"), snapshot())
    assert calls[0][0] == {"_id": "test/replay"}
    assert calls[0][2] == {"upsert": True}


def large_integer_snapshot():
    completion_ms = 1_792_348_800_000
    large_count = (1 << 31) + 9
    latency = summarize_ingestion_to_output(
        {"count": large_count, "sum": (completion_ms - 100) * large_count,
         "min": completion_ms - 100, "max": completion_ms - 100},
        completion_ms,
    )
    document = build_snapshot(
        {
            "id": "query-large",
            "batchId": (1 << 31) + 7,
            "numInputRows": (1 << 31) + 11,
            "inputRowsPerSecond": 5.0,
            "processedRowsPerSecond": 7.0,
        },
        {"batch_id": (1 << 31) + 7, "ingestion_to_output_latency": latency},
        source_namespace="test/replay",
        source_scope="fixture",
        run_id="run-large",
    )
    document["ingestion_to_output_latency"]["latency_ms"] = {
        "minimum": 3_000_000_000,
        "mean": 3_000_000_001,
        "maximum": 3_000_000_002,
    }
    return document


class BSONDatabase:
    """Fake Mongo boundary that performs an actual BSON encode/decode round trip."""

    def __init__(self, document=None, error=False):
        self.wire_document = (
            BSON.encode({"_id": document.get("source_namespace", "test/replay"), **document})
            if document is not None
            else None
        )
        self.error = error
        self.queries = []

    def __getitem__(self, key):
        assert key == "streaming_metrics"
        return self

    def find_one(self, query, projection, **kwargs):
        self.queries.append((query, projection, kwargs))
        if self.error:
            raise RuntimeError("private detail")
        if self.wire_document is None:
            return None
        decoded = BSON(self.wire_document).decode()
        return {key: value for key, value in decoded.items() if projection.get(key) == 1}


def test_bson_round_trip_and_actual_reader_accept_current_epoch_int64():
    wire_database = BSONDatabase(large_integer_snapshot())
    decoded = BSON(wire_database.wire_document).decode()
    assert isinstance(decoded["batch_id"], Int64)
    assert isinstance(decoded["num_input_rows"], Int64)
    assert isinstance(decoded["ingestion_to_output_latency"]["sample_count"], Int64)
    assert isinstance(decoded["ingestion_to_output_latency"]["output_completion_epoch_ms"], Int64)
    assert isinstance(decoded["ingestion_to_output_latency"]["latency_ms"]["minimum"], Int64)

    result = read_streaming_metrics(wire_database, "test/replay", max_time_ms=5000)

    assert result["available"] is True
    assert result["metrics"]["batch_id"] == (1 << 31) + 7
    assert result["metrics"]["num_input_rows"] == (1 << 31) + 11
    assert result["metrics"]["ingestion_to_output_latency"]["sample_count"] == (1 << 31) + 9
    assert result["metrics"]["ingestion_to_output_latency"]["output_completion_epoch_ms"] == 1_792_348_800_000
    assert type(result["metrics"]["ingestion_to_output_latency"]["latency_ms"]["minimum"]) is int
    assert all(
        type(result["metrics"][key]) is int
        for key in ("schema_version", "batch_id", "num_input_rows")
    )


def test_bson_round_trip_reaches_fastapi_and_existing_ui_validator():
    wire_database = BSONDatabase(large_integer_snapshot())
    app = create_app(
        ApiConfig("mongodb://user:pass@invalid:27017/test", "test", ("test/replay",)),
        database=wire_database,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/streaming/metrics", params={"source_namespace": "test/replay"})

    assert response.status_code == 200
    body = response.json()
    assert body["source_namespace"] == "test/replay"
    assert body["metrics"]["source_namespace"] == "test/replay"
    assert body["metrics"]["source_scope"] == "fixture"
    assert body["metrics"]["run_id"] == "run-large"
    assert body["metrics"]["query_id"] == "query-large"
    assert body["metrics"]["batch_id"] == (1 << 31) + 7
    assert body["metrics"]["ingestion_to_output_latency"]["output_completion_epoch_ms"] == 1_792_348_800_000
    assert body["metrics"]["ingestion_to_output_latency"]["latency_ms"] == {
        "minimum": 3_000_000_000,
        "mean": 3_000_000_001,
        "maximum": 3_000_000_002,
    }
    assert _validate_response("streaming_metrics", body) == body


@pytest.mark.parametrize("change", [
    {"batch_id": True},
    {"batch_id": 1.0},
    {"num_input_rows": -1},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "sample_count": True}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "sample_count": 1.0}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "sample_count": 0}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "sample_count": -1}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "output_completion_epoch_ms": True}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "output_completion_epoch_ms": 0}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "output_completion_epoch_ms": float(1_792_348_800_000)}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "output_completion_epoch_ms": 1 << 63}},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "sample_count": 1 << 63}},
    {"batch_id": 1 << 63},
    {"ingestion_to_output_latency": {**large_integer_snapshot()["ingestion_to_output_latency"], "unexpected": "field"}},
])
def test_metrics_reject_invalid_integer_and_nested_values(change):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        validate_snapshot({**large_integer_snapshot(), **change})


def test_unavailable_latency_survives_bson_round_trip():
    unavailable = build_snapshot(
        {"id": "query-empty", "batchId": 0, "numInputRows": 0,
         "inputRowsPerSecond": 0.0, "processedRowsPerSecond": 0.0},
        {"batch_id": 0, "ingestion_to_output_latency": summarize_ingestion_to_output(
            {"count": 0, "sum": 0, "min": None, "max": None}, 1_792_348_800_000)},
        source_namespace="test/replay", source_scope="fixture", run_id="run-empty",
    )
    wire_database = BSONDatabase(unavailable)

    result = read_streaming_metrics(wire_database, "test/replay", max_time_ms=5000)

    assert result["available"] is True
    assert result["metrics"]["ingestion_to_output_latency"] == unavailable["ingestion_to_output_latency"]


@pytest.mark.parametrize("document,status", [(None, 200), (snapshot(), 200), ({"invalid": True}, 502)])
def test_read_api_is_allowlisted_bounded_and_client_validated(document, status):
    database = BSONDatabase(document)
    app = create_app(ApiConfig("mongodb://user:pass@invalid:27017/test", "test", ("test/replay",)), database=database)
    with TestClient(app) as client:
        assert client.get("/api/v1/streaming/metrics", params={"source_namespace": "other"}).status_code == 422
        assert database.queries == []
        response = client.get("/api/v1/streaming/metrics", params={"source_namespace": "test/replay"})
        assert response.status_code == status
        if status == 200:
            _validate_response("streaming_metrics", response.json())
        assert database.queries[0][0] == {"_id": "test/replay"}
        assert 0 < database.queries[0][2]["max_time_ms"] <= 5000


def test_database_failure_is_sanitized():
    app = create_app(ApiConfig("mongodb://user:pass@invalid:27017/test", "test", ("test/replay",)), database=BSONDatabase(error=True))
    with TestClient(app) as client:
        response = client.get("/api/v1/streaming/metrics", params={"source_namespace": "test/replay"})
    assert response.status_code == 503
    assert "private detail" not in response.text
