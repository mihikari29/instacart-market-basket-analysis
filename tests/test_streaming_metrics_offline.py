"""No-service coverage of metrics publication, progress pairing and read access."""

from copy import deepcopy
from threading import Event
from types import SimpleNamespace

from fastapi.testclient import TestClient
import pytest

from src.module3.latency import summarize_ingestion_to_output
from src.module3.metrics import build_snapshot, publish_snapshot, validate_snapshot
from src.module3.progress_log import progress_logger
from src.module4.dashboard.api import create_app
from src.module4.dashboard.config import ApiConfig
from src.module4.dashboard.ui.api_client import _validate_response


def snapshot():
    return build_snapshot(
        {"id": "query-1", "batchId": 3, "numInputRows": 10, "inputRowsPerSecond": 5., "processedRowsPerSecond": 7.},
        {"batch_id": 3, "ingestion_to_output_latency": summarize_ingestion_to_output(
            {"count": 2, "sum": 2200, "min": 1000, "max": 1200}, 1100)},
        source_namespace="test/replay", source_scope="fixture", run_id="run-1",
    )


def test_metrics_pair_same_batch_and_preserve_clock_skew():
    doc = snapshot()
    assert doc["ingestion_to_output_latency"]["clock_skew_detected"] is True
    assert doc["ingestion_to_output_latency"]["latency_ms"] == {"minimum": -100, "mean": 0, "maximum": 100}
    with pytest.raises(ValueError, match="batch identities"):
        build_snapshot({"batchId": 2}, {"batch_id": 3}, source_namespace="test", source_scope="fixture", run_id="run")


@pytest.mark.parametrize("change", [
    {"input_rows_per_second": float("inf")}, {"num_input_rows": True}, {"batch_id": -1},
    {"schema_version": 2}, {"captured_at": "2026-10-18T10:00:00"}, {"extra": "unbounded"},
])
def test_metrics_reject_invalid_documents(change):
    with pytest.raises(ValueError):
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


def test_progress_publishes_latest_completed_batch_once_and_surfaces_failure(tmp_path):
    query = SimpleNamespace(recentProgress=[{"batchId": 0}, {"batchId": 1}], isActive=False)
    stop = Event()
    stop.set()
    calls, errors = [], []
    thread = progress_logger(query, tmp_path, stop, calls.append, errors)
    thread.start()
    thread.join(2)
    assert calls == [{"batchId": 1}]
    assert errors == []
    def fail(_progress):
        raise RuntimeError("private detail")
    thread = progress_logger(query, tmp_path, stop, fail, errors)
    thread.start()
    thread.join(2)
    assert errors == ["RuntimeError"]


class Database:
    def __init__(self, document=None, error=False):
        self.document, self.error, self.queries = document, error, []
    def __getitem__(self, key):
        assert key == "streaming_metrics"
        return self
    def find_one(self, query, projection, **kwargs):
        self.queries.append((query, projection, kwargs))
        if self.error:
            raise RuntimeError("private detail")
        return deepcopy(self.document)


@pytest.mark.parametrize("document,status", [(None, 200), (snapshot(), 200), ({"invalid": True}, 502)])
def test_read_api_is_allowlisted_bounded_and_client_validated(document, status):
    database = Database(document)
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
    app = create_app(ApiConfig("mongodb://user:pass@invalid:27017/test", "test", ("test/replay",)), database=Database(error=True))
    with TestClient(app) as client:
        response = client.get("/api/v1/streaming/metrics", params={"source_namespace": "test/replay"})
    assert response.status_code == 503
    assert "private detail" not in response.text
