"""Focused tests for the optional Module 3 trend-bundle publisher."""

# ruff: noqa: E402 -- PySpark imports follow an optional dependency guard.

from datetime import datetime, timedelta
import json

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from src.module3 import cli, stream
from src.module3.config import Config
from src.module3.spark import session
from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    TrendBundleConflictError,
    TrendBundleWriteReport,
)


WINDOW_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("product_id", LongType(), True),
        StructField("purchase_count_30m", LongType(), True),
        StructField("purchase_count_120m", LongType(), True),
        StructField("trend_score", DoubleType(), True),
        StructField("trend_rank", IntegerType(), True),
    ]
)


class FakeUpdateOne:
    def __init__(self, filter_document, update_document, upsert):
        self.filter = filter_document
        self.update = update_document
        self.upsert = upsert


class FakeBulkResult:
    def __init__(self, upserted=0, matched=0):
        self.upserted_count = upserted
        self.matched_count = matched


class FakeBundleCollection:
    name = BUNDLE_COLLECTION_NAME

    def __init__(self):
        self.documents = {}
        self.indexes = {}
        self.touched = False

    def index_information(self):
        self.touched = True
        return self.indexes

    def find(self, query, projection=None):
        self.touched = True
        identities = query["_id"]["$in"]
        return iter(
            {
                key: value
                for key, value in self.documents[identity].items()
                if projection is None or projection.get(key)
            }
            for identity in identities
            if identity in self.documents
        )

    def create_index(self, keys, **options):
        self.touched = True
        self.indexes[options["name"]] = {
            "key": list(keys),
            **options,
        }

    def drop_index(self, name):
        self.touched = True
        self.indexes.pop(name, None)

    def bulk_write(self, operations, ordered):
        self.touched = True
        assert ordered is False
        upserted = matched = 0
        for operation in operations:
            identity = operation.filter["_id"]
            digest = operation.filter["content_sha256"]
            if identity in self.documents:
                assert self.documents[identity]["content_sha256"] == digest
                matched += 1
            else:
                document = operation.update["$setOnInsert"]
                self.documents[identity] = dict(document)
                upserted += 1
        return FakeBulkResult(upserted, matched)


class FakeMongoDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        assert name == BUNDLE_COLLECTION_NAME
        return self.collection


class FakeMongoClient:
    def __init__(self, collection, events=None):
        self.collection = collection
        self.events = events if events is not None else []
        self.closed = False

    def __enter__(self):
        self.events.append("mongo-enter")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.events.append("mongo-exit")
        self.closed = True
        return False

    def __getitem__(self, database):
        assert database == "instacart"
        return FakeMongoDatabase(self.collection)


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory):
    directory = tmp_path_factory.mktemp("module3-bundle-integration")
    config = Config(
        root=(directory / "fixture").as_uri(),
        master="local[2]",
        shuffle_partitions=2,
        ttl_seconds=0,
    )
    spark = session(config, directory / "events")
    yield spark
    spark.stop()


@pytest.fixture
def fake_update_one(monkeypatch):
    import pymongo

    monkeypatch.setattr(pymongo, "UpdateOne", FakeUpdateOne)


def configured_pipeline(monkeypatch, top_k, events, row_report=None):
    ranked = object()
    monkeypatch.setattr(stream, "rank_trending_batch", lambda batch, _config: (events.append("rank"), ranked)[1])
    monkeypatch.setattr(stream, "select_top_k", lambda value, _config: (events.append("top-k"), top_k)[1])

    def write_rows(batch, batch_id, _config):
        events.append("rows")
        assert batch is top_k
        assert batch_id == 7
        return row_report or {
            "batch_id": batch_id,
            "rows": 2,
            "skipped": False,
            "windows": 1,
            "max_rows_per_window": 2,
            "top_k_per_window": 20,
            "replaced_documents": 0,
            "duration_ms": 1.25,
            "upserted": 2,
            "modified": 0,
            "matched": 0,
        }

    monkeypatch.setattr(stream, "write_mongo", write_rows)


def test_config_defaults_and_bundle_opt_in_validation():
    default = Config()
    assert default.publish_trend_bundles is False
    assert default.trend_source_namespace is None
    assert default.trend_source_scope == "unknown_or_mixed"

    with pytest.raises(ValueError, match="namespace is required"):
        Config(publish_trend_bundles=True)
    with pytest.raises(ValueError, match="between 1 and 20"):
        Config(
            publish_trend_bundles=True,
            trend_source_namespace="instacart:stream:orders-v1",
            mongo_top_k_per_window=21,
        )


@pytest.mark.parametrize(
    "namespace",
    ["", " padded ", "bad name", "bad?name", "éclair", "a" * 257],
)
def test_config_rejects_invalid_stable_namespace(namespace):
    with pytest.raises(ValueError, match="trend_source_namespace"):
        Config(
            publish_trend_bundles=True,
            trend_source_namespace=namespace,
        )


def test_config_environment_reads_opt_in_values(monkeypatch):
    monkeypatch.setenv("MODULE3_PUBLISH_TREND_BUNDLES", "true")
    monkeypatch.setenv("MODULE3_TREND_SOURCE_NAMESPACE", "instacart:orders:dev-v1")
    monkeypatch.setenv("MODULE3_TREND_SOURCE_SCOPE", "unknown_or_mixed")

    config = Config.environment()

    assert config.publish_trend_bundles is True
    assert config.trend_source_namespace == "instacart:orders:dev-v1"
    assert config.trend_source_scope == "unknown_or_mixed"


def _run_cli(tmp_path, monkeypatch, args):
    tmp_path.mkdir(parents=True, exist_ok=True)
    reports = []
    monkeypatch.setattr(cli, "validate_kafka", lambda _config: {"ok": True})
    monkeypatch.setattr(cli, "validate_mongo", lambda _config: {"ok": True})
    monkeypatch.setattr(cli, "save", lambda _path, report: reports.append(report))
    result = cli.main(["validate", "--output", str(tmp_path), *args])
    assert result == 0
    return reports[0]["environment"]["config"]


def test_cli_keeps_old_defaults_and_adds_opt_in_flags(tmp_path, monkeypatch):
    for variable in (
        "MODULE3_PUBLISH_TREND_BUNDLES",
        "MODULE3_TREND_SOURCE_NAMESPACE",
        "MODULE3_TREND_SOURCE_SCOPE",
    ):
        monkeypatch.delenv(variable, raising=False)

    defaults = _run_cli(tmp_path / "default", monkeypatch, [])
    assert defaults["publish_trend_bundles"] is False
    assert defaults["trend_source_namespace"] is None
    assert defaults["trend_source_scope"] == "unknown_or_mixed"
    assert defaults["topic"] == Config.topic
    assert defaults["mongo_collection"] == Config.mongo_collection
    assert defaults["mongo_top_k_per_window"] == Config.mongo_top_k_per_window
    assert defaults["trigger_interval"] == Config.trigger_interval

    enabled = _run_cli(
        tmp_path / "enabled",
        monkeypatch,
        [
            "--publish-trend-bundles",
            "--trend-source-namespace",
            "instacart:orders:fixture-v1",
            "--trend-source-scope",
            "fixture",
        ],
    )
    assert enabled["publish_trend_bundles"] is True
    assert enabled["trend_source_namespace"] == "instacart:orders:fixture-v1"
    assert enabled["trend_source_scope"] == "fixture"


def test_disabled_publisher_keeps_legacy_report_and_never_connects(tmp_path, monkeypatch):
    events = []
    top_k = object()
    configured_pipeline(monkeypatch, top_k, events)

    def forbidden_client(*_args, **_kwargs):
        raise AssertionError("disabled bundle publication must not connect to MongoDB")

    monkeypatch.setattr("pymongo.MongoClient", forbidden_client)
    monkeypatch.setattr(
        stream,
        "write_trend_bundles",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("disabled bundle publication must not call its writer")
        ),
    )
    sink_log = tmp_path / "sink_batches.jsonl"
    config = Config()

    result = stream._foreach_batch(object(), 7, config, sink_log)

    assert events == ["rank", "top-k", "rows"]
    assert "trend_bundles" not in result
    assert set(result) == {
        "batch_id",
        "rows",
        "skipped",
        "windows",
        "max_rows_per_window",
        "top_k_per_window",
        "replaced_documents",
        "duration_ms",
        "upserted",
        "modified",
        "matched",
    }
    assert json.loads(sink_log.read_text(encoding="utf-8")) == result


def test_enabled_publisher_runs_after_rows_and_passes_source_contract(monkeypatch):
    events = []
    top_k = object()
    configured_pipeline(monkeypatch, top_k, events)
    collection = FakeBundleCollection()
    client = FakeMongoClient(collection, events)
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: client)
    seen = {}

    def write_bundles(batch, actual_collection, config, **kwargs):
        events.append("bundles")
        seen.update(batch=batch, collection=actual_collection, config=config, kwargs=kwargs)
        return TrendBundleWriteReport(
            bundles_created=2,
            windows_processed=2,
            trend_rows_represented=4,
            duration_ms=3.5,
        )

    monkeypatch.setattr(stream, "write_trend_bundles", write_bundles)
    config = Config(
        publish_trend_bundles=True,
        trend_source_namespace="instacart:orders:main-v1",
        trend_source_scope="unknown_or_mixed",
        ttl_seconds=123,
    )

    result = stream._publish_finalized_batch(object(), 7, config)

    assert events == ["rank", "top-k", "rows", "mongo-enter", "bundles", "mongo-exit"]
    assert client.closed is True
    assert seen["batch"] is top_k
    assert seen["collection"] is collection
    assert seen["config"] is config
    assert seen["kwargs"] == {
        "source_namespace": "instacart:orders:main-v1",
        "source_scope": "unknown_or_mixed",
        "retention_seconds": 123,
    }
    assert result["rows"] == 2
    assert result["trend_bundles"]["collection"] == BUNDLE_COLLECTION_NAME
    assert result["trend_bundles"]["source_namespace"] == "instacart:orders:main-v1"
    assert result["trend_bundles"]["source_scope"] == "unknown_or_mixed"
    assert result["trend_bundles"]["bundles_created"] == 2
    assert result["trend_bundles"]["windows_processed"] == 2
    assert result["trend_bundles"]["duration_ms"] == 3.5
    assert result["trend_bundles"]["publication_duration_ms"] >= 0


def test_row_failure_prevents_bundle_connection_and_success_log(tmp_path, monkeypatch):
    events = []
    top_k = object()
    configured_pipeline(monkeypatch, top_k, events)

    def fail_rows(*_args, **_kwargs):
        events.append("rows-failed")
        raise RuntimeError("row sink failed")

    monkeypatch.setattr(stream, "write_mongo", fail_rows)
    monkeypatch.setattr(
        "pymongo.MongoClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("bundle connection must follow successful row publication")
        ),
    )
    sink_log = tmp_path / "sink_batches.jsonl"

    with pytest.raises(RuntimeError, match="row sink failed"):
        stream._foreach_batch(
            object(),
            7,
            Config(
                publish_trend_bundles=True,
                trend_source_namespace="instacart:orders:main-v1",
            ),
            sink_log,
        )

    assert events == ["rank", "top-k", "rows-failed"]
    assert not sink_log.exists()


def test_bundle_failure_closes_client_propagates_and_does_not_log(tmp_path, monkeypatch):
    events = []
    top_k = object()
    configured_pipeline(monkeypatch, top_k, events)
    client = FakeMongoClient(FakeBundleCollection(), events)
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: client)

    def fail_bundles(*_args, **_kwargs):
        events.append("bundles-failed")
        raise RuntimeError("bundle conflict")

    monkeypatch.setattr(stream, "write_trend_bundles", fail_bundles)
    sink_log = tmp_path / "sink_batches.jsonl"

    with pytest.raises(RuntimeError, match="bundle conflict"):
        stream._foreach_batch(
            object(),
            7,
            Config(
                publish_trend_bundles=True,
                trend_source_namespace="instacart:orders:main-v1",
            ),
            sink_log,
        )

    assert events == ["rank", "top-k", "rows", "mongo-enter", "bundles-failed", "mongo-exit"]
    assert client.closed is True
    assert not sink_log.exists()


def _window_frame(spark, *, changed=False, empty=False):
    if empty:
        return spark.createDataFrame([], schema=WINDOW_SCHEMA)
    start = datetime(2024, 1, 1)
    end = start + timedelta(hours=2)
    row = (start, end, 101, 6 if changed else 5, 9, 1.0, 1)
    return spark.createDataFrame([row], schema=WINDOW_SCHEMA)


def test_actual_writer_retry_conflict_empty_and_bounded_driver_flow(
    spark_session, fake_update_one, monkeypatch, tmp_path
):
    events = []
    collection = FakeBundleCollection()
    client = FakeMongoClient(collection, events)
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: client)
    row_inputs = []
    empty_frame = _window_frame(spark_session, empty=True)
    monkeypatch.setattr(stream, "rank_trending_batch", lambda batch, _config: batch)
    monkeypatch.setattr(stream, "select_top_k", lambda ranked, _config: ranked)

    def write_rows(batch, batch_id, _config):
        row_inputs.append(batch)
        is_empty = batch is empty_frame
        return {
            "batch_id": batch_id,
            "rows": 0 if is_empty else 1,
            "skipped": is_empty,
            "windows": 0 if is_empty else 1,
        }

    monkeypatch.setattr(stream, "write_mongo", write_rows)
    original_writer = stream.write_trend_bundles
    writer_inputs = []

    def observe_writer(batch, *args, **kwargs):
        writer_inputs.append(batch)
        return original_writer(batch, *args, **kwargs)

    monkeypatch.setattr(stream, "write_trend_bundles", observe_writer)
    config = Config(
        publish_trend_bundles=True,
        trend_source_namespace="instacart:orders:fixture-v1",
        ttl_seconds=0,
    )
    first_frame = _window_frame(spark_session)

    def forbidden_materialization(_self, *args, **kwargs):
        raise AssertionError("integration must not collect the complete Top-K batch")

    monkeypatch.setattr(DataFrame, "collect", forbidden_materialization)
    monkeypatch.setattr(DataFrame, "toPandas", forbidden_materialization)

    log = tmp_path / "sink_batches.jsonl"
    first = stream._foreach_batch(first_frame, 7, config, log)
    retry = stream._foreach_batch(first_frame, 7, config, log)

    assert first["trend_bundles"]["bundles_created"] == 1
    assert retry["trend_bundles"]["bundles_created"] == 0
    assert retry["trend_bundles"]["identical_content_retries"] == 1
    assert len(collection.documents) == 1
    original_document = dict(next(iter(collection.documents.values())))
    assert row_inputs[0] is first_frame and writer_inputs[0] is first_frame
    assert row_inputs[1] is first_frame and writer_inputs[1] is first_frame

    conflicting_log = tmp_path / "conflicting.jsonl"
    with pytest.raises(TrendBundleConflictError, match="conflicting content"):
        stream._foreach_batch(_window_frame(spark_session, changed=True), 7, config, conflicting_log)
    assert next(iter(collection.documents.values())) == original_document
    assert not conflicting_log.exists()

    empty_collection = FakeBundleCollection()
    empty_client = FakeMongoClient(empty_collection)
    monkeypatch.setattr("pymongo.MongoClient", lambda *_args, **_kwargs: empty_client)
    empty_log = tmp_path / "empty.jsonl"
    empty_result = stream._foreach_batch(
        empty_frame,
        7,
        config,
        empty_log,
    )
    assert empty_result["skipped"] is True
    assert empty_result["trend_bundles"]["bundles_created"] == 0
    assert empty_result["trend_bundles"]["windows_processed"] == 0
    assert empty_collection.documents == {}
    assert empty_collection.touched is False
    assert json.loads(empty_log.read_text(encoding="utf-8"))["trend_bundles"]["windows_processed"] == 0
