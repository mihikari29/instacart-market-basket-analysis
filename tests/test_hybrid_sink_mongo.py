"""Contract tests for immutable Module 4C recommendation publication."""


from __future__ import annotations

from tests.helpers import FakeClient

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from threading import Lock, Thread

import pytest
from bson import BSON

from src.module4.config import Config as Module4Config
from src.module4 import sink_mongo as module4_sink
from src.module4.hybrid import (
    ALSRecommendation,
    CapturedUserSources,
    HybridRankingConfig,
    HybridPublicationConflictError,
    HybridPublicationPromotionError,
    HybridPublicationResult,
    HybridPublicationWriteError,
    HybridSnapshotContractError,
    ProductMetadata,
    Recommendation,
    TrendRecommendation,
    TrendSnapshotReadResult,
    VerifiedSourceContext,
    publish_hybrid_recommendations,
    rank_hybrid_recommendations,
)
from src.module4.hybrid.sink_mongo import (
    FINAL_RECOMMENDATIONS_COLLECTION,
    ensure_final_recommendation_indexes,
    prepare_hybrid_snapshot,
)


USER_ID = 7
AS_OF = datetime(2024, 1, 1, 4, 0, 0, 123000, tzinfo=timezone.utc)
WINDOW_START = AS_OF - timedelta(hours=2)
WINDOW_END = AS_OF - timedelta(hours=1)
PROCESSING_TIME = datetime(2025, 1, 1, 0, 0, 0, 456000, tzinfo=timezone.utc)
RUN_ID = "module4-run-001"
MODEL_VERSION = "als-v1"
GRAPH_VERSION = "graph-v1"
NAMESPACE = "instacart:trend:v1"
SCOPE = "unknown_or_mixed"


def _captured(
    user_id=USER_ID,
    *,
    als_rows=((1, 10.0), (2, 4.0)),
    catalog=((1, "product-1"), (2, "product-2"), (3, "product-3")),
    run_id=RUN_ID,
    model_version=MODEL_VERSION,
    graph_version=GRAPH_VERSION,
):
    recommendations = tuple(
        ALSRecommendation(user_id, product_id, score, rank)
        for rank, (product_id, score) in enumerate(als_rows, start=1)
    )
    products = tuple(ProductMetadata(product_id, name) for product_id, name in catalog)
    return CapturedUserSources(
        user_id=user_id,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=recommendations,
        product_catalog=products,
        missing_prospective_trend_product_ids=(),
        run_id=run_id,
        model_version=model_version,
        graph_version=graph_version,
    )


def _trend_result(
    *,
    available=True,
    namespace=NAMESPACE,
    scope=SCOPE,
    window_start=WINDOW_START,
    window_end=WINDOW_END,
    digest="a" * 64,
    specs=((2, 0.8), (3, 0.4)),
    status=None,
    reason=None,
):
    if not available:
        return TrendSnapshotReadResult(
            available=False,
            status=status or "missing",
            reason=reason or "no_matching_snapshot",
            source_namespace=namespace,
            source_scope=scope,
            window_start=window_start,
            window_end=window_end,
            published_at=None,
            expires_at=None,
            schema_version=None,
            config_hash=None,
            content_sha256=digest if status in {"expired", "stale"} else None,
            trend_recommendations=(),
        )
    rows = tuple(
        TrendRecommendation(product_id, score, rank)
        for rank, (product_id, score) in enumerate(specs, start=1)
    )
    return TrendSnapshotReadResult(
        available=True,
        status="available",
        reason=None,
        source_namespace=namespace,
        source_scope=scope,
        window_start=window_start,
        window_end=window_end,
        published_at=WINDOW_END + timedelta(minutes=1),
        expires_at=None,
        schema_version=1,
        config_hash="b" * 64,
        content_sha256=digest,
        trend_recommendations=rows,
    )


def _rank(captured, trend, config=None):
    ranking_config = config or HybridRankingConfig(lambda_weight=0.5)
    result = rank_hybrid_recommendations(
        captured.user_id,
        captured.als_recommendations,
        trend.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        ranking_config,
    )
    return ranking_config, result


def _prepare(
    *,
    captured=None,
    trend=None,
    config=None,
    as_of=AS_OF,
    ranking_result=None,
):
    captured = captured or _captured()
    trend = trend or _trend_result()
    config, computed = _rank(captured, trend, config)
    return prepare_hybrid_snapshot(
        captured,
        trend,
        ranking_result or computed,
        config,
        as_of_event_time=as_of,
    )


class FakeCursor:
    def __init__(self, documents):
        self.documents = list(documents)
        self.limit_value = None

    def limit(self, value):
        self.limit_value = value
        self.documents = self.documents[:value]
        return self

    def __iter__(self):
        assert self.limit_value is not None, "publisher attempted an unbounded row read"
        return iter(self.documents)


class FakeCollection:
    def __init__(self, documents=(), *, name=""):
        self.documents = [dict(document) for document in documents]
        self.name = name
        self.indexes = {}
        self.create_index_calls = []
        self.find_calls = []
        self.events = []
        self.timeline = None
        self.insert_fail_before = False
        self.insert_fail_after = None
        self.on_insert = None
        self.index_fail = False
        self._lock = Lock()

    def create_index(self, keys, **options):
        self.events.append("create_index")
        if self.timeline is not None:
            self.timeline.append("create_index")
        self.create_index_calls.append((list(keys), dict(options)))
        if self.index_fail:
            raise OSError("injected index error")
        name = options["name"]
        self.indexes[name] = {"key": list(keys), **options}
        return name

    def index_information(self):
        return {name: dict(index) for name, index in self.indexes.items()}

    def find(self, query):
        self.find_calls.append(dict(query))
        if self.timeline is not None:
            self.timeline.append("row_find")
        assert query, "publisher attempted a collection-wide read"
        with self._lock:
            selected = [
                dict(document)
                for document in self.documents
                if all(document.get(field) == value for field, value in query.items())
            ]
        return FakeCursor(selected)

    def insert_many(self, documents, ordered=False):
        assert ordered is False
        self.events.append("insert_many")
        if self.timeline is not None:
            self.timeline.append("insert_many")
        if self.insert_fail_before:
            raise OSError("injected insert failure")
        documents = [dict(document) for document in documents]
        with self._lock:
            inserted = 0
            for document in documents:
                if any(existing.get("_id") == document["_id"] for existing in self.documents):
                    raise RuntimeError("duplicate _id")
                self.documents.append(document)
                inserted += 1
                if self.on_insert is not None:
                    self.on_insert()
                if self.insert_fail_after is not None and inserted >= self.insert_fail_after:
                    self.insert_fail_after = None
                    raise OSError("injected partial insert failure")
        return type("InsertResult", (), {"inserted_ids": [doc["_id"] for doc in documents]})()


class FakeReadinessCollection:
    def __init__(self, active=None):
        self.documents = {}
        if active is not None:
            self.documents["active"] = dict(active)
        self.find_one_calls = []
        self.events = []
        self.timeline = None
        self.fail_insert = False
        self.fail_replace_before = False
        self.fail_replace_after = False

    def find_one(self, query):
        self.find_one_calls.append(dict(query))
        self.events.append(("find_one", query.get("_id")))
        if self.timeline is not None:
            self.timeline.append(f"find_one:{query.get('_id')}")
        return self.documents.get(query.get("_id"))

    def insert_one(self, document):
        self.events.append(("insert_one", document["_id"]))
        if self.timeline is not None:
            self.timeline.append("insert_pointer")
        if self.fail_insert:
            raise OSError("injected pointer insert error")
        if document["_id"] in self.documents:
            raise RuntimeError("duplicate pointer id")
        self.documents[document["_id"]] = dict(document)
        return type("InsertResult", (), {"inserted_id": document["_id"]})()

    def replace_one(self, query, document, upsert=False):
        assert upsert is False
        self.events.append(("replace_one", query["_id"]))
        if self.timeline is not None:
            self.timeline.append("replace_pointer")
        if self.fail_replace_before:
            raise OSError("injected pre-promotion error")
        current = self.documents.get(query.get("_id"))
        if current is None or not all(current.get(key) == value for key, value in query.items()):
            return type("UpdateResult", (), {"matched_count": 0})()
        self.documents[document["_id"]] = dict(document)
        if self.fail_replace_after:
            raise OSError("injected post-promotion error")
        return type("UpdateResult", (), {"matched_count": 1})()


class FakeDatabase:
    def __init__(self, active=None):
        self.timeline = []
        self.collections = {
            "module4_snapshot_readiness": FakeReadinessCollection(active),
            FINAL_RECOMMENDATIONS_COLLECTION: FakeCollection(name=FINAL_RECOMMENDATIONS_COLLECTION),
        }
        for collection in self.collections.values():
            collection.timeline = self.timeline

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection(name=name))




def _active_readiness(**overrides):
    active = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
    }
    active.update(overrides)
    return active


def _install_lock(monkeypatch, client, *, serialize=False):
    mutex = Lock() if serialize else None
    calls = []

    @contextmanager
    def fake_publication_lock(config, run_id, lease_minutes=180):
        assert isinstance(config, Module4Config)
        assert run_id.startswith("module4c-hybrid-publish-")
        assert lease_minutes == 180
        calls.append(run_id)
        client.database.timeline.append("lock_enter")
        if mutex is None:
            try:
                yield client
            finally:
                client.database.timeline.append("lock_exit")
        else:
            with mutex:
                try:
                    yield client
                finally:
                    client.database.timeline.append("lock_exit")

    monkeypatch.setattr(module4_sink, "publication_lock", fake_publication_lock)
    return calls


def _publish(monkeypatch, prepared, *, client=None, processing_time=PROCESSING_TIME):
    if client is None:
        database = FakeDatabase(_active_readiness())
        client = FakeClient(database)
    _install_lock(monkeypatch, client)
    result = publish_hybrid_recommendations(
        Module4Config(),
        prepared,
        client=client,
        processing_time_utc=processing_time,
    )
    return result, client.database


def test_valid_hybrid_publication_persists_complete_schema_and_pointer(monkeypatch):
    prepared = _prepare()
    result, database = _publish(monkeypatch, prepared)
    rows = database[FINAL_RECOMMENDATIONS_COLLECTION].documents
    pointer = database["module4_snapshot_readiness"].documents["hybrid:7"]

    assert result.status == "published"
    assert result.idempotent is False
    assert result.serving_version == prepared.serving_version
    assert len(rows) == 3
    assert [row["rank"] for row in sorted(rows, key=lambda row: row["rank"])] == [1, 2, 3]
    assert {row["product_id"] for row in rows} == {1, 2, 3}
    assert all(row["serving_version"] == prepared.serving_version for row in rows)
    required_row_fields = {
        "_id", "schema_version", "serving_version", "algorithm_version", "user_id",
        "product_id", "product_name", "als_score", "trend_score", "final_score", "rank",
        "generated_at", "normalized_als_score", "normalized_trend_score", "from_als",
        "from_trend", "lambda_requested", "lambda_effective", "mode", "source_run_id",
        "model_version", "graph_version", "trend_source_namespace", "trend_source_scope",
        "trend_window_start", "trend_window_end", "trend_content_sha256", "as_of_event_time",
        "input_content_sha256", "reason", "als_top_n", "trend_top_m", "output_top_k", "epsilon",
        "ranking_config_sha256",
    }
    assert all(required_row_fields.issubset(row) for row in rows)
    assert all(row["schema_version"] == 1 for row in rows)
    assert all(
        row["_id"] == f"{prepared.serving_version}:{prepared.user_id}:{row['product_id']}"
        for row in rows
    )
    assert all(row["input_content_sha256"] == prepared.input_content_sha256 for row in rows)
    assert all(row["source_run_id"] == RUN_ID for row in rows)
    assert all(row["trend_source_namespace"] == NAMESPACE for row in rows)
    assert pointer["_id"] == "hybrid:7"
    assert pointer["ready"] is True and pointer["status"] == "ready"
    assert pointer["expected_rows"] == 3
    assert pointer["serving_version"] == prepared.serving_version
    assert pointer["ranking_config_sha256"] == prepared.ranking_config_sha256
    assert pointer["als_top_n"] == prepared.als_top_n
    assert pointer["trend_top_m"] == prepared.trend_top_m
    assert pointer["output_top_k"] == prepared.output_top_k
    assert pointer["epsilon"] == prepared.epsilon


def test_als_only_trend_only_empty_and_missing_scores_are_represented_explicitly(monkeypatch):
    als_trend = _trend_result(available=False)
    als_prepared = _prepare(trend=als_trend)
    als_result, als_db = _publish(monkeypatch, als_prepared)
    assert als_result.mode == "als_fallback_trend_unavailable"
    assert all(row["trend_score"] is None and row["from_trend"] is False for row in als_db[FINAL_RECOMMENDATIONS_COLLECTION].documents)

    cold_sources = _captured(als_rows=())
    cold_prepared = _prepare(captured=cold_sources)
    cold_result, cold_db = _publish(monkeypatch, cold_prepared)
    assert cold_result.mode == "cold_user_trend_only"
    assert all(row["als_score"] is None and row["from_als"] is False for row in cold_db[FINAL_RECOMMENDATIONS_COLLECTION].documents)

    empty_sources = _captured(als_rows=(), catalog=())
    empty_prepared = _prepare(captured=empty_sources, trend=als_trend)
    empty_result, empty_db = _publish(monkeypatch, empty_prepared)
    assert empty_result.expected_rows == 0
    assert empty_result.mode == "no_candidates"
    assert empty_result.reason
    assert empty_db[FINAL_RECOMMENDATIONS_COLLECTION].documents == []
    empty_pointer = empty_db["module4_snapshot_readiness"].documents["hybrid:7"]
    assert empty_pointer["expected_rows"] == 0
    assert empty_pointer["mode"] == "no_candidates"
    assert empty_pointer["reason"]


def test_present_zero_modality_scores_remain_distinct_from_missing_scores():
    captured = _captured(als_rows=((1, 0.0),), catalog=((1, "product-1"),))
    trends = _trend_result(specs=((1, 0.0),))
    prepared = _prepare(captured=captured, trend=trends)
    row = prepared.rows[0]
    assert row.als_score == 0.0 and row.trend_score == 0.0
    assert row.from_als is True and row.from_trend is True
    assert row.normalized_als_score == 0.0 and row.normalized_trend_score == 0.0


def test_indexes_are_exact_unique_and_idempotent():
    collection = FakeCollection()
    ensure_final_recommendation_indexes(collection)
    ensure_final_recommendation_indexes(collection)

    assert collection.indexes["uq_serving_user_product"]["key"] == [
        ("serving_version", 1), ("user_id", 1), ("product_id", 1)
    ]
    assert collection.indexes["uq_serving_user_rank"]["key"] == [
        ("serving_version", 1), ("user_id", 1), ("rank", 1)
    ]
    assert collection.indexes["uq_serving_user_product"]["unique"] is True
    assert collection.indexes["uq_serving_user_rank"]["unique"] is True
    assert len(collection.create_index_calls) == 4


def test_serving_version_is_stable_order_invariant_and_source_sensitive():
    captured = _captured()
    trend = _trend_result()
    config, result = _rank(captured, trend)
    first = prepare_hybrid_snapshot(captured, trend, result, config, as_of_event_time=AS_OF)
    reordered_sources = replace(
        captured,
        als_recommendations=tuple(reversed(captured.als_recommendations)),
        product_catalog=tuple(reversed(captured.product_catalog)),
    )
    reordered_trend = replace(trend, trend_recommendations=tuple(reversed(trend.trend_recommendations)))
    reordered_result = replace(result, recommendations=tuple(reversed(result.recommendations)))
    second = prepare_hybrid_snapshot(
        reordered_sources,
        reordered_trend,
        reordered_result,
        config,
        as_of_event_time=AS_OF,
    )
    assert first.serving_version == second.serving_version
    assert first.input_content_sha256 == second.input_content_sha256
    assert first.fingerprint_payload_json == second.fingerprint_payload_json
    assert first.input_content_sha256 == hashlib.sha256(
        first.input_fingerprint_payload_json.encode("utf-8")
    ).hexdigest()

    changed_sources = replace(captured, run_id="module4-run-002")
    changed_config, changed_result = _rank(changed_sources, trend)
    changed = prepare_hybrid_snapshot(
        changed_sources, trend, changed_result, changed_config, as_of_event_time=AS_OF
    )
    assert changed.serving_version != first.serving_version

    changed_als = _captured(als_rows=((1, 11.0), (2, 4.0)))
    changed_config, changed_result = _rank(changed_als, trend)
    changed_content = prepare_hybrid_snapshot(
        changed_als, trend, changed_result, changed_config, as_of_event_time=AS_OF
    )
    assert changed_content.serving_version != first.serving_version

    later_as_of = _prepare(as_of=AS_OF + timedelta(milliseconds=1))
    assert later_as_of.serving_version != first.serving_version

    changed_trend_identity = replace(trend, content_sha256="c" * 64)
    _, same_ranking = _rank(captured, changed_trend_identity)
    changed_trend = prepare_hybrid_snapshot(
        captured,
        changed_trend_identity,
        same_ranking,
        config,
        as_of_event_time=AS_OF,
    )
    assert changed_trend.serving_version != first.serving_version

    versioned_config = replace(config, algorithm_version="hybrid-v2")
    _, versioned_ranking = _rank(captured, trend, versioned_config)
    changed_algorithm = prepare_hybrid_snapshot(
        captured, trend, versioned_ranking, versioned_config, as_of_event_time=AS_OF
    )
    assert changed_algorithm.serving_version != first.serving_version


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: (replace(rows[0], final_score=float("nan")), *rows[1:]),
        lambda rows: (replace(rows[0], normalized_als_score=float("inf")), *rows[1:]),
        lambda rows: (replace(rows[0], product_id=rows[1].product_id), *rows[1:]),
        lambda rows: (replace(rows[0], rank=rows[1].rank), *rows[1:]),
        lambda rows: tuple(replace(row, rank=row.rank + 1) for row in rows),
    ],
)
def test_invalid_ranked_rows_are_rejected(mutate):
    prepared = _prepare()
    _, computed = _rank(_captured(), _trend_result())
    bad_result = replace(computed, recommendations=mutate(computed.recommendations))
    config, _ = _rank(_captured(), _trend_result())
    with pytest.raises(HybridSnapshotContractError):
        prepare_hybrid_snapshot(
            _captured(), _trend_result(), bad_result, config, as_of_event_time=AS_OF
        )
    assert prepared.expected_rows == 3


def test_more_than_twenty_recommendations_and_incorrect_metadata_are_rejected():
    prepared = _prepare()
    captured = _captured()
    trend = _trend_result()
    config, result = _rank(captured, trend)
    over_limit = tuple(
        Recommendation(
            USER_ID,
            product_id,
            f"p{product_id}",
            1.0,
            None,
            0.5,
            0.0,
            0.5,
            rank,
            True,
            False,
        )
        for rank, product_id in enumerate(range(1, 22), start=1)
    )
    with pytest.raises(HybridSnapshotContractError, match="Top-K"):
        prepare_hybrid_snapshot(
            captured,
            trend,
            replace(result, recommendations=over_limit),
            config,
            as_of_event_time=AS_OF,
        )
    with pytest.raises(HybridSnapshotContractError, match="does not match"):
        prepare_hybrid_snapshot(
            captured,
            trend,
            result,
            replace(config, algorithm_version="different-v2"),
            as_of_event_time=AS_OF,
        )
    assert prepared.expected_rows == 3


def test_non_millisecond_as_of_is_rejected_and_bson_dates_round_trip_exactly(monkeypatch):
    with pytest.raises(HybridSnapshotContractError, match="BSON millisecond"):
        _prepare(as_of=AS_OF + timedelta(microseconds=1))
    prepared = _prepare()
    same_instant = _prepare(as_of=AS_OF.astimezone(timezone(timedelta(hours=-5))))
    assert same_instant.serving_version == prepared.serving_version
    result, database = _publish(monkeypatch, prepared)
    row = database[FINAL_RECOMMENDATIONS_COLLECTION].documents[0]
    decoded = BSON.encode(row).decode()
    assert decoded["as_of_event_time"] == AS_OF.replace(tzinfo=None)
    assert decoded["generated_at"] == PROCESSING_TIME.replace(tzinfo=None)
    assert result.generated_at == PROCESSING_TIME


def test_row_failure_and_partial_write_never_advance_pointer_and_retry_converges(monkeypatch):
    prepared = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    rows = client.database[FINAL_RECOMMENDATIONS_COLLECTION]
    rows.insert_fail_after = 1
    with pytest.raises(HybridPublicationWriteError):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert "hybrid:7" not in client.database["module4_snapshot_readiness"].documents
    assert len(rows.documents) == 1

    later_processing_time = PROCESSING_TIME + timedelta(minutes=5)
    result = publish_hybrid_recommendations(
        Module4Config(), prepared, client=client, processing_time_utc=later_processing_time
    )
    assert result.status == "published"
    assert len(rows.documents) == 3
    assert {row["generated_at"] for row in rows.documents} == {PROCESSING_TIME}
    assert client.database["module4_snapshot_readiness"].documents["hybrid:7"]["ready"] is True


def test_failure_before_first_row_write_leaves_no_ready_pointer(monkeypatch):
    prepared = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    rows = client.database[FINAL_RECOMMENDATIONS_COLLECTION]
    rows.insert_fail_before = True
    with pytest.raises(HybridPublicationWriteError):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert rows.documents == []
    assert "hybrid:7" not in client.database["module4_snapshot_readiness"].documents


def test_pre_promotion_failure_preserves_previous_ready_pointer(monkeypatch):
    first = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    publish_hybrid_recommendations(Module4Config(), first, client=client, processing_time_utc=PROCESSING_TIME)
    readiness = client.database["module4_snapshot_readiness"]
    previous = dict(readiness.documents["hybrid:7"])

    newer = _prepare(as_of=AS_OF + timedelta(hours=1))
    readiness.fail_replace_before = True
    with pytest.raises(HybridPublicationPromotionError):
        publish_hybrid_recommendations(
            Module4Config(), newer, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert readiness.documents["hybrid:7"] == previous
    assert any(row["serving_version"] == newer.serving_version for row in client.database[FINAL_RECOMMENDATIONS_COLLECTION].documents)


def test_active_generation_change_during_row_publication_keeps_previous_pointer(monkeypatch):
    first = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    publish_hybrid_recommendations(Module4Config(), first, client=client, processing_time_utc=PROCESSING_TIME)
    readiness = client.database["module4_snapshot_readiness"]
    previous = dict(readiness.documents["hybrid:7"])
    rows = client.database[FINAL_RECOMMENDATIONS_COLLECTION]
    rows.on_insert = lambda: readiness.documents["active"].update(model_version="als-v2")

    newer = _prepare(as_of=AS_OF + timedelta(hours=1))
    with pytest.raises(HybridPublicationConflictError, match="generation changed"):
        publish_hybrid_recommendations(
            Module4Config(), newer, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert readiness.documents["hybrid:7"] == previous


def test_pointer_failure_after_successful_write_is_verified_and_retry_is_idempotent(monkeypatch):
    prepared = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    readiness = client.database["module4_snapshot_readiness"]
    readiness.fail_insert = True
    with pytest.raises(HybridPublicationPromotionError):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert "hybrid:7" not in readiness.documents
    readiness.fail_insert = False
    readiness.fail_replace_after = True
    # Seed a first pointer, then let a later compare-and-set apply and report an error.
    first = _prepare(as_of=AS_OF + timedelta(minutes=1))
    publish_hybrid_recommendations(
        Module4Config(), first, client=client, processing_time_utc=PROCESSING_TIME
    )
    newer = _prepare(as_of=AS_OF + timedelta(hours=1))
    result = publish_hybrid_recommendations(
        Module4Config(), newer, client=client, processing_time_utc=PROCESSING_TIME
    )
    assert result.status == "published"
    assert readiness.documents["hybrid:7"]["serving_version"] == newer.serving_version


def test_same_content_retry_is_idempotent_and_different_content_cannot_rewrite_rows(monkeypatch):
    prepared = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    first = publish_hybrid_recommendations(
        Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
    )
    second = publish_hybrid_recommendations(
        Module4Config(),
        prepared,
        client=client,
        processing_time_utc=PROCESSING_TIME + timedelta(minutes=1),
    )
    assert first.idempotent is False
    assert second.idempotent is True
    assert len(client.database[FINAL_RECOMMENDATIONS_COLLECTION].documents) == 3

    row = client.database[FINAL_RECOMMENDATIONS_COLLECTION].documents[0]
    row["final_score"] += 0.01
    pointer_before = dict(client.database["module4_snapshot_readiness"].documents["hybrid:7"])
    with pytest.raises(HybridPublicationConflictError, match="immutable"):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert client.database["module4_snapshot_readiness"].documents["hybrid:7"] == pointer_before


def test_manually_tampered_prepared_configuration_is_rejected_before_any_write(monkeypatch):
    prepared = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    with pytest.raises(HybridSnapshotContractError, match="configuration"):
        publish_hybrid_recommendations(
            Module4Config(),
            replace(prepared, epsilon=0.25),
            client=client,
            processing_time_utc=PROCESSING_TIME,
        )
    assert client.database[FINAL_RECOMMENDATIONS_COLLECTION].documents == []
    assert client.database[FINAL_RECOMMENDATIONS_COLLECTION].create_index_calls == []


def test_newer_as_of_replaces_older_pointer_within_the_same_source_namespace(monkeypatch):
    older = _prepare()
    newer = _prepare(as_of=AS_OF + timedelta(hours=1))
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    publish_hybrid_recommendations(Module4Config(), older, client=client, processing_time_utc=PROCESSING_TIME)
    result = publish_hybrid_recommendations(
        Module4Config(), newer, client=client, processing_time_utc=PROCESSING_TIME
    )
    pointer = client.database["module4_snapshot_readiness"].documents["hybrid:7"]
    assert result.serving_version == newer.serving_version
    assert pointer["serving_version"] == newer.serving_version
    assert pointer["as_of_event_time"] == newer.as_of_event_time


@pytest.mark.parametrize("active_status", ["publishing", "failed"])
def test_unready_or_changed_active_ab_generation_rejects_promotion(active_status, monkeypatch):
    prepared = _prepare()
    active = _active_readiness(status=active_status, ready=False)
    client = FakeClient(FakeDatabase(active))
    _install_lock(monkeypatch, client)
    with pytest.raises(HybridPublicationPromotionError, match="not publishable"):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert "hybrid:7" not in client.database["module4_snapshot_readiness"].documents

    active = _active_readiness(model_version="als-v2")
    client = FakeClient(FakeDatabase(active))
    _install_lock(monkeypatch, client)
    with pytest.raises(HybridPublicationConflictError, match="generation changed"):
        publish_hybrid_recommendations(
            Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert "hybrid:7" not in client.database["module4_snapshot_readiness"].documents


@pytest.mark.parametrize(
    "policy_error",
    ["older", "same_time", "namespace", "generation"],
)
def test_pointer_policy_rejects_stale_same_time_and_incomparable_sources(policy_error, monkeypatch):
    first = _prepare()
    client = FakeClient(FakeDatabase(_active_readiness()))
    _install_lock(monkeypatch, client)
    publish_hybrid_recommendations(Module4Config(), first, client=client, processing_time_utc=PROCESSING_TIME)
    previous = dict(client.database["module4_snapshot_readiness"].documents["hybrid:7"])

    if policy_error == "older":
        candidate = _prepare(as_of=AS_OF - timedelta(minutes=1))
        message = "stale as_of"
    elif policy_error == "same_time":
        candidate = _prepare(trend=_trend_result(specs=((2, 0.7), (3, 0.3))))
        message = "same as_of_event_time"
    elif policy_error == "namespace":
        candidate = _prepare(
            trend=_trend_result(namespace="instacart:other:v1"),
            as_of=AS_OF + timedelta(minutes=1),
        )
        message = "trend source namespaces"
    else:
        client.database["module4_snapshot_readiness"].documents["active"][
            "model_version"
        ] = "als-v2"
        sources = _captured(model_version="als-v2")
        candidate = _prepare(captured=sources, as_of=AS_OF + timedelta(minutes=1))
        message = "changed A/B source generations"

    with pytest.raises(HybridPublicationConflictError, match=message):
        publish_hybrid_recommendations(
            Module4Config(), candidate, client=client, processing_time_utc=PROCESSING_TIME
        )
    assert client.database["module4_snapshot_readiness"].documents["hybrid:7"] == previous


def test_per_user_pointer_isolation_active_document_untouched_and_reads_are_bounded(monkeypatch):
    active = _active_readiness()
    client = FakeClient(FakeDatabase(active))
    _install_lock(monkeypatch, client)
    first = _prepare()
    second_sources = _captured(user_id=8)
    second = _prepare(captured=second_sources)
    publish_hybrid_recommendations(Module4Config(), first, client=client, processing_time_utc=PROCESSING_TIME)
    publish_hybrid_recommendations(Module4Config(), second, client=client, processing_time_utc=PROCESSING_TIME)

    readiness = client.database["module4_snapshot_readiness"]
    assert readiness.documents["active"] == active
    assert readiness.documents["hybrid:7"]["user_id"] == 7
    assert readiness.documents["hybrid:8"]["user_id"] == 8
    assert readiness.documents["hybrid:7"]["serving_version"] == first.serving_version
    assert readiness.documents["hybrid:8"]["serving_version"] == second.serving_version
    rows = client.database[FINAL_RECOMMENDATIONS_COLLECTION]
    assert rows.find_calls
    assert all(set(query) == {"serving_version", "user_id"} for query in rows.find_calls)
    assert all(len(rows.documents) <= 20 for _ in [0])


def test_publication_order_writes_and_verifies_rows_before_lock_pointer_critical_section(monkeypatch):
    client = FakeClient(FakeDatabase(_active_readiness()))
    lock_calls = _install_lock(monkeypatch, client)
    prepared = _prepare()
    publish_hybrid_recommendations(Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME)
    timeline = client.database.timeline
    assert timeline.index("insert_many") < timeline.index("lock_enter")
    assert timeline.index("row_find", timeline.index("insert_many")) < timeline.index("lock_enter")
    assert timeline.index("lock_enter") < timeline.index("find_one:active")
    assert len(lock_calls) == 1
    assert client.database["module4_snapshot_readiness"].documents["hybrid:7"]["expected_rows"] == 3


def test_concurrent_initial_publications_never_regress_pointer_to_older_as_of(monkeypatch):
    client = FakeClient(FakeDatabase(_active_readiness()))
    lock_calls = _install_lock(monkeypatch, client, serialize=True)
    older = _prepare()
    newer = _prepare(as_of=AS_OF + timedelta(hours=1))
    outcomes = []

    def run(prepared):
        try:
            outcomes.append(
                publish_hybrid_recommendations(
                    Module4Config(), prepared, client=client, processing_time_utc=PROCESSING_TIME
                )
            )
        except Exception as exc:  # Captured so thread failures are asserted by the caller.
            outcomes.append(exc)

    threads = [Thread(target=run, args=(prepared,)) for prepared in (newer, older)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    pointer = client.database["module4_snapshot_readiness"].documents["hybrid:7"]
    assert pointer["serving_version"] == newer.serving_version
    assert pointer["as_of_event_time"] == newer.as_of_event_time
    assert len(lock_calls) == 2
    assert all(
        isinstance(outcome, HybridPublicationResult)
        or isinstance(outcome, HybridPublicationConflictError)
        for outcome in outcomes
    )
    assert len([outcome for outcome in outcomes if not isinstance(outcome, Exception)]) >= 1


def test_preparation_does_not_mutate_c1_c3_c5_input_objects():
    captured = _captured()
    trend = _trend_result()
    config, ranking = _rank(captured, trend)
    captured_before = replace(captured)
    trend_before = replace(trend)
    ranking_before = replace(ranking)
    recommendations_before = tuple(ranking.recommendations)
    prepared = prepare_hybrid_snapshot(
        captured, trend, ranking, config, as_of_event_time=AS_OF
    )
    assert prepared.rows == recommendations_before
    assert captured == captured_before
    assert trend == trend_before
    assert ranking == ranking_before

def test_preparation_preserves_pagerank_scores():
    products = (
        ProductMetadata(1, "product-1", pagerank=0.85),
        ProductMetadata(2, "product-2", pagerank=0.15),
    )
    captured = replace(_captured(), product_catalog=products)
    trend = _trend_result()
    config, ranking = _rank(captured, trend)
    prepared = prepare_hybrid_snapshot(
        captured, trend, ranking, config, as_of_event_time=AS_OF
    )
    assert len(prepared.rows) == 2
    assert prepared.rows[0].pagerank_score == 0.85
    assert prepared.rows[0].normalized_pagerank_score == pytest.approx(1.0)
    assert prepared.rows[1].pagerank_score == 0.15
    assert prepared.rows[1].normalized_pagerank_score == pytest.approx(0.0)

