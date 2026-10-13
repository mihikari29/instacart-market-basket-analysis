"""Unit tests for the bounded, verified Module 4C source reader."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import math

import pytest

from src.module4.config import Config as Module4Config
from src.module4c import (
    ReadinessContractError,
    SourceLockError,
    SourceQueryError,
    SourceSnapshotError,
    TrendRecommendation,
    TrendSnapshot,
    capture_verified_user_sources,
    rank_hybrid_recommendations,
)
from src.module4c import data


def readiness(**overrides):
    document = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": "module4-run-001",
        "model_version": "als-v1",
        "graph_version": "graph-v1",
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
    }
    document.update(overrides)
    return document


def als_rows(count=20, *, user_id=7):
    return [
        {
            "user_id": user_id,
            "product_id": 100 + rank,
            "als_score": float(100 - rank),
            "als_rank": rank,
            "model_version": "als-v1",
            "run_id": "module4-run-001",
        }
        for rank in range(1, count + 1)
    ]


def graph_rows(product_ids):
    return [
        {
            "product_id": product_id,
            "product_name": f"Product {product_id}",
            "graph_version": "graph-v1",
            "run_id": "module4-run-001",
        }
        for product_id in product_ids
    ]


class FakeCursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.sort_fields = None
        self.limit_value = None

    def sort(self, fields):
        self.sort_fields = list(fields)
        for field, direction in reversed(self.sort_fields):
            self.rows.sort(key=lambda row: row.get(field), reverse=direction < 0)
        return self

    def limit(self, value):
        self.limit_value = value
        self.rows = self.rows[:value]
        return self

    def __iter__(self):
        return iter(self.rows)


class FakeCollection:
    def __init__(
        self,
        rows=(),
        *,
        forced_rows=None,
        force_query_results=False,
        readiness_rows=None,
        fail_find=False,
    ):
        self.rows = list(rows)
        self.forced_rows = forced_rows
        self.force_query_results = force_query_results
        self.readiness_rows = list(readiness_rows) if readiness_rows is not None else None
        self.fail_find = fail_find
        self.find_calls = []
        self.find_one_calls = []
        self.cursors = []

    def find_one(self, query):
        self.find_one_calls.append(query)
        if self.readiness_rows is not None:
            if not self.readiness_rows:
                return None
            if len(self.readiness_rows) > 1:
                return self.readiness_rows.pop(0)
            return self.readiness_rows[0]
        return next(
            (row for row in self.rows if all(row.get(key) == value for key, value in query.items())),
            None,
        )

    def find(self, query, projection):
        self.find_calls.append((query, projection))
        if self.fail_find:
            raise OSError("fake Mongo read failed")
        source_rows = self.forced_rows if self.forced_rows is not None else self.rows
        selected = []
        for row in source_rows:
            if self.force_query_results:
                selected.append(
                    {key: row[key] for key, enabled in projection.items() if enabled and key in row}
                )
                continue
            matches = True
            for key, value in query.items():
                if isinstance(value, dict) and "$in" in value:
                    matches = row.get(key) in value["$in"]
                else:
                    matches = row.get(key) == value
                if not matches:
                    break
            if matches:
                selected.append({key: row[key] for key, enabled in projection.items() if enabled and key in row})
        cursor = FakeCursor(selected)
        self.cursors.append(cursor)
        return cursor


class FakeDatabase:
    def __init__(self, collections):
        self.collections = collections

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


class FakeClient:
    def __init__(self, database):
        self.database = database

    def __getitem__(self, name):
        assert name == "instacart"
        return self.database


@pytest.fixture
def harness(monkeypatch):
    readiness_collection = FakeCollection(readiness_rows=[readiness()])
    als_collection = FakeCollection(als_rows())
    graph_collection = FakeCollection(graph_rows(range(101, 121)))
    collections = {
        "module4_snapshot_readiness": readiness_collection,
        "user_recommendations": als_collection,
        "product_graph_metrics": graph_collection,
    }
    database = FakeDatabase(collections)
    client = FakeClient(database)
    calls = {"acquire": [], "release": []}

    @contextmanager
    def fake_publication_lock(config, run_id, lease_minutes=180):
        calls["acquire"].append((config, run_id, lease_minutes))
        try:
            yield client
        finally:
            calls["release"].append(run_id)

    monkeypatch.setattr(data, "publication_lock", fake_publication_lock)
    return Module4Config(), collections, calls


def capture(harness, **kwargs):
    config, _, _ = harness
    return capture_verified_user_sources(config, 7, **kwargs)


def test_captures_verified_pair_and_returns_ranker_compatible_data(harness):
    config, collections, calls = harness
    captured = capture_verified_user_sources(config, 7)

    assert captured.user_id == 7
    assert captured.run_id == "module4-run-001"
    assert captured.model_version == "als-v1"
    assert captured.graph_version == "graph-v1"
    assert captured.source_context.als_available is True
    assert captured.source_context.graph_available is True
    assert isinstance(captured.als_recommendations[0], data.ALSRecommendation)
    assert isinstance(captured.product_catalog[0], data.ProductMetadata)
    assert [row.als_rank for row in captured.als_recommendations] == list(range(1, 21))
    result = rank_hybrid_recommendations(
        captured.user_id,
        captured.als_recommendations,
        TrendSnapshot(available=True),
        captured.product_catalog,
        captured.source_context,
    )
    assert result.user_id == 7
    assert len(result.recommendations) == 20
    assert len(calls["acquire"]) == len(calls["release"]) == 1
    assert calls["acquire"][0][1].startswith("module4c-read-")
    assert calls["acquire"][0][2] == 180
    assert collections["module4_snapshot_readiness"].find_one_calls == [
        {"_id": "active"},
        {"_id": "active"},
    ]


@pytest.mark.parametrize(
    ("pointer", "message"),
    [
        (None, "missing"),
        (readiness(status="publishing"), "status"),
        (readiness(status="failed"), "status"),
        (readiness(ready=False), "ready"),
        (readiness(ready=1), "ready"),
        (readiness(run_id=None), "run_id"),
        (readiness(model_version="../bad"), "model_version"),
        (readiness(graph_version=""), "graph_version"),
        (readiness(recommendation_published=False), "ALS publication"),
        (readiness(graph_published=1), "graph publication"),
        (readiness(single_writer_policy="other-lock"), "single-writer policy"),
        (readiness(error="publication failed"), "contains an error"),
    ],
)
def test_rejects_missing_or_inconsistent_readiness(harness, pointer, message):
    _, collections, calls = harness
    collections["module4_snapshot_readiness"].readiness_rows = [] if pointer is None else [pointer]

    with pytest.raises(ReadinessContractError, match=message):
        capture(harness)
    assert calls["release"] == [calls["acquire"][0][1]]


@pytest.mark.parametrize("field", ["run_id", "model_version", "graph_version"])
def test_rejects_each_missing_readiness_version(harness, field):
    _, collections, _ = harness
    collections["module4_snapshot_readiness"].readiness_rows = [readiness(**{field: None})]

    with pytest.raises(ReadinessContractError, match=field):
        capture(harness)


@pytest.mark.parametrize(
    ("collection", "row_change", "message"),
    [
        ("user_recommendations", {"run_id": "another-run"}, "run_id"),
        ("user_recommendations", {"model_version": "another-model"}, "model_version"),
        ("product_graph_metrics", {"run_id": "another-run"}, "run_id"),
        ("product_graph_metrics", {"graph_version": "another-graph"}, "graph_version"),
    ],
)
def test_rejects_wrong_source_identity_even_if_driver_returns_row(
    harness, collection, row_change, message
):
    _, collections, _ = harness
    target = collections[collection]
    source = target.rows if collection == "user_recommendations" else target.rows
    target.forced_rows = [dict(source[0], **row_change)] + source[1:]
    target.force_query_results = True

    with pytest.raises(SourceSnapshotError, match=message):
        capture(harness)


def test_source_queries_use_exact_pair_filters_projections_sort_and_limits(harness):
    _, collections, _ = harness
    capture(harness, prospective_trend_product_ids=[301, 302])

    readiness_collection = collections["module4_snapshot_readiness"]
    als_collection = collections["user_recommendations"]
    graph_collection = collections["product_graph_metrics"]
    assert readiness_collection.find_one_calls == [{"_id": "active"}, {"_id": "active"}]
    assert als_collection.find_calls == [
        (
            {"model_version": "als-v1", "run_id": "module4-run-001", "user_id": 7},
            data.ALS_PROJECTION,
        )
    ]
    assert als_collection.cursors[0].sort_fields == [("als_rank", 1), ("product_id", 1)]
    assert als_collection.cursors[0].limit_value == 21
    assert graph_collection.find_calls == [
        (
            {
                "graph_version": "graph-v1",
                "run_id": "module4-run-001",
                "product_id": {"$in": list(range(101, 121)) + [301, 302]},
            },
            data.GRAPH_PROJECTION,
        )
    ]
    assert graph_collection.cursors[0].sort_fields == [("product_id", 1)]
    assert graph_collection.cursors[0].limit_value == 23
    assert "realtime_trending" not in collections


def test_als_input_is_sorted_deterministically_by_rank_and_product(harness):
    _, collections, _ = harness
    collections["user_recommendations"].rows.reverse()

    captured = capture(harness)

    assert [(row.als_rank, row.product_id) for row in captured.als_recommendations] == [
        (rank, 100 + rank) for rank in range(1, 21)
    ]


@pytest.mark.parametrize(
    ("mutate", "message", "k"),
    [
        (lambda rows: rows.__setitem__(1, dict(rows[1], product_id=rows[0]["product_id"])), "duplicate product", 20),
        (lambda rows: rows.__setitem__(1, dict(rows[1], als_rank=rows[0]["als_rank"])), "duplicate als_rank", 20),
        (lambda rows: rows.__setitem__(9, dict(rows[9], als_rank=11)), "contiguous", 10),
        (lambda rows: rows.__setitem__(0, dict(rows[0], als_score=math.inf)), "finite", 20),
        (lambda rows: rows.__setitem__(0, dict(rows[0], als_score=True)), "finite", 20),
        (lambda rows: rows.__setitem__(0, dict(rows[0], product_id=0)), "positive integer", 20),
        (lambda rows: rows.__setitem__(0, dict(rows[0], user_id=8)), "requested user", 20),
    ],
)
def test_rejects_corrupt_als_rows(harness, mutate, message, k):
    config, collections, _ = harness
    rows = als_rows(k)
    mutate(rows)
    collections["user_recommendations"].forced_rows = rows
    collections["user_recommendations"].force_query_results = True
    config = replace(config, recommendation_k=k)
    harness = (config, collections, harness[2])

    with pytest.raises(SourceSnapshotError, match=message):
        capture(harness)


def test_rejects_oversized_and_truncated_als_results(harness):
    _, collections, _ = harness
    collections["user_recommendations"].forced_rows = als_rows(21)
    with pytest.raises(SourceSnapshotError, match="Top-20"):
        capture(harness)

    collections["user_recommendations"].forced_rows = als_rows(19)
    with pytest.raises(SourceSnapshotError, match="truncated"):
        capture(harness)


def test_valid_new_user_with_no_als_rows_is_a_cold_user(harness):
    _, collections, _ = harness
    # The exact user filter yields no rows for a valid user outside the factor-backed cohort.
    captured = capture_verified_user_sources(harness[0], 999)
    assert captured.als_recommendations == ()
    assert captured.product_catalog == ()
    assert captured.source_context.als_available is True
    assert captured.source_context.graph_available is True
    assert collections["product_graph_metrics"].find_calls == []
    result = rank_hybrid_recommendations(
        captured.user_id,
        captured.als_recommendations,
        TrendSnapshot(available=True),
        captured.product_catalog,
        captured.source_context,
    )
    assert result.metadata.mode == "cold_user_trend_only"


def test_missing_graph_metadata_for_als_candidate_is_a_source_failure(harness):
    _, collections, _ = harness
    collections["product_graph_metrics"].rows.pop()

    with pytest.raises(SourceSnapshotError, match="ALS candidates are missing"):
        capture(harness)


@pytest.mark.parametrize(
    ("row_change", "message"),
    [
        ({"product_id": 9999}, "was not requested"),
        ({"product_name": "Duplicate product row"}, "duplicate product_id"),
    ],
)
def test_rejects_unrequested_or_duplicate_graph_rows(harness, row_change, message):
    _, collections, _ = harness
    graph = collections["product_graph_metrics"]
    graph.forced_rows = graph.rows[:-1] + [dict(graph.rows[0], **row_change)]
    graph.force_query_results = True

    with pytest.raises(SourceSnapshotError, match=message):
        capture(harness)


def test_missing_prospective_trend_catalog_entry_is_returned_for_ranker_exclusion(harness):
    captured = capture(harness, prospective_trend_product_ids=[999])
    assert captured.missing_prospective_trend_product_ids == (999,)
    trend = TrendSnapshot(
        available=True,
        rows=(TrendRecommendation(product_id=999, trend_score=1.0, trend_rank=1),),
    )
    result = rank_hybrid_recommendations(
        captured.user_id,
        captured.als_recommendations,
        trend,
        captured.product_catalog,
        captured.source_context,
    )
    assert result.metadata.excluded_trend_product_ids == (999,)


@pytest.mark.parametrize("name", [None, "", "  ", 12])
def test_rejects_invalid_product_names(harness, name):
    _, collections, _ = harness
    collections["product_graph_metrics"].rows[0]["product_name"] = name

    with pytest.raises(SourceSnapshotError, match="product_name"):
        capture(harness)


def test_no_trend_candidates_means_only_als_graph_ids_are_queried(harness):
    _, collections, _ = harness
    captured = capture(harness)

    query = collections["product_graph_metrics"].find_calls[0][0]
    assert query["product_id"] == {"$in": list(range(101, 121))}
    assert captured.missing_prospective_trend_product_ids == ()


def test_candidate_input_is_bounded_and_deduplicated(harness):
    consumed = []

    def endless_ids():
        for product_id in range(1, 1000000):
            consumed.append(product_id)
            yield product_id

    with pytest.raises(SourceSnapshotError, match="bounded Top-20"):
        capture(harness, prospective_trend_product_ids=endless_ids())
    assert len(consumed) == 21

    with pytest.raises(SourceSnapshotError, match="duplicates"):
        capture(harness, prospective_trend_product_ids=[301, 301])


def test_rejects_nonpositive_requested_user_and_k_above_top20(harness):
    config, _, _ = harness
    with pytest.raises(SourceSnapshotError, match="requested user_id"):
        capture_verified_user_sources(config, True)
    with pytest.raises(ValueError, match="between 1 and 20"):
        capture_verified_user_sources(replace(config, recommendation_k=21), 7)


def test_publication_lock_acquisition_failure_is_explicit(monkeypatch):
    @contextmanager
    def broken_lock(config, run_id, lease_minutes=180):
        raise RuntimeError("writer owns the active publication lock")
        yield  # pragma: no cover

    monkeypatch.setattr(data, "publication_lock", broken_lock)
    with pytest.raises(SourceLockError, match="publication lock scope"):
        capture_verified_user_sources(Module4Config(), 7)


def test_lock_is_released_on_readiness_validation_exception(harness):
    _, collections, calls = harness
    collections["module4_snapshot_readiness"].readiness_rows = [readiness(status="publishing")]

    with pytest.raises(ReadinessContractError):
        capture(harness)
    assert len(calls["acquire"]) == 1
    assert calls["release"] == [calls["acquire"][0][1]]


def test_revalidates_pointer_identity_before_releasing_lock(harness):
    _, collections, calls = harness
    collections["module4_snapshot_readiness"].readiness_rows = [
        readiness(),
        readiness(run_id="module4-run-002"),
    ]

    with pytest.raises(ReadinessContractError, match="changed during"):
        capture(harness)
    assert calls["release"] == [calls["acquire"][0][1]]


def test_source_query_failure_is_explicit_and_does_not_look_like_cold_start(harness):
    _, collections, _ = harness
    collections["user_recommendations"].fail_find = True

    with pytest.raises(SourceQueryError, match="ALS source query failed"):
        capture(harness)


def test_all_find_queries_are_bounded_and_scoped_to_accepted_collections(harness):
    _, collections, _ = harness
    capture(harness, prospective_trend_product_ids=range(301, 321))

    assert set(collections) == {
        "module4_snapshot_readiness",
        "user_recommendations",
        "product_graph_metrics",
    }
    for name in ("user_recommendations", "product_graph_metrics"):
        collection = collections[name]
        assert len(collection.find_calls) == 1
        assert collection.cursors[0].limit_value is not None
        assert collection.cursors[0].limit_value <= 41
    assert collections["user_recommendations"].cursors[0].limit_value == 21
    assert collections["product_graph_metrics"].cursors[0].limit_value == 41
