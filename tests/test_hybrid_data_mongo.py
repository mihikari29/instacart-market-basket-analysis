"""Live MongoDB contract checks for the bounded Module 4C source reader."""

from __future__ import annotations

from tests.helpers import FindCommandRecorder

from datetime import datetime, timezone
import os
import uuid

import pytest
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

from src.module4.config import Config as Module4Config
from src.module4.sink_mongo import (
    ensure_graph_indexes,
    ensure_recommendation_indexes,
    publication_lock,
)
from src.module4.hybrid import (
    HybridRankingConfig,
    ReadinessContractError,
    SourceSnapshotError,
    TrendRecommendation,
    TrendSnapshot,
    capture_verified_user_sources,
    rank_hybrid_recommendations,
)
from src.module4.hybrid.data import READINESS_COLLECTION


pytestmark = pytest.mark.integration

TEST_URI_ENV = "I1_MONGODB_TEST_URI"
FORBIDDEN_DATABASES = {"admin", "config", "instacart", "local"}
RUN_ID = "i1-fixture-run-001"
MODEL_VERSION = "i1-als-v1"
GRAPH_VERSION = "i1-graph-v1"
WARM_USER_ID = 701


def _dedicated_uri() -> str:
    uri = os.environ.get(TEST_URI_ENV)
    if not uri:
        pytest.skip(f"{TEST_URI_ENV} is not set; live MongoDB tests require the isolated I1 service")
    parsed = parse_uri(uri)
    hosts = {host.lower() for host, _port in parsed.get("nodelist", ())}
    if hosts != {"i1-mongo"}:
        pytest.fail(f"{TEST_URI_ENV} must target only the isolated Docker alias 'i1-mongo'")
    if parsed.get("username") is not None or parsed.get("password") is not None:
        pytest.fail(f"{TEST_URI_ENV} must not contain application credentials")
    if parsed.get("database") not in (None, ""):
        pytest.fail(f"{TEST_URI_ENV} must not select a database; tests create owned databases")
    return uri


@pytest.fixture
def live_database():
    uri = _dedicated_uri()
    client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, tz_aware=True)
    database_name = f"i1_module4c_data_{uuid.uuid4().hex}"
    if database_name.lower() in FORBIDDEN_DATABASES:
        client.close()
        pytest.fail("generated an unsafe integration database name")

    owned = False
    try:
        client.admin.command("ping")
        if database_name in client.list_database_names():
            pytest.fail("generated integration database already exists")
        database = client[database_name]
        database["_i1_test_ownership"].insert_one(
            {"_id": "created-by-test", "created_at": datetime.now(timezone.utc)}
        )
        owned = True
        yield client, database, uri
    finally:
        try:
            if owned:
                client.drop_database(database_name)
        finally:
            client.close()


def _config(uri: str, database_name: str, *, recommendation_k: int = 20) -> Module4Config:
    return Module4Config(
        mongodb_uri=uri,
        mongo_database=database_name,
        recommendation_k=recommendation_k,
        mongo_recommendation_collection="user_recommendations",
        mongo_graph_collection="product_graph_metrics",
    )


def _readiness(**overrides):
    document = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
        "updated_at": datetime.now(timezone.utc),
    }
    document.update(overrides)
    return document


def _seed_snapshot(
    database,
    *,
    run_id: str = RUN_ID,
    model_version: str = MODEL_VERSION,
    graph_version: str = GRAPH_VERSION,
    omit_graph_product: int | None = None,
    include_other_user: bool = True,
):
    recommendation_collection = database["user_recommendations"]
    graph_collection = database["product_graph_metrics"]
    ensure_recommendation_indexes(recommendation_collection)
    ensure_graph_indexes(graph_collection)

    recommendation_rows = []
    for user_id in (WARM_USER_ID, 702) if include_other_user else (WARM_USER_ID,):
        for rank in range(1, 21):
            product_id = 1000 + rank
            recommendation_rows.append(
                {
                    "_id": f"{model_version}__{user_id}__{product_id}",
                    "user_id": user_id,
                    "product_id": product_id,
                    "als_score": float(21 - rank),
                    "als_rank": rank,
                    "model_version": model_version,
                    "run_id": run_id,
                    "generated_at": datetime.now(timezone.utc),
                }
            )
    recommendation_collection.insert_many(recommendation_rows)

    graph_rows = []
    for product_id in range(1001, 1021):
        if product_id == omit_graph_product:
            continue
        graph_rows.append(
            {
                "_id": f"{graph_version}__{product_id}",
                "graph_version": graph_version,
                "run_id": run_id,
                "product_id": product_id,
                "product_name": f"Fixture Product {product_id}",
                "neighbor_count": 3,
                "weighted_degree": 12,
                "pagerank": 0.01 * product_id,
                "min_edge_weight": 2,
                "computed_at": datetime.now(timezone.utc),
            }
        )
    graph_rows.append(
        {
            "_id": f"{graph_version}__2001",
            "graph_version": graph_version,
            "run_id": run_id,
            "product_id": 2001,
            "product_name": "Fixture Trend Product",
            "neighbor_count": 2,
            "weighted_degree": 7,
            "pagerank": 0.25,
            "min_edge_weight": 2,
            "computed_at": datetime.now(timezone.utc),
        }
    )
    graph_collection.insert_many(graph_rows)
    database[READINESS_COLLECTION].replace_one(
        {"_id": "active"}, _readiness(run_id=run_id, model_version=model_version, graph_version=graph_version),
        upsert=True,
    )




def _install_command_recorder(monkeypatch, recorder):
    import pymongo

    original_client = pymongo.MongoClient

    def observed_client(uri, *args, **kwargs):
        kwargs["event_listeners"] = [recorder]
        return original_client(uri, *args, **kwargs)

    monkeypatch.setattr(pymongo, "MongoClient", observed_client)


def test_live_capture_verifies_user_versions_catalog_bounded_queries_and_ranker(
    live_database, monkeypatch
):
    _client, database, uri = live_database
    _seed_snapshot(database)
    recorder = FindCommandRecorder()
    _install_command_recorder(monkeypatch, recorder)
    config = _config(uri, database.name)

    captured = capture_verified_user_sources(
        config,
        WARM_USER_ID,
        prospective_trend_product_ids=(2001, 2002),
    )

    assert captured.user_id == WARM_USER_ID
    assert captured.model_version == MODEL_VERSION
    assert captured.graph_version == GRAPH_VERSION
    assert captured.run_id == RUN_ID
    assert len(captured.als_recommendations) == 20
    assert all(row.user_id == WARM_USER_ID for row in captured.als_recommendations)
    assert [row.als_rank for row in captured.als_recommendations] == list(range(1, 21))
    assert [row.product_id for row in captured.als_recommendations] == list(range(1001, 1021))
    assert captured.als_recommendations[0].als_score == 20.0
    catalog = {row.product_id: row.product_name for row in captured.product_catalog}
    assert catalog[1001] == "Fixture Product 1001"
    assert catalog[1020] == "Fixture Product 1020"
    assert catalog[2001] == "Fixture Trend Product"
    assert captured.missing_prospective_trend_product_ids == (2002,)

    recommendation_find = next(
        command for command in recorder.commands
        if command.get("find") == "user_recommendations"
    )
    graph_find = next(
        command for command in recorder.commands
        if command.get("find") == "product_graph_metrics"
    )
    assert recommendation_find["filter"] == {
        "model_version": MODEL_VERSION,
        "run_id": RUN_ID,
        "user_id": WARM_USER_ID,
    }
    assert recommendation_find["limit"] == 21
    assert graph_find["filter"]["graph_version"] == GRAPH_VERSION
    assert graph_find["filter"]["run_id"] == RUN_ID
    assert set(graph_find["filter"]["product_id"]["$in"]) == set(range(1001, 1021)) | {2001, 2002}
    assert graph_find["limit"] == 23

    ranked = rank_hybrid_recommendations(
        user_id=captured.user_id,
        als_recommendations=captured.als_recommendations,
        trend_snapshot=TrendSnapshot(
            available=True,
            rows=(TrendRecommendation(product_id=2001, trend_score=0.9, trend_rank=1),),
        ),
        product_catalog=captured.product_catalog,
        source_context=captured.source_context,
        config=HybridRankingConfig(output_top_k=20),
    )
    assert ranked.metadata.mode == "hybrid"
    assert ranked.metadata.candidate_count == 21
    assert ranked.recommendations[0].product_id == 1001
    assert any(row.product_id == 2001 and row.from_trend for row in ranked.recommendations)


def test_live_cold_user_has_no_als_rows_with_ready_sources(live_database):
    _client, database, uri = live_database
    _seed_snapshot(database)

    captured = capture_verified_user_sources(_config(uri, database.name), 999_999)

    assert captured.als_recommendations == ()
    assert captured.source_context.als_available is True
    assert captured.source_context.graph_available is True
    assert captured.run_id == RUN_ID


@pytest.mark.parametrize(
    ("status", "ready"),
    [("publishing", False), ("failed", False)],
)
def test_live_capture_rejects_non_ready_active_pointer(live_database, status, ready):
    _client, database, uri = live_database
    _seed_snapshot(database)
    database[READINESS_COLLECTION].replace_one(
        {"_id": "active"},
        _readiness(status=status, ready=ready, recommendation_published=False, graph_published=False),
    )

    with pytest.raises(ReadinessContractError, match="status is not 'ready'"):
        capture_verified_user_sources(_config(uri, database.name), WARM_USER_ID)


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [("model_version", "i1-als-v2"), ("run_id", "i1-fixture-run-002")],
)
def test_live_capture_never_consumes_rows_from_mismatched_identity(
    live_database, changed_field, changed_value
):
    _client, database, uri = live_database
    _seed_snapshot(database)
    active = _readiness()
    active[changed_field] = changed_value
    database[READINESS_COLLECTION].replace_one({"_id": "active"}, active)

    captured = capture_verified_user_sources(_config(uri, database.name), WARM_USER_ID)

    assert captured.als_recommendations == ()
    assert captured.run_id == active["run_id"]
    assert captured.model_version == active["model_version"]
    assert captured.model_version != MODEL_VERSION or captured.run_id != RUN_ID


def test_live_capture_rejects_missing_graph_metadata_for_als_candidate(live_database):
    _client, database, uri = live_database
    _seed_snapshot(database, omit_graph_product=1007)

    with pytest.raises(SourceSnapshotError, match="missing graph/catalog metadata"):
        capture_verified_user_sources(_config(uri, database.name), WARM_USER_ID)


def test_live_publication_lock_rejects_conflicts_and_releases_after_exceptions(live_database):
    _client, database, uri = live_database
    config = _config(uri, database.name)
    lock_collection = database["module4_publication_lock"]

    with publication_lock(config, "i1-lock-owner-a"):
        assert lock_collection.find_one({"_id": "module4-active-publication"})["run_id"] == "i1-lock-owner-a"
        with pytest.raises(RuntimeError, match="another Module 4 publication"):
            with publication_lock(config, "i1-lock-owner-b"):
                pytest.fail("conflicting publication lock was acquired")
        assert lock_collection.find_one({"_id": "module4-active-publication"})["run_id"] == "i1-lock-owner-a"

    assert lock_collection.find_one({"_id": "module4-active-publication"}) is None

    with pytest.raises(LookupError, match="injected exception"):
        with publication_lock(config, "i1-lock-owner-exception"):
            raise LookupError("injected exception")
    assert lock_collection.find_one({"_id": "module4-active-publication"}) is None

    with publication_lock(config, "i1-lock-owner-after-exception"):
        assert lock_collection.find_one({"_id": "module4-active-publication"})["run_id"] == "i1-lock-owner-after-exception"

