"""Live MongoDB snapshot/replay checks; CI supplies an isolated Mongo service."""

import os
import uuid

import pytest

from src.module4.config import Config
from src.module4.sink_mongo import (
    _write_snapshot,
    ensure_graph_indexes,
    publication_lock,
    update_pair_readiness,
)


@pytest.mark.integration
def test_live_mongo_snapshot_replay_and_pre_promote_failure_preserve_active():
    uri = os.environ.get("MODULE4_MONGO_TEST_URI")
    if not uri:
        pytest.skip("MODULE4_MONGO_TEST_URI is not configured")

    pymongo = pytest.importorskip("pymongo")
    database_name = f"module4_test_{uuid.uuid4().hex}"
    collection_name = "product_graph_metrics"
    run_id = uuid.uuid4().hex
    graph_version = "graph-fixture"
    documents = [
        {
            "_id": f"{graph_version}__{product_id}",
            "graph_version": graph_version,
            "run_id": run_id,
            "product_id": product_id,
            "neighbor_count": 1,
            "weighted_degree": 3,
            "pagerank": 0.5,
            "min_edge_weight": 2,
        }
        for product_id in (1, 2)
    ]

    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        client.admin.command("ping")
        db = client[database_name]
        try:
            config = Config(mongo_database=database_name, mongodb_uri=uri)
            with publication_lock(config, run_id) as locked_client:
                readiness = update_pair_readiness(
                    locked_client,
                    config,
                    run_id,
                    "als-fixture",
                    graph_version,
                    "publishing",
                    False,
                    False,
                )
                assert readiness["ready"] is False
                with pytest.raises(RuntimeError, match="another Module 4 publication"):
                    with publication_lock(config, uuid.uuid4().hex):
                        pass

            first = _write_snapshot(
                db,
                collection_name,
                iter(documents),
                ensure_graph_indexes,
                "graph_version",
                graph_version,
                run_id,
                len(documents),
            )
            assert first["validation_after_promote"]["valid"] is True
            assert db[collection_name].count_documents({"run_id": run_id}) == 2

            replay = _write_snapshot(
                db,
                collection_name,
                iter(documents),
                ensure_graph_indexes,
                "graph_version",
                graph_version,
                run_id,
                len(documents),
            )
            assert replay["validation_after_promote"]["documents"] == 2
            assert db[collection_name].count_documents({"run_id": run_id}) == 2

            duplicate_products = [
                {**documents[0], "_id": "different-1"},
                {**documents[0], "_id": "different-2"},
            ]
            with pytest.raises(Exception):
                _write_snapshot(
                    db,
                    collection_name,
                    iter(duplicate_products),
                    ensure_graph_indexes,
                    "graph_version",
                    graph_version,
                    run_id,
                    len(duplicate_products),
                )
            active = list(
                db[collection_name]
                .find({"run_id": run_id}, {"_id": 1, "product_id": 1})
                .sort("product_id", 1)
            )
            assert [document["product_id"] for document in active] == [1, 2]
            assert db["module4_snapshot_readiness"].find_one({"_id": "active"})["ready"] is False
        finally:
            client.drop_database(database_name)
