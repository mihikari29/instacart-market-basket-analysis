"""Validated, versioned MongoDB snapshots for Module 4A/4B."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import sys
import time
import uuid


def recommendation_identity(model_version: str, user_id: int, product_id: int) -> str:
    return f"{model_version}__{int(user_id)}__{int(product_id)}"


def graph_identity(graph_version: str, product_id: int) -> str:
    return f"{graph_version}__{int(product_id)}"


def ensure_recommendation_indexes(collection) -> None:
    collection.create_index(
        [("model_version", 1), ("user_id", 1), ("product_id", 1)],
        unique=True,
        name="uq_model_user_product",
    )
    collection.create_index(
        [("model_version", 1), ("user_id", 1), ("als_rank", 1)],
        unique=True,
        name="uq_model_user_rank",
    )
    collection.create_index([("model_version", 1), ("user_id", 1)], name="idx_model_user")


def ensure_graph_indexes(collection) -> None:
    collection.create_index(
        [("graph_version", 1), ("product_id", 1)],
        unique=True,
        name="uq_graph_product",
    )
    collection.create_index(
        [("graph_version", 1), ("pagerank", -1)], name="idx_graph_pagerank"
    )
    collection.create_index(
        [("graph_version", 1), ("weighted_degree", -1)], name="idx_graph_weighted_degree"
    )


def snapshot_validation(collection, version_field, version, expected_count, run_id):
    query = {version_field: version, "run_id": run_id}
    count = int(collection.count_documents(query))
    logical_keys = (
        {"user_id": "$user_id", "product_id": "$product_id"}
        if version_field == "model_version"
        else {"product_id": "$product_id"}
    )
    duplicate = list(
        collection.aggregate(
            [
                {"$match": query},
                {"$group": {"_id": logical_keys, "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
                {"$limit": 1},
            ]
        )
    )
    indexes = collection.index_information()
    expected_indexes = (
        {
            "uq_model_user_product": ([("model_version", 1), ("user_id", 1), ("product_id", 1)], True),
            "uq_model_user_rank": ([("model_version", 1), ("user_id", 1), ("als_rank", 1)], True),
            "idx_model_user": ([("model_version", 1), ("user_id", 1)], False),
        } if version_field == "model_version" else {
            "uq_graph_product": ([("graph_version", 1), ("product_id", 1)], True),
            "idx_graph_pagerank": ([("graph_version", 1), ("pagerank", -1)], False),
            "idx_graph_weighted_degree": ([("graph_version", 1), ("weighted_degree", -1)], False),
        }
    )
    missing_indexes = sorted(set(expected_indexes).difference(indexes))
    invalid_indexes = sorted(
        name for name, (keys, unique) in expected_indexes.items()
        if name in indexes and (
            list(indexes[name]["key"]) != keys or bool(indexes[name].get("unique", False)) != unique
        )
    )
    return {
        "documents": count,
        "expected_documents": int(expected_count),
        "duplicate_keys": len(duplicate),
        "indexes": sorted(indexes),
        "missing_indexes": missing_indexes,
        "invalid_indexes": invalid_indexes,
        "valid": count == int(expected_count) and not duplicate and not missing_indexes and not invalid_indexes,
    }


def _write_snapshot(db, collection_name, documents, ensure_indexes, version_field, version, run_id, expected_count):
    """Validate an owned temporary collection before promoting it by rename."""
    temp_name = f"{collection_name}__staging_{uuid.uuid4().hex}"
    temp = db[temp_name]
    inserted = 0
    batch = []
    try:
        for document in documents:
            batch.append(document)
            if len(batch) == 5000:
                temp.insert_many(batch, ordered=False)
                inserted += len(batch)
                batch.clear()
        if batch:
            temp.insert_many(batch, ordered=False)
            inserted += len(batch)
        if inserted != int(expected_count):
            raise RuntimeError(
                f"snapshot writer inserted {inserted} documents; expected {expected_count}"
            )
        ensure_indexes(temp)
        before = snapshot_validation(temp, version_field, version, expected_count, run_id)
        if not before["valid"]:
            raise RuntimeError(f"temporary snapshot validation failed: {before}")
        temp.rename(collection_name, dropTarget=True)
        active = db[collection_name]
        after = snapshot_validation(active, version_field, version, expected_count, run_id)
        if not after["valid"]:
            raise RuntimeError(f"promoted snapshot validation failed: {after}")
        return {
            "collection": collection_name,
            "documents": inserted,
            "temporary": temp_name,
            "validation_before_promote": before,
            "validation_after_promote": after,
        }
    except Exception as exc:
        try:
            temp.drop()
        except Exception as cleanup_error:
            if hasattr(exc, "add_note"):
                exc.add_note(f"Could not remove owned temporary collection {temp_name}: {cleanup_error}")
        raise


def _check_recommendations(frame, config):
    from pyspark.sql import functions as F

    required = {"user_id", "product_id", "als_score", "als_rank"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"recommendations: missing required columns {sorted(missing)}")
    score = F.col("als_score").cast("double")
    if frame.where(
        F.col("user_id").isNull()
        | (F.col("user_id") <= 0)
        | F.col("product_id").isNull()
        | (F.col("product_id") <= 0)
        | F.col("als_rank").isNull()
        | (F.col("als_rank") < 1)
        | (F.col("als_rank") > config.recommendation_k)
        | (F.col("als_rank") != F.col("als_rank").cast("long"))
        | score.isNull()
        | F.isnan(score)
        | (F.abs(score) == float("inf"))
    ).limit(1).count():
        raise ValueError("recommendations contain invalid IDs, ranks or scores")
    for keys in (("user_id", "product_id"), ("user_id", "als_rank")):
        if frame.groupBy(*keys).count().where("count > 1").limit(1).count():
            raise ValueError(f"recommendations contain duplicate key {keys}")


def _check_graph_metrics(frame, min_edge_weight):
    from pyspark.sql import functions as F

    required = {"product_id", "neighbor_count", "weighted_degree", "pagerank"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"graph metrics: missing required columns {sorted(missing)}")
    rank = F.col("pagerank").cast("double")
    weighted = F.col("weighted_degree").cast("double")
    if frame.where(
        F.col("product_id").isNull()
        | (F.col("product_id") <= 0)
        | F.col("neighbor_count").isNull()
        | (F.col("neighbor_count") < 0)
        | (F.col("neighbor_count") != F.col("neighbor_count").cast("long"))
        | F.col("weighted_degree").isNull()
        | (F.col("weighted_degree") < 0)
        | (weighted != weighted.cast("long").cast("double"))
        | F.isnan(weighted)
        | (F.abs(weighted) == float("inf"))
        | rank.isNull()
        | F.isnan(rank)
        | (F.abs(rank) == float("inf"))
        | (rank < 0)
    ).limit(1).count():
        raise ValueError("graph metrics contain null, negative or non-finite values")
    if frame.groupBy("product_id").count().where("count > 1").limit(1).count():
        raise ValueError("graph metrics contain duplicate products")
    if min_edge_weight < 1:
        raise ValueError("min_edge_weight must be positive")


@contextmanager
def publication_lock(config, run_id, lease_minutes=180):
    """Enforce a single writer across both Module 4 collection promotions."""
    from pymongo import MongoClient, ReturnDocument
    from pymongo.errors import DuplicateKeyError

    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        locks = client[config.mongo_database]["module4_publication_lock"]
        now = datetime.now(timezone.utc)
        try:
            lock = locks.find_one_and_update(
                {
                    "_id": "module4-active-publication",
                    "$or": [
                        {"lease_expires_at": {"$lte": now}},
                        {"lease_expires_at": {"$exists": False}},
                    ],
                },
                {
                    "$set": {
                        "run_id": run_id,
                        "acquired_at": now,
                        "lease_expires_at": now + timedelta(minutes=lease_minutes),
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as exc:
            raise RuntimeError("another Module 4 publication holds the MongoDB writer lock") from exc
        if not lock or lock.get("run_id") != run_id:
            raise RuntimeError("could not acquire the Module 4 MongoDB writer lock")
        try:
            yield client
        finally:
            active_error = sys.exc_info()[1]
            try:
                result = locks.delete_one({"_id": "module4-active-publication", "run_id": run_id})
                if result.deleted_count != 1:
                    raise RuntimeError("Module 4 MongoDB writer lock ownership was lost")
            except Exception as cleanup_error:
                if active_error is not None and hasattr(active_error, "add_note"):
                    active_error.add_note(f"Could not release MongoDB writer lock: {cleanup_error}")
                elif active_error is None:
                    raise


def update_pair_readiness(
    client,
    config,
    run_id,
    model_version,
    graph_version,
    status,
    recommendation_published,
    graph_published,
    error=None,
):
    """Write the consumer gate only after both versioned snapshots are checked."""
    document = {
        "_id": "active",
        "status": status,
        "ready": status == "ready",
        "run_id": run_id,
        "model_version": model_version,
        "graph_version": graph_version,
        "recommendation_published": bool(recommendation_published),
        "graph_published": bool(graph_published),
        "single_writer_policy": "module4_publication_lock",
        "updated_at": datetime.now(timezone.utc),
    }
    if error:
        document["error"] = error
    client[config.mongo_database]["module4_snapshot_readiness"].replace_one(
        {"_id": "active"}, document, upsert=True
    )
    return {key: value for key, value in document.items() if key != "updated_at"}


def _iter_documents(frame, version_field, version, run_id, timestamp_field, now, identity, extra=None):
    extra = extra or {}
    for row in frame.toLocalIterator():
        document = row.asDict(recursive=True)
        document.update(extra)
        document[version_field] = version
        document["run_id"] = run_id
        document[timestamp_field] = now
        if version_field == "model_version":
            document["_id"] = identity(version, document["user_id"], document["product_id"])
        else:
            document["_id"] = identity(version, document["product_id"])
        yield document


def publish_recommendations(frame, config, model_version, run_id, client=None):
    """Publish a complete warm-user recommendation snapshot in bounded batches."""
    from pymongo import MongoClient

    started = time.perf_counter()
    _check_recommendations(frame, config)
    expected_count = int(frame.count())
    now = datetime.now(timezone.utc)
    if client is None:
        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as owned_client:
            owned_client.admin.command("ping")
            result = _write_snapshot(
                owned_client[config.mongo_database],
                config.mongo_recommendation_collection,
                _iter_documents(
                    frame, "model_version", model_version, run_id, "generated_at", now,
                    recommendation_identity,
                ),
                ensure_recommendation_indexes,
                "model_version", model_version, run_id, expected_count,
            )
    else:
        result = _write_snapshot(
            client[config.mongo_database],
            config.mongo_recommendation_collection,
            _iter_documents(
                frame, "model_version", model_version, run_id, "generated_at", now,
                recommendation_identity,
            ),
            ensure_recommendation_indexes,
            "model_version", model_version, run_id, expected_count,
        )
    return {
        **result,
        "model_version": model_version,
        "run_id": run_id,
        "expected_documents": expected_count,
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
    }


def publish_graph_metrics(frame, config, graph_version, run_id, min_edge_weight, client=None):
    """Publish catalog-complete graph metrics without collecting rows on driver."""
    from pymongo import MongoClient

    started = time.perf_counter()
    _check_graph_metrics(frame, min_edge_weight)
    expected_count = int(frame.count())
    now = datetime.now(timezone.utc)
    if client is None:
        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as owned_client:
            owned_client.admin.command("ping")
            result = _write_snapshot(
                owned_client[config.mongo_database],
                config.mongo_graph_collection,
                _iter_documents(
                    frame, "graph_version", graph_version, run_id, "computed_at", now,
                    graph_identity, {"min_edge_weight": min_edge_weight},
                ),
                ensure_graph_indexes,
                "graph_version", graph_version, run_id, expected_count,
            )
    else:
        result = _write_snapshot(
            client[config.mongo_database],
            config.mongo_graph_collection,
            _iter_documents(
                frame, "graph_version", graph_version, run_id, "computed_at", now,
                graph_identity, {"min_edge_weight": min_edge_weight},
            ),
            ensure_graph_indexes,
            "graph_version", graph_version, run_id, expected_count,
        )
    return {
        **result,
        "graph_version": graph_version,
        "run_id": run_id,
        "min_edge_weight": min_edge_weight,
        "expected_documents": expected_count,
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
    }


def validate_snapshot(client, database, collection_name, version_field, version, expected_count=None):
    """Read-only active snapshot check for the operational smoke runner."""
    collection = client[database][collection_name]
    count = int(collection.count_documents({version_field: version}))
    duplicate = list(
        collection.aggregate(
            [
                {"$match": {version_field: version}},
                {"$group": {"_id": {"product_id": "$product_id", "user_id": "$user_id"}, "n": {"$sum": 1}}},
                {"$match": {"n": {"$gt": 1}}},
                {"$limit": 1},
            ]
        )
    )
    valid = not duplicate and (expected_count is None or count == int(expected_count))
    return {
        "collection": collection_name,
        "version": version,
        "documents": count,
        "expected_documents": expected_count,
        "duplicate_keys": len(duplicate),
        "valid": valid,
    }
