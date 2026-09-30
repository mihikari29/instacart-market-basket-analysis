"""Idempotent MongoDB upsert sink (Proposal §21, §22, §31.2).

Per-batch upsert keyed by (window_end, product_id). Replays of a micro-batch
overwrite the same logical document instead of duplicating. Indexes are
created with upsert_one / create_index -- idempotent.

TTL: disabled by default (ttl_seconds=0, Q-B). Enable via --ttl-seconds
to demo W4 eventual-consistency / automatic window cleanup.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame

from .config import Config


def ensure_indexes(collection, ttl_seconds: int) -> None:
    """Create indexes idempotently; design follows Proposal §31.2."""
    collection.create_index(
        [("window_end", 1), ("product_id", 1)],
        unique=True,
        name="uq_window_product",
    )
    collection.create_index(
        [("window_end", 1), ("trend_rank", 1)],
        name="idx_window_rank",
    )
    if ttl_seconds > 0:
        collection.create_index(
            [("updated_at", 1)],
            expireAfterSeconds=ttl_seconds,
            name="ttl_updated_at",
        )


def write_mongo(batch_df: DataFrame, batch_id: int, config: Config) -> dict:
    """foreachBatch sink: upsert each batch into realtime_trending."""
    from pymongo import UpdateOne

    if batch_df.rdd.isEmpty():
        return {"batch_id": batch_id, "upserted": 0, "skipped": True}

    rows = list(batch_df.toLocalIterator())
    if not rows:
        return {"batch_id": batch_id, "upserted": 0, "skipped": True}

    now = datetime.now(timezone.utc)
    operations = []
    for row in rows:
        doc = row.asDict()
        doc["updated_at"] = now
        # _id is the natural composite key -> idempotent across replays
        doc_id = f"{doc['window_end'].isoformat()}__{doc['product_id']}"
        operations.append(
            UpdateOne(
                {"_id": doc_id},
                {"$set": doc},
                upsert=True,
            )
        )

    from pymongo import MongoClient

    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        db = client[config.mongo_database]
        col = db[config.mongo_collection]
        result = col.bulk_write(operations, ordered=False)
        ensure_indexes(col, config.ttl_seconds)

    return {
        "batch_id": batch_id,
        "upserted": result.upserted_count,
        "modified": result.modified_count,
        "matched": result.matched_count,
    }
