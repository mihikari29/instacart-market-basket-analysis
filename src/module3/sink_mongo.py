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


def write_mongo(batch_df: DataFrame, batch_id: int, config: Config, batch_size: int = 1000) -> dict:
    """Upsert a micro-batch with bounded-memory unordered bulk writes."""
    from pymongo import UpdateOne

    from pymongo import MongoClient

    totals = {"upserted": 0, "modified": 0, "matched": 0}
    row_count = 0
    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        col = client[config.mongo_database][config.mongo_collection]
        ensure_indexes(col, config.ttl_seconds)

        operations = []

        def flush():
            if not operations:
                return
            result = col.bulk_write(operations, ordered=False)
            totals["upserted"] += result.upserted_count
            totals["modified"] += result.modified_count
            totals["matched"] += result.matched_count
            operations.clear()

        now = datetime.now(timezone.utc)
        for row in batch_df.toLocalIterator():
            row_count += 1
            doc = row.asDict()
            doc["updated_at"] = now
            doc_id = f"{doc['window_end'].isoformat()}__{doc['product_id']}"
            operations.append(UpdateOne({"_id": doc_id}, {"$set": doc}, upsert=True))
            if len(operations) >= batch_size:
                flush()
        flush()

    return {
        "batch_id": batch_id,
        "rows": row_count,
        "skipped": row_count == 0,
        **totals,
    }
