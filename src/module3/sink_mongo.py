"""Idempotent MongoDB upsert sink (Proposal §21, §22, §31.2).

Each complete finalized-window snapshot replaces any earlier version, then uses
deterministic (window_end, product_id) upserts. Replays converge to the same
Top-K documents without duplicates. Delete + upsert is not transactionally
atomic for concurrent readers. Indexes are created idempotently.

TTL defaults to seven days. Set ttl_seconds=0 to explicitly disable retention.
"""

from datetime import datetime, timezone
import time

from pyspark.sql import DataFrame

from .config import Config


def document_identity(window_end, product_id: int) -> str:
    """Stable logical identity used for replay-safe upserts."""
    return f"{window_end.isoformat()}__{product_id}"


def ensure_indexes(collection, ttl_seconds: int) -> None:
    """Create indexes idempotently; design follows Proposal §31.2."""
    existing = collection.index_information()
    rank_index = existing.get("idx_window_rank")
    if rank_index is not None and not rank_index.get("unique", False):
        collection.drop_index("idx_window_rank")
    ttl_index = existing.get("ttl_updated_at")
    if ttl_index is not None and (
        ttl_seconds == 0 or ttl_index.get("expireAfterSeconds") != ttl_seconds
    ):
        collection.drop_index("ttl_updated_at")
        ttl_index = None
    collection.create_index(
        [("window_end", 1), ("product_id", 1)],
        unique=True,
        name="uq_window_product",
    )
    collection.create_index(
        [("window_end", 1), ("trend_rank", 1)],
        unique=True,
        name="idx_window_rank",
    )
    if ttl_seconds > 0:
        collection.create_index(
            [("updated_at", 1)],
            expireAfterSeconds=ttl_seconds,
            name="ttl_updated_at",
        )


def write_mongo(batch_df: DataFrame, batch_id: int, config: Config, batch_size: int = 5000) -> dict:
    """Upsert a Top-K micro-batch with bounded-memory unordered bulk writes.

    ``toLocalIterator`` is intentionally retained: Top-K is applied in Spark
    after full ranking, and this iterator buffers only Spark partitions plus at
    most ``batch_size`` Mongo operations on the driver.
    """
    from pymongo import UpdateOne

    from pymongo import MongoClient

    started = time.perf_counter()
    totals = {"upserted": 0, "modified": 0, "matched": 0}
    row_count = 0
    windows: dict = {}
    replaced_documents = 0
    cached = batch_df.persist()
    try:
        window_ends = [row.window_end for row in cached.select("window_end").distinct().collect()]
        windows = {window_end: 0 for window_end in window_ends}
        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
            col = client[config.mongo_database][config.mongo_collection]
            ensure_indexes(col, config.ttl_seconds)

            # A bounded number of set-based deletes replaces complete finalized
            # snapshots without one Mongo round trip per window.
            for start in range(0, len(window_ends), 10_000):
                replaced_documents += col.delete_many(
                    {"window_end": {"$in": window_ends[start : start + 10_000]}}
                ).deleted_count

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
            for row in cached.toLocalIterator():
                row_count += 1
                doc = row.asDict()
                if not 1 <= int(doc["trend_rank"]) <= config.mongo_top_k_per_window:
                    raise ValueError(
                        f"Sink row rank {doc['trend_rank']} exceeds Top-K "
                        f"{config.mongo_top_k_per_window}"
                    )
                windows[doc["window_end"]] += 1
                if windows[doc["window_end"]] > config.mongo_top_k_per_window:
                    raise ValueError(
                        f"Window {doc['window_end']} exceeds Top-K "
                        f"{config.mongo_top_k_per_window}"
                    )
                doc["updated_at"] = now
                doc_id = document_identity(doc["window_end"], doc["product_id"])
                operations.append(UpdateOne({"_id": doc_id}, {"$set": doc}, upsert=True))
                if len(operations) >= batch_size:
                    flush()
            flush()
    finally:
        cached.unpersist()
    return {
        "batch_id": batch_id,
        "rows": row_count,
        "skipped": row_count == 0,
        "windows": len(windows),
        "max_rows_per_window": max(windows.values(), default=0),
        "top_k_per_window": config.mongo_top_k_per_window,
        "replaced_documents": replaced_documents,
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        **totals,
    }
