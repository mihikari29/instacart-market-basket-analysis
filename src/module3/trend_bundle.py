"""Atomic, replay-safe bundles for finalized Module 3 trending windows.

This module is deliberately standalone. The production streaming query does
not call it until the bundle reader and rollout are reviewed separately.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from numbers import Real
import time
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from .config import Config


BUNDLE_COLLECTION_NAME = "realtime_trending_windows"
BUNDLE_SCHEMA_VERSION = 1
DEFAULT_SOURCE_SCOPE = "unknown_or_mixed"
MAX_TREND_TOP_K = 20
DEFAULT_WRITE_BATCH_SIZE = 250
MAX_WRITE_BATCH_SIZE = 500
TTL_INDEX_NAME = "ttl_expires_at"

REQUIRED_ROW_FIELDS = (
    "window_start",
    "window_end",
    "product_id",
    "purchase_count_30m",
    "purchase_count_120m",
    "trend_score",
    "trend_rank",
)


@dataclass(frozen=True, slots=True)
class TrendBundleWriteReport:
    """Counts for a completed write or a failed publication attempt."""

    bundles_created: int = 0
    identical_content_retries: int = 0
    windows_processed: int = 0
    trend_rows_represented: int = 0
    rejected_publications: int = 0
    duration_ms: float = 0.0


class _ReportedError:
    def __init__(self, message: str, report: TrendBundleWriteReport):
        super().__init__(message)
        self.report = report


class TrendBundleValidationError(_ReportedError, ValueError):
    """Input rows or publication metadata violate the bundle contract."""


class TrendBundleConflictError(_ReportedError, RuntimeError):
    """A namespace/window identity already contains different logical content."""


class TrendBundlePublicationError(_ReportedError, RuntimeError):
    """MongoDB rejected an otherwise validated bundle publication."""


@dataclass(slots=True)
class _Counters:
    bundles_created: int = 0
    identical_content_retries: int = 0
    windows_processed: int = 0
    trend_rows_represented: int = 0
    rejected_publications: int = 0

    def report(self, started: float) -> TrendBundleWriteReport:
        return TrendBundleWriteReport(
            bundles_created=self.bundles_created,
            identical_content_retries=self.identical_content_retries,
            windows_processed=self.windows_processed,
            trend_rows_represented=self.trend_rows_represented,
            rejected_publications=self.rejected_publications,
            duration_ms=round((time.perf_counter() - started) * 1000, 3),
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validate_namespace(value: Any, field: str, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ValueError(f"{field} must be a non-empty, stable string of at most {maximum} characters")
    return value


def _canonical_datetime(value: Any, field: str, *, allow_naive_utc: bool) -> tuple[datetime, str]:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a timestamp")
    if value.tzinfo is None or value.utcoffset() is None:
        if not allow_naive_utc:
            raise ValueError(f"{field} must include an explicit UTC offset")
        # Spark TimestampType rows are returned as naive datetimes. The writer
        # accepts those only after verifying that the Spark session timezone is UTC.
        value = value.replace(tzinfo=timezone.utc)
    utc_value = value.astimezone(timezone.utc)
    canonical = utc_value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return utc_value, canonical


def _canonical_window_datetime(value: Any, field: str) -> tuple[datetime, str]:
    """Normalize a window bound to UTC and require BSON-exact milliseconds.

    Spark TimestampType values are returned as naive Python datetimes when the
    session timezone is UTC; ``_canonical_datetime`` explicitly interprets
    those values as UTC. MongoDB BSON Date preserves milliseconds only, so
    rejecting finer precision here keeps the stored value, identity, and
    logical digest in agreement without silently changing the event time.
    """
    utc_value, canonical = _canonical_datetime(value, field, allow_naive_utc=True)
    if utc_value.microsecond % 1000 != 0:
        raise ValueError(f"{field} must be aligned to BSON millisecond precision")
    return utc_value, canonical


def _config_payload(config: Config, top_k: int) -> dict[str, Any]:
    return {
        "formula_version": "module3-trend-v1",
        "window_short": config.window_short,
        "window_long": config.window_long,
        "slide": config.slide,
        "watermark": config.watermark,
        "weight_short": float(config.weight_short),
        "weight_long": float(config.weight_long),
        "top_k": top_k,
    }


def _item_from_row(row: Any) -> dict[str, Any]:
    item = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    product_id = item.get("product_id")
    if type(product_id) is not int or product_id <= 0:
        raise ValueError("product_id must be a positive integer")

    counts = {}
    for field in ("purchase_count_30m", "purchase_count_120m"):
        value = item.get(field)
        if type(value) is not int or value < 0:
            raise ValueError(f"{field} must be a nonnegative integer")
        counts[field] = value
    if counts["purchase_count_30m"] > counts["purchase_count_120m"]:
        raise ValueError("purchase_count_30m cannot exceed purchase_count_120m")

    score = item.get("trend_score")
    if isinstance(score, bool) or not isinstance(score, Real):
        raise ValueError("trend_score must be a finite normalized number")
    score = float(score)
    if not math.isfinite(score) or not 0.0 <= score <= 1.0:
        raise ValueError("trend_score must be finite and within [0, 1]")

    rank = item.get("trend_rank")
    if type(rank) is not int or rank <= 0:
        raise ValueError("trend_rank must be a positive integer")

    return {
        "product_id": product_id,
        "purchase_count_30m": counts["purchase_count_30m"],
        "purchase_count_120m": counts["purchase_count_120m"],
        "trend_score": score,
        "trend_rank": rank,
    }


def _build_document(
    group: Any,
    *,
    source_namespace: str,
    source_scope: str,
    top_k: int,
    config_hash: str,
    published_at: datetime,
    retention_seconds: int,
) -> dict[str, Any]:
    window_start, start_iso = _canonical_window_datetime(group["window_start"], "window_start")
    window_end, end_iso = _canonical_window_datetime(group["window_end"], "window_end")
    if window_end <= window_start:
        raise ValueError("window_end must be later than window_start")

    raw_items = group["items"]
    items = [_item_from_row(item) for item in raw_items]
    row_count = group["_row_count"]
    if type(row_count) is not int or row_count != len(items):
        raise ValueError("row_count must equal the number of window items")
    if not items:
        raise ValueError("an empty window cannot be published")
    if len(items) > top_k:
        raise ValueError(f"window contains more than configured Top-{top_k}")

    product_ids = [item["product_id"] for item in items]
    ranks = [item["trend_rank"] for item in items]
    if len(set(product_ids)) != len(product_ids):
        raise ValueError("window contains duplicate product IDs")
    if len(set(ranks)) != len(ranks):
        raise ValueError("window contains duplicate trend ranks")
    items.sort(key=lambda item: item["trend_rank"])
    if [item["trend_rank"] for item in items] != list(range(1, len(items) + 1)):
        raise ValueError("trend ranks must be contiguous, beginning at 1")

    logical_content = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "source_namespace": source_namespace,
        "source_scope": source_scope,
        "window_start": start_iso,
        "window_end": end_iso,
        "top_k": top_k,
        "row_count": len(items),
        "items": items,
        "config_hash": config_hash,
    }
    namespace_digest = hashlib.sha256(source_namespace.encode("utf-8")).hexdigest()
    document = {
        "_id": f"trend-window:{namespace_digest}:{end_iso}",
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "source_namespace": source_namespace,
        "source_scope": source_scope,
        "window_start": window_start,
        "window_end": window_end,
        "top_k": top_k,
        "row_count": len(items),
        "items": items,
        "content_sha256": _sha256(logical_content),
        "config_hash": config_hash,
        "published_at": published_at,
    }
    if retention_seconds > 0:
        document["expires_at"] = published_at + timedelta(seconds=retention_seconds)
    return document


def _chunks(iterator: Iterable[dict[str, Any]], batch_size: int):
    chunk = []
    for value in iterator:
        chunk.append(value)
        if len(chunk) >= batch_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _bundle_documents(
    grouped: DataFrame,
    *,
    source_namespace: str,
    source_scope: str,
    top_k: int,
    config_hash: str,
    published_at: datetime,
    retention_seconds: int,
):
    for row in grouped.toLocalIterator():
        yield _build_document(
            row,
            source_namespace=source_namespace,
            source_scope=source_scope,
            top_k=top_k,
            config_hash=config_hash,
            published_at=published_at,
            retention_seconds=retention_seconds,
        )


def ensure_bundle_indexes(collection: Any, retention_seconds: int) -> None:
    """Maintain the absolute-expiration TTL index on the new bundle collection."""
    existing = collection.index_information()
    ttl_index = existing.get(TTL_INDEX_NAME)
    correct_ttl_index = bool(
        ttl_index
        and list(ttl_index.get("key", [])) == [("expires_at", 1)]
        and ttl_index.get("expireAfterSeconds") == 0
    )
    if ttl_index and (retention_seconds == 0 or not correct_ttl_index):
        collection.drop_index(TTL_INDEX_NAME)
        ttl_index = None
    if retention_seconds > 0 and not correct_ttl_index:
        collection.create_index(
            [("expires_at", 1)],
            expireAfterSeconds=0,
            name=TTL_INDEX_NAME,
        )


def _existing_for_chunk(collection: Any, documents: list[dict[str, Any]]) -> dict[str, Any]:
    identities = [document["_id"] for document in documents]
    cursor = collection.find({"_id": {"$in": identities}})
    existing = {}
    for row in cursor:
        if isinstance(row, Mapping):
            row = dict(row)
        else:
            row = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        existing[row.get("_id")] = row
    return existing


def _existing_metadata_error(expected: dict[str, Any], stored: Any) -> str | None:
    if not isinstance(stored, Mapping):
        return "stored bundle is not a document"
    if stored.get("_id") != expected["_id"]:
        return "stored bundle _id does not match the requested identity"

    for field in (
        "schema_version",
        "source_namespace",
        "source_scope",
        "top_k",
        "row_count",
        "items",
        "config_hash",
    ):
        try:
            if _canonical_json(stored.get(field)) != _canonical_json(expected[field]):
                return f"stored bundle {field} does not match the expected publication"
        except (TypeError, ValueError):
            return f"stored bundle {field} is malformed"

    for field in ("window_start", "window_end"):
        try:
            actual, _actual_iso = _canonical_window_datetime(stored.get(field), field)
        except (TypeError, ValueError, OverflowError):
            return f"stored bundle {field} is not a valid BSON-compatible timestamp"
        if actual != expected[field]:
            return f"stored bundle {field} does not match the expected publication"

    try:
        published_at, _published_iso = _canonical_datetime(
            stored.get("published_at"), "published_at", allow_naive_utc=True
        )
    except (TypeError, ValueError, OverflowError):
        return "stored bundle published_at is not a valid BSON timestamp"

    if "expires_at" in stored:
        try:
            expires_at, _expires_iso = _canonical_datetime(
                stored["expires_at"], "expires_at", allow_naive_utc=True
            )
        except (TypeError, ValueError, OverflowError):
            return "stored bundle expires_at is not a valid BSON timestamp"
        if expires_at <= published_at:
            return "stored bundle expires_at is not later than published_at"
    return None


def _validate_existing(
    collection: Any,
    documents: list[dict[str, Any]],
    counters: _Counters,
    started: float,
) -> int:
    try:
        existing = _existing_for_chunk(collection, documents)
    except Exception as exc:
        counters.rejected_publications += 1
        raise TrendBundlePublicationError(
            "failed to check existing trending bundle identities", counters.report(started)
        ) from exc

    conflicts = []
    corrupted = []
    for document in documents:
        prior = existing.get(document["_id"])
        if prior is None:
            continue
        if prior.get("content_sha256") != document["content_sha256"]:
            conflicts.append(document["_id"])
            continue
        metadata_error = _existing_metadata_error(document, prior)
        if metadata_error:
            corrupted.append((document["_id"], metadata_error))
    if conflicts:
        counters.rejected_publications += len(conflicts)
        raise TrendBundleConflictError(
            f"conflicting content already exists for {len(conflicts)} trending bundle identity/identities",
            counters.report(started),
        )
    if corrupted:
        counters.rejected_publications += len(corrupted)
        raise TrendBundlePublicationError(
            f"stored trending bundle metadata is corrupted for {len(corrupted)} identity/identities",
            counters.report(started),
        )
    return len(existing)


def _duplicate_key_outcome(exc: Exception, documents: list[dict[str, Any]]):
    """Return reliable partial counts only for duplicate _id write errors."""
    from pymongo.errors import BulkWriteError, DuplicateKeyError

    details = getattr(exc, "details", None)
    if not isinstance(details, Mapping):
        return None

    if isinstance(exc, BulkWriteError):
        write_errors = details.get("writeErrors")
        if not isinstance(write_errors, list) or not write_errors:
            return None
        if details.get("writeConcernErrors"):
            return None
    elif isinstance(exc, DuplicateKeyError) and len(documents) == 1:
        write_errors = [
            {
                "index": 0,
                "code": exc.code,
                "keyPattern": details.get("keyPattern"),
                "keyValue": details.get("keyValue"),
            }
        ]
    else:
        return None

    duplicate_indexes = set()
    for error in write_errors:
        if not isinstance(error, Mapping) or error.get("code") != 11000:
            return None
        index = error.get("index")
        if type(index) is not int or not 0 <= index < len(documents):
            return None
        if error.get("keyPattern") != {"_id": 1}:
            return None
        if error.get("keyValue") != {"_id": documents[index]["_id"]}:
            return None
        if index in duplicate_indexes:
            return None
        duplicate_indexes.add(index)

    upserted_count = details.get("nUpserted", 0)
    matched_count = details.get("nMatched", 0)
    upserted = details.get("upserted", [])
    if (
        type(upserted_count) is not int
        or upserted_count < 0
        or type(matched_count) is not int
        or matched_count < 0
        or not isinstance(upserted, list)
        or upserted_count != len(upserted)
        or upserted_count + matched_count + len(duplicate_indexes) > len(documents)
    ):
        return None

    upserted_indexes = set()
    for item in upserted:
        if not isinstance(item, Mapping):
            return None
        index = item.get("index")
        if type(index) is not int or not 0 <= index < len(documents):
            return None
        if item.get("_id") != documents[index]["_id"] or index in upserted_indexes:
            return None
        upserted_indexes.add(index)
    if duplicate_indexes & upserted_indexes:
        return None
    return upserted_count, matched_count


def _reconcile_duplicate_publication(
    collection: Any,
    documents_factory,
    *,
    batch_size: int,
    counters: _Counters,
    started: float,
) -> None:
    missing_count = 0
    matching_count = 0
    conflicts = []
    corrupted = []

    for chunk in _chunks(documents_factory(), batch_size):
        try:
            existing = _existing_for_chunk(collection, chunk)
        except Exception as exc:
            counters.rejected_publications += 1
            raise TrendBundlePublicationError(
                "failed to verify every trending bundle identity after a duplicate-key write",
                counters.report(started),
            ) from exc

        for document in chunk:
            prior = existing.get(document["_id"])
            if prior is None:
                missing_count += 1
                continue
            if prior.get("content_sha256") != document["content_sha256"]:
                conflicts.append(document["_id"])
                continue
            metadata_error = _existing_metadata_error(document, prior)
            if metadata_error:
                corrupted.append((document["_id"], metadata_error))
                continue
            matching_count += 1

    counters.identical_content_retries = max(
        0, matching_count - counters.bundles_created
    )
    if conflicts:
        counters.rejected_publications += len(conflicts)
        raise TrendBundleConflictError(
            f"conflicting content already exists for {len(conflicts)} trending bundle identity/identities",
            counters.report(started),
        )
    if corrupted:
        counters.rejected_publications += len(corrupted)
        raise TrendBundlePublicationError(
            f"stored trending bundle metadata is corrupted for {len(corrupted)} identity/identities",
            counters.report(started),
        )
    if missing_count:
        counters.rejected_publications += 1
        raise TrendBundlePublicationError(
            f"duplicate-key recovery found {missing_count} missing expected trending bundle identity/identities",
            counters.report(started),
        )


def _upsert_chunk(
    collection: Any,
    documents: list[dict[str, Any]],
    counters: _Counters,
    started: float,
    *,
    documents_factory,
    batch_size: int,
) -> bool:
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError

    operations = [
        UpdateOne(
            {"_id": document["_id"], "content_sha256": document["content_sha256"]},
            {"$setOnInsert": document},
            upsert=True,
        )
        for document in documents
    ]
    try:
        result = collection.bulk_write(operations, ordered=False)
    except Exception as exc:
        details = getattr(exc, "details", None) or {}
        if isinstance(exc, BulkWriteError) and isinstance(details, Mapping):
            partial_upserted = details.get("nUpserted", 0)
            partial_matched = details.get("nMatched", 0)
            if type(partial_upserted) is int and partial_upserted >= 0:
                counters.bundles_created += partial_upserted
            if type(partial_matched) is int and partial_matched >= 0:
                counters.identical_content_retries += partial_matched

        duplicate_outcome = _duplicate_key_outcome(exc, documents)
        if duplicate_outcome is not None:
            # A successful reconciliation counts the complete publication, including
            # windows that were present before this bulk operation began.
            _reconcile_duplicate_publication(
                collection,
                documents_factory,
                batch_size=batch_size,
                counters=counters,
                started=started,
            )
            return True
        counters.rejected_publications += 1
        raise TrendBundlePublicationError(
            "MongoDB failed to atomically publish a trending bundle chunk",
            counters.report(started),
        ) from exc

    counters.bundles_created += int(result.upserted_count)
    counters.identical_content_retries += int(result.matched_count)
    return False


def write_trend_bundles(
    batch_df: DataFrame,
    collection: Any,
    config: Config,
    *,
    source_namespace: str,
    source_scope: str = DEFAULT_SOURCE_SCOPE,
    published_at: datetime | None = None,
    retention_seconds: int | None = None,
    batch_size: int = DEFAULT_WRITE_BATCH_SIZE,
) -> TrendBundleWriteReport:
    """Validate and atomically publish one immutable document per finalized window.

    ``source_namespace`` must be explicitly supplied by the caller and remain
    stable across retries. The default ``source_scope`` is descriptive
    ``unknown_or_mixed`` metadata; it is not evidence of prior-only input.
    Positive retention is measured from processing-time ``published_at`` and
    controls MongoDB TTL only. It does not establish event-time freshness.
    """
    from pyspark.sql import DataFrame, functions as F
    from pyspark.sql.types import TimestampType

    started = time.perf_counter()
    counters = _Counters()

    def invalid(message: str, cause: Exception | None = None):
        counters.rejected_publications += 1
        error = TrendBundleValidationError(message, counters.report(started))
        if cause is None:
            raise error
        raise error from cause

    if not isinstance(batch_df, DataFrame):
        invalid("batch_df must be a PySpark DataFrame")
    if batch_df.isStreaming:
        invalid("write_trend_bundles accepts only a static finalized-window DataFrame")
    if not isinstance(config, Config):
        invalid("config must be a Module 3 Config")
    collection_name = getattr(collection, "name", None)
    if collection_name is not None and collection_name != BUNDLE_COLLECTION_NAME:
        invalid(f"collection must be {BUNDLE_COLLECTION_NAME!r}")
    if any(
        not callable(getattr(collection, method, None))
        for method in ("index_information", "find", "create_index", "drop_index", "bulk_write")
    ):
        invalid("collection must provide Mongo index, find, and bulk-write operations")

    try:
        namespace = _validate_namespace(source_namespace, "source_namespace", maximum=256)
        scope = _validate_namespace(source_scope, "source_scope", maximum=128)
        top_k = config.mongo_top_k_per_window
        if type(top_k) is not int or not 1 <= top_k <= MAX_TREND_TOP_K:
            raise ValueError("mongo_top_k_per_window must be an integer between 1 and 20")
        retention = config.ttl_seconds if retention_seconds is None else retention_seconds
        if type(retention) is not int or retention < 0:
            raise ValueError("retention_seconds must be a nonnegative integer")
        if type(batch_size) is not int or not 1 <= batch_size <= MAX_WRITE_BATCH_SIZE:
            raise ValueError(f"batch_size must be between 1 and {MAX_WRITE_BATCH_SIZE}")
        if published_at is None:
            publication_time = datetime.now(timezone.utc)
        else:
            publication_time, _ = _canonical_datetime(
                published_at, "published_at", allow_naive_utc=False
            )

        missing = sorted(set(REQUIRED_ROW_FIELDS).difference(batch_df.columns))
        if missing:
            raise ValueError(f"trending batch is missing required fields: {missing}")
        for field in ("window_start", "window_end"):
            if not isinstance(batch_df.schema[field].dataType, TimestampType):
                raise ValueError(f"{field} must have Spark TimestampType")
        spark_timezone = batch_df.sparkSession.conf.get("spark.sql.session.timeZone", "UTC")
        if spark_timezone.upper() not in {"UTC", "ETC/UTC", "GMT", "ETC/GMT", "Z"}:
            raise ValueError("Spark session timezone must be UTC for canonical window timestamps")

        config_hash = _sha256(_config_payload(config, top_k))
    except (TypeError, ValueError) as exc:
        invalid(str(exc), exc)

    source = batch_df.select(*REQUIRED_ROW_FIELDS)
    try:
        for field in ("window_start", "window_end"):
            has_submillisecond_value = source.where(
                F.col(field).isNotNull()
                & (F.pmod(F.unix_micros(F.col(field)), F.lit(1000)) != F.lit(0))
            ).limit(1).count()
            if has_submillisecond_value:
                invalid(f"{field} must be aligned to BSON millisecond precision")

        window_summary = source.groupBy("window_end").agg(
            F.count(F.lit(1)).alias("_rows"),
            F.countDistinct("window_start").alias("_starts"),
        )
        if window_summary.where(F.col("_rows") > top_k).limit(1).count():
            invalid(f"input contains a finalized window larger than configured Top-{top_k}")
        if window_summary.where(F.col("_starts") > 1).limit(1).count():
            invalid("one window_end is associated with multiple window_start values")
        backwards = source.where(
            F.col("window_start").isNotNull()
            & F.col("window_end").isNotNull()
            & (F.col("window_end") <= F.col("window_start"))
        ).limit(1).count()
        if backwards:
            invalid("window_end must be later than window_start")
        expected_window_end = F.col("window_start") + F.expr(f"INTERVAL {config.window_long}")
        duration_mismatch = source.where(
            F.col("window_start").isNotNull()
            & F.col("window_end").isNotNull()
            & (F.col("window_end") != expected_window_end)
        ).limit(1).count()
        if duration_mismatch:
            invalid("window_start and window_end do not match configured window_long")
    except TrendBundleValidationError:
        raise
    except Exception as exc:
        counters.rejected_publications += 1
        raise TrendBundleValidationError(
            "Spark could not validate finalized window row bounds and timestamps",
            counters.report(started),
        ) from exc

    item_struct = F.struct(*(F.col(field).alias(field) for field in REQUIRED_ROW_FIELDS[2:]))
    grouped = (
        source.groupBy("window_start", "window_end")
        .agg(
            F.count(F.lit(1)).alias("_row_count"),
            F.collect_list(item_struct).alias("items"),
        )
        .persist()
    )

    def make_documents():
        return _bundle_documents(
            grouped,
            source_namespace=namespace,
            source_scope=scope,
            top_k=top_k,
            config_hash=config_hash,
            published_at=publication_time,
            retention_seconds=retention,
        )

    try:
        # Complete validation and conflict checks happen before the first write.
        try:
            for chunk in _chunks(make_documents(), batch_size):
                counters.windows_processed += len(chunk)
                counters.trend_rows_represented += sum(doc["row_count"] for doc in chunk)
                _validate_existing(collection, chunk, counters, started)
        except (TrendBundleConflictError, TrendBundlePublicationError):
            raise
        except Exception as exc:
            counters.rejected_publications += 1
            raise TrendBundleValidationError(
                f"trending window failed bundle validation: {exc}", counters.report(started)
            ) from exc

        if counters.windows_processed == 0:
            return counters.report(started)

        try:
            ensure_bundle_indexes(collection, retention)
        except Exception as exc:
            counters.rejected_publications += 1
            raise TrendBundlePublicationError(
                "failed to configure the trending bundle TTL index", counters.report(started)
            ) from exc

        # Re-iterate the persisted grouped DataFrame. Each chunk is bounded and
        # every operation is an atomic insert-only upsert of one whole document.
        for chunk in _chunks(make_documents(), batch_size):
            recovered = _upsert_chunk(
                collection,
                chunk,
                counters,
                started,
                documents_factory=make_documents,
                batch_size=batch_size,
            )
            if recovered:
                return counters.report(started)
        return counters.report(started)
    finally:
        grouped.unpersist()
