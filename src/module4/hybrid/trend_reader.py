"""Read and validate one immutable Module 3 trending-window bundle."""

from __future__ import annotations
from ...common.runtime import positive_int

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import re
from numbers import Real
from typing import Any

from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    BUNDLE_SCHEMA_VERSION,
    MAX_TREND_TOP_K,
    build_document,
    canonical_window_datetime,
    validate_namespace,
)

from .config import TrendRecommendation, TrendSnapshot


_ITEM_FIELDS = {
    "product_id",
    "purchase_count_30m",
    "purchase_count_120m",
    "trend_score",
    "trend_rank",
}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class TrendSnapshotReaderError(RuntimeError):
    """Base error raised by the atomic trend snapshot reader."""


class TrendSnapshotRequestError(TrendSnapshotReaderError, ValueError):
    """The reader arguments or supplied collection do not meet its contract."""


class TrendSnapshotIntegrityError(TrendSnapshotReaderError, ValueError):
    """A selected atomic bundle is malformed or fails identity/hash checks."""


class TrendSnapshotQueryError(TrendSnapshotReaderError):
    """MongoDB failed to execute the bounded bundle query."""


@dataclass(frozen=True, slots=True)
class TrendSnapshotReadResult:
    """Validated snapshot metadata and its C1-C2-compatible trend view."""

    available: bool
    status: str
    reason: str | None
    source_namespace: str
    source_scope: str | None
    window_start: datetime | None
    window_end: datetime | None
    published_at: datetime | None
    expires_at: datetime | None
    schema_version: int | None
    config_hash: str | None
    content_sha256: str | None
    trend_recommendations: tuple[TrendRecommendation, ...]

    @property
    def trend_snapshot(self) -> TrendSnapshot:
        """Return the existing C1-C2 input type without changing trend scores."""
        return TrendSnapshot(
            available=self.available,
            rows=self.trend_recommendations if self.available else (),
            window_start=self.window_start,
            window_end=self.window_end,
            unavailable_reason=self.reason,
        )


def _required_aware_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TrendSnapshotRequestError(f"{field} must be a timezone-aware datetime")
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise TrendSnapshotRequestError(f"{field} must be a valid timezone-aware datetime") from exc


def _stored_bson_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TrendSnapshotIntegrityError(f"{field} must be a BSON datetime")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            # PyMongo's default BSON decoder returns naive datetimes as UTC.
            # Attach UTC directly; never route them through the host timezone.
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise TrendSnapshotIntegrityError(f"{field} is not a valid UTC instant") from exc


def _validate_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise TrendSnapshotIntegrityError(f"{field} must be a lowercase hexadecimal SHA-256 digest")
    return value



def _validate_document(
    document: Any,
    *,
    requested_namespace: str,
    as_of_event_time: datetime,
) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise TrendSnapshotIntegrityError("selected bundle is not a document")

    schema_version = document.get("schema_version")
    if type(schema_version) is not int or schema_version != BUNDLE_SCHEMA_VERSION:
        raise TrendSnapshotIntegrityError("bundle schema_version is unsupported")

    try:
        source_namespace = validate_namespace(
            document.get("source_namespace"), "source_namespace", maximum=256
        )
        source_scope = validate_namespace(
            document.get("source_scope"), "source_scope", maximum=128
        )
    except (TypeError, ValueError) as exc:
        raise TrendSnapshotIntegrityError(str(exc)) from exc
    if source_namespace != requested_namespace:
        raise TrendSnapshotIntegrityError("selected bundle source_namespace does not match the request")

    try:
        window_start, _start_iso = canonical_window_datetime(
            document.get("window_start"), "window_start"
        )
        window_end, _end_iso = canonical_window_datetime(document.get("window_end"), "window_end")
    except (TypeError, ValueError, OverflowError) as exc:
        raise TrendSnapshotIntegrityError(f"bundle window timestamp is invalid: {exc}") from exc
    if window_start >= window_end:
        raise TrendSnapshotIntegrityError("window_start must be earlier than window_end")
    if window_end > as_of_event_time:
        raise TrendSnapshotIntegrityError("selected bundle window_end exceeds as_of_event_time")

    top_k = document.get("top_k")
    if type(top_k) is not int or not 1 <= top_k <= MAX_TREND_TOP_K:
        raise TrendSnapshotIntegrityError("top_k must be an integer between 1 and 20")

    items = document.get("items")
    if not isinstance(items, list) or not items:
        raise TrendSnapshotIntegrityError("a published bundle must contain a nonempty items array")
    if len(items) > top_k:
        raise TrendSnapshotIntegrityError("bundle item count exceeds top_k")

    row_count = document.get("row_count")
    if type(row_count) is not int or row_count != len(items):
        raise TrendSnapshotIntegrityError("row_count must equal the number of items")

    product_ids: set[int] = set()
    ranks: list[int] = []
    recommendations: list[TrendRecommendation] = []
    for position, item in enumerate(items, start=1):
        if not isinstance(item, Mapping):
            raise TrendSnapshotIntegrityError(f"items[{position - 1}] must be a document")
        if set(item) != _ITEM_FIELDS:
            raise TrendSnapshotIntegrityError(
                f"items[{position - 1}] must contain exactly the C4A item fields"
            )

        product_id = positive_int(item.get("product_id"), "product_id", error=TrendSnapshotIntegrityError)
        rank = positive_int(item.get("trend_rank"), "trend_rank", error=TrendSnapshotIntegrityError)
        if product_id in product_ids:
            raise TrendSnapshotIntegrityError(f"duplicate product_id {product_id}")
        product_ids.add(product_id)
        ranks.append(rank)

        purchase_count_30m = item.get("purchase_count_30m")
        purchase_count_120m = item.get("purchase_count_120m")
        if type(purchase_count_30m) is not int or purchase_count_30m < 0:
            raise TrendSnapshotIntegrityError("purchase_count_30m must be a nonnegative integer")
        if type(purchase_count_120m) is not int or purchase_count_120m < 0:
            raise TrendSnapshotIntegrityError("purchase_count_120m must be a nonnegative integer")
        if purchase_count_30m > purchase_count_120m:
            raise TrendSnapshotIntegrityError(
                "purchase_count_30m cannot exceed purchase_count_120m"
            )

        trend_score = item.get("trend_score")
        if type(trend_score) is not float or not isinstance(trend_score, Real):
            raise TrendSnapshotIntegrityError("trend_score must be a stored finite float")
        if not math.isfinite(trend_score) or not 0.0 <= trend_score <= 1.0:
            raise TrendSnapshotIntegrityError("trend_score must be finite and within [0, 1]")
        recommendations.append(
            TrendRecommendation(
                product_id=product_id,
                trend_score=trend_score,
                trend_rank=rank,
            )
        )

    if ranks != list(range(1, len(items) + 1)):
        raise TrendSnapshotIntegrityError("items must be ordered by contiguous ranks starting at 1")

    config_hash = _validate_hash(document.get("config_hash"), "config_hash")
    content_sha256 = _validate_hash(document.get("content_sha256"), "content_sha256")
    published_at = _stored_bson_utc(document.get("published_at"), "published_at")
    expires_at = None
    if "expires_at" in document:
        expires_at = _stored_bson_utc(document["expires_at"], "expires_at")
        if expires_at <= published_at:
            raise TrendSnapshotIntegrityError("expires_at must be later than published_at")

    try:
        rebuilt = build_document(
            {
                "window_start": window_start,
                "window_end": window_end,
                "items": items,
                "_row_count": row_count,
            },
            source_namespace=source_namespace,
            source_scope=source_scope,
            top_k=top_k,
            config_hash=config_hash,
            published_at=published_at,
            retention_seconds=0,
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise TrendSnapshotIntegrityError(f"bundle logical content is invalid: {exc}") from exc

    if document.get("_id") != rebuilt["_id"]:
        raise TrendSnapshotIntegrityError("bundle _id does not match the C4A identity algorithm")
    if content_sha256 != rebuilt["content_sha256"]:
        raise TrendSnapshotIntegrityError("content_sha256 does not match the C4A logical content")

    return {
        "source_namespace": source_namespace,
        "source_scope": source_scope,
        "window_start": window_start,
        "window_end": window_end,
        "published_at": published_at,
        "expires_at": expires_at,
        "schema_version": schema_version,
        "config_hash": config_hash,
        "content_sha256": content_sha256,
        "trend_recommendations": tuple(recommendations),
    }


def _unavailable_result(
    *,
    status: str,
    reason: str,
    source_namespace: str,
    metadata: Mapping[str, Any] | None = None,
) -> TrendSnapshotReadResult:
    values = metadata or {}
    return TrendSnapshotReadResult(
        available=False,
        status=status,
        reason=reason,
        source_namespace=source_namespace,
        source_scope=values.get("source_scope"),
        window_start=values.get("window_start"),
        window_end=values.get("window_end"),
        published_at=values.get("published_at"),
        expires_at=values.get("expires_at"),
        schema_version=values.get("schema_version"),
        config_hash=values.get("config_hash"),
        content_sha256=values.get("content_sha256"),
        trend_recommendations=(),
    )


def read_verified_trend_snapshot(
    collection: Any,
    *,
    source_namespace: str,
    as_of_event_time: datetime,
    max_event_age: timedelta | None = None,
    processing_time_utc: datetime | None = None,
) -> TrendSnapshotReadResult:
    """Read the newest complete C4A bundle eligible at an event-time instant.

    The collection is supplied by the caller and is only queried. BSON's
    millisecond date resolution is accounted for when encoding the as-of bound.
    An optional ``max_event_age`` is an explicit event-time policy; it is
    independent of processing-time TTL expiry.
    """
    try:
        namespace = validate_namespace(source_namespace, "source_namespace", maximum=256)
    except (TypeError, ValueError) as exc:
        raise TrendSnapshotRequestError(str(exc)) from exc

    collection_name = getattr(collection, "name", None)
    if collection_name is not None and collection_name != BUNDLE_COLLECTION_NAME:
        raise TrendSnapshotRequestError(f"collection must be {BUNDLE_COLLECTION_NAME!r}")
    if not callable(getattr(collection, "find", None)):
        raise TrendSnapshotRequestError("collection must provide a find operation")

    event_time = _required_aware_utc(as_of_event_time, "as_of_event_time")
    if max_event_age is not None and (
        not isinstance(max_event_age, timedelta) or max_event_age < timedelta(0)
    ):
        raise TrendSnapshotRequestError("max_event_age must be a nonnegative timedelta or None")
    if processing_time_utc is None:
        processing_time = datetime.now(timezone.utc)
    else:
        processing_time = _required_aware_utc(processing_time_utc, "processing_time_utc")

    # C4A windows are millisecond-aligned. Flooring the as-of query bound to a
    # BSON millisecond includes every eligible stored instant while excluding
    # the next millisecond; the exact Python boundary is checked after reading.
    query_cutoff = event_time.replace(microsecond=(event_time.microsecond // 1000) * 1000)
    query = {
        "source_namespace": namespace,
        "window_end": {"$lte": query_cutoff},
    }
    try:
        cursor = collection.find(query).sort([("window_end", -1)]).limit(1)
        document = next(iter(cursor), None)
    except Exception as exc:
        raise TrendSnapshotQueryError("failed to read the latest atomic trend bundle") from exc

    if document is None:
        return _unavailable_result(
            status="missing",
            reason="no_matching_snapshot",
            source_namespace=namespace,
        )

    metadata = _validate_document(
        document,
        requested_namespace=namespace,
        as_of_event_time=event_time,
    )

    expires_at = metadata["expires_at"]
    if expires_at is not None and expires_at <= processing_time:
        return _unavailable_result(
            status="expired",
            reason="snapshot_expired",
            source_namespace=namespace,
            metadata=metadata,
        )

    if max_event_age is not None and event_time - metadata["window_end"] > max_event_age:
        return _unavailable_result(
            status="stale",
            reason="snapshot_event_time_stale",
            source_namespace=namespace,
            metadata=metadata,
        )

    return TrendSnapshotReadResult(
        available=True,
        status="available",
        reason=None,
        source_namespace=metadata["source_namespace"],
        source_scope=metadata["source_scope"],
        window_start=metadata["window_start"],
        window_end=metadata["window_end"],
        published_at=metadata["published_at"],
        expires_at=metadata["expires_at"],
        schema_version=metadata["schema_version"],
        config_hash=metadata["config_hash"],
        content_sha256=metadata["content_sha256"],
        trend_recommendations=metadata["trend_recommendations"],
    )
