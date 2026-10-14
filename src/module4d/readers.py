"""Fail-closed, read-only readers for published Module 4D snapshots."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import math
from numbers import Real
import re
from typing import Any

from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME
from src.module4c.trend_reader import (
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
    TrendSnapshotRequestError,
    read_verified_trend_snapshot,
)


READINESS_COLLECTION = "module4_snapshot_readiness"
FINAL_RECOMMENDATIONS_COLLECTION = "final_recommendations"
TREND_BUNDLE_COLLECTION = BUNDLE_COLLECTION_NAME
SERVING_SCHEMA_VERSION = 1
MAX_RECOMMENDATIONS = 20
MAX_PRODUCT_NAME_CHARS = 4096
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_POINTER_FIELDS = {
    "schema_version",
    "_id",
    "user_id",
    "status",
    "ready",
    "serving_version",
    "expected_rows",
    "source_run_id",
    "model_version",
    "graph_version",
    "trend_source_namespace",
    "trend_window_end",
    "trend_content_sha256",
    "as_of_event_time",
    "algorithm_version",
    "mode",
    "reason",
    "input_content_sha256",
    "ranking_config_sha256",
    "als_top_n",
    "trend_top_m",
    "output_top_k",
    "epsilon",
    "updated_at",
}
_ROW_FIELDS = {
    "_id",
    "schema_version",
    "serving_version",
    "algorithm_version",
    "user_id",
    "product_id",
    "product_name",
    "als_score",
    "trend_score",
    "normalized_als_score",
    "normalized_trend_score",
    "final_score",
    "rank",
    "from_als",
    "from_trend",
    "lambda_requested",
    "lambda_effective",
    "mode",
    "reason",
    "source_run_id",
    "model_version",
    "graph_version",
    "trend_source_namespace",
    "trend_source_scope",
    "trend_window_start",
    "trend_window_end",
    "trend_content_sha256",
    "as_of_event_time",
    "input_content_sha256",
    "generated_at",
    "als_top_n",
    "trend_top_m",
    "output_top_k",
    "epsilon",
    "ranking_config_sha256",
}
_POINTER_IDENTITY_FIELDS = tuple(sorted(_POINTER_FIELDS))


class SnapshotReadError(RuntimeError):
    """Base error that the HTTP layer maps to a stable response."""


class SnapshotNotFound(SnapshotReadError):
    """No per-user Hybrid pointer has been published."""


class SnapshotNotReady(SnapshotReadError):
    """The per-user readiness pointer is not a valid ready contract."""


class SnapshotChanged(SnapshotReadError):
    """The selected per-user pointer changed during the read."""


class InvalidSnapshot(SnapshotReadError):
    """The persisted pointer or rows violate the C6 published contract."""


class DatabaseReadFailure(SnapshotReadError):
    """MongoDB could not complete a bounded read."""


@dataclass(frozen=True, slots=True)
class _ValidatedPointer:
    document: dict[str, Any]

    def identity(self) -> tuple[Any, ...]:
        return tuple(self.document.get(field) for field in _POINTER_IDENTITY_FIELDS)


def _stored_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise SnapshotNotReady(f"pointer {field} is not a BSON datetime")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise SnapshotNotReady(f"pointer {field} is not a valid UTC instant") from exc


def _finite_number(value: Any, field: str, *, minimum: float | None = None, maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise InvalidSnapshot(f"{field} must be a finite stored number")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise InvalidSnapshot(f"{field} must be a finite stored number") from exc
    if not math.isfinite(result):
        raise InvalidSnapshot(f"{field} must be a finite stored number")
    if minimum is not None and result < minimum:
        raise InvalidSnapshot(f"{field} is below its allowed range")
    if maximum is not None and result > maximum:
        raise InvalidSnapshot(f"{field} is above its allowed range")
    return result


def _sha256(value: Any, field: str, *, nullable: bool = False) -> str | None:
    if nullable and value is None:
        return None
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SnapshotNotReady(f"pointer {field} is not a lowercase SHA-256 digest")
    return value


def _nonempty(value: Any, field: str, *, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or value != value.strip()
        or "\x00" in value
    ):
        raise SnapshotNotReady(f"pointer {field} is missing or invalid")
    return value


def _validated_pointer(document: Any, user_id: int) -> _ValidatedPointer:
    if not isinstance(document, Mapping):
        raise SnapshotNotReady("per-user readiness pointer is not a document")
    if set(document) != _POINTER_FIELDS:
        raise SnapshotNotReady("per-user readiness pointer has an unsupported field schema")
    if type(document.get("schema_version")) is not int or document["schema_version"] != SERVING_SCHEMA_VERSION:
        raise SnapshotNotReady("per-user readiness pointer schema_version is unsupported")
    if document.get("_id") != f"hybrid:{user_id}" or type(document.get("user_id")) is not int or document[
        "user_id"
    ] != user_id:
        raise SnapshotNotReady("per-user readiness pointer identity is inconsistent")
    if document.get("status") != "ready" or document.get("ready") is not True:
        raise SnapshotNotReady("per-user readiness pointer is not ready")
    expected_rows = document.get("expected_rows")
    if type(expected_rows) is not int or not 0 <= expected_rows <= MAX_RECOMMENDATIONS:
        raise SnapshotNotReady("per-user readiness pointer expected_rows is invalid")

    normalized = dict(document)
    for field in (
        "source_run_id",
        "model_version",
        "graph_version",
        "algorithm_version",
        "mode",
        "trend_source_namespace",
    ):
        normalized[field] = _nonempty(document.get(field), field)
    normalized["serving_version"] = _nonempty(document.get("serving_version"), "serving_version", maximum=71)
    if not normalized["serving_version"].startswith("hybrid:") or _SHA256.fullmatch(
        normalized["serving_version"][7:]
    ) is None:
        raise SnapshotNotReady("per-user readiness pointer serving_version is invalid")
    normalized["input_content_sha256"] = _sha256(document.get("input_content_sha256"), "input_content_sha256")
    normalized["ranking_config_sha256"] = _sha256(
        document.get("ranking_config_sha256"), "ranking_config_sha256"
    )
    normalized["trend_content_sha256"] = _sha256(
        document.get("trend_content_sha256"), "trend_content_sha256", nullable=True
    )
    for field in ("als_top_n", "trend_top_m", "output_top_k"):
        if type(document.get(field)) is not int or not 1 <= document[field] <= MAX_RECOMMENDATIONS:
            raise SnapshotNotReady(f"per-user readiness pointer {field} is invalid")
    if expected_rows > document["output_top_k"]:
        raise SnapshotNotReady("per-user readiness pointer expected_rows exceeds output_top_k")
    try:
        normalized["epsilon"] = _finite_number(document.get("epsilon"), "epsilon", minimum=0.0)
    except InvalidSnapshot as exc:
        raise SnapshotNotReady("per-user readiness pointer epsilon is invalid") from exc
    if normalized["epsilon"] <= 0.0:
        raise SnapshotNotReady("per-user readiness pointer epsilon must be positive")
    reason = document.get("reason")
    if reason is not None and (not isinstance(reason, str) or not reason.strip() or len(reason) > 512):
        raise SnapshotNotReady("per-user readiness pointer reason is invalid")
    if expected_rows == 0 and reason is None:
        raise SnapshotNotReady("empty per-user readiness pointer must include a reason")
    normalized["trend_window_end"] = (
        None
        if document.get("trend_window_end") is None
        else _stored_utc(document.get("trend_window_end"), "trend_window_end")
    )
    normalized["as_of_event_time"] = _stored_utc(document.get("as_of_event_time"), "as_of_event_time")
    normalized["updated_at"] = _stored_utc(document.get("updated_at"), "updated_at")
    if normalized["trend_window_end"] is not None and normalized["trend_window_end"] > normalized[
        "as_of_event_time"
    ]:
        raise SnapshotNotReady("per-user readiness pointer trend window exceeds its as-of time")
    if (normalized["trend_content_sha256"] is None) != (normalized["trend_window_end"] is None):
        raise SnapshotNotReady("per-user readiness pointer trend digest and window end must agree")
    return _ValidatedPointer(normalized)


def _row_datetime(row: Mapping[str, Any], field: str) -> datetime | None:
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise InvalidSnapshot(f"row {field} must be a BSON datetime or null")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise InvalidSnapshot(f"row {field} is not a valid UTC instant") from exc


def _validate_rows(
    rows: list[Any], pointer: _ValidatedPointer, user_id: int
) -> tuple[list[dict[str, Any]], datetime | None, str | None, datetime | None]:
    document = pointer.document
    expected = document["expected_rows"]
    if len(rows) > MAX_RECOMMENDATIONS:
        raise InvalidSnapshot("stored recommendation rows exceed the accepted Top-20 bound")
    if len(rows) != expected:
        raise InvalidSnapshot("stored recommendation row count differs from the readiness pointer")
    if expected == 0:
        return [], None, None, None

    validated: list[dict[str, Any]] = []
    product_ids: set[int] = set()
    ranks: list[int] = []
    pointer_match = {
        "serving_version": document["serving_version"],
        "algorithm_version": document["algorithm_version"],
        "user_id": user_id,
        "mode": document["mode"],
        "reason": document["reason"],
        "source_run_id": document["source_run_id"],
        "model_version": document["model_version"],
        "graph_version": document["graph_version"],
        "trend_source_namespace": document["trend_source_namespace"],
        "trend_window_end": document["trend_window_end"],
        "trend_content_sha256": document["trend_content_sha256"],
        "as_of_event_time": document["as_of_event_time"],
        "input_content_sha256": document["input_content_sha256"],
        "als_top_n": document["als_top_n"],
        "trend_top_m": document["trend_top_m"],
        "output_top_k": document["output_top_k"],
        "epsilon": document["epsilon"],
        "ranking_config_sha256": document["ranking_config_sha256"],
    }
    trend_scopes: set[str | None] = set()
    trend_starts: set[datetime | None] = set()
    generated_times: set[datetime] = set()
    requested_lambdas: set[float] = set()
    effective_lambdas: set[float] = set()

    for row in rows:
        if not isinstance(row, Mapping) or set(row) != _ROW_FIELDS:
            raise InvalidSnapshot("stored recommendation row has an unsupported field schema")
        if type(row.get("schema_version")) is not int or row["schema_version"] != SERVING_SCHEMA_VERSION:
            raise InvalidSnapshot("stored recommendation row schema_version is unsupported")
        product_id = row.get("product_id")
        rank = row.get("rank")
        if type(product_id) is not int or product_id <= 0:
            raise InvalidSnapshot("stored recommendation product_id is invalid")
        if type(rank) is not int or not 1 <= rank <= MAX_RECOMMENDATIONS:
            raise InvalidSnapshot("stored recommendation rank is invalid")
        if product_id in product_ids:
            raise InvalidSnapshot("stored recommendation products are not unique")
        if row.get("_id") != f"{document['serving_version']}:{user_id}:{product_id}":
            raise InvalidSnapshot("stored recommendation _id does not match its immutable identity")
        product_ids.add(product_id)
        ranks.append(rank)
        for field, expected_value in pointer_match.items():
            value = row.get(field)
            if field in {"trend_window_end", "as_of_event_time"}:
                value = _row_datetime(row, field)
            if value != expected_value:
                raise InvalidSnapshot(f"stored recommendation {field} differs from its pointer")
        if row.get("reason") is not None and not isinstance(row.get("reason"), str):
            raise InvalidSnapshot("stored recommendation reason is invalid")
        product_name = row.get("product_name")
        if (
            not isinstance(product_name, str)
            or not product_name.strip()
            or len(product_name) > MAX_PRODUCT_NAME_CHARS
        ):
            raise InvalidSnapshot("stored recommendation product_name is invalid")

        from_als = row.get("from_als")
        from_trend = row.get("from_trend")
        if type(from_als) is not bool or type(from_trend) is not bool or not (from_als or from_trend):
            raise InvalidSnapshot("stored recommendation modality flags are invalid")
        als_score = row.get("als_score")
        trend_score = row.get("trend_score")
        normalized_als = _finite_number(
            row.get("normalized_als_score"), "normalized_als_score", minimum=0.0, maximum=1.0
        )
        normalized_trend = _finite_number(
            row.get("normalized_trend_score"), "normalized_trend_score", minimum=0.0, maximum=1.0
        )
        if from_als:
            _finite_number(als_score, "als_score")
        elif als_score is not None or normalized_als != 0.0:
            raise InvalidSnapshot("ALS-only score fields violate modality nullability")
        if from_trend:
            _finite_number(trend_score, "trend_score", minimum=0.0, maximum=1.0)
            if document["trend_content_sha256"] is None:
                raise InvalidSnapshot("trend recommendation lacks verified trend provenance")
        elif trend_score is not None or normalized_trend != 0.0:
            raise InvalidSnapshot("trend score fields violate modality nullability")
        _finite_number(row.get("final_score"), "final_score", minimum=0.0, maximum=1.0)
        requested_lambda = _finite_number(
            row.get("lambda_requested"), "lambda_requested", minimum=0.0, maximum=1.0
        )
        effective_lambda = _finite_number(
            row.get("lambda_effective"), "lambda_effective", minimum=0.0, maximum=1.0
        )
        requested_lambdas.add(requested_lambda)
        effective_lambdas.add(effective_lambda)
        for field in ("als_top_n", "trend_top_m", "output_top_k"):
            if type(row.get(field)) is not int or not 1 <= row[field] <= MAX_RECOMMENDATIONS:
                raise InvalidSnapshot(f"stored recommendation {field} is invalid")
        _finite_number(row.get("epsilon"), "epsilon", minimum=0.0)
        if row["epsilon"] <= 0.0:
            raise InvalidSnapshot("stored recommendation epsilon must be positive")
        for field in ("input_content_sha256", "ranking_config_sha256"):
            if not isinstance(row.get(field), str) or _SHA256.fullmatch(row[field]) is None:
                raise InvalidSnapshot(f"stored recommendation {field} is invalid")
        scope = row.get("trend_source_scope")
        if scope is not None and (not isinstance(scope, str) or not scope or len(scope) > 128):
            raise InvalidSnapshot("stored recommendation trend_source_scope is invalid")
        row_trend_start = _row_datetime(row, "trend_window_start")
        row_trend_end = _row_datetime(row, "trend_window_end")
        if document["trend_content_sha256"] is None:
            if scope is not None or row_trend_start is not None or row_trend_end is not None:
                raise InvalidSnapshot("recommendation has trend metadata without a verified trend digest")
        elif scope is None or row_trend_start is None or row_trend_end is None:
            raise InvalidSnapshot("recommendation is missing verified trend metadata")
        trend_scopes.add(scope)
        trend_starts.add(row_trend_start)
        generated = _row_datetime(row, "generated_at")
        if generated is None:
            raise InvalidSnapshot("stored recommendation generated_at is missing")
        generated_times.add(generated)
        normalized = dict(row)
        normalized["trend_window_start"] = _row_datetime(row, "trend_window_start")
        normalized["trend_window_end"] = _row_datetime(row, "trend_window_end")
        normalized["as_of_event_time"] = _row_datetime(row, "as_of_event_time")
        normalized["generated_at"] = generated
        normalized["als_score"] = float(als_score) if als_score is not None else None
        normalized["trend_score"] = float(trend_score) if trend_score is not None else None
        normalized["normalized_als_score"] = normalized_als
        normalized["normalized_trend_score"] = normalized_trend
        normalized["final_score"] = float(row["final_score"])
        validated.append(normalized)

    if sorted(ranks) != list(range(1, expected + 1)):
        raise InvalidSnapshot("stored recommendation ranks are not contiguous from 1")
    if (
        len(trend_scopes) != 1
        or len(trend_starts) != 1
        or len(generated_times) != 1
        or len(requested_lambdas) != 1
        or len(effective_lambdas) != 1
    ):
        raise InvalidSnapshot("stored recommendation provenance differs across rows")
    if next(iter(generated_times)) != document["updated_at"]:
        raise InvalidSnapshot("stored recommendation generated_at differs from pointer updated_at")
    trend_start = next(iter(trend_starts))
    if trend_start is not None and (
        document["trend_window_end"] is None or trend_start >= document["trend_window_end"]
    ):
        raise InvalidSnapshot("stored recommendation trend window bounds are inconsistent")
    validated.sort(key=lambda item: item["rank"])
    return validated, next(iter(generated_times)), next(iter(trend_scopes)), trend_start


def _close_cursor(cursor: Any) -> None:
    close = getattr(cursor, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def read_hybrid_snapshot(database: Any, user_id: int, *, max_time_ms: int = 5_000) -> dict[str, Any]:
    """Read one exact immutable Hybrid version bracketed by its readiness pointer."""
    if type(user_id) is not int or not 0 < user_id <= 9_223_372_036_854_775_807:
        raise ValueError("user_id must be a positive integer")
    readiness = database[READINESS_COLLECTION]
    try:
        first_raw = readiness.find_one({"_id": f"hybrid:{user_id}"}, max_time_ms=max_time_ms)
    except Exception as exc:
        raise DatabaseReadFailure("MongoDB readiness lookup failed") from exc
    if first_raw is None:
        raise SnapshotNotFound("no published Hybrid snapshot was found")
    first = _validated_pointer(first_raw, user_id)

    query = {"user_id": user_id, "serving_version": first.document["serving_version"]}
    cursor = None
    try:
        projection = {field: 1 for field in _ROW_FIELDS}
        cursor = database[FINAL_RECOMMENDATIONS_COLLECTION].find(
            query, projection=projection, max_time_ms=max_time_ms
        )
        cursor = (
            cursor.hint("uq_serving_user_rank")
            .sort([("rank", 1)])
            .limit(MAX_RECOMMENDATIONS + 1)
            .max_time_ms(max_time_ms)
        )
        rows = list(cursor)
    except Exception as exc:
        raise DatabaseReadFailure("MongoDB recommendation row lookup failed") from exc
    finally:
        if cursor is not None:
            _close_cursor(cursor)
    if len(rows) > MAX_RECOMMENDATIONS:
        raise InvalidSnapshot("stored recommendation rows exceed the accepted Top-20 bound")
    result_rows, generated_at, scope, trend_start = _validate_rows(rows, first, user_id)

    try:
        second_raw = readiness.find_one({"_id": f"hybrid:{user_id}"}, max_time_ms=max_time_ms)
    except Exception as exc:
        raise DatabaseReadFailure("MongoDB readiness recheck failed") from exc
    if second_raw is None:
        raise SnapshotChanged("per-user readiness pointer disappeared during the read")
    try:
        second = _validated_pointer(second_raw, user_id)
    except SnapshotNotReady as exc:
        raise SnapshotChanged("per-user readiness pointer changed during the read") from exc
    if first.identity() != second.identity():
        raise SnapshotChanged("per-user readiness pointer changed during the read")

    pointer = first.document
    return {
        "user_id": user_id,
        "serving_version": pointer["serving_version"],
        "status": "ready",
        "recommendation_count": len(result_rows),
        "mode": pointer["mode"],
        "reason": pointer["reason"],
        "source": {
            "source_run_id": pointer["source_run_id"],
            "model_version": pointer["model_version"],
            "graph_version": pointer["graph_version"],
            "trend_source_namespace": pointer["trend_source_namespace"],
            "trend_source_scope": scope,
            "trend_window_start": trend_start,
            "trend_window_end": pointer["trend_window_end"],
            "trend_content_sha256": pointer["trend_content_sha256"],
            "as_of_event_time": pointer["as_of_event_time"],
            "input_content_sha256": pointer["input_content_sha256"],
            "pointer_updated_at": pointer["updated_at"],
        },
        "ranking_config": {
            "algorithm_version": pointer["algorithm_version"],
            "lambda_requested": result_rows[0]["lambda_requested"] if result_rows else None,
            "lambda_effective": result_rows[0]["lambda_effective"] if result_rows else None,
            "als_top_n": pointer["als_top_n"],
            "trend_top_m": pointer["trend_top_m"],
            "output_top_k": pointer["output_top_k"],
            "epsilon": pointer["epsilon"],
            "ranking_config_sha256": pointer["ranking_config_sha256"],
        },
        "generated_at": generated_at,
        "recommendations": [
            {
                key: row[key]
                for key in (
                    "product_id",
                    "product_name",
                    "rank",
                    "from_als",
                    "from_trend",
                    "als_score",
                    "trend_score",
                    "normalized_als_score",
                    "normalized_trend_score",
                    "final_score",
                )
            }
            for row in result_rows
        ],
    }


class _TrendCursorAdapter:
    """Translate C5's logical query/sort to the bundle's indexed _id range."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def sort(self, keys: list[tuple[str, int]]) -> _TrendCursorAdapter:
        if keys != [("window_end", -1)]:
            raise ValueError("C5 trend read requested an unsupported sort")
        self._cursor.sort([("_id", -1)])
        return self

    def limit(self, value: int) -> _TrendCursorAdapter:
        if value != 1:
            raise ValueError("C5 trend read must remain limited to one bundle")
        self._cursor.limit(1)
        return self

    def __iter__(self):
        try:
            yield from self._cursor
        finally:
            _close_cursor(self._cursor)

    def close(self) -> None:
        _close_cursor(self._cursor)


class _IndexedTrendCollectionAdapter:
    """Expose the C5 collection protocol while querying only deterministic bundle IDs."""

    name = BUNDLE_COLLECTION_NAME

    def __init__(self, collection: Any, max_time_ms: int) -> None:
        self._collection = collection
        self._max_time_ms = max_time_ms

    def find(self, query: Mapping[str, Any]) -> _TrendCursorAdapter:
        if set(query) != {"source_namespace", "window_end"}:
            raise ValueError("C5 trend query has an unsupported shape")
        namespace = query["source_namespace"]
        bound = query["window_end"]
        if not isinstance(namespace, str) or not isinstance(bound, Mapping) or set(bound) != {"$lte"}:
            raise ValueError("C5 trend query has invalid namespace or event-time bound")
        cutoff = bound["$lte"]
        if not isinstance(cutoff, datetime):
            raise ValueError("C5 trend query cutoff must be a datetime")
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        cutoff = cutoff.astimezone(timezone.utc)
        canonical_cutoff = cutoff.isoformat(timespec="microseconds").replace("+00:00", "Z")
        prefix = f"trend-window:{hashlib.sha256(namespace.encode('utf-8')).hexdigest()}:"
        cursor = self._collection.find(
            {"_id": {"$gte": prefix, "$lte": prefix + canonical_cutoff}},
            max_time_ms=self._max_time_ms,
        )
        cursor = cursor.hint("_id_")
        return _TrendCursorAdapter(cursor)


def read_trend_snapshot(
    database: Any,
    *,
    source_namespace: str,
    as_of_event_time: datetime,
    max_event_age_seconds: int | None = None,
    max_time_ms: int = 5_000,
) -> Any:
    """Call the accepted C5 verifier using the deterministic indexed bundle key."""
    age = None
    if max_event_age_seconds is not None:
        if type(max_event_age_seconds) is not int or not 0 <= max_event_age_seconds <= 31_536_000:
            raise TrendSnapshotRequestError("max_event_age_seconds must be between 0 and 31536000")
        age = timedelta(seconds=max_event_age_seconds)
    try:
        collection = database[TREND_BUNDLE_COLLECTION]
        return read_verified_trend_snapshot(
            _IndexedTrendCollectionAdapter(collection, max_time_ms),
            source_namespace=source_namespace,
            as_of_event_time=as_of_event_time,
            max_event_age=age,
        )
    except (TrendSnapshotRequestError, TrendSnapshotIntegrityError, TrendSnapshotQueryError):
        raise
    except Exception as exc:
        raise TrendSnapshotQueryError("failed to read the finalized trend bundle") from exc
