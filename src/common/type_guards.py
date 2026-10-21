"""Numeric and record guards shared by the Module 3 and Module 4 readers.

These helpers exist so that a malformed stored document fails with a precise
message instead of a `TypeError` from deep inside a comparison. They are
deliberately strict: a boolean is never accepted as a number, and a string is
never coerced.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
import math
from numbers import Real
from typing import Any


def positive_int(value: Any, field: str, *, error: type[Exception] = ValueError, message: str | None = None) -> int:
    """Require a strictly positive integer.

    `error` lets a caller raise its own domain error while the check itself
    stays in one place.
    """
    if type(value) is not int or value <= 0:
        raise error(message or f"{field} must be a positive integer")
    return value


def stored_utc(value: Any, field: str, *, error: type[Exception] = ValueError, message: str | None = None) -> datetime:
    """Return a stored BSON datetime as UTC.

    A naive datetime is read as UTC, which is how the writers store it.
    """
    if not isinstance(value, datetime):
        raise error(message or f"{field} must be a BSON datetime")
    try:
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise error(f"{field} must be a valid UTC instant") from exc


def aware_utc_millisecond(value: Any, field: str, *, error: type[Exception] = ValueError) -> datetime:
    """Return a timezone-aware datetime as UTC, aligned to BSON milliseconds."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise error(f"{field} must be a timezone-aware datetime")
    try:
        normalized = value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise error(f"{field} must be a valid UTC instant") from exc
    if normalized.microsecond % 1000:
        raise error(f"{field} must be aligned to BSON millisecond precision")
    return normalized


def finite_number(value: Any, field: str, *, minimum: float | None = None) -> float:
    """Require a finite number, optionally within a lower bound."""
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    if minimum is not None and number < minimum:
        raise ValueError(f"{field} is outside its allowed range")
    return number


def record_mapping(row: Any, field: str) -> Mapping[str, Any]:
    """Return a row as a mapping, accepting a Spark Row as well."""
    if isinstance(row, Mapping):
        return row
    as_dict = getattr(row, "asDict", None)
    if callable(as_dict):
        result = as_dict(recursive=True)
        if isinstance(result, Mapping):
            return result
    raise ValueError(f"{field} returned a non-record row")


def canonical_json(value: Any) -> str:
    """Serialize deterministically so a digest depends only on the content."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_hex(payload: bytes | str) -> str:
    """Return the hex SHA-256 digest of bytes or text."""
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    return hashlib.sha256(data).hexdigest()


def require_columns(columns: Sequence[str], expected: frozenset[str], field: str) -> None:
    """Require an exact column set and report the difference."""
    missing = sorted(expected.difference(columns))
    unexpected = sorted(set(columns).difference(expected))
    if missing or unexpected:
        problems = []
        if missing:
            problems.append("missing fields " + ", ".join(missing))
        if unexpected:
            problems.append("unexpected fields " + ", ".join(unexpected))
        raise ValueError(f"{field} schema mismatch: " + "; ".join(problems))
