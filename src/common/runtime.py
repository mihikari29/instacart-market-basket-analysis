"""Environment, redaction, run identity and numeric guards.

Every module in the project needs the same handful of cross-cutting
concerns. They live here once so the modules that use them cannot
drift apart.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
import math
from numbers import Real
import os
from pathlib import Path
import re
import subprocess
import uuid
from typing import Any
from urllib.parse import urlsplit, urlunsplit




def env_int(name: str, default: int | None) -> int | None:
    """Read an integer environment variable, falling back to `default`."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {value!r}") from exc


def env_float(name: str, default: float | None) -> float | None:
    """Read a float environment variable, falling back to `default`."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc


def parse_bool(value: str | bool) -> bool:
    """Parse an explicit environment/CLI boolean without truthy-string traps."""
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        "Boolean value must be one of: true, false, 1, 0, yes, no, on, off"
    )



MONGODB_URI_PATTERN = re.compile(r"(?i)mongodb(?:\+srv)?://[^\s\"'<>]+")
SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)"
)


def redact_uri(uri: str) -> str:
    """Return a connection string with its credentials removed."""
    try:
        parsed = urlsplit(uri)
        if not parsed.scheme:
            return "<redacted>"
        hostname = parsed.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        if parsed.port:
            hostname = f"{hostname}:{parsed.port}"
        authority = f"***@{hostname}" if parsed.username or parsed.password else hostname
        return urlunsplit(
            (parsed.scheme, authority, parsed.path, "<redacted>" if parsed.query else "", "")
        )
    except (TypeError, ValueError):
        return "<redacted>"


def redact_sensitive_text(value: str) -> str:
    """Remove MongoDB credentials and inline secret assignments from text."""
    redacted = MONGODB_URI_PATTERN.sub(lambda match: redact_uri(match.group(0)), value)
    return SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2<redacted>", redacted)


def safe_error_text(exc: BaseException) -> str:
    """Render an exception for a report or log without leaking credentials."""
    return redact_sensitive_text(f"{type(exc).__name__}: {exc}")


RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}")


def new_run_id() -> str:
    """Return a sortable, collision-resistant run identifier."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]


def validate_run_id(run_id: str) -> str:
    """Require a path-safe run identifier."""
    if not RUN_ID_PATTERN.fullmatch(run_id or "") or ".." in run_id:
        raise ValueError("run_id must be a 1-80 character path-safe identifier")
    return run_id


def create_run_directory(base: Path, run_id: str) -> Path:
    """Create and return the directory that owns one run's artifacts."""
    validate_run_id(run_id)
    directory = base / run_id
    directory.mkdir(parents=True, exist_ok=False)
    return directory


def save_json(path: Path, value) -> None:
    """Write a JSON report, creating the parent directory when needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )



#: Repository root, derived from this file's location rather than the cwd.
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def repository_root() -> Path:
    """Return the repository root."""
    return REPOSITORY_ROOT


def git_revision(fallback_env: str) -> dict:
    """Return the current commit and whether the tree is dirty.

    The `safe.directory` override keeps this usable on a WSL or Windows bind
    mount, where the repository is owned by a different user than the caller.
    """
    root = REPOSITORY_ROOT
    try:
        git = ["git", "-c", f"safe.directory={root}"]
        commit = subprocess.check_output([*git, "rev-parse", "HEAD"], cwd=root, text=True).strip()
        dirty = bool(subprocess.check_output([*git, "status", "--porcelain"], cwd=root, text=True))
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": os.environ.get(fallback_env, "unavailable"), "dirty": None}


def code_digest() -> str:
    """Return a digest over every Python source file in the repository."""
    digest = hashlib.sha256()
    for path in sorted(REPOSITORY_ROOT.joinpath("src").rglob("*.py")):
        digest.update(path.relative_to(REPOSITORY_ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()




def positive_int(value: Any, field: str, *, error: type[Exception] = ValueError, message: str | None = None) -> int:
    """Require a strictly positive integer.

    `error` lets a caller raise its own domain error while the check itself
    stays in one place.
    """
    if type(value) is not int or value <= 0:
        raise error(message or f"{field} must be a positive integer")
    return value


def file_sha256(path: str | Path) -> str:
    """Compute the SHA-256 digest of a file in 8 MiB chunks."""
    target = Path(path)
    digest = hashlib.sha256()
    with target.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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
