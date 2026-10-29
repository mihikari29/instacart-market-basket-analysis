"""Environment, redaction, run identity and numeric guards.

Every module in the project needs the same handful of cross-cutting
concerns. They live here once so the modules that use them cannot
drift apart.
"""

from __future__ import annotations

import argparse
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
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None


def env_float(name: str, default: float | None) -> float | None:
    """Read a float environment variable, falling back to `default`."""
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"{name} must be a number") from None


def read_yaml_mapping(path: Path) -> dict[str, Any]:
    """Read an explicitly selected YAML mapping without echoing source values."""
    import yaml

    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ValueError("pipeline configuration could not be read as valid YAML") from None
    if not isinstance(value, dict):
        raise ValueError("pipeline configuration must be a mapping")
    return value


def config_section(value: Mapping[str, Any], *keys: str) -> Mapping[str, Any]:
    """Read an optional mapping section, rejecting malformed present sections."""
    for index, key in enumerate(keys):
        value = value.get(key, {})
        if not isinstance(value, Mapping):
            raise ValueError(f"pipeline configuration {'.'.join(keys[:index + 1])} must be a mapping")
    return value


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



MONGODB_URI_PATTERN = re.compile(r"(?i)mongodb(?:\+srv)?://(?:<redacted>|[^\s\"'<>])+")
_SECRET_NAME = (
    r"(?:[a-z0-9]+[_-])*(?:user(?:name)?|password|passwd|pwd|token|secret|credentials?|"
    r"api[_-]?key|(?:access|private)[_-]?key)"
)
SECRET_FIELD_PATTERN = re.compile(_SECRET_NAME, re.IGNORECASE)
SECRET_ASSIGNMENT_PATTERN = re.compile(
    rf"(?i)(?<![\w-])({_SECRET_NAME})([\"']?\s*[:=]\s*)"
    r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;&}\]]+)"
)
SECRET_OPTION_PATTERN = re.compile(
    rf"(?i)(--{_SECRET_NAME})(\s+|=)"
    r"(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\r\n]+?(?=\s+--[\w-]+|$))"
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
    redacted = SECRET_OPTION_PATTERN.sub(r"\1\2<redacted>", redacted)
    return SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2<redacted>", redacted)


def redact_arguments(arguments: Sequence[str]) -> list[str]:
    """Keep argument positions while removing secret option values and URIs."""
    result = []
    secret_value = False
    for argument in arguments:
        option, separator, _ = argument.partition("=")
        if secret_value:
            result.append("<redacted>")
            secret_value = False
        elif option.startswith("--") and SECRET_FIELD_PATTERN.fullmatch(option[2:]):
            result.append(option + "=<redacted>" if separator else option)
            secret_value = not separator
        else:
            result.append(redact_sensitive_text(argument))
    return result


def redact_sensitive_values(value: Any) -> Any:
    """Copy report data with secret fields and strings sanitized for output."""
    if isinstance(value, Mapping):
        return {
            key: "<redacted>" if SECRET_FIELD_PATTERN.fullmatch(str(key).rsplit(".", 1)[-1]) else redact_sensitive_values(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_sensitive_values(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    return value


class SensitiveArgumentParser(argparse.ArgumentParser):
    """Keep argparse diagnostics useful without echoing secret arguments."""

    def error(self, message: str) -> None:
        super().error(redact_sensitive_text(message))


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


def save_redacted_json(path: Path, value) -> None:
    """Serialize human-readable evidence without changing live configuration."""
    save_json(path, redact_sensitive_values(value))



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


def strict_canonical_json(value: Any) -> str:
    """Encode publication fingerprints as UTF-8 JSON without non-finite numbers."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


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
