"""Shared primitives for the Kubernetes coordinator modules.

The coordinator grew one phase at a time — B2 serving, B3 pipeline, B4
acceptance, then teardown and image provenance — and each phase re-implemented
the same primitives: hashing a file, digesting a JSON document, writing a
receipt atomically, running `kubectl` and parsing its JSON output, and
redacting credentials from captured text.

This module owns those primitives once. The domain errors stay with the
module that raised them: callers translate a failure here into their own
error type so the error taxonomy the acceptance receipts record is unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping

from src.common.type_guards import canonical_json

#: A MongoDB connection string anywhere in free text.
MONGO_URI_PATTERN = re.compile(r"(?i)mongodb(?:\+srv)?://[^\s\"'<>]+")

#: An inline `password=`, `token:` style assignment in free text.
SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)"
)


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of a file, read in bounded blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(value: bytes) -> str:
    """Return the hex SHA-256 digest of a byte string."""
    return hashlib.sha256(value).hexdigest()




def sha256_json(value: Any) -> str:
    """Return the hex SHA-256 digest of a JSON-serializable value."""
    return sha256_bytes(canonical_json(value).encode("utf-8"))


def redact_secrets(value: str) -> str:
    """Remove MongoDB connection strings and inline secrets from text.

    The whole connection string is replaced rather than only its credentials,
    because coordinator output is published as acceptance evidence and must
    not reveal which MongoDB instance a run talked to.
    """
    redacted = MONGO_URI_PATTERN.sub("mongodb://<redacted>", value)
    return SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2<redacted>", redacted)


def write_json_atomic(
    path: Path,
    payload: Mapping[str, Any],
    *,
    mode: int = 0o600,
    replace: bool = False,
    private_parent: bool = False,
) -> None:
    """Write a JSON receipt atomically, so a crash cannot truncate it.

    The payload is written to a temporary file, flushed to disk and renamed
    into place. A reader therefore observes either the previous file or the
    complete new one, never a half-written receipt.
    """
    if path.is_symlink() or (path.exists() and not replace):
        raise FileExistsError(f"refusing to overwrite {path}")
    if private_parent and (not path.parent.is_dir() or path.parent.stat().st_mode & 0o077):
        raise PermissionError(f"{path.parent} must be a private directory")
    temporary = path.with_name(f".{path.name}.tmp")
    if temporary.exists() or temporary.is_symlink():
        raise FileExistsError(f"temporary path {temporary} already exists")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(mode)
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def read_json_object(path: Path) -> dict[str, Any]:
    """Read a JSON object from a file, rejecting symlinks and other shapes."""
    if path.is_symlink() or not path.is_file():
        raise FileNotFoundError(f"{path} is missing or is a symbolic link")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value
