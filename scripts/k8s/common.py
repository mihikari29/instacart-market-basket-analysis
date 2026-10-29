"""Shared primitives and constants for the Kubernetes coordinator.

Hashing, atomic receipt writing, redaction, claim sizes and name builders
that every phase agrees on.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping



from src.common.runtime import canonical_json


class PipelineError(RuntimeError):
    """Pipeline plan, identity, manifest, or execution failure."""

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


#: Persistent volume claims the B3 pipeline creates, with their requested size.
PIPELINE_CLAIMS: dict[str, str] = {
    "it4931-hdfs-namenode": "1Gi",
    "it4931-hdfs-datanode": "16Gi",
    "it4931-kafka-data": "2Gi",
    "it4931-pipeline-data": "16Gi",
    "it4931-pipeline-checkpoints": "2Gi",
}

#: Total storage the pipeline run is allowed to request.
PIPELINE_STORAGE_BUDGET = "39Gi"

#: The MongoDB database the demo run publishes into.
DEMO_DATABASE_PREFIX = "module4d_demo_"

#: The trend source namespace the pipeline publishes finalized windows under.
TREND_SOURCE_NAMESPACE = "instacart-k8s-demo-v1"


def demo_database(run_id: str) -> str:
    """Return the per-run MongoDB database name."""
    return f"{DEMO_DATABASE_PREFIX}{run_id}"


def pipeline_job_name(step: str, run_id: str) -> str:
    """Return the name of one B3 pipeline Job."""
    return f"it4931-{step}-{run_id}"


def acceptance_job_name(run_id: str) -> str:
    """Return the name of the B4 acceptance Job."""
    return f"it4931-acceptance-{run_id}"


def als_model_version(run_id: str) -> str:
    """Return the ALS model version the pipeline publishes under."""
    return f"als-k8s-{run_id}"


def graph_version(run_id: str) -> str:
    """Return the product-graph version the pipeline publishes under."""
    return f"graph-k8s-{run_id}"


def scoped_run_id(run_id: str, prefix: str) -> str:
    """Return a run-scoped identifier for one module inside the run."""
    return f"{prefix}-{run_id}"


def claim_names() -> set[str]:
    """Return the set of PVC names the pipeline is allowed to create."""
    return set(PIPELINE_CLAIMS)


def expected_claim_requests() -> dict[str, str]:
    """Return the PVC name to requested-size mapping."""
    return dict(PIPELINE_CLAIMS)
