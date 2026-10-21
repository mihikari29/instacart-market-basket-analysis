"""Shared building blocks used across Modules 1 to 4.

The environment readers, credential redaction, run identity, numeric guards
and the Spark session contract live in `runtime` and `spark`. They are
imported here so a caller can use one import path.
"""

from .runtime import (
    canonical_json,
    create_run_directory,
    env_float,
    env_int,
    finite_number,
    new_run_id,
    parse_bool,
    positive_int,
    record_mapping,
    redact_sensitive_text,
    redact_uri,
    require_columns,
    safe_error_text,
    save_json,
    sha256_hex,
    stored_utc,
    validate_run_id,
    aware_utc_millisecond,
    code_digest,
    git_revision,
    repository_root,
    REPOSITORY_ROOT,
)
from .spark import build_session

__all__ = [
    "REPOSITORY_ROOT",
    "aware_utc_millisecond",
    "build_session",
    "canonical_json",
    "code_digest",
    "create_run_directory",
    "env_float",
    "env_int",
    "finite_number",
    "git_revision",
    "new_run_id",
    "parse_bool",
    "positive_int",
    "record_mapping",
    "redact_sensitive_text",
    "redact_uri",
    "repository_root",
    "require_columns",
    "safe_error_text",
    "save_json",
    "sha256_hex",
    "stored_utc",
    "validate_run_id",
]
