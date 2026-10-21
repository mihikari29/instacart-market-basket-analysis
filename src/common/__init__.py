"""Shared building blocks used across Modules 1 to 4.

The environment readers, credential redaction, run identity, numeric guards
and the Spark session contract live in `runtime` and `spark`. They are
imported here so a caller can use one import path.
"""

from .runtime import (
    aware_utc_millisecond,
    canonical_json,
    code_digest,
    create_run_directory,
    env_float,
    env_int,
    file_sha256,
    finite_number,
    git_revision,
    new_run_id,
    parse_bool,
    positive_int,
    record_mapping,
    redact_sensitive_text,
    redact_uri,
    repository_root,
    require_columns,
    safe_error_text,
    save_json,
    sha256_hex,
    stored_utc,
    validate_run_id,
    REPOSITORY_ROOT,
)
from .spark import build_session, clear_job_group

__all__ = [
    "REPOSITORY_ROOT",
    "aware_utc_millisecond",
    "build_session",
    "canonical_json",
    "clear_job_group",
    "code_digest",
    "create_run_directory",
    "env_float",
    "env_int",
    "file_sha256",
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
