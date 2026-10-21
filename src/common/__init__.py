"""Shared, dependency-free building blocks shared by Modules 1-4.

Every module in this project needs the same handful of cross-cutting
concerns: reading typed values from the environment, redacting credentials
before they reach a log or an evidence artifact, naming and creating a run
directory, writing a JSON report, and building a Spark session with the
project's fixed safety invariants. Each of those was previously
re-implemented per module, which let the copies drift apart.

Nothing in this package imports Spark, PyMongo or any module-specific code,
so it stays importable from a host environment that has none of the heavy
runtime dependencies installed.
"""

from .env import env_float, env_int, parse_bool
from .redaction import redact_sensitive_text, redact_uri, safe_error_text
from .run_context import create_run_directory, new_run_id, save_json
from .type_guards import canonical_json, finite_number, positive_int, sha256_hex

__all__ = [
    "canonical_json",
    "create_run_directory",
    "env_float",
    "env_int",
    "finite_number",
    "new_run_id",
    "parse_bool",
    "positive_int",
    "redact_sensitive_text",
    "redact_uri",
    "safe_error_text",
    "save_json",
    "sha256_hex",
]
