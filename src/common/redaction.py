"""Credential redaction for logs, error messages and evidence artifacts.

PyMongo and several HTTP clients embed the full connection string in their
exception text, so an unredacted error message can leak a MongoDB URI with a
password into a run summary that is then uploaded as a CI artifact.

The Mongo redaction keeps the scheme, host, port and database visible, because
those are the parts an operator needs to diagnose a connection problem. The
credentials are replaced, never the address.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

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
