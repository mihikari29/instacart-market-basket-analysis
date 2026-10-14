"""Environment configuration for the Module 4D read API."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from pathlib import Path
import re

from pymongo.uri_parser import parse_uri


_DATABASE_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,63}\Z")
_MAX_TREND_NAMESPACES = 32


@dataclass(frozen=True, slots=True)
class ApiConfig:
    """Required backend-only MongoDB settings and fixed read bounds."""

    mongodb_uri: str = field(repr=False)
    mongo_database: str
    trend_source_namespaces: tuple[str, ...]
    server_selection_timeout_ms: int = 3_000
    connect_timeout_ms: int = 3_000
    socket_timeout_ms: int = 5_000
    max_time_ms: int = 5_000
    application_root: Path = field(default_factory=lambda: Path(__file__).resolve().parents[3], repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.mongodb_uri, str) or not self.mongodb_uri.strip():
            raise ValueError("MODULE4D_MONGODB_URI must be explicitly configured")
        if not isinstance(self.mongo_database, str) or not _DATABASE_PATTERN.fullmatch(
            self.mongo_database
        ):
            raise ValueError("MODULE4D_MONGO_DATABASE must be a valid explicit database name")
        try:
            parsed = parse_uri(self.mongodb_uri)
        except Exception as exc:
            raise ValueError("MODULE4D_MONGODB_URI must be a valid MongoDB URI") from exc
        if parsed.get("database") not in (None, "", self.mongo_database):
            raise ValueError("MongoDB URI database must match MODULE4D_MONGO_DATABASE")

        namespaces = self.trend_source_namespaces
        if not isinstance(namespaces, tuple) or not 1 <= len(namespaces) <= _MAX_TREND_NAMESPACES:
            raise ValueError("at least one and at most 32 trend namespaces must be configured")
        if len(set(namespaces)) != len(namespaces):
            raise ValueError("configured trend namespaces must be unique")
        for namespace in namespaces:
            if (
                not isinstance(namespace, str)
                or not namespace
                or namespace != namespace.strip()
                or len(namespace) > 256
                or "\x00" in namespace
            ):
                raise ValueError("configured trend namespaces must be stable strings of at most 256 characters")
        for name in (
            "server_selection_timeout_ms",
            "connect_timeout_ms",
            "socket_timeout_ms",
            "max_time_ms",
        ):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= 60_000:
                raise ValueError(f"{name} must be an integer between 1 and 60000")
        if not isinstance(self.application_root, (str, Path)) or not str(self.application_root).strip():
            raise ValueError("MODULE4D_APP_ROOT must be a configured application root")
        object.__setattr__(self, "application_root", Path(self.application_root).expanduser().resolve())

    @classmethod
    def from_environment(cls) -> ApiConfig:
        """Load required settings without a production database fallback."""
        uri = os.environ.get("MODULE4D_MONGODB_URI")
        database = os.environ.get("MODULE4D_MONGO_DATABASE")
        namespaces_raw = os.environ.get("MODULE4D_TREND_SOURCE_NAMESPACES", "")
        if not uri:
            raise ValueError("MODULE4D_MONGODB_URI is required")
        if not database:
            raise ValueError("MODULE4D_MONGO_DATABASE is required")
        namespaces = tuple(value.strip() for value in namespaces_raw.split(",") if value.strip())
        if not namespaces:
            raise ValueError("MODULE4D_TREND_SOURCE_NAMESPACES is required")
        root = Path(os.environ.get("MODULE4D_APP_ROOT", Path(__file__).resolve().parents[3]))
        return cls(uri, database, namespaces, application_root=root)
