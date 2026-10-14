"""Read-only FastAPI surface for verified Module 4C recommendations and trends."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import uuid
from typing import Any, Callable

from fastapi import FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.responses import JSONResponse
from pymongo import MongoClient

from src.module4c.trend_reader import (
    TrendSnapshotIntegrityError,
    TrendSnapshotQueryError,
    TrendSnapshotRequestError,
)

from .config import ApiConfig
from .readers import (
    DatabaseReadFailure,
    InvalidSnapshot,
    SnapshotChanged,
    SnapshotNotFound,
    SnapshotNotReady,
    read_hybrid_snapshot,
    read_trend_snapshot,
)
from .schemas import (
    ErrorEnvelope,
    HealthResponse,
    RecommendationResponse,
    TrendResponse,
)


def _request_id(request: Request) -> str:
    current = getattr(request.state, "request_id", None)
    if current is None:
        current = uuid.uuid4().hex
        request.state.request_id = current
    return current


def _error(request: Request, status: int, code: str, message: str) -> JSONResponse:
    envelope = ErrorEnvelope(
        error={"code": code, "message": message, "request_id": _request_id(request)}
    )
    return JSONResponse(status_code=status, content=envelope.model_dump(mode="json"))


def _parse_event_time(value: str) -> datetime:
    if len(value) > 64:
        raise ValueError("timestamp is too long")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise ValueError("timestamp must be an ISO-8601 instant") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    try:
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise ValueError("timestamp is outside the supported range") from exc


def create_app(
    config: ApiConfig | None = None,
    *,
    client_factory: Callable[..., Any] | None = None,
    database: Any | None = None,
) -> FastAPI:
    """Create an app with a lifespan-owned client or an injected test database."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.api_config = config or ApiConfig.from_environment()
        if database is not None:
            app.state.database = database
            app.state.mongo_client = None
            try:
                yield
            finally:
                app.state.database = None
            return

        factory = client_factory or MongoClient
        settings = app.state.api_config
        client = factory(
            settings.mongodb_uri,
            serverSelectionTimeoutMS=settings.server_selection_timeout_ms,
            connectTimeoutMS=settings.connect_timeout_ms,
            socketTimeoutMS=settings.socket_timeout_ms,
            appname="module4d-read-api",
        )
        app.state.mongo_client = client
        app.state.database = client[settings.mongo_database]
        try:
            yield
        finally:
            client.close()
            app.state.database = None
            app.state.mongo_client = None

    app = FastAPI(
        title="Module 4D Verified Read API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(request: Request, _exc: RequestValidationError):
        return _error(request, 422, "invalid_request", "Request parameters are invalid.")

    @app.exception_handler(ResponseValidationError)
    async def response_validation_error(request: Request, _exc: ResponseValidationError):
        return _error(request, 502, "invalid_source_document", "The stored source failed response validation.")

    @app.exception_handler(SnapshotNotFound)
    async def snapshot_not_found(request: Request, _exc: SnapshotNotFound):
        return _error(request, 404, "recommendation_snapshot_not_found", "No published snapshot was found.")

    @app.exception_handler(SnapshotNotReady)
    async def snapshot_not_ready(request: Request, _exc: SnapshotNotReady):
        return _error(request, 503, "snapshot_not_ready", "The recommendation snapshot is not ready.")

    @app.exception_handler(SnapshotChanged)
    async def snapshot_changed(request: Request, _exc: SnapshotChanged):
        return _error(request, 409, "snapshot_changed", "The snapshot changed during the read; retry the request.")

    @app.exception_handler(InvalidSnapshot)
    async def invalid_snapshot(request: Request, _exc: InvalidSnapshot):
        return _error(request, 502, "invalid_snapshot", "The stored recommendation snapshot failed validation.")

    @app.exception_handler(DatabaseReadFailure)
    async def database_unavailable(request: Request, _exc: DatabaseReadFailure):
        return _error(request, 503, "service_unavailable", "The data service is temporarily unavailable.")

    @app.exception_handler(TrendSnapshotIntegrityError)
    async def trend_integrity_error(request: Request, _exc: TrendSnapshotIntegrityError):
        return _error(request, 502, "invalid_source_document", "The finalized trend bundle failed validation.")

    @app.exception_handler(TrendSnapshotQueryError)
    async def trend_query_error(request: Request, _exc: TrendSnapshotQueryError):
        return _error(request, 503, "service_unavailable", "The data service is temporarily unavailable.")

    @app.exception_handler(TrendSnapshotRequestError)
    async def trend_request_error(request: Request, _exc: TrendSnapshotRequestError):
        return _error(request, 422, "invalid_filter", "Trend request parameters are invalid.")

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    def health() -> dict[str, str]:
        """Process liveness only; MongoDB is not contacted."""
        return {"status": "ok"}

    @app.get(
        "/api/v1/recommendations/{user_id}",
        response_model=RecommendationResponse,
        responses={
            404: {"model": ErrorEnvelope},
            409: {"model": ErrorEnvelope},
            422: {"model": ErrorEnvelope},
            502: {"model": ErrorEnvelope},
            503: {"model": ErrorEnvelope},
        },
        tags=["recommendations"],
    )
    def recommendations(
        request: Request, user_id: int = Path(..., gt=0, le=9_223_372_036_854_775_807)
    ) -> dict[str, Any]:
        settings: ApiConfig = request.app.state.api_config
        result = read_hybrid_snapshot(request.app.state.database, user_id, max_time_ms=settings.max_time_ms)
        result["observed_at"] = datetime.now(timezone.utc)
        return result

    @app.get(
        "/api/v1/trends",
        response_model=TrendResponse,
        responses={422: {"model": ErrorEnvelope}, 502: {"model": ErrorEnvelope}, 503: {"model": ErrorEnvelope}},
        tags=["trends"],
    )
    def trends(
        request: Request,
        source_namespace: str = Query(..., min_length=1, max_length=256),
        as_of_event_time: str = Query(..., min_length=1, max_length=64),
        max_event_age_seconds: int | None = Query(None, ge=0, le=31_536_000),
    ) -> dict[str, Any]:
        settings: ApiConfig = request.app.state.api_config
        if source_namespace != source_namespace.strip() or source_namespace not in settings.trend_source_namespaces:
            return _error(request, 422, "invalid_namespace", "Trend source namespace is not configured.")
        try:
            event_time = _parse_event_time(as_of_event_time)
        except ValueError:
            return _error(
                request,
                422,
                "invalid_filter",
                "as_of_event_time must be a timezone-aware ISO-8601 instant.",
            )
        result = read_trend_snapshot(
            request.app.state.database,
            source_namespace=source_namespace,
            as_of_event_time=event_time,
            max_event_age_seconds=max_event_age_seconds,
            max_time_ms=settings.max_time_ms,
        )
        items = [
            {"product_id": row.product_id, "trend_score": row.trend_score, "trend_rank": row.trend_rank}
            for row in result.trend_recommendations
        ]
        return {
            "available": result.available,
            "status": result.status,
            "reason": result.reason,
            "source_namespace": result.source_namespace,
            "source_scope": result.source_scope,
            "window_start": result.window_start,
            "window_end": result.window_end,
            "published_at": result.published_at,
            "expires_at": result.expires_at,
            "schema_version": result.schema_version,
            "config_hash": result.config_hash,
            "content_sha256": result.content_sha256,
            "row_count": len(items),
            "items": items,
            "observed_at": datetime.now(timezone.utc),
        }

    return app


app = create_app()
