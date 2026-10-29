"""Command-line entry point for bounded Module 4C recommendation serving."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import re
import sys
from typing import Sequence

from pymongo import MongoClient

from src.common.runtime import MONGODB_URI_PATTERN, SensitiveArgumentParser, redact_sensitive_text, redact_sensitive_values
from src.module4.config import Config as Module4Config

from .config import HybridRankingConfig
from .service import HybridServingResult, serve_hybrid_user


def _parse_user_id(value: str) -> int:
    if not re.fullmatch(r"[0-9]+", value):
        raise argparse.ArgumentTypeError("user ID must be a positive base-10 integer")
    user_id = int(value)
    if user_id <= 0:
        raise argparse.ArgumentTypeError("user ID must be positive")
    return user_id


def _parse_namespace(value: str) -> str:
    if not value or value != value.strip() or "\x00" in value or len(value) > 256:
        raise argparse.ArgumentTypeError("source namespace must be non-empty and stable")
    return value


def _parse_as_of_event_time(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("as-of event time must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("as-of event time must include a timezone offset")
    try:
        parsed = parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise argparse.ArgumentTypeError("as-of event time must be a valid UTC instant") from exc
    if parsed.microsecond % 1000:
        raise argparse.ArgumentTypeError(
            "as-of event time must be aligned to BSON millisecond precision"
        )
    return parsed


def _parse_lambda_weight(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("lambda weight must be a finite number between 0 and 1") from exc
    if not math.isfinite(parsed) or not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("lambda weight must be a finite number between 0 and 1")
    return parsed


def _parse_max_event_age_seconds(value: str) -> timedelta:
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("maximum event age must be a finite nonnegative number") from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError("maximum event age must be a finite nonnegative number")
    try:
        return timedelta(seconds=seconds)
    except OverflowError as exc:
        raise argparse.ArgumentTypeError("maximum event age is too large") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = SensitiveArgumentParser(prog="python3 -m src.module4.hybrid.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve-user", help="serve recommendations for one user")
    serve.add_argument("--user-id", required=True, type=_parse_user_id)
    serve.add_argument("--source-namespace", required=True, type=_parse_namespace)
    serve.add_argument("--as-of-event-time", required=True, type=_parse_as_of_event_time)
    serve.add_argument("--mongodb-uri", required=True)
    serve.add_argument("--mongo-database", required=True)
    serve.add_argument("--lambda-weight", type=_parse_lambda_weight, default=0.7)
    serve.add_argument("--max-event-age-seconds", type=_parse_max_event_age_seconds)
    return parser


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _success_document(result: HybridServingResult) -> dict[str, object]:
    fallback_reason = (
        result.reason
        if result.mode in {"als_fallback_trend_unavailable", "no_candidates"}
        else None
    )
    return {
        "status": result.publication_status,
        "user_id": result.user_id,
        "serving_version": result.serving_version,
        "published_rows": result.recommendation_count,
        "ranking_mode": result.mode,
        "fallback_reason": fallback_reason,
        "source_run_id": result.source_run_id,
        "model_version": result.model_version,
        "graph_version": result.graph_version,
        "trend_source_namespace": result.trend_source_namespace,
        "trend_window_end": _iso_utc(result.trend_window_end),
        "trend_content_sha256": result.trend_content_sha256,
        "trend_availability_status": result.trend_status,
        "readiness_pointer_id": result.readiness_pointer_id,
        "as_of_event_time": _iso_utc(result.as_of_event_time),
    }


def _sanitize_error(error: BaseException) -> str:
    messages = []
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        message = str(current) or "operation failed"
        message = MONGODB_URI_PATTERN.sub("mongodb://<redacted>", message)
        message = redact_sensitive_text(message)
        messages.append(f"{type(current).__name__}: {message}")
        current = current.__cause__ or current.__context__
    return " <- caused by ".join(messages)


def _serve_user(arguments: argparse.Namespace) -> int:
    client = None
    output = None
    exit_code = 0
    try:
        config = Module4Config(
            mongodb_uri=arguments.mongodb_uri,
            mongo_database=arguments.mongo_database,
        )
        ranking_config = HybridRankingConfig(lambda_weight=arguments.lambda_weight)
        client = MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000)
        result = serve_hybrid_user(
            config,
            user_id=arguments.user_id,
            source_namespace=arguments.source_namespace,
            as_of_event_time=arguments.as_of_event_time,
            ranking_config=ranking_config,
            max_event_age=arguments.max_event_age_seconds,
            client=client,
        )
        output = json.dumps(redact_sensitive_values(_success_document(result)), separators=(",", ":"), ensure_ascii=False)
    except Exception as exc:
        print(f"module4c serve-user failed: {_sanitize_error(exc)}", file=sys.stderr)
        exit_code = 1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                exit_code = 1
                print(f"module4c client close failed: {_sanitize_error(exc)}", file=sys.stderr)
    if exit_code == 0:
        print(output)
    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.command == "serve-user":
        return _serve_user(arguments)
    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
