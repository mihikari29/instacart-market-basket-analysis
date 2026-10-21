#!/usr/bin/env python3
"""Run a bounded, reproducible Module 4C synthetic responsiveness experiment."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import time
import uuid
from typing import Any, Sequence

from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

from src.module3.config import Config as Module3Config
from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME, write_trend_bundles
from src.module3.trending import rank_trending_batch, select_top_k
from src.module4.hybrid.data import READINESS_COLLECTION
from src.module4.hybrid.sink_mongo import FINAL_RECOMMENDATIONS_COLLECTION


USER_ID = 8401
SOURCE_NAMESPACE = "c8a:synthetic:finalized-windows:v1"
SOURCE_SCOPE = "synthetic_controlled_fixture"
SOURCE_RUN_ID = "c8a-fixed-source-run-v1"
MODEL_VERSION = "c8a-fixed-als-model-v1"
GRAPH_VERSION = "c8a-fixed-graph-v1"
BASE_START = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
BASE_END = BASE_START + timedelta(hours=2)
CHANGED_START = BASE_START + timedelta(hours=1)
CHANGED_END = BASE_START + timedelta(hours=3)
FIXTURE_PUBLISHED_AT = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
LAMBDAS = (0.0, 0.5, 0.7, 0.9, 1.0)
ORACLE_PRODUCT_IDS = (1, 2, 3)
OUTPUT_TOP_K = 20
MONGODB_HOST = ("c8a-mongo", 27017)
RESERVED_DATABASES = frozenset({"admin", "config", "instacart", "local"})
_URI_SECRET_PATTERN = re.compile(
    r"mongodb(?:\+srv)?://[^\s\"'<>]+", re.IGNORECASE
)
_KEY_SECRET_PATTERN = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)"
)
_SAFE_DB_PATTERN = re.compile(r"i3_c8a_[a-z0-9_]{8,100}\Z")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def redact_secrets(value: str) -> str:
    """Remove MongoDB URIs and common inline credentials from recorded output."""
    value = _URI_SECRET_PATTERN.sub("mongodb://<redacted>", value)
    return _KEY_SECRET_PATTERN.sub(r"\1\2<redacted>", value)


def validate_mongodb_target(uri: str) -> None:
    """Require the one credential-free host on this task's isolated network."""
    try:
        parsed = parse_uri(uri)
    except Exception as exc:
        raise ValueError("MongoDB URI is invalid") from exc
    nodes = parsed.get("nodelist", ())
    if len(nodes) != 1 or (nodes[0][0].lower(), nodes[0][1]) != MONGODB_HOST:
        raise ValueError("MongoDB URI must target only isolated c8a-mongo:27017")
    if parsed.get("username") is not None or parsed.get("password") is not None:
        raise ValueError("MongoDB URI must not contain credentials")
    if parsed.get("database") not in (None, "") or parsed.get("options"):
        raise ValueError("MongoDB URI must not select a database or contain options")


def validate_disposable_database_name(name: str) -> None:
    if (
        not isinstance(name, str)
        or name.lower() in RESERVED_DATABASES
        or _SAFE_DB_PATTERN.fullmatch(name) is None
    ):
        raise ValueError("database name is not an allowed unique C8A disposable database")


def build_cli_command(
    *,
    python_executable: str,
    mongodb_uri: str,
    database_name: str,
    lambda_weight: float,
    as_of_event_time: datetime,
) -> list[str]:
    """Build the real serving CLI command; freshness remains caller-controlled."""
    validate_disposable_database_name(database_name)
    timestamp = as_of_event_time.astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")
    return [
        python_executable,
        "-m",
        "src.module4.hybrid.cli",
        "serve-user",
        "--user-id",
        str(USER_ID),
        "--source-namespace",
        SOURCE_NAMESPACE,
        "--as-of-event-time",
        timestamp,
        "--mongodb-uri",
        mongodb_uri,
        "--mongo-database",
        database_name,
        "--lambda-weight",
        format(lambda_weight, ".17g"),
    ]


def calculate_rank_metrics(
    baseline_product_ids: Sequence[int],
    changed_product_ids: Sequence[int],
    *,
    comparison_k: int,
) -> dict[str, Any]:
    """Compare fixed-K rankings; missing products receive the K+1 sentinel rank."""
    if type(comparison_k) is not int or comparison_k <= 0:
        raise ValueError("comparison_k must be a positive integer")
    baseline = tuple(baseline_product_ids[:comparison_k])
    changed = tuple(changed_product_ids[:comparison_k])
    if len(set(baseline)) != len(baseline) or len(set(changed)) != len(changed):
        raise ValueError("ranked product IDs must be unique")
    baseline_ranks = {product_id: rank for rank, product_id in enumerate(baseline, 1)}
    changed_ranks = {product_id: rank for rank, product_id in enumerate(changed, 1)}
    union = set(baseline_ranks).union(changed_ranks)
    sentinel = comparison_k + 1
    rank_changes = {
        product_id: (
            baseline_ranks.get(product_id, sentinel),
            changed_ranks.get(product_id, sentinel),
        )
        for product_id in sorted(union)
    }
    overlap = set(baseline).intersection(changed)
    union_count = len(set(baseline).union(changed))
    displacement = [abs(before - after) for before, after in rank_changes.values()]
    return {
        "comparison_k": comparison_k,
        "candidate_universe": sorted(union),
        "missing_product_rank_sentinel": sentinel,
        "top_k_overlap_count": len(overlap),
        "top_k_jaccard_similarity": len(overlap) / union_count if union_count else 1.0,
        "rank_changed_product_count": sum(
            before != after for before, after in rank_changes.values()
        ),
        "mean_absolute_rank_displacement": (
            statistics.fmean(displacement) if displacement else 0.0
        ),
    }


def independent_oracle(
    lambda_weight: float,
    *,
    changed: bool,
    epsilon: float = 1e-12,
) -> dict[int, float]:
    """Compute P1-P3 scores directly from the stated normalization equations."""
    als = {1: 3.0, 2: 2.0, 3: 1.0}
    trend = {1: 10.0 / 99.0, 2: 1.0, 3: 0.0} if changed else {
        1: 1.0,
        2: 0.0,
        3: 0.0,
    }
    als_min = min(als.values())
    als_max = max(als.values())
    trend_max = max(trend.values())
    expected: dict[int, float] = {}
    for product_id in ORACLE_PRODUCT_IDS:
        normalized_als = (als[product_id] - als_min) / (als_max - als_min + epsilon)
        normalized_trend = trend[product_id] / (trend_max + epsilon) if trend_max else 0.0
        expected[product_id] = (
            lambda_weight * normalized_als
            + (1.0 - lambda_weight) * normalized_trend
        )
    return expected


def validate_report(report: dict[str, Any]) -> None:
    required = {
        "experiment_id",
        "status",
        "git_commit",
        "git_dirty",
        "experiment_type",
        "dataset_type",
        "source_scope",
        "source_namespace",
        "user_id",
        "lambda_values",
        "ranking_config",
        "window_definitions",
        "fixture_checksums",
        "results_by_lambda",
        "responsiveness_metrics",
        "cli_timing_summary",
        "runtime_environment",
        "limitations",
    }
    missing = required.difference(report)
    if missing:
        raise ValueError(f"report is missing required fields: {sorted(missing)}")

    def ensure_finite(value: Any, location: str = "report") -> None:
        if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
            return
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(f"{location} contains a non-finite number")
            return
        if isinstance(value, dict):
            for key, item in value.items():
                ensure_finite(item, f"{location}.{key}")
            return
        if isinstance(value, (tuple, list)):
            for index, item in enumerate(value):
                ensure_finite(item, f"{location}[{index}]")

    ensure_finite(report)
    serialized = canonical_json(report)
    if re.search(r"mongodb(?:\+srv)?://[^/@\s:]+:[^/@\s]+@", serialized, re.IGNORECASE):
        raise ValueError("report contains a credential-bearing MongoDB URI")
    if report.get("experiment_type") != "synthetic_finalized_window_responsiveness":
        raise ValueError("report must identify the controlled synthetic experiment")
    if report.get("recommendation_accuracy_measured") is not False:
        raise ValueError("report must state recommendation accuracy was not measured")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _git_state() -> tuple[str, bool, list[str]]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
    except FileNotFoundError:
        # The pinned Spark image intentionally omits Git. The launcher passes
        # metadata collected from the mounted checkout immediately before run.
        commit = os.environ.get("C8A_GIT_COMMIT", "")
        try:
            status = json.loads(os.environ["C8A_GIT_STATUS_JSON"])
        except (KeyError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "Git metadata is unavailable; pass host-verified C8A_GIT_COMMIT and C8A_GIT_STATUS_JSON"
            ) from exc
        if not isinstance(status, list) or any(not isinstance(item, str) for item in status):
            raise RuntimeError("C8A_GIT_STATUS_JSON must be a JSON array of status lines")
    if re.fullmatch(r"[0-9a-f]{40,64}", commit) is None:
        raise RuntimeError("Git commit metadata is not a full hexadecimal object ID")
    return commit, bool(status), status


def _counts_and_scores_rows() -> list[dict[str, Any]]:
    rows = []
    for changed, start, end in (
        (False, BASE_START, BASE_END),
        (True, CHANGED_START, CHANGED_END),
    ):
        for product_id in range(1, OUTPUT_TOP_K + 1):
            count = 99 if product_id == (2 if changed else 1) else 10 if changed and product_id == 1 else 0
            rows.append(
                {
                    "window_start": _timestamp(start),
                    "window_end": _timestamp(end),
                    "product_id": product_id,
                    "purchase_count_30m": count,
                    "purchase_count_120m": count,
                }
            )
    return rows


def _source_fixture_payload() -> dict[str, Any]:
    return {
        "user_id": USER_ID,
        "readiness": {
            "run_id": SOURCE_RUN_ID,
            "model_version": MODEL_VERSION,
            "graph_version": GRAPH_VERSION,
            "status": "ready",
            "ready": True,
            "recommendation_published": True,
            "graph_published": True,
            "single_writer_policy": "module4_publication_lock",
        },
        "als": [
            {
                "product_id": product_id,
                "als_rank": rank,
                "als_score": 3.0 if product_id == 1 else 2.0 if product_id == 2 else 1.0,
            }
            for rank, product_id in enumerate(range(1, OUTPUT_TOP_K + 1), 1)
        ],
        "graph_catalog": [
            {"product_id": product_id, "product_name": f"C8A Fixture Product {product_id}"}
            for product_id in range(1, OUTPUT_TOP_K + 1)
        ],
    }


def _seed_sources(database: Any, experiment_id: str) -> dict[str, Any]:
    ownership = {"_id": "c8a_ownership", "experiment_id": experiment_id}
    database["_c8a_ownership"].insert_one(ownership)
    readiness = {
        "_id": "active",
        "status": "ready",
        "ready": True,
        "run_id": SOURCE_RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "recommendation_published": True,
        "graph_published": True,
        "single_writer_policy": "module4_publication_lock",
        "updated_at": FIXTURE_PUBLISHED_AT,
        "c8a_fixture_marker": experiment_id,
    }
    database[READINESS_COLLECTION].insert_one(readiness)
    recommendations = []
    for rank, product_id in enumerate(range(1, OUTPUT_TOP_K + 1), start=1):
        recommendations.append(
            {
                "_id": f"c8a-als:{USER_ID}:{product_id}",
                "user_id": USER_ID,
                "product_id": product_id,
                "als_score": 3.0 if product_id == 1 else 2.0 if product_id == 2 else 1.0,
                "als_rank": rank,
                "model_version": MODEL_VERSION,
                "run_id": SOURCE_RUN_ID,
            }
        )
    database["user_recommendations"].insert_many(recommendations)
    graph = [
        {
            "_id": f"c8a-graph:{product_id}",
            "product_id": product_id,
            "product_name": f"C8A Fixture Product {product_id}",
            "graph_version": GRAPH_VERSION,
            "run_id": SOURCE_RUN_ID,
        }
        for product_id in range(1, OUTPUT_TOP_K + 1)
    ]
    database["product_graph_metrics"].insert_many(graph)
    return readiness


def _build_finalized_windows(spark: Any) -> Any:
    schema = "window_start timestamp, window_end timestamp, product_id int, purchase_count_30m long, purchase_count_120m long"
    rows = []
    for changed, start, end in (
        (False, BASE_START, BASE_END),
        (True, CHANGED_START, CHANGED_END),
    ):
        for product_id in range(1, OUTPUT_TOP_K + 1):
            count = 99 if product_id == (2 if changed else 1) else 10 if changed and product_id == 1 else 0
            rows.append((start.replace(tzinfo=None), end.replace(tzinfo=None), product_id, count, count))
    counts = spark.createDataFrame(rows, schema=schema)
    config = Module3Config(
        root="file:///tmp/c8a-fixture",
        master="local[2]",
        window_short="30 minutes",
        window_long="120 minutes",
        slide="5 minutes",
        watermark="10 minutes",
        weight_short=0.7,
        weight_long=0.3,
        mongo_top_k_per_window=OUTPUT_TOP_K,
        ttl_seconds=0,
    )
    return select_top_k(rank_trending_batch(counts, config), config)


def _publish_trend_bundles(spark: Any, database: Any) -> list[dict[str, Any]]:
    config = Module3Config(
        root="file:///tmp/c8a-fixture",
        master="local[2]",
        window_short="30 minutes",
        window_long="120 minutes",
        slide="5 minutes",
        watermark="10 minutes",
        weight_short=0.7,
        weight_long=0.3,
        mongo_top_k_per_window=OUTPUT_TOP_K,
        ttl_seconds=0,
    )
    write_trend_bundles(
        _build_finalized_windows(spark),
        database[BUNDLE_COLLECTION_NAME],
        config,
        source_namespace=SOURCE_NAMESPACE,
        source_scope=SOURCE_SCOPE,
        published_at=FIXTURE_PUBLISHED_AT,
        retention_seconds=0,
    )
    bundles = list(
        database[BUNDLE_COLLECTION_NAME]
        .find({"source_namespace": SOURCE_NAMESPACE})
        .sort("window_end", 1)
    )
    if len(bundles) != 2:
        raise RuntimeError("C4A writer did not publish exactly two finalized windows")
    if not bundles[0]["window_end"] < bundles[1]["window_end"]:
        raise RuntimeError("fixture windows are not strictly increasing")
    return bundles


def _database_name(run_id: str, label: str) -> str:
    compact_run = re.sub(r"[^a-z0-9_]", "_", run_id.lower())[:50]
    compact_label = re.sub(r"[^a-z0-9_]", "_", label.lower())[:32]
    name = f"i3_c8a_{compact_run}_{compact_label}_{uuid.uuid4().hex[:8]}"
    validate_disposable_database_name(name)
    return name


def _owned_database(client: MongoClient, owned: list[tuple[str, str]], run_id: str, label: str) -> Any:
    name = _database_name(run_id, label)
    if name in client.list_database_names():
        raise RuntimeError("generated disposable database already exists")
    owned.append((name, run_id))
    return client[name]


def _prepare_database(
    client: MongoClient,
    owned: list[tuple[str, str]],
    spark: Any,
    experiment_id: str,
    label: str,
) -> tuple[Any, str, dict[str, Any], list[dict[str, Any]]]:
    database = _owned_database(client, owned, experiment_id, label)
    readiness_before = _seed_sources(database, experiment_id)
    bundles = _publish_trend_bundles(spark, database)
    return database, database.name, readiness_before, bundles


def _read_active(database: Any, user_id: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pointer = database[READINESS_COLLECTION].find_one({"_id": f"hybrid:{user_id}"})
    if pointer is None or pointer.get("ready") is not True or pointer.get("status") != "ready":
        raise RuntimeError("serving CLI did not publish a ready per-user pointer")
    rows = list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"user_id": user_id, "serving_version": pointer["serving_version"]})
        .sort("rank", 1)
    )
    if len(rows) != pointer["expected_rows"] or [row["rank"] for row in rows] != list(
        range(1, len(rows) + 1)
    ):
        raise RuntimeError("active pointer does not resolve to a complete ranked row set")
    return pointer, rows


def _rows_for_version(database: Any, user_id: int, version: str) -> list[dict[str, Any]]:
    return list(
        database[FINAL_RECOMMENDATIONS_COLLECTION]
        .find({"user_id": user_id, "serving_version": version})
        .sort("rank", 1)
    )


def _invoke_cli(
    *,
    uri: str,
    database_name: str,
    lambda_weight: float,
    as_of: datetime,
) -> tuple[dict[str, Any], float, list[str]]:
    command = build_cli_command(
        python_executable=sys.executable,
        mongodb_uri=uri,
        database_name=database_name,
        lambda_weight=lambda_weight,
        as_of_event_time=as_of,
    )
    safe_command = [redact_secrets(part) for part in command]
    process_env = os.environ.copy()
    process_env["PYTHONDONTWRITEBYTECODE"] = "1"
    started = time.perf_counter()
    process = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[2],
        env=process_env,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    duration_ms = (time.perf_counter() - started) * 1000.0
    if process.returncode != 0:
        raise RuntimeError(
            "serving CLI failed with exit "
            f"{process.returncode}: {redact_secrets(process.stderr.strip())}"
        )
    if process.stderr:
        raise RuntimeError(f"serving CLI wrote unexpected stderr: {redact_secrets(process.stderr)}")
    try:
        output = json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("serving CLI did not return valid JSON") from exc
    if output.get("status") not in {"published", "already_active"}:
        raise RuntimeError("serving CLI returned a non-ready publication status")
    return output, duration_ms, safe_command


def _verify_oracle_scores(
    rows: list[dict[str, Any]], lambda_weight: float, *, changed: bool
) -> dict[str, dict[str, float]]:
    actual_by_product = {row["product_id"]: float(row["final_score"]) for row in rows}
    expected = independent_oracle(lambda_weight, changed=changed)
    comparison = {}
    for product_id, expected_score in expected.items():
        actual_score = actual_by_product.get(product_id)
        if actual_score is None or not math.isclose(
            actual_score, expected_score, rel_tol=0.0, abs_tol=5e-14
        ):
            raise RuntimeError(
                f"oracle mismatch for product {product_id}: actual={actual_score}, "
                f"expected={expected_score}"
            )
        comparison[str(product_id)] = {
            "expected": expected_score,
            "actual": actual_score,
            "absolute_error": abs(actual_score - expected_score),
        }
    return comparison


def _run_lambda_arm(
    client: MongoClient,
    owned: list[tuple[str, str]],
    spark: Any,
    experiment_id: str,
    uri: str,
    lambda_weight: float,
    commands: list[list[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    database, database_name, readiness_before, bundles = _prepare_database(
        client, owned, spark, experiment_id, f"lambda_{lambda_weight:g}"
    )
    baseline_output, baseline_ms, baseline_command = _invoke_cli(
        uri=uri,
        database_name=database_name,
        lambda_weight=lambda_weight,
        as_of=BASE_END,
    )
    commands.append(baseline_command)
    baseline_pointer, baseline_rows = _read_active(database, USER_ID)
    if baseline_output["serving_version"] != baseline_pointer["serving_version"]:
        raise RuntimeError("baseline CLI output does not match the published pointer")
    if baseline_output["trend_content_sha256"] != bundles[0]["content_sha256"]:
        raise RuntimeError("baseline CLI selected an unexpected trend bundle")

    changed_output, changed_ms, changed_command = _invoke_cli(
        uri=uri,
        database_name=database_name,
        lambda_weight=lambda_weight,
        as_of=CHANGED_END,
    )
    commands.append(changed_command)
    changed_pointer, changed_rows = _read_active(database, USER_ID)
    if changed_output["serving_version"] != changed_pointer["serving_version"]:
        raise RuntimeError("changed-window CLI output does not match the published pointer")
    if changed_output["trend_content_sha256"] != bundles[1]["content_sha256"]:
        raise RuntimeError("changed CLI selected an unexpected trend bundle")
    if changed_pointer["serving_version"] == baseline_pointer["serving_version"]:
        raise RuntimeError("changed trend content did not produce a distinct serving identity")
    if changed_pointer.get("as_of_event_time") != CHANGED_END:
        raise RuntimeError("per-user pointer did not advance to the changed as-of time")
    if database[READINESS_COLLECTION].find_one({"_id": "active"}) != readiness_before:
        raise RuntimeError("serving changed the accepted A/B readiness pointer")
    preserved_baseline_rows = _rows_for_version(
        database, USER_ID, baseline_pointer["serving_version"]
    )
    if preserved_baseline_rows != baseline_rows:
        raise RuntimeError("publishing the new version modified immutable baseline rows")
    if not changed_pointer["as_of_event_time"] > baseline_pointer["as_of_event_time"]:
        raise RuntimeError("per-user pointer event time did not increase")

    baseline_ids = [row["product_id"] for row in baseline_rows]
    changed_ids = [row["product_id"] for row in changed_rows]
    metrics = calculate_rank_metrics(
        baseline_ids, changed_ids, comparison_k=OUTPUT_TOP_K
    )
    baseline_oracle = _verify_oracle_scores(baseline_rows, lambda_weight, changed=False)
    changed_oracle = _verify_oracle_scores(changed_rows, lambda_weight, changed=True)
    if lambda_weight == 0.5 and changed_ids.index(2) > changed_ids.index(1):
        raise RuntimeError("lambda=0.5 did not rank changed-trend P2 above P1")
    if lambda_weight == 0.0 and changed_ids[0] != 2:
        raise RuntimeError("lambda=0 did not respect the trend-only endpoint")
    if lambda_weight == 1.0 and changed_ids[0] != 1:
        raise RuntimeError("lambda=1 did not respect the ALS-only endpoint")

    oracle_delta = {
        str(product_id): float(changed_oracle[str(product_id)]["actual"])
        - float(baseline_oracle[str(product_id)]["actual"])
        for product_id in ORACLE_PRODUCT_IDS
    }
    result = {
        "lambda": lambda_weight,
        "baseline_top1_product_id": baseline_ids[0],
        "changed_trend_top1_product_id": changed_ids[0],
        "top1_changed": baseline_ids[0] != changed_ids[0],
        **metrics,
        "oracle_scores": {
            "baseline": baseline_oracle,
            "changed": changed_oracle,
            "changed_minus_baseline": oracle_delta,
        },
        "baseline_serving_version": baseline_pointer["serving_version"],
        "changed_serving_version": changed_pointer["serving_version"],
        "baseline_trend_content_sha256": bundles[0]["content_sha256"],
        "changed_trend_content_sha256": bundles[1]["content_sha256"],
        "readiness_pointer_advanced_correctly": True,
        "ab_readiness_unchanged": True,
        "baseline_cli_wall_time_ms": baseline_ms,
        "changed_cli_wall_time_ms": changed_ms,
        "source_identity": {
            "source_run_id": changed_pointer["source_run_id"],
            "model_version": changed_pointer["model_version"],
            "graph_version": changed_pointer["graph_version"],
        },
    }
    compact = {
        "lambda": lambda_weight,
        "baseline_top1_product_id": baseline_ids[0],
        "changed_trend_top1_product_id": changed_ids[0],
        "top_k_overlap_count": metrics["top_k_overlap_count"],
        "top_k_jaccard_similarity": metrics["top_k_jaccard_similarity"],
        "rank_changed_product_count": metrics["rank_changed_product_count"],
        "mean_absolute_rank_displacement": metrics[
            "mean_absolute_rank_displacement"
        ],
        "oracle_score_deltas": oracle_delta,
    }
    return result, compact


def _run_timing_observation(
    client: MongoClient,
    owned: list[tuple[str, str]],
    spark: Any,
    experiment_id: str,
    uri: str,
    *,
    label: str,
    as_of: datetime,
    measured: bool,
    commands: list[list[str]],
) -> dict[str, Any]:
    database, database_name, _readiness, _bundles = _prepare_database(
        client, owned, spark, experiment_id, label
    )
    output, duration_ms, command = _invoke_cli(
        uri=uri,
        database_name=database_name,
        lambda_weight=0.5,
        as_of=as_of,
    )
    commands.append(command)
    expected_status = "published"
    if output["status"] != expected_status:
        raise RuntimeError("first-publication timing observation was not first publication")
    pointer, rows = _read_active(database, USER_ID)
    if pointer["serving_version"] != output["serving_version"] or len(rows) != OUTPUT_TOP_K:
        raise RuntimeError("timed CLI command did not publish a complete first snapshot")
    return {
        "scenario": "baseline_window" if as_of == BASE_END else "changed_window",
        "publication_kind": "first_publication",
        "is_measured": measured,
        "wall_time_ms": duration_ms,
        "publication_status": output["status"],
        "serving_version": output["serving_version"],
        "database_name": database_name,
    }


def _timing_summary(observations: list[dict[str, Any]]) -> dict[str, Any]:
    measured = [item for item in observations if item["is_measured"]]
    summary: dict[str, Any] = {}
    for scenario in ("baseline_window", "changed_window"):
        values = [
            item["wall_time_ms"]
            for item in measured
            if item["scenario"] == scenario
        ]
        if len(values) != 3:
            raise RuntimeError("expected exactly three measured first publications per scenario")
        summary[scenario] = {
            "publication_kind": "first_publication",
            "repetitions": len(values),
            "median_ms": statistics.median(values),
            "minimum_ms": min(values),
            "maximum_ms": max(values),
            "observations_ms": values,
            "includes": "actual CLI subprocess wall time, including process startup and MongoDB serving",
            "excludes": "fixture construction and C4A trend-bundle publication",
        }
    return {
        "lambda": 0.5,
        "warmup_count": 1,
        "warmup_observation": next(item for item in observations if not item["is_measured"]),
        "measured_observations": measured,
        "summary_by_scenario": summary,
        "retries_in_aggregate": 0,
        "interpretation": "CLI process wall time only; not Kafka event-to-serving latency.",
    }


def _cleanup_owned_databases(
    client: MongoClient, owned: list[tuple[str, str]]
) -> list[str]:
    cleaned = []
    for name, experiment_id in reversed(owned):
        validate_disposable_database_name(name)
        database = client[name]
        marker = database["_c8a_ownership"].find_one({"_id": "c8a_ownership"})
        if marker is None:
            if name in client.list_database_names():
                raise RuntimeError(f"refusing to drop {name}: ownership marker is missing")
            cleaned.append(name)
            continue
        if marker.get("experiment_id") != experiment_id:
            raise RuntimeError(f"refusing to drop {name}: ownership marker does not match")
        client.drop_database(name)
        if name in client.list_database_names():
            raise RuntimeError(f"owned disposable database {name} remained after cleanup")
        cleaned.append(name)
    return cleaned


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    validate_mongodb_target(args.mongodb_uri)
    output_dir = Path(args.output_dir).resolve()
    repo_root = Path(__file__).resolve().parents[2]
    if output_dir == repo_root or repo_root not in output_dir.parents:
        raise ValueError("output directory must be inside this repository's results tree")
    expected_results_root = repo_root / "results" / "module4c-responsiveness"
    if expected_results_root not in output_dir.parents:
        raise ValueError("output directory must be under results/module4c-responsiveness")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise ValueError("output directory must be empty before the experiment starts")

    experiment_id = args.run_id or (
        "c8a-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    )
    if re.fullmatch(r"c8a-[A-Za-z0-9_-]{8,80}", experiment_id) is None:
        raise ValueError("run ID must be a stable C8A identifier")
    client = MongoClient(
        args.mongodb_uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        tz_aware=True,
    )
    owned: list[tuple[str, str]] = []
    spark = None
    cleanup_names: list[str] = []
    run_error: BaseException | None = None
    results_by_lambda: dict[str, Any] = {}
    compact_metrics = []
    timing_observations = []
    recorded_commands: list[list[str]] = []
    try:
        client.admin.command("ping")
        mongo_version = client.server_info()["version"]
        if mongo_version != "7.0.16":
            raise RuntimeError(f"experiment requires MongoDB 7.0.16, got {mongo_version}")
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("module4c-c8a-responsiveness")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", "/tmp/c8a-spark-warehouse")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        for lambda_weight in LAMBDAS:
            arm_result, metrics = _run_lambda_arm(
                client,
                owned,
                spark,
                experiment_id,
                args.mongodb_uri,
                lambda_weight,
                recorded_commands,
            )
            results_by_lambda[format(lambda_weight, ".1f")] = arm_result
            compact_metrics.append(metrics)
        timing_observations.append(
            _run_timing_observation(
                client,
                owned,
                spark,
                experiment_id,
                args.mongodb_uri,
                label="warmup",
                as_of=BASE_END,
                measured=False,
                commands=recorded_commands,
            )
        )
        for scenario, as_of in (("baseline", BASE_END), ("changed", CHANGED_END)):
            for repetition in range(1, 4):
                timing_observations.append(
                    _run_timing_observation(
                        client,
                        owned,
                        spark,
                        experiment_id,
                        args.mongodb_uri,
                        label=f"timing_{scenario}_{repetition}",
                        as_of=as_of,
                        measured=True,
                        commands=recorded_commands,
                    )
                )
        timing = _timing_summary(timing_observations)
        cleanup_names = _cleanup_owned_databases(client, owned)
        source_fixture_hash = sha256_json(_source_fixture_payload())
        trend_input_hash = sha256_json(_counts_and_scores_rows())
        selected_hashes = {
            "baseline_window": results_by_lambda["0.5"]["baseline_trend_content_sha256"],
            "changed_window": results_by_lambda["0.5"]["changed_trend_content_sha256"],
        }
        arm_window_ids = {
            key: {
                "baseline": results["baseline_trend_content_sha256"],
                "changed": results["changed_trend_content_sha256"],
            }
            for key, results in results_by_lambda.items()
        }
        git_commit, git_dirty, git_status = _git_state()
        report = {
            "experiment_id": experiment_id,
            "status": "passed",
            "git_commit": git_commit,
            "git_dirty": git_dirty,
            "git_status_porcelain": git_status,
            "experiment_type": "synthetic_finalized_window_responsiveness",
            "dataset_type": "deterministic synthetic fixture; no Instacart records",
            "source_scope": SOURCE_SCOPE,
            "source_namespace": SOURCE_NAMESPACE,
            "user_id": USER_ID,
            "recommendation_accuracy_measured": False,
            "lambda_values": list(LAMBDAS),
            "ranking_config": {
                "algorithm_version": "hybrid-v1",
                "als_top_n": OUTPUT_TOP_K,
                "trend_top_m": OUTPUT_TOP_K,
                "output_top_k": OUTPUT_TOP_K,
                "epsilon": 1e-12,
                "lambda_by_arm": list(LAMBDAS),
                "trend_normalization": "score / (maximum trend score + epsilon)",
                "als_normalization": "min-max span plus epsilon",
            },
            "window_definitions": {
                "window_duration": "120 minutes",
                "baseline": {
                    "window_start": _timestamp(BASE_START),
                    "window_end": _timestamp(BASE_END),
                    "as_of_event_time": _timestamp(BASE_END),
                    "counts": "P1 has 99 in each count; P2-P20 have 0",
                    "independent_oracle_trend_scores": {"P1": 1.0, "P2": 0.0, "P3": 0.0},
                },
                "changed_trend": {
                    "window_start": _timestamp(CHANGED_START),
                    "window_end": _timestamp(CHANGED_END),
                    "as_of_event_time": _timestamp(CHANGED_END),
                    "counts": "P1 has 10; P2 has 99; P3-P20 have 0 in each count",
                    "independent_oracle_trend_scores": {
                        "P1": 10.0 / 99.0,
                        "P2": 1.0,
                        "P3": 0.0,
                    },
                },
                "strictly_increasing_window_ends": True,
                "bson_millisecond_aligned": True,
            },
            "fixture_checksums": {
                "source_fixture_sha256": source_fixture_hash,
                "trend_input_rows_sha256": trend_input_hash,
                "selected_window_content_sha256": selected_hashes,
                "selected_window_hashes_by_lambda": arm_window_ids,
                "logical_serving_versions_repeat_across_disposable_databases": True,
            },
            "results_by_lambda": results_by_lambda,
            "responsiveness_metrics": compact_metrics,
            "cli_timing_summary": timing,
            "runtime_environment": {
                "mongodb_version": mongo_version,
                "mongodb_container_image": args.mongo_image,
                "mongodb_image_digest": args.mongo_image_digest,
                "python_version": platform.python_version(),
                "pinned_docker_image": args.runner_image,
                "pinned_docker_image_digest": args.runner_image_digest,
                "docker_network": args.docker_network,
                "mongodb_alias": "c8a-mongo:27017",
                "mongodb_has_published_ports": False,
                "mongodb_data_and_config_storage": "tmpfs",
                "repository_mount": "read-only, except narrowly scoped results output directory",
                "client_target": "mongodb://c8a-mongo:27017 (credential-free isolated target)",
                "max_event_age_seconds": None,
                "cli_omits_max_event_age_option": True,
                "runner_invocation": redact_secrets(args.runner_invocation or "not-recorded"),
                "mongodb_launch_invocation": redact_secrets(args.mongo_launch_invocation or "not-recorded"),
                "actual_cli_invocations": recorded_commands,
                "fixture_setup_outside_measured_cli_interval": True,
                "cli_timing_definition": "time.perf_counter around subprocess.run for actual CLI",
            },
            "database_cleanup": {
                "owned_disposable_databases": [name for name, _owner in owned],
                "dropped_and_verified_absent": cleanup_names,
                "complete": len(cleanup_names) == len(owned),
            },
            "limitations": [
                "Trend-window contents are controlled synthetic counts, not observed real-world trend shifts.",
                "The experiment measures ranking responsiveness, MongoDB serving correctness, and CLI subprocess wall time only.",
                "Kafka/Spark event-to-serving latency and recommendation accuracy were not measured.",
                "No official-train recommendation evaluation was run.",
                "Concurrent A/B source-generation race coverage depends on I2 C6; this experiment fixes one stable A/B generation.",
            ],
        }
        validate_report(report)
        report_path = output_dir / "report.json"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        report_digest = hashlib.sha256(report_path.read_bytes()).hexdigest()
        markdown = _summary_markdown(report, report_digest)
        (output_dir / "summary.md").write_text(markdown)
        return {
            "report_path": str(report_path),
            "report_sha256": report_digest,
            "summary_path": str(output_dir / "summary.md"),
            "results_by_lambda": results_by_lambda,
            "timing_summary": timing,
            "database_cleanup": report["database_cleanup"],
        }
    except BaseException as exc:
        run_error = exc
        raise
    finally:
        try:
            cleanup_names = _cleanup_owned_databases(client, owned)
        finally:
            if spark is not None:
                spark.stop()
            client.close()
        if run_error is not None:
            # Preserve the original experiment exception; cleanup is still attempted.
            pass


def _summary_markdown(report: dict[str, Any], report_digest: str) -> str:
    lines = [
        f"# C8A responsiveness experiment {report['experiment_id']}",
        "",
        "Status: **PASS**. This is a controlled synthetic experiment; no recommendation accuracy or streaming latency was measured.",
        "",
        "| Lambda | Baseline Top-1 | Changed Top-1 | Top-1 changed | Overlap / Jaccard | Rank changes | Mean rank displacement |",
        "|---:|---:|---:|:---:|---:|---:|---:|",
    ]
    for key in ("0.0", "0.5", "0.7", "0.9", "1.0"):
        result = report["results_by_lambda"][key]
        lines.append(
            "| {lam} | {base} | {changed} | {top_changed} | {overlap} / {jaccard:.3f} | {rank_changes} | {displacement:.3f} |".format(
                lam=key,
                base=result["baseline_top1_product_id"],
                changed=result["changed_trend_top1_product_id"],
                top_changed=str(result["top1_changed"]).lower(),
                overlap=result["top_k_overlap_count"],
                jaccard=result["top_k_jaccard_similarity"],
                rank_changes=result["rank_changed_product_count"],
                displacement=result["mean_absolute_rank_displacement"],
            )
        )
    lines.extend(["", "CLI first-publication subprocess timing:"])
    for scenario, values in report["cli_timing_summary"]["summary_by_scenario"].items():
        lines.append(
            f"- {scenario}: n={values['repetitions']}, median={values['median_ms']:.3f} ms, "
            f"min={values['minimum_ms']:.3f} ms, max={values['maximum_ms']:.3f} ms."
        )
    lines.extend(
        [
            "",
            "All lambda arms published baseline then changed windows from the same fixed A/B generation in separate disposable databases. The changed pointer advanced, baseline rows remained immutable, and the A/B readiness document stayed unchanged.",
            "",
            "Concurrent A/B source-generation race coverage depends on I2 C6. This run used a stable source generation. See `docs/module4c-evaluation-plan.md` for temporal-evaluation constraints.",
            "",
            f"JSON evidence: `report.json` (SHA-256 `{report_digest}`).",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongodb-uri", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--runner-image", default="instacart-spark:3.5.5")
    parser.add_argument("--runner-image-digest", default=os.environ.get("C8A_RUNTIME_IMAGE_DIGEST", "unknown"))
    parser.add_argument("--mongo-image", default="mongo:7.0.16")
    parser.add_argument("--mongo-image-digest", default=os.environ.get("C8A_MONGO_IMAGE_DIGEST", "unknown"))
    parser.add_argument("--docker-network", default=os.environ.get("C8A_DOCKER_NETWORK", "unknown"))
    parser.add_argument("--runner-invocation", default=os.environ.get("C8A_RUNNER_COMMAND"))
    parser.add_argument("--mongo-launch-invocation", default=os.environ.get("C8A_MONGO_LAUNCH_COMMAND"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        summary = run_experiment(args)
    except Exception as exc:
        safe_message = redact_secrets(str(exc)) or "experiment failed"
        print(f"C8A experiment failed: {type(exc).__name__}: {safe_message}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
