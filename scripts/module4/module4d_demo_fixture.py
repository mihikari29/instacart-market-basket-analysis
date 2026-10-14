#!/usr/bin/env python3
"""Publish a small synthetic Module 4D fixture through accepted publishers.

Run only from the task-scoped demo Compose service. The root account is used
for fixture publication and index setup; FastAPI receives only the database
scoped read account created after all fixture rows and indexes are verified.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
from typing import Any

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.uri_parser import parse_uri
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from src.module2.analytics import metrics as module2_metrics
from src.module2.analytics import publish_mongo as publish_module2
from src.module3.config import Config as Module3Config
from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME, write_trend_bundles
from src.module4.config import Config as Module4Config
from src.module4.hybrid import HybridRankingConfig
from src.module4.hybrid.service import serve_hybrid_user
from src.module4.hybrid.sink_mongo import (
    FINAL_RECOMMENDATIONS_COLLECTION,
    ensure_final_recommendation_indexes,
)
from src.module4.sink_mongo import (
    publish_graph_metrics,
    publish_recommendations,
    update_pair_readiness,
    validate_snapshot,
)
from src.module4.dashboard.batch_readers import PRODUCT_INDEXES
from src.module4.dashboard.readers import TREND_SELECTION_INDEX_NAME


DATABASE_RE = re.compile(r"module4d_demo_[a-f0-9]{12}\Z")
NAMESPACE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
WARM_USER_ID = 601
FALLBACK_USER_ID = 602
EMPTY_USER_ID = 603
UNKNOWN_USER_ID = 9999
SOURCE_RUN_ID = "module4d-demo-source-001"
MODEL_VERSION = "module4d-demo-als-v1"
GRAPH_VERSION = "module4d-demo-graph-v1"
SCOPE = "synthetic-demonstration-window"


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _validate_target(uri: str, database_name: str) -> None:
    parsed = parse_uri(uri)
    hosts = parsed.get("nodelist", ())
    options = {str(key).lower(): value for key, value in (parsed.get("options") or {}).items()}
    if (
        not DATABASE_RE.fullmatch(database_name)
        or parsed.get("database") != database_name
        or len(hosts) != 1
        or (hosts[0][0].lower(), hosts[0][1]) != ("module4d-mongo", 27017)
        or parsed.get("username") != "root"
        or not parsed.get("password")
        or options != {"authsource": "admin"}
    ):
        raise RuntimeError("fixture publication is limited to its uniquely named demo MongoDB service/database")


def _spark_session() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("module4d-synthetic-demo-fixture")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", f"/tmp/module4d-demo-{os.getpid()}")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session


def _seed_batch(spark: SparkSession, admin_uri: str, database_name: str) -> dict[str, Any]:
    prior_rows = [
        (1, 1001, 0), (1, 1002, 1),
        (2, 1001, 1), (2, 1003, 0),
        (3, 1001, 1), (3, 1002, 0),
        (4, 1004, 1), (4, 1003, 1),
        (5, 1002, 1), (5, 1004, 0),
    ]
    prior = spark.createDataFrame(prior_rows, ["order_id", "product_id", "reordered"])
    orders = spark.createDataFrame(
        [(1, 11, 0, 9), (2, 12, 1, 12), (3, 13, 3, 17), (4, 14, 5, 19), (5, 15, 6, 21)],
        ["order_id", "user_id", "order_dow", "order_hour_of_day"],
    )
    products = spark.createDataFrame(
        [
            (1001, 4, "Fixture apples"),
            (1002, 7, "Fixture oat milk"),
            (1003, 4, "Fixture bananas"),
            (1004, 11, "Fixture pasta"),
        ],
        ["product_id", "department_id", "product_name"],
    )
    _enriched, product_metrics, department_metrics, _user_metrics = module2_metrics(
        {"prior": prior, "orders": orders, "products": products}
    )
    admin_uri_for_publish = admin_uri
    if parse_uri(admin_uri_for_publish).get("database") != database_name:
        raise RuntimeError("Module 2 publisher URI must select the isolated demo database")
    run_id = "module4d-demo-batch-001"
    publish_module2(
        {"batch_product_metrics": product_metrics, "department_metrics": department_metrics},
        admin_uri_for_publish,
        run_id,
    )
    return {"run_id": run_id, "products": 4, "departments": 3, "scope": "prior-only item purchases"}


def _seed_trends(spark: SparkSession, database: Any, namespace: str, now: datetime) -> dict[str, Any]:
    current_minute = now.replace(second=0, microsecond=0)
    latest_end = current_minute - timedelta(minutes=10)
    older_end = current_minute - timedelta(minutes=30)
    rows = []
    for window_end, scores in (
        (older_end, ((1001, 0.90), (1002, 0.70), (1021, 0.50))),
        (latest_end, ((1021, 0.96), (1001, 0.82), (1002, 0.61))),
    ):
        window_start = window_end - timedelta(minutes=120)
        for rank, (product_id, score) in enumerate(scores, start=1):
            rows.append(
                (
                    window_start.replace(tzinfo=None),
                    window_end.replace(tzinfo=None),
                    product_id,
                    20 - rank,
                    60 - rank,
                    score,
                    rank,
                )
            )
    schema = StructType(
        [
            StructField("window_start", TimestampType(), False),
            StructField("window_end", TimestampType(), False),
            StructField("product_id", LongType(), False),
            StructField("purchase_count_30m", LongType(), False),
            StructField("purchase_count_120m", LongType(), False),
            StructField("trend_score", DoubleType(), False),
            StructField("trend_rank", IntegerType(), False),
        ]
    )
    frame = spark.createDataFrame(rows, schema=schema)
    config = Module3Config(
        master="local[1]",
        mongodb_uri=os.environ["MODULE4D_DEMO_ROOT_URI"],
        mongo_database=database.name,
        mongo_top_k_per_window=20,
        ttl_seconds=0,
        publish_trend_bundles=True,
        trend_source_namespace=namespace,
        trend_source_scope=SCOPE,
    )
    report = write_trend_bundles(
        frame,
        database[BUNDLE_COLLECTION_NAME],
        config,
        source_namespace=namespace,
        source_scope=SCOPE,
        published_at=current_minute - timedelta(minutes=1),
        retention_seconds=0,
    )
    if report.bundles_created != 2:
        raise RuntimeError(f"expected two new finalized trend bundles, got {report.bundles_created}")
    database[BUNDLE_COLLECTION_NAME].create_index(
        [("source_namespace", 1), ("window_end", -1)],
        name=TREND_SELECTION_INDEX_NAME,
    )
    bundles = list(database[BUNDLE_COLLECTION_NAME].find({"source_namespace": namespace}).sort("window_end", 1))
    if len(bundles) != 2 or len({bundle["content_sha256"] for bundle in bundles}) != 2:
        raise RuntimeError("Module 3 writer did not publish two distinct hashed finalized bundles")
    return {
        "namespace": namespace,
        "source_scope": SCOPE,
        "as_of_event_time": current_minute,
        "older_window_end": bundles[0]["window_end"],
        "selected_window_end": bundles[1]["window_end"],
        "bundles": [
            {"window_end": bundle["window_end"], "content_sha256": bundle["content_sha256"], "row_count": bundle["row_count"]}
            for bundle in bundles
        ],
        "selected_items": bundles[1]["items"],
    }


def _seed_ab(spark: SparkSession, admin_client: MongoClient, database_name: str) -> dict[str, Any]:
    config = Module4Config(
        master="local[1]",
        mongodb_uri=os.environ["MODULE4D_DEMO_ROOT_URI"],
        mongo_database=database_name,
        recommendation_k=20,
    )
    recommendation_rows = [
        (user_id, 1001 + (user_id - WARM_USER_ID) * 20 + rank - 1, float(21 - rank), rank)
        for user_id in (WARM_USER_ID, FALLBACK_USER_ID)
        for rank in range(1, 21)
    ]
    recommendation_frame = spark.createDataFrame(
        recommendation_rows,
        ["user_id", "product_id", "als_score", "als_rank"],
    )
    graph_rows = [
        (product_id, f"Fixture product {product_id}", 4, 15, product_id / 100_000.0)
        for product_id in range(1001, 1041)
    ]
    graph_frame = spark.createDataFrame(
        graph_rows,
        ["product_id", "product_name", "neighbor_count", "weighted_degree", "pagerank"],
    )
    als_report = publish_recommendations(
        recommendation_frame, config, MODEL_VERSION, SOURCE_RUN_ID, client=admin_client
    )
    graph_report = publish_graph_metrics(
        graph_frame, config, GRAPH_VERSION, SOURCE_RUN_ID, 2, client=admin_client
    )
    als_check = validate_snapshot(
        admin_client, database_name, config.mongo_recommendation_collection,
        "model_version", MODEL_VERSION, als_report["expected_documents"],
    )
    graph_check = validate_snapshot(
        admin_client, database_name, config.mongo_graph_collection,
        "graph_version", GRAPH_VERSION, graph_report["expected_documents"],
    )
    if not als_check["valid"] or not graph_check["valid"]:
        raise RuntimeError("Module 4A/4B published snapshot validation failed")
    update_pair_readiness(
        admin_client, config, SOURCE_RUN_ID, MODEL_VERSION, GRAPH_VERSION,
        "ready", True, True,
    )
    return {
        "run_id": SOURCE_RUN_ID,
        "model_version": MODEL_VERSION,
        "graph_version": GRAPH_VERSION,
        "als_rows": als_check["documents"],
        "graph_rows": graph_check["documents"],
        "users": [WARM_USER_ID, FALLBACK_USER_ID],
        "readiness": "ready",
    }


def _publish_hybrid(
    admin_client: MongoClient,
    database_name: str,
    namespace: str,
    as_of: datetime,
    processing_time: datetime,
) -> dict[str, Any]:
    config = Module4Config(
        master="local[1]",
        mongodb_uri=os.environ["MODULE4D_DEMO_ROOT_URI"],
        mongo_database=database_name,
        recommendation_k=20,
    )
    result = serve_hybrid_user(
        config,
        user_id=WARM_USER_ID,
        source_namespace=namespace,
        as_of_event_time=as_of,
        ranking_config=HybridRankingConfig(lambda_weight=0.7),
        processing_time_utc=processing_time,
        client=admin_client,
    )
    repeat = serve_hybrid_user(
        config,
        user_id=WARM_USER_ID,
        source_namespace=namespace,
        as_of_event_time=as_of,
        ranking_config=HybridRankingConfig(lambda_weight=0.7),
        processing_time_utc=processing_time,
        client=admin_client,
    )
    if result.mode != "hybrid" or not result.trend_available:
        raise RuntimeError("warm fixture was not published as a Hybrid snapshot with finalized trends")
    if not repeat.idempotent or repeat.serving_version != result.serving_version:
        raise RuntimeError("accepted Hybrid publisher did not converge on an idempotent repeat")
    fallback_as_of = as_of - timedelta(hours=1)
    fallback = serve_hybrid_user(
        config,
        user_id=FALLBACK_USER_ID,
        source_namespace=namespace,
        as_of_event_time=fallback_as_of,
        ranking_config=HybridRankingConfig(lambda_weight=0.7),
        processing_time_utc=processing_time,
        client=admin_client,
    )
    empty = serve_hybrid_user(
        config,
        user_id=EMPTY_USER_ID,
        source_namespace=namespace,
        as_of_event_time=fallback_as_of,
        ranking_config=HybridRankingConfig(lambda_weight=0.7),
        processing_time_utc=processing_time,
        client=admin_client,
    )
    if fallback.mode != "als_fallback_trend_unavailable" or fallback.recommendation_count == 0:
        raise RuntimeError("supported ALS fallback fixture did not publish populated rows")
    if empty.mode != "no_candidates" or empty.recommendation_count != 0:
        raise RuntimeError("accepted publisher did not create the ready empty snapshot fixture")
    return {
        "warm": {
            "user_id": WARM_USER_ID,
            "serving_version": result.serving_version,
            "mode": result.mode,
            "recommendation_count": result.recommendation_count,
            "idempotent_repeat": repeat.idempotent,
            "source_run_id": result.source_run_id,
            "model_version": result.model_version,
            "graph_version": result.graph_version,
            "recommendations": [
                {"product_id": row["product_id"], "rank": row["rank"], "final_score": row["final_score"]}
                for row in admin_client[database_name][FINAL_RECOMMENDATIONS_COLLECTION]
                .find({"user_id": WARM_USER_ID, "serving_version": result.serving_version})
                .sort("rank", 1)
            ],
        },
        "fallback": {
            "user_id": FALLBACK_USER_ID,
            "serving_version": fallback.serving_version,
            "mode": fallback.mode,
            "reason": fallback.reason,
            "recommendation_count": fallback.recommendation_count,
            "as_of_event_time": fallback_as_of,
        },
        "ready_empty": {
            "user_id": EMPTY_USER_ID,
            "serving_version": empty.serving_version,
            "mode": empty.mode,
            "reason": empty.reason,
            "recommendation_count": empty.recommendation_count,
        },
    }


def _provision_and_verify_indexes(database: Any, namespace: str) -> dict[str, Any]:
    for order, (name, keys) in PRODUCT_INDEXES.items():
        database["batch_product_metrics"].create_index(list(keys), name=name)
    database[BUNDLE_COLLECTION_NAME].create_index(
        [("source_namespace", 1), ("window_end", -1)], name=TREND_SELECTION_INDEX_NAME
    )
    ensure_final_recommendation_indexes(database[FINAL_RECOMMENDATIONS_COLLECTION])
    expected = {
        "final_recommendations": {
            "uq_serving_user_rank": [("serving_version", 1), ("user_id", 1), ("rank", 1)],
            "uq_serving_user_product": [("serving_version", 1), ("user_id", 1), ("product_id", 1)],
        },
        BUNDLE_COLLECTION_NAME: {
            TREND_SELECTION_INDEX_NAME: [("source_namespace", 1), ("window_end", -1)],
        },
        "batch_product_metrics": {
            name: list(keys) for name, keys in PRODUCT_INDEXES.values()
        },
    }
    found: dict[str, Any] = {}
    for collection_name, named_indexes in expected.items():
        info = database[collection_name].index_information()
        found[collection_name] = {}
        for name, keys in named_indexes.items():
            actual = info.get(name)
            if actual is None or list(actual.get("key", ())) != keys:
                raise RuntimeError(f"required MongoDB index is missing or differs: {collection_name}.{name}")
            found[collection_name][name] = list(actual["key"])
        if "_id_" not in info:
            raise RuntimeError(f"default _id index is missing from {collection_name}")
    return found


def _verify_readonly_role(client: MongoClient, database_name: str) -> None:
    database = client[database_name]
    database["final_recommendations"].find_one({"user_id": WARM_USER_ID})
    database["final_recommendations"].index_information()
    for operation in (
        lambda: database["final_recommendations"].insert_one({"_id": "forbidden-demo-write"}),
        lambda: database["final_recommendations"].create_index([("forbidden_probe", 1)]),
    ):
        try:
            operation()
        except OperationFailure as exc:
            if exc.code != 13:
                raise RuntimeError(f"read-only permission probe failed with MongoDB code {exc.code}") from exc
        else:
            raise RuntimeError("database read role unexpectedly wrote a document or created an index")


def seed() -> dict[str, Any]:
    database_name = _required("MODULE4D_DEMO_DATABASE")
    namespace = _required("MODULE4D_DEMO_TREND_NAMESPACE")
    root_uri = _required("MODULE4D_DEMO_ROOT_URI")
    reader_name = _required("MODULE4D_DEMO_READONLY_USERNAME")
    reader_password = _required("MODULE4D_DEMO_READONLY_PASSWORD")
    if not NAMESPACE_RE.fullmatch(namespace):
        raise RuntimeError("demo trend namespace contains unsupported characters")
    _validate_target(root_uri, database_name)
    now = datetime.now(timezone.utc)
    processing_time = now.replace(microsecond=(now.microsecond // 1000) * 1000)
    client = MongoClient(
        root_uri,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=5_000,
        tz_aware=True,
        appname="module4d-demo-fixture-admin",
    )
    database_owned = False
    user_created = False
    spark = None
    try:
        client.admin.command("ping")
        if client.server_info()["version"] != "7.0.16":
            raise RuntimeError("the demo fixture requires MongoDB 7.0.16")
        if database_name in client.list_database_names():
            raise RuntimeError("generated demo database already exists; refusing to overwrite it")
        database = client[database_name]
        database["_module4d_demo_ownership"].insert_one(
            {"_id": "created-by-module4d-demo-fixture", "created_at": now, "synthetic": True}
        )
        database_owned = True
        spark = _spark_session()
        batch_summary = _seed_batch(spark, root_uri, database_name)
        trend_summary = _seed_trends(spark, database, namespace, now)
        ab_summary = _seed_ab(spark, client, database_name)
        hybrid_summary = _publish_hybrid(
            client,
            database_name,
            namespace,
            trend_summary["as_of_event_time"],
            processing_time,
        )
        indexes = _provision_and_verify_indexes(database, namespace)
        client[database_name].command(
            "createUser",
            reader_name,
            pwd=reader_password,
            roles=[{"role": "read", "db": database_name}],
        )
        user_created = True
        readonly_uri = (
            f"mongodb://{reader_name}:{reader_password}@module4d-mongo:27017/"
            f"{database_name}?authSource={database_name}"
        )
        readonly = MongoClient(
            readonly_uri,
            serverSelectionTimeoutMS=5_000,
            tz_aware=True,
            appname="module4d-demo-readonly-check",
        )
        try:
            _verify_readonly_role(readonly, database_name)
        finally:
            readonly.close()
        return {
            "fixture": "synthetic demonstration data; not observed customer purchases",
            "mongodb_version": "7.0.16",
            "database": database_name,
            "namespace": namespace,
            "batch": batch_summary,
            "trends": trend_summary,
            "module4_source_generation": ab_summary,
            "hybrid_snapshots": hybrid_summary,
            "required_indexes": indexes,
            "default_id_indexes": True,
            "readonly_role": "database-scoped read; find/listIndexes succeeded; insert/createIndexes denied with code 13",
            "credentials_reported": False,
        }
    except Exception:
        if user_created:
            try:
                client[database_name].command("dropUser", reader_name)
            except Exception:
                pass
        if database_owned:
            client.drop_database(database_name)
        raise
    finally:
        if spark is not None:
            spark.stop()
        client.close()


def main() -> int:
    try:
        summary = seed()
    except Exception as exc:
        print(f"Module 4D fixture publication failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, default=_json_default, indent=2))
    return 0


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    raise TypeError(f"cannot serialize {type(value).__name__}")


if __name__ == "__main__":
    raise SystemExit(main())
