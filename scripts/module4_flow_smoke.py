"""Run a bounded staged-layout Module 4 flow and same-version publication replay."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from src.module4.config import Config
from src.module4.data import _hdfs_sha256
from src.module4.sink_mongo import validate_snapshot
from src.module4.spark import session


def _write_json_hdfs(spark, path: str, value: dict):
    jvm = spark._jvm
    location = jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(location, False)
    try:
        stream.write(jvm.java.lang.String(json.dumps(value, sort_keys=True)).getBytes("UTF-8"))
    finally:
        stream.close()


def _fixture_frames(spark):
    orders = spark.createDataFrame(
        [
            (1, 1, "prior", 1),
            (2, 1, "prior", 2),
            (3, 1, "train", 3),
            (4, 2, "prior", 1),
            (5, 2, "prior", 2),
            (6, 2, "train", 3),
            (7, 3, "prior", 1),
            (8, 3, "test", 2),
        ],
        "order_id long, user_id int, eval_set string, order_number int",
    )
    prior = spark.createDataFrame(
        [
            (1, 1, 1, False), (1, 2, 2, False),
            (2, 1, 1, True), (2, 3, 2, False),
            (4, 1, 1, False), (4, 2, 2, False),
            (5, 1, 1, True), (5, 3, 2, False),
            (7, 2, 1, False), (7, 3, 2, False),
        ],
        "order_id long, product_id int, add_to_cart_order int, reordered boolean",
    )
    train = spark.createDataFrame(
        [(3, 1, 1, True), (3, 4, 2, False), (6, 2, 1, True), (6, 4, 2, False)],
        "order_id long, product_id int, add_to_cart_order int, reordered boolean",
    )
    products = spark.createDataFrame(
        [
            (1, "one", 1, 1),
            (2, "two", 1, 1),
            (3, "three", 1, 1),
            (4, "cold truth", 1, 1),
            (5, "isolated catalog product", 1, 1),
        ],
        "product_id int, product_name string, aisle_id int, department_id int",
    )
    return {
        "orders": orders,
        "order_products_prior": prior,
        "order_products_train": train,
        "products": products,
    }


def _prepare_fixture(spark, root: str):
    tables = _fixture_frames(spark)
    jvm = spark._jvm
    hadoop_config = spark.sparkContext._jsc.hadoopConfiguration()
    sources = {}
    for name, frame in tables.items():
        relative = "dimensions/products" if name == "products" else name
        path = f"{root}/curated/{relative}"
        frame.coalesce(1).write.parquet(path)
        location = jvm.org.apache.hadoop.fs.Path(path)
        fs = location.getFileSystem(hadoop_config)
        files = [status.getPath() for status in fs.listStatus(location) if status.isFile()]
        parquet_files = [file_path for file_path in files if file_path.getName().endswith(".parquet")]
        if len(parquet_files) != 1:
            raise RuntimeError(f"Fixture staging expected one Parquet file under {path}")
        source_name = "products" if name == "products" else name
        sources[source_name] = {
            "rows": frame.count(),
            "sha256": _hdfs_sha256(spark, fs, parquet_files[0]),
        }
    _write_json_hdfs(spark, f"{root}/curated/_sources.json", sources)
    return sources


def _run_cli(root: Path, fixture_root: str, output_root: str, run_id: str, model_version: str, graph_version: str):
    args = [
        sys.executable,
        "-m",
        "src.module4.cli",
        "all",
        "--root",
        fixture_root,
        "--output",
        output_root,
        "--run-id",
        run_id,
        "--model-version",
        model_version,
        "--graph-version",
        graph_version,
        "--pilot-users",
        "20",
        "--max-trials",
        "1",
    ]
    result = subprocess.run(args, cwd=root, text=True, capture_output=True, check=False)
    log_path = root / output_root / run_id / "flow-command.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(result.stdout + "\n--- STDERR ---\n" + result.stderr, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(
            f"Module 4 flow {run_id} failed with exit {result.returncode}; see {log_path}"
        )
    summary = json.loads((log_path.parent / "summary.json").read_text(encoding="utf-8"))
    if summary.get("status") != "passed" or not summary.get("pair_readiness", {}).get("ready"):
        raise RuntimeError(f"Module 4 flow did not publish a ready A/B pair: {summary}")
    return summary


def main():
    from pymongo import MongoClient

    runtime = Config.environment()
    run_token = uuid.uuid4().hex
    fixture_root = runtime.root.rstrip("/") + f"/_module4_flow_fixture/{run_token}"
    mongo_database = f"module4_flow_{run_token}"
    mongo_uri = f"mongodb://mongodb:27017/{mongo_database}"
    os.environ["MONGODB_URI"] = mongo_uri
    os.environ["MODULE4_MONGO_DATABASE"] = mongo_database
    os.environ["MODULE4_MONGO_TEST_URI"] = "mongodb://mongodb:27017"
    spark = None
    hdfs_path = None
    try:
        spark = session(runtime)
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(fixture_root)
        fs = hdfs_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(hdfs_path):
            raise FileExistsError(f"Refusing to overwrite fixture root {fixture_root}")
        sources = _prepare_fixture(spark, fixture_root)
        spark.stop()
        spark = None

        model_version = f"als-flow-{run_token}"
        graph_version = f"graph-flow-{run_token}"
        output_root = "results/module4-flow"
        first_run_id = f"flow-{run_token}-first"
        first = _run_cli(
            Path.cwd(), fixture_root, output_root, first_run_id, model_version, graph_version
        )
        replay_id = f"flow-{run_token}-replay"
        replay = _run_cli(
            Path.cwd(), fixture_root, output_root, replay_id, model_version, graph_version
        )
        if replay["als"]["model"].get("reused_existing_model") is not True:
            raise RuntimeError("same-version replay did not load the existing verified ALS model")

        with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
            readiness = client[mongo_database]["module4_snapshot_readiness"].find_one({"_id": "active"})
            if not readiness or readiness.get("ready") is not True or readiness.get("run_id") != replay_id:
                raise RuntimeError(f"Mongo readiness pointer does not describe the replay: {readiness}")
            recommendation_check = validate_snapshot(
                client,
                mongo_database,
                "user_recommendations",
                "model_version",
                model_version,
            )
            graph_check = validate_snapshot(
                client,
                mongo_database,
                "product_graph_metrics",
                "graph_version",
                graph_version,
            )
            recommendation_count = client[mongo_database]["user_recommendations"].count_documents(
                {"model_version": model_version, "run_id": replay_id}
            )
            graph_count = client[mongo_database]["product_graph_metrics"].count_documents(
                {"graph_version": graph_version, "run_id": replay_id}
            )
            if not recommendation_check["valid"] or not graph_check["valid"]:
                raise RuntimeError("Mongo snapshot read-back validation failed")
            if recommendation_count != 9 or graph_count != 5:
                raise RuntimeError(
                    f"Unexpected fixture snapshot sizes: recommendations={recommendation_count}, graph={graph_count}"
                )
            client.drop_database(mongo_database)

        output = {
            "status": "passed",
            "scope": "staged_fixture",
            "source_receipt_tables": sources,
            "first_run_id": first_run_id,
            "replay_run_id": replay_id,
            "same_model_version_reused": True,
            "recommendation_documents": recommendation_count,
            "graph_documents": graph_count,
            "both_runs_ready": bool(first["pair_readiness"]["ready"] and replay["pair_readiness"]["ready"]),
        }
        print(json.dumps(output, indent=2, sort_keys=True))
    finally:
        if spark is not None:
            spark.stop()
        if hdfs_path is not None and fs.exists(hdfs_path):
            fs.delete(hdfs_path, True)


if __name__ == "__main__":
    main()
