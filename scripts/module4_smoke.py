"""Bounded Module 4 checks against the pinned Docker runtime.

`runtime` proves the image can run a standalone worker, GraphFrames
PageRank, an HDFS round trip and a MongoDB ping. `flow` publishes a
staged fixture and replays the same model version. `cross-module` adds
the Module 2 batch step in front of the Module 4 step.
"""

from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

from pymongo import MongoClient

from src.module2.analytics import analyze
from src.module2.config import Config as BatchConfig
from src.module2.data import read_tables, validate
from src.module2.fixtures import create_fixture
from src.module4.config import Config
from src.module4.data import hdfs_sha256, read_inputs, validate_inputs, validate_source_receipts
from src.module4.graph import pagerank
from src.module4.sink_mongo import validate_snapshot
from src.module4.spark import session

def _write_json_hdfs(spark, path: str, value: dict):
    jvm = spark._jvm
    location = jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.create(location, False)
    try:
        stream.write(bytearray(json.dumps(value, sort_keys=True).encode("utf-8")))
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
            "sha256": hdfs_sha256(spark, fs, parquet_files[0]),
        }
    _write_json_hdfs(spark, f"{root}/curated/_sources.json", sources)
    return sources


def run_cli(root: Path, fixture_root: str, output_root: str, run_id: str, model_version: str, graph_version: str):
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
        "--scope",
        "staged_fixture",
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


def run_flow() -> int:
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
        first = run_cli(
            Path.cwd(), fixture_root, output_root, first_run_id, model_version, graph_version
        )
        replay_id = f"flow-{run_token}-replay"
        replay = run_cli(
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
        with MongoClient(mongo_uri, serverSelectionTimeoutMS=5000) as client:
            client.drop_database(mongo_database)


def run_runtime() -> int:
    from pymongo import MongoClient

    config = Config.environment()
    if not config.master.startswith("spark://"):
        raise RuntimeError(f"Standalone smoke requires spark:// master, got {config.master!r}")
    run_id = uuid.uuid4().hex
    smoke_root = config.root.rstrip("/") + f"/_module4_runtime_smoke/{run_id}"
    spark = None
    fs = None
    smoke_path = None
    try:
        spark = session(config)
        if not spark.sparkContext.master.startswith("spark://"):
            raise RuntimeError("Spark did not connect to the configured standalone master")

        vertices = spark.createDataFrame([(1,), (2,), (3,)], "id int")
        edges = spark.createDataFrame([(1, 2), (2, 1)], "src int, dst int")
        ranks = pagerank(vertices, edges, backend="graphframes")
        rank_rows = ranks.collect()
        if {row.id for row in rank_rows} != {1, 2, 3}:
            raise RuntimeError("GraphFrames PageRank lost an isolated catalog vertex")
        if any(row.pagerank is None or row.pagerank != row.pagerank or abs(row.pagerank) == float("inf") for row in rank_rows):
            raise RuntimeError("GraphFrames returned a null or non-finite PageRank")

        smoke_path = spark._jvm.org.apache.hadoop.fs.Path(smoke_root)
        fs = smoke_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(smoke_path):
            raise FileExistsError(f"Refusing to reuse runtime smoke path {smoke_root}")
        spark.createDataFrame([(run_id, 1), (run_id, 2)], "run_id string, value int").repartition(2).write.parquet(
            smoke_root
        )
        hdfs_count = spark.read.parquet(smoke_root).where(f"run_id = '{run_id}'").count()
        if hdfs_count != 2:
            raise RuntimeError(f"HDFS worker write/read count mismatch: {hdfs_count}")

        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=5000) as client:
            client.admin.command("ping")

        graphframes_jars = []
        for path in sorted(Path("/opt/spark/jars").glob("graphframes*.jar")):
            graphframes_jars.append(
                {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            )
        result = {
            "status": "passed",
            "run_id": run_id,
            "spark_master": spark.sparkContext.master,
            "spark_version": spark.version,
            "scala_version": spark._jvm.scala.util.Properties.versionNumberString(),
            "graphframes_backend": "graphframes",
            "graphframes_python_version": metadata.version("graphframes"),
            "graphframes_jars": graphframes_jars,
            "pagerank_vertices": len(rank_rows),
            "hdfs_rows_round_tripped": hdfs_count,
            "mongodb_ping": "passed",
        }
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        if spark is not None:
            spark.stop()
        if fs is not None and smoke_path is not None and fs.exists(smoke_path):
            fs.delete(smoke_path, True)


def run_cross_module() -> int:
    runtime = Config.environment()
    token = uuid.uuid4().hex
    root = runtime.root.rstrip("/") + f"/_module4_cross_module/{token}"
    database = f"module4_cross_{token}"
    os.environ["MODULE4_MONGO_DATABASE"] = database
    spark = session(runtime)
    path = spark._jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    try:
        if fs.exists(path):
            raise FileExistsError(root)
        with tempfile.TemporaryDirectory(prefix="module4-cross-") as directory:
            fixture = create_fixture(Path(directory) / "fixture")
            fs.copyFromLocalFile(False, False, spark._jvm.org.apache.hadoop.fs.Path(str(fixture)), path)
        batch_config = BatchConfig(root=root, master=runtime.master, shuffle_partitions=2)
        tables = read_tables(spark, batch_config)
        batch_validation = validate(spark, batch_config, tables)
        _, outputs = analyze(spark, tables, batch_validation["counts"], min_support=1)
        outputs["user_features"].write.mode("error").parquet(batch_config.features())
        feature_users = spark.read.parquet(batch_config.features()).count()
        module4_tables = read_inputs(spark, Config(root=root))
        module4_validation = validate_inputs(module4_tables)
        receipt = validate_source_receipts(spark, Config(root=root), module4_tables)
        for name in ("orders", "prior", "train", "products"):
            if batch_validation["counts"][name] != module4_validation["counts"][name]:
                raise RuntimeError(f"Module 2/4 handoff mismatch for {name}")
        spark.stop()
        spark = None
        run_id = f"cross-{token}"
        result = run_cli(Path.cwd(), root, "results/module4-cross", run_id,
                          f"als-{run_id}", f"graph-{run_id}")
        with MongoClient(runtime.mongodb_uri, serverSelectionTimeoutMS=5000) as client:
            if client[database]["user_recommendations"].count_documents({}) != 24:
                raise RuntimeError("cross-module recommendation count mismatch")
            if client[database]["product_graph_metrics"].count_documents({}) != 6:
                raise RuntimeError("cross-module graph count mismatch")
        print(json.dumps({"status": "passed", "scope": "staged_fixture", "run_id": run_id,
                          "module1_partition_receipt_validated": True,
                          "module2_user_features": feature_users,
                          "module2_module4_counts_match": True, "source_receipt": receipt,
                          "pair_ready": result["pair_readiness"]["ready"]}, indent=2), flush=True)
    finally:
        if spark is not None:
            spark.stop()
        if fs.exists(path):
            fs.delete(path, True)
        with MongoClient(runtime.mongodb_uri, serverSelectionTimeoutMS=5000) as client:
            client.drop_database(database)


def main(argv=None) -> int:
    """Run one Module 4 smoke scenario."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="scenario", required=True)
    commands.add_parser("runtime", help="check the standalone worker, GraphFrames, HDFS and MongoDB")
    commands.add_parser("flow", help="publish a staged fixture and replay the same version")
    commands.add_parser("cross-module", help="run the Module 2 batch step in front of Module 4")
    arguments = parser.parse_args(argv)
    if arguments.scenario == "runtime":
        return run_runtime()
    if arguments.scenario == "flow":
        return run_flow()
    return run_cross_module()


if __name__ == "__main__":
    raise SystemExit(main())
