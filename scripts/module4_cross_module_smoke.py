"""Check the Module 1 partition handoff, Module 2 features and Module 4 together."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import uuid

from pymongo import MongoClient

from src.module2.analytics import analyze
from src.module2.config import Config as BatchConfig
from src.module2.data import read_tables, validate
from src.module2.fixtures import create_fixture
from src.module4.config import Config
from src.module4.data import read_inputs, validate_inputs, validate_source_receipts
from src.module4.spark import session
from scripts.module4_flow_smoke import run_cli


def main():
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


if __name__ == "__main__":
    main()
