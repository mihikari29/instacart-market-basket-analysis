"""Require full-data standalone Spark/HDFS execution and matching Mongo outputs."""

import json
import os
from pathlib import Path
import pyarrow.dataset as ds
from pymongo import MongoClient


def main():
    path = max(Path("results/module2").glob("*/summary.json"), key=lambda p: p.parent.name)
    report = json.loads(path.read_text())
    assert report["status"] == "passed"
    assert report["environment"]["config"]["root"].startswith("hdfs://namenode:8020/")
    assert report["environment"]["config"]["master"] == "spark://spark-master:7077"
    assert report["validation"]["counts"] == {
        "orders": 3421083,
        "prior": 32434489,
        "train": 1384617,
        "products": 49688,
        "aisles": 134,
        "departments": 21,
        "interactions": 33819106,
        "users": 206209,
    }
    assert report["analytics"]["mongo_published"]
    assert report["join_benchmark"]["outputs_equal"]
    assert report["partition_pruning"]["equivalent_filters_equal"]
    assert report["cache_benchmark"]["outputs_equal"]
    with MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=10000) as client:
        database = client.get_default_database()
        for name in ("batch_product_metrics", "department_metrics"):
            expected = ds.dataset(path.parent / name, format="parquet").count_rows()
            assert database[name].count_documents({}) == expected
            assert database[name].count_documents({"run_id": report["run_id"]}) == expected
            print(f"Mongo {name}: {expected} documents", flush=True)
    print("FULL DATA + STANDALONE SPARK + HDFS + MONGODB: PASS", flush=True)


if __name__ == "__main__":
    main()
