"""Standalone-worker smoke for Spark, GraphFrames, HDFS and MongoDB."""

from __future__ import annotations

import hashlib
from importlib import metadata
import json
from pathlib import Path
import uuid

from src.module4.config import Config
from src.module4.graph import pagerank
from src.module4.spark import session


def main():
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


if __name__ == "__main__":
    main()
