import hashlib
import json
import re
import statistics
import time
from datetime import date, timedelta
from functools import reduce
from pyspark import StorageLevel
from pyspark.sql import functions as F
from .data import require


def canonical(rows):
    records = [r.asDict(recursive=True) for r in rows]
    return json.dumps(
        sorted(records, key=lambda r: json.dumps(r, sort_keys=True)), sort_keys=True, default=str
    )


def checksum(rows):
    return hashlib.sha256(canonical(rows).encode()).hexdigest()


def plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def verify_join(text, operator):
    require(text.count(operator) == 2, f"Expected two {operator} operators")
    other = "BroadcastHashJoin" if operator == "SortMergeJoin" else "SortMergeJoin"
    require(
        other not in text and "ShuffledHashJoin" not in text and "AdaptiveSparkPlan" not in text,
        "Uncontrolled join/AQE strategy",
    )


def verify_pruning(text, restricted):
    filters = re.findall(r"PartitionFilters: \[(.*?)\],", text)
    require(bool(filters), "Missing file scan PartitionFilters evidence")
    if restricted:
        require(
            any("synthetic_year" in f and "synthetic_month" in f and "synthetic_day" in f for f in filters),
            "No date partition pruning",
        )
    else:
        require(all(not f.strip() for f in filters), "Baseline unexpectedly has partition filters")


def join_query(tables, strategy):
    facts = tables["prior"].select("product_id", "reordered")
    products = tables["products"].select("product_id", "department_id")
    departments = tables["departments"].select("department_id", "department")
    if strategy == "broadcast_hash":
        products, departments = F.broadcast(products), F.broadcast(departments)
    elif strategy == "sort_merge":
        facts, products, departments = (df.hint("merge") for df in (facts, products, departments))
    else:
        raise ValueError(strategy)
    return (
        facts.join(products, "product_id")
        .join(departments, "department_id")
        .groupBy("department_id", "department")
        .agg(
            F.count("*").alias("purchase_count"),
            F.sum(F.col("reordered").cast("long")).alias("reorder_count"),
        )
    )


def trial(spark, df, label, verifier=None):
    spark.sparkContext.setJobGroup(label, label)
    try:
        before = plan(df)
        if verifier:
            verifier(before)
        start = time.perf_counter()
        result = df.collect()  # only small departmental/scan aggregates
        seconds = time.perf_counter() - start
        after = plan(df)
        if verifier:
            verifier(after)
        return {
            "job_group": label,
            "seconds": seconds,
            "checksum": checksum(result),
            "physical_plan": after,
            "result": json.loads(canonical(result)),
        }
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)


def summarize(trials):
    require(bool(trials), "No measured trials")
    require(len({t["checksum"] for t in trials}) == 1, "Trial outputs differ")
    return {
        "runs_seconds": [t["seconds"] for t in trials],
        "median_seconds": statistics.median(t["seconds"] for t in trials),
        "trials": trials,
    }


def benchmark_joins(spark, tables, config):
    results = {"sort_merge": [], "broadcast_hash": []}
    warmups = []
    for i in range(-config.warmups, config.repetitions):
        names = list(results) if i % 2 == 0 else list(reversed(results))
        for name in names:
            spark.catalog.clearCache()
            operator = "SortMergeJoin" if name == "sort_merge" else "BroadcastHashJoin"
            measured = trial(
                spark, join_query(tables, name), f"joins/{name}/{i}", lambda text: verify_join(text, operator)
            )
            (results[name] if i >= 0 else warmups).append(measured)
    all_trials = [t for group in results.values() for t in group] + warmups
    require(len({t["checksum"] for t in all_trials}) == 1, "Join strategies returned different outputs")
    return {
        **{k: summarize(v) for k, v in results.items()},
        "warmups": warmups,
        "outputs_equal": True,
        "operators_verified": True,
        "aqe": False,
        "cache_policy": "clear Spark cache before every arm; OS/HDFS caches are not flushed",
    }


def partition_predicate(start, end):
    require(start <= end and (end - start).days <= 31, "Window must be 1..32 days")
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    return reduce(
        lambda a, b: a | b,
        [
            (F.col("synthetic_year") == d.year)
            & (F.col("synthetic_month") == d.month)
            & (F.col("synthetic_day") == d.day)
            for d in days
        ],
    )


def scan_query(df):
    # count alone can read only Parquet metadata; sum forces a payload column scan.
    return df.agg(
        F.count("*").alias("events"),
        F.sum(F.col("reordered").cast("long")).alias("reorders"),
        F.sum("product_id").alias("product_id_sum"),
    )


def benchmark_partitions(spark, events, config):
    first = events.agg(F.min("synthetic_date")).first()[0]
    require(first is not None, "Empty interaction dataset")
    start = date.fromisoformat(str(first))
    end = start + timedelta(days=6)
    builders = {
        "full_scan": lambda: events,
        "date_filter": lambda: events.where(
            F.col("synthetic_date").between(start.isoformat(), end.isoformat())
        ),
        "partition_filter": lambda: events.where(partition_predicate(start, end)),
    }
    results = {name: [] for name in builders}
    warmups = []
    for i in range(-config.warmups, config.repetitions):
        names = list(builders)
        names = names[i % len(names) :] + names[: i % len(names)]
        for name in names:
            spark.catalog.clearCache()
            measured = trial(
                spark,
                scan_query(builders[name]()),
                f"partitions/{name}/{i}",
                lambda text: verify_pruning(text, name == "partition_filter"),
            )
            (results[name] if i >= 0 else warmups).append(measured)
    require(results["partition_filter"][0]["result"][0]["events"] > 0, "Selected empty date window")
    require(
        {t["checksum"] for t in results["date_filter"]}
        == {t["checksum"] for t in results["partition_filter"]},
        "Equivalent date filters disagree",
    )
    return {
        **{k: summarize(v) for k, v in results.items()},
        "warmups": warmups,
        "window": [start.isoformat(), end.isoformat()],
        "equivalent_filters_equal": True,
        "partition_filters_verified": True,
        "interpretation": "Full scan has a different scope; only date_filter and partition_filter are equivalent.",
    }


def benchmark_cache(spark, tables, config):
    results = {"uncached": [], "cached_reuse": []}
    materializations, warmups = [], []
    for i in range(-config.warmups, config.repetitions):
        # Alternate which arm goes first; fully remove persisted state between arms.
        for name in ["uncached", "cached_reuse"] if i % 2 == 0 else ["cached_reuse", "uncached"]:
            spark.catalog.clearCache()
            df = join_query(tables, "broadcast_hash")
            try:
                if name == "cached_reuse":
                    df.persist(StorageLevel.MEMORY_AND_DISK)
                    built = trial(spark, df, f"cache/materialize/{i}")
                    (materializations if i >= 0 else warmups).append(built)
                measured = trial(spark, df, f"cache/{name}/{i}")
                if name == "cached_reuse":
                    require("InMemoryTableScan" in measured["physical_plan"], "Cache was not scanned")
                (results[name] if i >= 0 else warmups).append(measured)
            finally:
                df.unpersist(blocking=True)
    require(len({t["checksum"] for values in results.values() for t in values}) == 1, "Cache outputs differ")
    return {
        **{k: summarize(v) for k, v in results.items()},
        "warmups": warmups,
        "materializations": materializations,
        "outputs_equal": True,
        "scope": "persist reused department aggregate; build cost reported separately",
    }
