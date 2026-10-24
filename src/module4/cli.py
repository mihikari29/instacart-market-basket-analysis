"""End-to-end Module 4A/4B runner, validation gates and evidence writer."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import traceback
import uuid
from urllib.parse import urlsplit, urlunsplit

from ..common.runtime import new_run_id, save_json
from ..common.runtime import REPOSITORY_ROOT
from .als import (
    ALSParams,
    fit_model,
    load_model,
    model_summary,
    recommend_top_k,
    save_model,
)
from .config import Config
from .data import (
    build_prior_interactions,
    build_train_truth,
    enrich_prior,
    read_inputs,
    validate_inputs,
    validate_source_receipts,
)
from .data import read_user_features
from .evaluation import (
    PRIOR_ONLY_HYBRID_LAMBDAS,
    evaluate_at_k,
    global_popularity,
    offline_hybrid_recommendations,
    personal_frequency,
    popularity_products,
    prior_frequency_trend_products,
    prior_only_hybrid_evaluation_provenance,
    prior_trend_only_recommendations,
    ranking_metrics,
    split_truth_by_history,
)
from .graph import (
    build_edges,
    build_metrics,
    build_vertices,
    directed_edges,
    validate_edges,
    validate_metrics,
    weighted_degree,
)
from .sink_mongo import (
    publication_lock,
    publish_graph_metrics,
    publish_recommendations,
    update_pair_readiness,
)
from .split import make_internal_holdout
from ..common.spark import clear_job_group
from .config import session


REPO_ROOT = REPOSITORY_ROOT
EVALUATION_KS = (5, 10, 20)
_MONGODB_URI = re.compile(r"(?i)mongodb(?:\+srv)?://[^\s\"'<>]+")
_SECRET_ASSIGNMENT = re.compile(r"(?i)\b(password|passwd|pwd|token|secret)(\s*[:=]\s*)([^\s,;&]+)")


def revision() -> dict:
    try:
        git = ["git", "-c", f"safe.directory={REPO_ROOT}"]
        commit = subprocess.check_output([*git, "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
        dirty = bool(subprocess.check_output([*git, "status", "--porcelain"], cwd=REPO_ROOT, text=True))
        return {"commit": commit, "dirty": dirty}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": os.environ.get("MODULE4_COMMIT", "unavailable"), "dirty": None}


def source_digest() -> str:
    """Hash code and pinned runtime declarations that can change this run."""
    digest = hashlib.sha256()
    paths = [
        *REPO_ROOT.joinpath("src/module4").glob("*.py"),
        *REPO_ROOT.joinpath("src/common").glob("*.py"),
        REPO_ROOT / "scripts/module4.py",
        REPO_ROOT / "scripts/module4_smoke.py",
        REPO_ROOT / "Dockerfile",
        REPO_ROOT / "compose.yaml",
        REPO_ROOT / "requirements.txt",
        REPO_ROOT / "requirements-dev.txt",
    ]
    for path in sorted(paths, key=lambda item: item.as_posix()):
        digest.update(path.relative_to(REPO_ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _redact_uri(uri: str) -> str:
    parsed = urlsplit(uri)
    if not parsed.scheme:
        return "<redacted>"
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    if parsed.port:
        hostname = f"{hostname}:{parsed.port}"
    netloc = f"***@{hostname}" if parsed.username or parsed.password else hostname
    return urlunsplit((parsed.scheme, netloc, parsed.path, "<redacted>" if parsed.query else "", ""))


def _redact_sensitive_text(value: str) -> str:
    redacted = _MONGODB_URI.sub(lambda match: _redact_uri(match.group(0)), value)
    return _SECRET_ASSIGNMENT.sub(r"\1\2<redacted>", redacted)


def _safe_config(config: Config) -> dict:
    values = asdict(config)
    values["mongodb_uri"] = _redact_uri(config.mongodb_uri)
    return values


def _valid_version(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", value) or ".." in value:
        raise ValueError(f"{label} must be a 1–80 character path-safe version identifier")
    return value


def _sample_users(frame, limit: int | None, seed: int):
    if limit is None or limit <= 0:
        return frame.select("user_id").distinct()
    from pyspark.sql import functions as F

    users = frame.select("user_id").distinct().withColumn(
        "stable_hash", F.xxhash64("user_id", F.lit(int(seed)))
    )
    return users.orderBy("stable_hash", "user_id").limit(limit).drop("stable_hash")


def _trial_grid(max_trials: int = 12) -> list[ALSParams]:
    if not 1 <= max_trials <= 12:
        raise ValueError("max_trials must be between 1 and 12")
    candidates = [
        ALSParams(),
        ALSParams(rank=16, reg_param=0.01, alpha=10.0, max_iter=5, strength_col="strength_raw"),
        ALSParams(rank=16, reg_param=0.05, alpha=10.0, max_iter=10),
        ALSParams(rank=32, reg_param=0.05, alpha=10.0, max_iter=10),
        ALSParams(rank=32, reg_param=0.1, alpha=40.0, max_iter=10),
        ALSParams(rank=64, reg_param=0.1, alpha=10.0, max_iter=10),
        ALSParams(rank=16, reg_param=0.1, alpha=10.0, max_iter=15, strength_col="strength_log"),
        ALSParams(rank=32, reg_param=0.05, alpha=40.0, max_iter=10, strength_col="strength_log"),
        ALSParams(rank=64, reg_param=0.1, alpha=40.0, max_iter=10, strength_col="strength_log"),
        ALSParams(rank=16, reg_param=0.01, alpha=40.0, max_iter=10, strength_col="strength_log"),
        ALSParams(rank=32, reg_param=0.1, alpha=10.0, max_iter=5, strength_col="strength_raw"),
        ALSParams(rank=64, reg_param=0.05, alpha=10.0, max_iter=5, strength_col="strength_raw"),
    ]
    unique = {}
    for params in candidates:
        unique.setdefault(tuple(sorted(asdict(params).items())), params)
    return list(unique.values())[:max_trials]


def _metric_summary(recommendations, truth):
    return ranking_metrics(recommendations, truth, 10)


def _trial_order(trial):
    metrics = trial.get("metrics") or {}
    return (
        metrics.get("recall_at_k") if metrics.get("recall_at_k") is not None else -1.0,
        metrics.get("precision_at_k") if metrics.get("precision_at_k") is not None else -1.0,
        -trial.get("total_runtime_ms", float("inf")),
    )


def _unpersist_model(model):
    for name in ("userFactors", "itemFactors"):
        frame = getattr(model, name, None)
        if frame is not None:
            frame.unpersist(blocking=False)


def tune_als(
    split,
    config: Config,
    pilot_users: int = 5000,
    max_trials: int = 12,
    evidence_path: Path | None = None,
) -> dict:
    """Run seeded pilot trials, then validate its best three on full internal holdout."""
    from pyspark import StorageLevel

    if pilot_users < 1:
        raise ValueError("pilot_users must be positive")
    if not 1 <= max_trials <= 12:
        raise ValueError("max_trials must be between 1 and 12")
    started = time.perf_counter()
    full_training = build_prior_interactions(split["training"]).persist(StorageLevel.MEMORY_AND_DISK)
    full_training_rows = full_training.count()
    full_truth = split["validation_truth"].cache()
    full_truth.count()
    users = _sample_users(full_truth, pilot_users, config.seed).cache()
    pilot_training = full_training.join(users, "user_id", "inner").cache()
    pilot_truth = full_truth.join(users, "user_id", "inner").cache()
    state = {
        "status": "running",
        "seed": config.seed,
        "pilot_sampling": {"method": "xxhash64(user_id, seed), then user_id", "seed": config.seed},
        "pilot_users_requested": pilot_users,
        "pilot_users_actual": users.count(),
        "internal_training_interactions": full_training_rows,
        "pilot_training_interactions": pilot_training.count(),
        "max_pilot_trials": max_trials,
        "pilot_trials": [],
        "full_validation_trials": [],
        "official_train_used": False,
        "executor_shuffle_config": {
            "spark_master": config.master,
            "shuffle_partitions": config.shuffle_partitions,
        },
    }

    def checkpoint():
        if evidence_path is not None:
            save_json(evidence_path, state)

    try:
        checkpoint()
        if state["pilot_users_actual"] == 0:
            state.update(
                status="not_performed",
                reason="no user has at least two prior orders; internal holdout is empty",
                selected_params=None,
            )
            checkpoint()
            return state

        spark_context = pilot_training.sparkSession.sparkContext
        spark_context.setJobGroup("module4.als.pilot_baseline", "Module 4 pilot baselines", True)
        try:
            baseline_frames = {
                "global_popularity": global_popularity(pilot_training, users, 10),
                "personal_frequency": personal_frequency(pilot_training, users, 10),
            }
            state["pilot_baselines"] = {
                name: _metric_summary(frame, pilot_truth) for name, frame in baseline_frames.items()
            }
        finally:
            clear_job_group(spark_context)
        for trial_id, params in enumerate(_trial_grid(max_trials), 1):
            trial = {
                "trial_id": trial_id,
                "phase": "pilot",
                "params": asdict(params),
                "seed": config.seed,
                "input_interactions": state["pilot_training_interactions"],
                "input_users": users.count(),
                "input_products": pilot_training.select("product_id").distinct().count(),
                "status": "failed",
            }
            total_started = time.perf_counter()
            model = None
            recommendations = None
            spark_context.setJobGroup(
                f"module4.als.pilot.{trial_id}", f"Module 4 ALS pilot trial {trial_id}", True
            )
            try:
                fit_started = time.perf_counter()
                model = fit_model(pilot_training, params, config.seed, config.als_blocks)
                trial["fit_runtime_ms"] = round((time.perf_counter() - fit_started) * 1000, 3)
                trial["user_factors"] = model.userFactors.count()
                trial["item_factors"] = model.itemFactors.count()
                recommend_started = time.perf_counter()
                recommendations = recommend_top_k(model, users, 10)
                recommendations.count()
                trial["recommendation_runtime_ms"] = round(
                    (time.perf_counter() - recommend_started) * 1000, 3
                )
                evaluation_started = time.perf_counter()
                trial["metrics"] = _metric_summary(recommendations, pilot_truth)
                trial["evaluation_runtime_ms"] = round(
                    (time.perf_counter() - evaluation_started) * 1000, 3
                )
                trial["status"] = "passed"
            except Exception as exc:
                trial["error"] = f"{type(exc).__name__}: {exc}"
                trial["traceback"] = traceback.format_exc()
            finally:
                clear_job_group(spark_context)
                if recommendations is not None:
                    recommendations.unpersist(blocking=False)
                if model is not None:
                    _unpersist_model(model)
            trial["total_runtime_ms"] = round((time.perf_counter() - total_started) * 1000, 3)
            state["pilot_trials"].append(trial)
            checkpoint()

        pilot_passed = [trial for trial in state["pilot_trials"] if trial["status"] == "passed"]
        if not pilot_passed:
            state.update(status="failed", reason="all pilot ALS trials failed", selected_params=None)
            checkpoint()
            return state

        shortlist = sorted(pilot_passed, key=_trial_order, reverse=True)[:3]
        state["shortlist_trial_ids"] = [trial["trial_id"] for trial in shortlist]
        full_users = full_truth.select("user_id").distinct().cache()
        spark_context.setJobGroup("module4.als.full_baseline", "Module 4 full internal baselines", True)
        try:
            full_baselines = {
                "global_popularity": global_popularity(full_training, full_users, 10),
                "personal_frequency": personal_frequency(full_training, full_users, 10),
            }
            state["full_validation_baselines"] = {
                name: _metric_summary(frame, full_truth) for name, frame in full_baselines.items()
            }
        finally:
            clear_job_group(spark_context)
        for full_id, pilot_trial in enumerate(shortlist, 1):
            params = ALSParams(**pilot_trial["params"])
            trial = {
                "trial_id": full_id,
                "pilot_trial_id": pilot_trial["trial_id"],
                "phase": "full_internal_validation",
                "params": asdict(params),
                "seed": config.seed,
                "input_interactions": full_training_rows,
                "input_users": full_users.count(),
                "input_products": full_training.select("product_id").distinct().count(),
                "status": "failed",
            }
            total_started = time.perf_counter()
            model = None
            recommendations = None
            spark_context.setJobGroup(
                f"module4.als.full_validation.{full_id}",
                f"Module 4 ALS full validation trial {full_id}",
                True,
            )
            try:
                fit_started = time.perf_counter()
                model = fit_model(full_training, params, config.seed, config.als_blocks)
                trial["fit_runtime_ms"] = round((time.perf_counter() - fit_started) * 1000, 3)
                trial["user_factors"] = model.userFactors.count()
                trial["item_factors"] = model.itemFactors.count()
                recommend_started = time.perf_counter()
                recommendations = recommend_top_k(model, full_users, 10)
                recommendations.count()
                trial["recommendation_runtime_ms"] = round(
                    (time.perf_counter() - recommend_started) * 1000, 3
                )
                evaluation_started = time.perf_counter()
                trial["metrics"] = _metric_summary(recommendations, full_truth)
                trial["evaluation_runtime_ms"] = round(
                    (time.perf_counter() - evaluation_started) * 1000, 3
                )
                trial["status"] = "passed"
            except Exception as exc:
                trial["error"] = f"{type(exc).__name__}: {exc}"
                trial["traceback"] = traceback.format_exc()
            finally:
                clear_job_group(spark_context)
                if recommendations is not None:
                    recommendations.unpersist(blocking=False)
                if model is not None:
                    _unpersist_model(model)
            trial["total_runtime_ms"] = round((time.perf_counter() - total_started) * 1000, 3)
            state["full_validation_trials"].append(trial)
            checkpoint()
        full_passed = [trial for trial in state["full_validation_trials"] if trial["status"] == "passed"]
        if not full_passed:
            state.update(
                status="failed",
                reason="all full-size internal validation trials failed",
                selected_params=None,
            )
            checkpoint()
            return state
        selected = max(full_passed, key=_trial_order)
        state.update(
            status="passed" if len(full_passed) == len(shortlist) else "incomplete",
            selected_trial_id=selected["trial_id"],
            selected_pilot_trial_id=selected["pilot_trial_id"],
            selected_params=selected["params"],
            selection_metric="full internal Recall@10, then Precision@10, then runtime",
            shortlist_requested=len(shortlist),
            shortlist_passed=len(full_passed),
            total_runtime_ms=round((time.perf_counter() - started) * 1000, 3),
        )
        checkpoint()
        return state
    except Exception as exc:
        state.update(
            status="failed",
            reason=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc(),
        )
        checkpoint()
        raise
    finally:
        if "full_users" in locals():
            full_users.unpersist(blocking=False)
        users.unpersist(blocking=False)
        pilot_training.unpersist(blocking=False)
        pilot_truth.unpersist(blocking=False)
        full_truth.unpersist(blocking=False)
        full_training.unpersist(blocking=False)


def _hdfs_exists(spark, path: str) -> bool:
    location = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return bool(fs.exists(location))


def _write_hdfs_json(spark, path: str, value: dict) -> None:
    jvm = spark._jvm
    location = jvm.org.apache.hadoop.fs.Path(path)
    fs = location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(location):
        raise FileExistsError(f"Refusing to overwrite model metadata: {path}")
    stream = fs.create(location, False)
    try:
        stream.write(bytearray(json.dumps(value, sort_keys=True).encode("utf-8")))
    finally:
        stream.close()


def _assert_recommendation_cohort(frame, requested_users, products, k, item_factor_count):
    from pyspark.sql import functions as F

    expected_users = requested_users.select("user_id").distinct()
    actual_users = frame.select("user_id").distinct()
    missing = expected_users.join(actual_users, "user_id", "left_anti").limit(1).count()
    unexpected = actual_users.join(expected_users, "user_id", "left_anti").limit(1).count()
    if missing or unexpected:
        raise ValueError("recommendation user set does not match prior factor-backed users")
    target_rows = min(k, item_factor_count)
    counts = frame.groupBy("user_id").agg(
        F.count("*").alias("rows"),
        F.countDistinct("als_rank").alias("distinct_ranks"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.min("als_rank").alias("min_rank"),
        F.max("als_rank").alias("max_rank"),
    )
    if counts.where(
        (F.col("rows") != target_rows)
        | (F.col("distinct_ranks") != F.col("rows"))
        | (F.col("distinct_products") != F.col("rows"))
        | (F.col("min_rank") != 1)
        | (F.col("max_rank") != F.col("rows"))
    ).limit(1).count():
        raise ValueError("recommendation ranks/products/counts do not satisfy the Top-K contract")
    if frame.where(
        F.col("user_id").isNull()
        | F.col("product_id").isNull()
        | (F.col("user_id") <= 0)
        | (F.col("product_id") <= 0)
        | F.col("als_score").isNull()
        | F.isnan("als_score")
        | (F.abs("als_score") == float("inf"))
    ).limit(1).count():
        raise ValueError("recommendation output contains invalid key/score values")
    if frame.select("product_id").distinct().join(
        products.select("product_id").distinct(), "product_id", "left_anti"
    ).limit(1).count():
        raise ValueError("recommendation output references products outside catalog")
    return {
        "requested_users": expected_users.count(),
        "generated_users": actual_users.count(),
        "expected_rows": expected_users.count() * target_rows,
        "actual_rows": frame.count(),
        "products_per_user": target_rows,
        "user_set_exact": True,
        "rank_product_keys_unique": True,
        "product_foreign_keys_valid": True,
    }


def _evaluate_methods(recommendation_frames, truth, catalog_size):
    return {
        name: evaluate_at_k(frame, truth, EVALUATION_KS, catalog_size=catalog_size)
        for name, frame in recommendation_frames.items()
    }


def _evaluation_deltas(cohorts):
    deltas = {}
    for cohort_name, cohort in cohorts.items():
        methods = cohort["metrics"]
        als_metrics = methods.get("als", {})
        deltas[cohort_name] = {}
        for baseline_name in ("global_popularity", "personal_frequency"):
            baseline = methods.get(baseline_name, {})
            deltas[cohort_name][baseline_name] = {}
            for k in map(str, EVALUATION_KS):
                left = als_metrics.get(k, {})
                right = baseline.get(k, {})
                deltas[cohort_name][baseline_name][k] = {
                    metric: (
                        left.get(metric) - right.get(metric)
                        if left.get(metric) is not None and right.get(metric) is not None
                        else None
                    )
                    for metric in ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k")
                }
    return deltas


def _evaluate_prior_only_hybrid(
    interactions,
    als_recommendations,
    train_truth,
    all_users,
    warm_users,
    global_baseline,
    personal_baseline,
    catalog_size,
    source_receipt,
    cold_start_summary,
    run_id,
    model_version,
    source_code_sha256,
    als_params,
):
    """Compare fixed Hybrid lambda values using only prior-derived candidates."""
    from pyspark import StorageLevel

    provenance = prior_only_hybrid_evaluation_provenance(
        source_receipt,
        prior_eval_set="prior",
        truth_eval_set="train",
        official_train_used_for_fit=False,
        official_train_used_for_tuning=False,
        mixed_module1_event_feed_used=False,
    )
    trend_products = prior_frequency_trend_products(interactions, top_m=20).persist(
        StorageLevel.DISK_ONLY
    )
    trend_only = prior_trend_only_recommendations(all_users, trend_products).persist(
        StorageLevel.DISK_ONLY
    )
    cohorts = {
        "all_official_train_users": (all_users, train_truth),
        "warm_users_and_all_truth_items": (
            warm_users,
            train_truth.join(warm_users, "user_id", "inner"),
        ),
    }
    result = {
        "status": "evaluated",
        "run_id": run_id,
        "model_version": model_version,
        "source_code_sha256": source_code_sha256,
        "als_parameters": asdict(als_params),
        "catalog_size": catalog_size,
        "provenance": provenance,
        "candidate_policy": {
            "als_source": (
                "prior-only Module 4A recommendations with the established prior-global-popularity "
                "fallback for users without ALS factors"
            ),
            "trend_source": "global prior-frequency Top-20 proxy",
            "candidate_union": "ALS Top-20 and the same fixed prior-only trend Top-20 for each cohort user",
            "score_normalization": "per-user min-max ALS and Top-M max-normalized prior product frequency",
            "output_order": "descending unrounded hybrid score, then ascending product_id",
            "epsilon": 1e-12,
            "output_top_k": max(EVALUATION_KS),
            "lambda_values_preregistered": list(PRIOR_ONLY_HYBRID_LAMBDAS),
            "lambda_selection": "fixed sweep; official-train truth is not used to select a value",
        },
        "cold_start_summary": cold_start_summary,
        "cohorts": {},
        "interpretation_boundary": (
            "Metrics compare held-out official-train truth with prior-only recommendations. "
            "They do not establish that Hybrid or ALS is superior, and the frequency proxy "
            "is not equivalent to temporal Module 3 trends."
        ),
    }
    try:
        baseline_frames = {
            "als": als_recommendations,
            "trend_only_prior_frequency": trend_only,
            "global_popularity": global_baseline,
            "personal_frequency": personal_baseline,
        }
        for cohort_name, (cohort_users, cohort_truth) in cohorts.items():
            result["cohorts"][cohort_name] = {
                "users": cohort_users.select("user_id").distinct().count(),
                "metrics": {
                    name: evaluate_at_k(
                        frame.join(cohort_users, "user_id", "inner"),
                        cohort_truth,
                        EVALUATION_KS,
                        catalog_size=catalog_size,
                    )
                    for name, frame in baseline_frames.items()
                },
            }
        for lambda_weight in PRIOR_ONLY_HYBRID_LAMBDAS:
            method_name = f"hybrid_lambda_{lambda_weight:.1f}".replace(".", "_")
            predictions = offline_hybrid_recommendations(
                als_recommendations,
                all_users,
                trend_products,
                lambda_weight=lambda_weight,
                top_k=max(EVALUATION_KS),
            ).persist(StorageLevel.DISK_ONLY)
            try:
                for cohort_name, (cohort_users, cohort_truth) in cohorts.items():
                    result["cohorts"][cohort_name]["metrics"][method_name] = evaluate_at_k(
                        predictions.join(cohort_users, "user_id", "inner"),
                        cohort_truth,
                        EVALUATION_KS,
                        catalog_size=catalog_size,
                    )
            finally:
                predictions.unpersist(blocking=False)
        for cohort in result["cohorts"].values():
            methods = cohort["metrics"]
            personal = methods.get("personal_frequency", {})
            cohort["deltas_vs_personal_frequency"] = {
                name: {
                    k: {
                        metric: (
                            values.get(k, {}).get(metric) - personal.get(k, {}).get(metric)
                            if values.get(k, {}).get(metric) is not None
                            and personal.get(k, {}).get(metric) is not None
                            else None
                        )
                        for metric in ("precision_at_k", "recall_at_k", "map_at_k", "ndcg_at_k")
                    }
                    for k in map(str, EVALUATION_KS)
                }
                for name, values in methods.items()
                if name != "personal_frequency"
            }
        result["trend_products"] = trend_products.count()
        return result
    finally:
        trend_only.unpersist(blocking=False)
        trend_products.unpersist(blocking=False)


def run_als(
    spark,
    tables,
    prior,
    config: Config,
    run_id: str,
    model_version: str,
    input_receipt: dict,
    code_hash: str,
    output: Path,
    pilot_users=5000,
    max_trials=12,
):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started = time.perf_counter()
    interactions = build_prior_interactions(prior).persist(StorageLevel.MEMORY_AND_DISK)
    train_truth = build_train_truth(tables).cache()
    all_prior_users = prior.select("user_id").distinct().cache()
    retain_publish_frame = False
    try:
        official_train_orders = tables["orders"].where("eval_set = 'train'").select("order_id")
        split = make_internal_holdout(prior, official_train_orders)
        save_json(output / "split_summary.json", split["summary"])
        tuning = tune_als(
            split,
            config,
            pilot_users,
            max_trials,
            evidence_path=output / "als_trials.json",
        )
        if tuning["status"] != "passed" or not tuning.get("selected_params"):
            raise RuntimeError(f"ALS tuning did not select a validated model: {tuning.get('reason')}")
        spark.sparkContext.setJobGroup("module4.als.final", "Module 4 final prior-only ALS model", True)
        selected = ALSParams(**tuning["selected_params"])
        model_path = config.model_path(model_version)
        marker_path = model_path + ".module4.json"
        expected_model_metadata = {
            "model_version": model_version,
            "params": asdict(selected),
            "seed": config.seed,
            "als_blocks": config.als_blocks,
            "source_receipt_sha256": input_receipt["receipt_sha256"],
            "source_code_sha256": code_hash,
            "interaction_users": interactions.select("user_id").distinct().count(),
            "interaction_products": interactions.select("product_id").distinct().count(),
            "interaction_rows": interactions.count(),
        }
        reused = False
        if _hdfs_exists(spark, model_path):
            if not _hdfs_exists(spark, marker_path):
                raise FileExistsError(
                    f"Model path exists without Module 4 metadata; refusing to overwrite: {model_path}"
                )
            from .data import read_hdfs_json

            existing = read_hdfs_json(spark, marker_path)
            if existing != expected_model_metadata:
                raise ValueError("existing model version has different source/config/code metadata")
            model = load_model(model_path)
            reused = True
        else:
            model = fit_model(interactions, selected, config.seed, config.als_blocks)
            verification_user_ids = [
                r.user_id
                for r in interactions.select("user_id").distinct().orderBy("user_id").limit(5).collect()
            ]
            verification_users = spark.createDataFrame([(u,) for u in verification_user_ids], "user_id int")
            verification = recommend_top_k(model, verification_users, config.recommendation_k)
            try:
                before_save = verification.orderBy("user_id", "als_rank").collect()
            finally:
                verification.unpersist(blocking=False)
            save_model(model, model_path)
            _write_hdfs_json(spark, marker_path, expected_model_metadata)
            _unpersist_model(model)
            model = load_model(model_path)
            verification = recommend_top_k(model, verification_users, config.recommendation_k)
            try:
                after_load = verification.orderBy("user_id", "als_rank").collect()
            finally:
                verification.unpersist(blocking=False)
            if len(before_save) != len(after_load) or any(
                (left.user_id, left.product_id, left.als_rank) != (right.user_id, right.product_id, right.als_rank)
                or abs(left.als_score - right.als_score) > 1e-6
                for left, right in zip(before_save, after_load)
            ):
                raise ValueError("saved/reloaded ALS recommendation verification failed")
        model_report = model_summary(model, selected, config.seed, interactions, config.als_blocks)
        expected_user_count = expected_model_metadata["interaction_users"]
        expected_item_count = expected_model_metadata["interaction_products"]
        if model_report["user_factors"] != expected_user_count:
            raise ValueError("saved ALS user-factor count does not match prior interaction users")
        if model_report["product_factors"] != expected_item_count:
            raise ValueError("saved ALS item-factor count does not match prior interaction products")
        model_report.update(
            {
                **expected_model_metadata,
                "model_path": model_path,
                "reused_existing_model": reused,
                "save_load_verified": True,
                "save_load_recommendation_tolerance": 1e-6,
                "model_bytes": int(spark._jvm.org.apache.hadoop.fs.Path(model_path).getFileSystem(
                    spark.sparkContext._jsc.hadoopConfiguration()
                ).getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(model_path)).getLength()),
            }
        )
        save_json(output / "als_model.json", model_report)

        factor_users = model.userFactors.select(F.col("id").alias("user_id")).distinct().cache()
        factor_items = model.itemFactors.select(F.col("id").alias("product_id")).distinct().cache()
        missing_factors = all_prior_users.join(factor_users, "user_id", "left_anti").count()
        extra_factors = factor_users.join(all_prior_users, "user_id", "left_anti").count()
        if missing_factors or extra_factors:
            raise ValueError("ALS user factors do not exactly match prior-active users")

        truth_users = train_truth.select("user_id").distinct().cache()
        truth_items = train_truth.select("user_id", F.explode("ground_truth").alias("product_id"))
        warm_truth_users = (
            truth_users.join(factor_users, "user_id", "inner")
            .join(
                truth_items.select("user_id", "product_id").distinct()
                .join(factor_items, "product_id", "left_anti")
                .select("user_id").distinct(),
                "user_id",
                "left_anti",
            )
            .cache()
        )
        cold_truth_items = truth_items.select("product_id").distinct().join(
            factor_items, "product_id", "left_anti"
        )
        cold_truth_users = truth_items.join(cold_truth_items, "product_id").select("user_id").distinct()
        warm_truth_items = truth_items.select("product_id").distinct().join(factor_items, "product_id")
        warm_truth_users_with_item = truth_items.join(factor_items, "product_id").select(
            "user_id"
        ).distinct()
        mixed_users = warm_truth_users_with_item.join(cold_truth_users, "user_id", "inner")
        evaluation_k = max(max(EVALUATION_KS), config.recommendation_k)
        all_factor_recommendations = recommend_top_k(model, factor_users, evaluation_k)
        publish_recommendations_frame = all_factor_recommendations.where(
            F.col("als_rank") <= config.recommendation_k
        ).persist(StorageLevel.DISK_ONLY)
        publication_validation = _assert_recommendation_cohort(
            publish_recommendations_frame,
            all_prior_users,
            tables["products"],
            config.recommendation_k,
            model_report["product_factors"],
        )

        # The popularity table does not depend on the requested users, so it is
        # aggregated once over the prior interaction table and reused for every
        # cohort and subgroup below. The fallback table holds twice as many
        # products because the personal-frequency baseline pads short lists.
        popular_products = popularity_products(interactions, evaluation_k).cache()
        fallback_products = popularity_products(interactions, 2 * evaluation_k).cache()
        all_als_for_truth = all_factor_recommendations.join(truth_users, "user_id", "inner")
        no_factor_predictions = truth_users.join(
            all_als_for_truth.select("user_id").distinct(), "user_id", "left_anti"
        )
        all_als_recommendations = all_als_for_truth.unionByName(
            global_popularity(interactions, no_factor_predictions, evaluation_k, popular_products)
        ).cache()
        all_global = global_popularity(interactions, truth_users, evaluation_k, popular_products)
        all_personal = personal_frequency(
            interactions, truth_users, evaluation_k, fallback_products
        )
        catalog_size = tables["products"].select("product_id").distinct().count()
        all_truth_summary = {
            "users": truth_users.count(),
            "users_with_user_factors": truth_users.join(factor_users, "user_id").count(),
            "users_without_user_factors": truth_users.join(factor_users, "user_id", "left_anti").count(),
            "cold_truth_products": cold_truth_items.count(),
            "truth_products": truth_items.select("product_id").distinct().count(),
            "truth_products_with_item_factors": warm_truth_items.count(),
            "users_with_mixed_warm_and_cold_truth": mixed_users.count(),
        }
        cohort_reports = {
            "all_official_train_users": {
                "users": truth_users.count(),
                "definition": "all users with non-empty official train truth; cold items remain in denominator",
                "metrics": _evaluate_methods(
                    {
                        "als": all_als_recommendations,
                        "global_popularity": all_global,
                        "personal_frequency": all_personal,
                    },
                    train_truth,
                    catalog_size,
                ),
            },
            "warm_users_and_all_truth_items": {
                "users": warm_truth_users.count(),
                "definition": "user factor exists and every official train truth product has an item factor",
                "metrics": _evaluate_methods(
                    {
                        "als": all_factor_recommendations.join(warm_truth_users, "user_id", "inner"),
                        "global_popularity": global_popularity(
                            interactions, warm_truth_users, evaluation_k, popular_products
                        ),
                        "personal_frequency": personal_frequency(
                            interactions, warm_truth_users, evaluation_k, fallback_products
                        ),
                    },
                    train_truth.join(warm_truth_users, "user_id", "inner"),
                    catalog_size,
                ),
            },
        }
        truth_cohorts = split_truth_by_history(train_truth, interactions).cache()
        subgroup_reports = {}
        for subgroup, array_col in (
            ("reordered", "reordered_truth"),
            ("new_to_user", "new_to_user_truth"),
        ):
            subgroup_truth = truth_cohorts.select(
                "user_id", F.col(array_col).alias("ground_truth")
            ).where(F.size("ground_truth") > 0)
            subgroup_users = subgroup_truth.select("user_id")
            subgroup_reports[subgroup] = {
                "users": subgroup_users.count(),
                "metrics": _evaluate_methods(
                    {
                        "als": all_als_recommendations,
                        "global_popularity": global_popularity(
                            interactions, subgroup_users, evaluation_k, popular_products
                        ),
                        "personal_frequency": personal_frequency(
                            interactions, subgroup_users, evaluation_k, fallback_products
                        ),
                    },
                    subgroup_truth,
                    catalog_size,
                ),
            }
        evaluation = {
            "ground_truth": "official order_products_train, distinct product IDs per user",
            "official_test_truth_available": False,
            "cohort_exclusion": "users with empty official train truth are excluded and counted in data report",
            "ks": list(EVALUATION_KS),
            "truth_summary": all_truth_summary,
            "cohorts": cohort_reports,
            "history_subgroups": subgroup_reports,
            "als_vs_baseline_deltas": _evaluation_deltas(cohort_reports),
            "fallback": "global prior popularity for users without ALS user factors in all-user evaluation only",
        }
        hybrid_evaluation = _evaluate_prior_only_hybrid(
            interactions=interactions,
            als_recommendations=all_als_recommendations,
            train_truth=train_truth,
            all_users=truth_users,
            warm_users=warm_truth_users,
            global_baseline=all_global,
            personal_baseline=all_personal,
            catalog_size=catalog_size,
            source_receipt=input_receipt,
            cold_start_summary=all_truth_summary,
            run_id=run_id,
            model_version=model_version,
            source_code_sha256=code_hash,
            als_params=selected,
        )
        save_json(output / "hybrid_evaluation.json", hybrid_evaluation)
        if config.recommendation_k < max(EVALUATION_KS):
            evaluation["publication_k"] = config.recommendation_k
            evaluation["evaluation_k"] = evaluation_k
        mongo = {"published": False, "skipped": True}
        tuning["final_model"] = model_report
        retain_publish_frame = True
        return {
            "tuning": tuning,
            "split_summary": split["summary"],
            "evaluation": evaluation,
            "hybrid_evaluation": hybrid_evaluation,
            "model": model_report,
            "recommendation_validation": publication_validation,
            "mongo": mongo,
            "recommendations": publish_recommendations_frame,
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        }
    finally:
        clear_job_group(spark.sparkContext)
        interactions.unpersist(blocking=False)
        train_truth.unpersist(blocking=False)
        all_prior_users.unpersist(blocking=False)
        if "all_factor_recommendations" in locals():
            all_factor_recommendations.unpersist(blocking=False)
        if "all_als_recommendations" in locals():
            all_als_recommendations.unpersist(blocking=False)
        if "factor_users" in locals():
            factor_users.unpersist(blocking=False)
        if "factor_items" in locals():
            factor_items.unpersist(blocking=False)
        if "truth_users" in locals():
            truth_users.unpersist(blocking=False)
        if "warm_truth_users" in locals():
            warm_truth_users.unpersist(blocking=False)
        if "truth_cohorts" in locals():
            truth_cohorts.unpersist(blocking=False)
        if not retain_publish_frame and "publish_recommendations_frame" in locals():
            publish_recommendations_frame.unpersist(blocking=False)


def _validate_directed_edges(edges):
    directed = directed_edges(edges).cache()
    expected = edges.count() * 2
    actual = directed.count()
    if actual != expected or directed.groupBy("src", "dst").count().where("count > 1").limit(1).count():
        directed.unpersist(blocking=False)
        raise ValueError("symmetric directed edge set must contain exactly two unique rows per edge")
    directed.unpersist(blocking=False)
    return actual


def run_graph(spark, tables, prior, config: Config, run_id: str, graph_version: str):
    from pyspark.sql import functions as F

    started = time.perf_counter()
    graph_started = time.perf_counter()
    catalog_count = tables["products"].count()
    full_prior_count = prior.count()
    graph_products = tables["products"]
    graph_scope = "full_catalog_vertices_and_prior_baskets"
    selected_ids_sha256 = None
    if config.graph_top_products is not None:
        selected_products = prior.groupBy("product_id").count().orderBy(
            F.desc("count"), F.asc("product_id")
        ).limit(config.graph_top_products).select("product_id").cache()
        selected_ids_sha256 = hashlib.sha256(
            json.dumps([row.product_id for row in selected_products.orderBy("product_id").collect()]).encode()
        ).hexdigest()
        graph_products = graph_products.join(selected_products, "product_id")
        prior = prior.join(selected_products, "product_id").cache()
        graph_scope = "top_prior_popularity_product_subset"
    vertices = build_vertices(prior, graph_products).cache()
    vertex_count = vertices.count()
    basket_sizes = prior.groupBy("order_id").agg(F.countDistinct("product_id").alias("basket_size"))
    basket_quantiles = basket_sizes.approxQuantile("basket_size", [0.5, 0.9, 0.95, 0.99, 1.0], 0.001)
    pair_expansion = basket_sizes.agg(
        F.sum((F.col("basket_size") * (F.col("basket_size") - 1) / 2).cast("double")).alias("pairs")
    ).first().pairs or 0.0
    prior_fact_count = prior.count()
    spark.sparkContext.setJobGroup("module4.graph.raw_edges", "Module 4 prior-only co-purchase edges", True)
    raw_edges = build_edges(prior, min_edge_weight=1).cache()
    raw_edge_count = raw_edges.count()
    clear_job_group(spark.sparkContext)
    thresholds = sorted({2, 5, 10, config.graph_min_edge_weight})
    trials = []
    for threshold in thresholds:
        spark.sparkContext.setJobGroup(
            f"module4.graph.threshold.{threshold}",
            f"Module 4 graph threshold {threshold}",
            True,
        )
        trial_started = time.perf_counter()
        edges = raw_edges.where(F.col("weight") >= threshold).cache()
        edge_summary = validate_edges(edges)
        directed_count = _validate_directed_edges(edges)
        endpoints = edges.select(F.col("src").alias("product_id")).unionByName(
            edges.select(F.col("dst").alias("product_id"))
        ).distinct()
        active_products = endpoints.count()
        # The weighted degree table is the same frame for both measurements
        # below, and building it means a union of the edge set plus a group
        # by, so it is computed once and reused.
        degrees = weighted_degree(edges, vertices).cache()
        isolated = degrees.where("neighbor_count = 0").count()
        covered_facts = prior.select("order_id", "product_id").distinct().join(
            endpoints, "product_id", "inner"
        ).count()
        degree_quantiles = degrees.approxQuantile(
            "neighbor_count", [0.5, 0.9, 0.95, 0.99, 1.0], 0.001
        )
        weight_quantiles = edges.approxQuantile("weight", [0.5, 0.9, 0.95, 0.99, 1.0], 0.001)
        trials.append(
            {
                "min_edge_weight": threshold,
                "graph_scope": graph_scope,
                "vertex_count": vertex_count,
                **edge_summary,
                "directed_edge_count": directed_count,
                "active_product_count": active_products,
                "catalog_product_coverage": active_products / catalog_count if catalog_count else 0.0,
                "prior_fact_coverage": covered_facts / full_prior_count if full_prior_count else 0.0,
                "scope_product_coverage": active_products / vertex_count if vertex_count else 0.0,
                "isolated_vertices": isolated,
                "neighbor_count_quantiles": degree_quantiles,
                "edge_weight_quantiles": weight_quantiles,
                "runtime_ms": round((time.perf_counter() - trial_started) * 1000, 3),
                "status": "passed",
            }
        )
        edges.unpersist(blocking=False)
        clear_job_group(spark.sparkContext)
    edges = raw_edges.where(F.col("weight") >= config.graph_min_edge_weight).cache()
    validate_edges(edges)
    spark.sparkContext.setJobGroup("module4.graph.final_metrics", "Module 4 final GraphFrames metrics", True)
    metrics = build_metrics(
        vertices,
        edges,
        config.graph_reset_probability,
        config.graph_max_iter,
        backend=config.graph_backend,
    ).cache()
    validation = validate_metrics(metrics, edges, vertices)
    validation.update(
        {
            "backend": config.graph_backend,
            "reset_probability": config.graph_reset_probability,
            "max_iter": config.graph_max_iter,
            "directed_edge_count": _validate_directed_edges(edges),
            "graph_scope": graph_scope,
            "top_products_requested": config.graph_top_products,
            "selected_product_ids_sha256": selected_ids_sha256,
            "catalog_products": catalog_count,
            "catalog_scope_coverage": vertex_count / catalog_count if catalog_count else 0.0,
            "prior_facts_in_scope": prior_fact_count,
            "prior_facts_total": full_prior_count,
            "prior_fact_scope_coverage": prior_fact_count / full_prior_count if full_prior_count else 0.0,
            "raw_edge_count_at_weight_1": raw_edge_count,
            "basket_count": basket_sizes.count(),
            "basket_size_quantiles": basket_quantiles,
            "pair_expansion_count": float(pair_expansion),
            "threshold_selection": "configured prior-only threshold; no official train signal used",
        }
    )
    metrics = metrics.select("product_id", "neighbor_count", "weighted_degree", "pagerank").join(
        tables["products"].select("product_id", "product_name"), "product_id", "inner"
    ).cache()
    top20 = {
        metric: [
            row.asDict()
            for row in metrics.orderBy(F.desc(metric), F.asc("product_id")).limit(20).collect()
        ]
        for metric in ("pagerank", "weighted_degree", "neighbor_count")
    }
    graph_runtime = round((time.perf_counter() - graph_started) * 1000, 3)
    clear_job_group(spark.sparkContext)
    raw_edges.unpersist(blocking=False)
    vertices.unpersist(blocking=False)
    edges.unpersist(blocking=False)
    if config.graph_top_products is not None:
        prior.unpersist(blocking=False)
        selected_products.unpersist(blocking=False)
    return {
        "trials": trials,
        "validation": validation,
        "top20": top20,
        "graph_version": graph_version,
        "mongo": {"published": False, "skipped": True},
        "metrics": metrics,
        "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        "graph_runtime_ms": graph_runtime,
    }


def _runtime_metadata(spark):
    jar_metadata = []
    for path in sorted(Path("/opt/spark/jars").glob("graphframes*.jar")):
        digest = hashlib.sha512(path.read_bytes()).hexdigest()
        jar_metadata.append({"file": path.name, "sha512": digest})
    try:
        graphframes_python = importlib.metadata.version("graphframes")
    except importlib.metadata.PackageNotFoundError:
        graphframes_python = None
    return {
        "spark": spark.version,
        "scala": spark._jvm.scala.util.Properties.versionNumberString(),
        "java": spark._jvm.java.lang.System.getProperty("java.version"),
        "graphframes_python": graphframes_python,
        "graphframes_jars": jar_metadata,
        "spark_conf": {
            key: value
            for key, value in spark.sparkContext.getConf().getAll()
            if key.startswith(
                ("spark.sql.", "spark.executor.", "spark.driver.memory", "spark.cores.", "spark.master")
            )
        },
    }


def _mongo_preflight(config, run_id):
    from pymongo import MongoClient

    name = f"module4_preflight_{run_id}"
    promoted_name = name + "_rename"
    with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        db = client[config.mongo_database]
        try:
            db[name].insert_one({"_id": run_id, "probe": 1})
            db[name].create_index("probe", unique=True)
            db[name].rename(promoted_name)
            if db[promoted_name].count_documents({"_id": run_id}) != 1:
                raise RuntimeError("MongoDB preflight rename/read-back failed")
        finally:
            db[name].drop()
            db[promoted_name].drop()
    return {"ping": "passed", "insert_index_rename_readback": "passed"}


def _save_run_artifacts(output: Path, report: dict):
    finished = datetime.now(timezone.utc).isoformat()
    report["finished_at_utc"] = finished
    save_json(output / "summary.json", report)
    manifest = {
        **report.get("environment", {}),
        "run_id": report.get("run_id"),
        "status": report.get("status"),
        "stages": report.get("stages", {}),
        "source_receipt": report.get("validation", {}).get("source_receipt"),
        "input_source_receipt": report.get("input_source_receipt"),
        "scope": report.get("scope"),
        "acceptance_status": report.get("acceptance_status"),
        "checkpoint": report.get("checkpoint"),
        "eventlog": report.get("eventlog"),
        "error": report.get("error"),
        "started_at_utc": report.get("started_at_utc"),
        "finished_at_utc": finished,
    }
    save_json(output / "manifest.json", manifest)
    save_json(
        output / "mongo_validation.json",
        {
            "recommendations": report.get("als", {}).get("mongo", {}),
            "graph": report.get("graph", {}).get("mongo", {}),
            "pair_readiness": report.get("pair_readiness"),
        },
    )
    save_json(
        output / "performance.json",
        {
            "stages": report.get("stage_timings_ms", {}),
            "als_ms": report.get("als", {}).get("duration_ms"),
            "graph_ms": report.get("graph", {}).get("duration_ms"),
            "spark_job_groups": report.get("spark_event_metrics", {}),
        },
    )
    (output / "environment.txt").write_text(
        json.dumps(report.get("environment", {}), indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    if not (output / "test-results.txt").exists():
        (output / "test-results.txt").write_text(
            "Tests were not run by the Module 4 CLI. See the runbook for the fixture/runtime commands.\n",
            encoding="utf-8",
        )
    (output / "README.md").write_text(
        f"# Module 4 evidence: {report.get('run_id')}\n\n"
        f"Status: **{report.get('status')}**.\n\n"
        f"Scope: `{report.get('scope', 'unknown')}`. Serving: `"
        f"{report.get('serving_status', 'not_run')}`.\n",
        encoding="utf-8",
    )


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["validate", "als", "graph", "all", "user-features"])
    parser.add_argument("--root")
    parser.add_argument("--master")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--recommendation-k", type=int)
    parser.add_argument("--graph-min-edge-weight", type=int)
    parser.add_argument("--graph-reset-probability", type=float)
    parser.add_argument("--graph-max-iter", type=int)
    parser.add_argument("--graph-backend", choices=["graphframes", "dataframe"])
    parser.add_argument("--graph-top-products", type=int)
    parser.add_argument("--executor-memory")
    parser.add_argument("--executor-cores", type=int)
    parser.add_argument("--total-cores", type=int)
    parser.add_argument("--als-blocks", type=int)
    parser.add_argument("--pilot-users", type=int, default=5000)
    parser.add_argument("--max-trials", type=int, default=12)
    parser.add_argument("--output", type=Path, default=Path("results/module4"))
    parser.add_argument("--scope", choices=["staged_curated", "staged_fixture", "full_data"],
                        default="staged_curated")
    parser.add_argument("--source-receipt", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--user-id", type=int, help="User ID for the descriptive user-features command")
    parser.add_argument("--model-version")
    parser.add_argument("--graph-version")
    parser.add_argument("--skip-serving", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    overrides = {
        "root": args.root,
        "master": args.master,
        "shuffle_partitions": args.shuffle_partitions,
        "seed": args.seed,
        "recommendation_k": args.recommendation_k,
        "graph_min_edge_weight": args.graph_min_edge_weight,
        "graph_reset_probability": args.graph_reset_probability,
        "graph_max_iter": args.graph_max_iter,
        "graph_backend": args.graph_backend,
        "graph_top_products": args.graph_top_products,
        "executor_memory": args.executor_memory,
        "executor_cores": args.executor_cores,
        "total_cores": args.total_cores,
        "als_blocks": args.als_blocks,
    }
    config = Config.environment(**{key: value for key, value in overrides.items() if value is not None})
    if config.graph_backend == "dataframe" and args.scope != "staged_fixture":
        raise ValueError("the dataframe graph backend is restricted to explicit staged_fixture scope")
    if args.scope == "full_data" and args.source_receipt is None:
        raise ValueError("full_data scope requires --source-receipt from the verified source download")
    if args.pilot_users < 1:
        raise ValueError("pilot_users must be positive")
    if not 1 <= args.max_trials <= 12:
        raise ValueError("max_trials must be between 1 and 12")
    if args.command == "user-features":
        if args.user_id is None or args.user_id <= 0:
            raise ValueError("user-features requires a positive --user-id")
        inspection_id = args.run_id or (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
        )
        inspection_id = _valid_version(inspection_id, "run_id")
        output = args.output / "user-features" / inspection_id
        output.mkdir(parents=True, exist_ok=False)
        started_at = datetime.now(timezone.utc).isoformat()
        spark = None
        try:
            spark = session(config, output / "eventlog")
            result = read_user_features(spark, config, args.user_id)
            report = {
                "inspection_id": inspection_id,
                "status": result["status"],
                "scope": result["scope"],
                "observed_at_utc": started_at,
                "module2_source": result["source"],
                "user_id": result["user_id"],
                "features": result["features"],
                "model_input": False,
                "module4_source_code_sha256": source_digest(),
                "command": ["module4", "user-features", "--user-id", str(args.user_id)],
            }
            save_json(output / "user_features.json", report)
            print(f"[USER FEATURES] {output.resolve()}", flush=True)
            return 0
        finally:
            if spark is not None:
                spark.stop()
    run_id = _valid_version(args.run_id or new_run_id(), "run_id")
    model_version = _valid_version(args.model_version or f"als-{run_id}", "model_version")
    graph_version = _valid_version(args.graph_version or f"graph-{run_id}", "graph_version")
    output = args.output / run_id
    output.mkdir(parents=True, exist_ok=False)
    started_at = datetime.now(timezone.utc).isoformat()
    code_hash = source_digest()
    report = {
        "run_id": run_id,
        "status": "running",
        "scope": args.scope,
        "serving_status": "skipped" if args.skip_serving else "pending",
        "started_at_utc": started_at,
        "stages": {"input_validation": "pending"},
        "stage_timings_ms": {},
        "environment": {
            **revision(),
            "source_code_sha256": code_hash,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "timestamp_utc": started_at,
            "config": _safe_config(config),
            "model_version": model_version,
            "graph_version": graph_version,
            "command": ["module4", *(argv if argv is not None else os.sys.argv[1:])],
        },
    }
    save_json(output / "manifest.json", report["environment"] | {"run_id": run_id, "status": "running"})
    spark = None
    als_result = None
    graph_result = None
    current_stage = "input_validation"
    exit_code = 0
    try:
        spark = session(config, output / "eventlog")
        checkpoint_path = config.root.rstrip("/") + "/checkpoints/module4/" + run_id
        spark.sparkContext.setCheckpointDir(checkpoint_path)
        report["checkpoint"] = {"path": checkpoint_path, "ownership": run_id, "retention": "retained for audit"}
        report["environment"].update(_runtime_metadata(spark))
        report["preflight"] = (
            {"mongo": "skipped_by_request_or_validation_only"}
            if args.skip_serving or args.command == "validate"
            else {"mongo": _mongo_preflight(config, run_id)}
        )
        input_started = time.perf_counter()
        tables = read_inputs(spark, config)
        spark.sparkContext.setJobGroup(
            "module4.input_validation", "Module 4 curated input validation", True
        )
        try:
            validation = validate_inputs(tables)
        finally:
            clear_job_group(spark.sparkContext)
        source_receipt = validate_source_receipts(spark, config, tables, verify_counts=True)
        if args.scope == "full_data":
            source_bytes = args.source_receipt.read_bytes()
            download_receipt = json.loads(source_bytes)
            for name, csv_name in {"orders": "orders.csv", "prior": "order_products__prior.csv",
                                   "train": "order_products__train.csv", "products": "products.csv"}.items():
                if download_receipt["files"][csv_name]["rows"] != validation["counts"][name]:
                    raise ValueError(f"full-data source/staged row count mismatch for {name}")
            report["input_source_receipt"] = {
                "file": str(args.source_receipt), "sha256": hashlib.sha256(source_bytes).hexdigest(),
                "receipt": download_receipt,
            }
        validation["schema"] = {name: frame.schema.simpleString() for name, frame in tables.items()}
        validation["source_receipt"] = source_receipt
        report["validation"] = validation
        report["stages"]["input_validation"] = "passed"
        report["stage_timings_ms"]["input_validation"] = round(
            (time.perf_counter() - input_started) * 1000, 3
        )
        save_json(output / "data_validation.json", validation)
        if args.command == "validate":
            report.update(
                status="passed",
                acceptance_status="input_contract_validated",
                serving_status="not_applicable_validation_only",
                pair_readiness={"ready": False, "status": "not_requested"},
            )
        else:
            from pyspark import StorageLevel

            prior = enrich_prior(tables).persist(StorageLevel.DISK_ONLY)
            if args.command in {"als", "all"}:
                current_stage = "als_training_and_evaluation"
                stage_started = time.perf_counter()
                als_result = run_als(
                    spark,
                    tables,
                    prior,
                    config,
                    run_id,
                    model_version,
                    source_receipt,
                    code_hash,
                    output,
                    args.pilot_users,
                    args.max_trials,
                )
                report["als"] = {
                    key: value
                    for key, value in als_result.items()
                    if key != "recommendations"
                }
                report["stages"]["als_compute"] = "passed"
                report["stage_timings_ms"]["als_compute"] = round(
                    (time.perf_counter() - stage_started) * 1000, 3
                )
                save_json(output / "split_summary.json", als_result["split_summary"])
                save_json(output / "als_trials.json", als_result["tuning"])
                save_json(output / "evaluation.json", als_result["evaluation"])
                save_json(output / "als_model.json", als_result["model"])
                save_json(
                    output / "recommendation_validation.json", als_result["recommendation_validation"]
                )
            if args.command in {"graph", "all"}:
                current_stage = "graph_build_and_metrics"
                stage_started = time.perf_counter()
                graph_result = run_graph(spark, tables, prior, config, run_id, graph_version)
                report["graph"] = {
                    key: value for key, value in graph_result.items() if key != "metrics"
                }
                report["stages"]["graph_compute"] = "passed"
                report["stage_timings_ms"]["graph_compute"] = round(
                    (time.perf_counter() - stage_started) * 1000, 3
                )
                save_json(output / "graph_trials.json", graph_result["trials"])
                save_json(output / "graph_validation.json", graph_result["validation"])
                save_json(
                    output / "graph_metrics.json",
                    {"validation": graph_result["validation"], "top20": graph_result["top20"]},
                )

            current_stage = "source_stability_check"
            report["stages"][current_stage] = "pending"
            final_receipt = validate_source_receipts(spark, config, tables, verify_counts=False)
            if final_receipt["receipt_sha256"] != source_receipt["receipt_sha256"] or {
                key: value["sha256"] for key, value in final_receipt["inputs"].items()
            } != {key: value["sha256"] for key, value in source_receipt["inputs"].items()}:
                raise RuntimeError("curated source files changed during Module 4 computation")
            report["source_stability_check"] = "passed"
            report["stages"][current_stage] = "passed"
            if args.skip_serving:
                report["serving_status"] = "skipped_by_request"
                report["pair_readiness"] = {
                    "ready": False,
                    "status": "compute_only_serving_skipped",
                    "run_id": run_id,
                }
                report["stages"]["serving"] = "skipped"
                report.update(status="compute_passed", acceptance_status="serving_and_full_data_pending")
            else:
                expected_publications = [als_result is not None, graph_result is not None]
                recommendation_published = False
                graph_published = False
                both_requested = all(expected_publications)
                with publication_lock(config, run_id) as mongo_client:
                    update_pair_readiness(
                        mongo_client,
                        config,
                        run_id,
                        model_version if als_result is not None else None,
                        graph_version if graph_result is not None else None,
                        "publishing",
                        False,
                        False,
                    )
                    try:
                        if als_result is not None:
                            current_stage = "recommendation_publication"
                            stage_started = time.perf_counter()
                            spark.sparkContext.setJobGroup(
                                "module4.mongo.recommendations",
                                "Module 4 Mongo recommendation validation",
                                True,
                            )
                            try:
                                als_mongo = publish_recommendations(
                                    als_result["recommendations"],
                                    config,
                                    model_version,
                                    run_id,
                                    client=mongo_client,
                                )
                            finally:
                                clear_job_group(spark.sparkContext)
                            als_mongo["published"] = True
                            als_result["mongo"] = als_mongo
                            report["als"]["mongo"] = als_mongo
                            recommendation_published = True
                            report["stages"]["recommendation_publication"] = "passed"
                            report["stage_timings_ms"]["recommendation_publication"] = round(
                                (time.perf_counter() - stage_started) * 1000, 3
                            )
                        if graph_result is not None:
                            current_stage = "graph_publication"
                            stage_started = time.perf_counter()
                            spark.sparkContext.setJobGroup(
                                "module4.mongo.graph", "Module 4 Mongo graph validation", True
                            )
                            try:
                                graph_mongo = publish_graph_metrics(
                                    graph_result["metrics"],
                                    config,
                                    graph_version,
                                    run_id,
                                    config.graph_min_edge_weight,
                                    client=mongo_client,
                                )
                            finally:
                                clear_job_group(spark.sparkContext)
                            graph_mongo["published"] = True
                            graph_result["mongo"] = graph_mongo
                            report["graph"]["mongo"] = graph_mongo
                            graph_published = True
                            report["stages"]["graph_publication"] = "passed"
                            report["stage_timings_ms"]["graph_publication"] = round(
                                (time.perf_counter() - stage_started) * 1000, 3
                            )
                    except Exception as publication_error:
                        update_pair_readiness(
                            mongo_client,
                            config,
                            run_id,
                            model_version if als_result is not None else None,
                            graph_version if graph_result is not None else None,
                            "failed",
                            recommendation_published,
                            graph_published,
                            f"{type(publication_error).__name__}: {publication_error}",
                        )
                        report["pair_readiness"] = {
                            "ready": False,
                            "status": "failed_or_partial_publication",
                            "run_id": run_id,
                            "recommendation_published": recommendation_published,
                            "graph_published": graph_published,
                            "readiness_collection": "module4_snapshot_readiness",
                        }
                        raise
                    both_published = recommendation_published and graph_published and both_requested
                    readiness_status = "ready" if both_published else "single_snapshot_ready"
                    readiness = update_pair_readiness(
                        mongo_client,
                        config,
                        run_id,
                        model_version if als_result is not None else None,
                        graph_version if graph_result is not None else None,
                        readiness_status,
                        recommendation_published,
                        graph_published,
                    )
                report["pair_readiness"] = {
                    "ready": both_published,
                    "single_snapshot_ready": any((recommendation_published, graph_published))
                    and not both_requested,
                    "status": "both_snapshots_validated" if both_published else "single_snapshot_validated",
                    "readiness_collection": "module4_snapshot_readiness",
                    "readiness_document": readiness,
                    "run_id": run_id,
                    "model_version": model_version if als_result is not None else None,
                    "graph_version": graph_version if graph_result is not None else None,
                    "recommendation_published": recommendation_published,
                    "graph_published": graph_published,
                    "single_writer_policy": "module4_publication_lock",
                    "pair_rename_is_atomic": False,
                }
                report["stages"]["serving"] = "passed"
                report["serving_status"] = "published_and_validated"
                report.update(
                    status="passed",
                    acceptance_status=(
                        "full_data_pair_validated" if args.scope == "full_data" and both_published
                        else "run_passed_full_data_acceptance_not_claimed"
                    ),
                )
        save_json(output / "summary.json", report)
        print(f"[ARTIFACTS] {output.resolve()}", flush=True)
    except Exception as exc:
        exit_code = 1
        report.update(
            status="failed",
            failed_stage=current_stage,
            error=f"{type(exc).__name__}: {_redact_sensitive_text(str(exc))}",
            traceback=_redact_sensitive_text(traceback.format_exc()),
        )
        report["stages"][current_stage] = "failed"
        if als_result is not None and als_result.get("mongo", {}).get("published"):
            report["partial_publication"] = {"recommendations": "published", "graph": "not_confirmed"}
        report["pair_readiness"] = {
            "ready": False,
            "status": "failed_or_partial_publication",
            "run_id": run_id,
            "recommendation_published": bool(
                als_result and als_result.get("mongo", {}).get("published")
            ),
            "graph_published": bool(graph_result and graph_result.get("mongo", {}).get("published")),
        }
        print(f"[FAILED] {current_stage}: {type(exc).__name__}: {_redact_sensitive_text(str(exc))}", flush=True)
    finally:
        if "prior" in locals():
            try:
                prior.unpersist(blocking=False)
            except Exception as cleanup_error:
                report.setdefault("cleanup_errors", []).append(
                    f"prior: {type(cleanup_error).__name__}: {cleanup_error}"
                )
        for result, name in ((als_result, "recommendations"), (graph_result, "metrics")):
            if result and result.get(name) is not None:
                try:
                    result[name].unpersist(blocking=False)
                except Exception as cleanup_error:
                    report.setdefault("cleanup_errors", []).append(
                        f"{name}: {type(cleanup_error).__name__}: {cleanup_error}"
                    )
        if spark is not None:
            try:
                spark.stop()
            except Exception as stop_error:
                report.setdefault("cleanup_errors", []).append(
                    f"Spark stop: {type(stop_error).__name__}: {stop_error}"
                )
                if report.get("status") != "failed":
                    report.update(status="failed", error="Spark session failed to stop cleanly")
                    exit_code = 1
            eventlog_directory = output / "eventlog"
            if eventlog_directory.exists():
                try:
                    from src.module2.eventlog import parse_event_logs

                    event_metrics = parse_event_logs(eventlog_directory)
                    report["spark_event_metrics"] = event_metrics
                    report["eventlog"] = {
                        "status": "parsed",
                        "directory": str(eventlog_directory.resolve()),
                        "groups": sorted(event_metrics),
                    }
                    save_json(output / "spark_event_metrics.json", event_metrics)
                except Exception as event_error:
                    report["eventlog"] = {
                        "status": "parse_failed",
                        "directory": str(eventlog_directory.resolve()),
                        "error": f"{type(event_error).__name__}: {event_error}",
                    }
                    report.setdefault("evidence_warnings", []).append(
                        f"Spark event log metrics could not be parsed: {event_error}"
                    )
                    if report.get("status") in {"passed", "compute_passed"}:
                        report.update(status="failed", failed_stage="event_log_validation")
                        exit_code = 1
            else:
                report["eventlog"] = {"status": "missing", "directory": str(eventlog_directory)}
        _save_run_artifacts(output, report)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
