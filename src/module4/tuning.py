"""Seeded pilot/full-holdout ALS experiments and deterministic model selection."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import time
import traceback

from ..common.runtime import (
    config_section,
    read_yaml_mapping,
    redact_sensitive_text,
    safe_error_text,
    save_redacted_json,
)
from ..common.spark import clear_job_group
from .als import ALSParams, fit_model, recommend_top_k
from .config import Config
from .data import build_prior_interactions
from .evaluation import global_popularity, personal_frequency, ranking_metrics


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
    custom_params = None
    pipeline_cfg = Path("config/pipeline.yaml")
    if pipeline_cfg.exists():
        als_cfg = config_section(read_yaml_mapping(pipeline_cfg), "models", "als")
        if set(als_cfg) - {"rank", "max_iter", "reg_param", "alpha", "strength_col", "recommendation_k", "pilot_users", "max_trials"}:
            raise ValueError("models.als contains unsupported settings")
        if als_cfg:
            integer_params = {}
            for name, default in (("rank", 32), ("max_iter", 10)):
                value = als_cfg.get(name, default)
                if isinstance(value, bool) or isinstance(value, float) and not value.is_integer():
                    raise ValueError(f"models.als.{name} must be an integer")
                try:
                    integer_params[name] = int(value)
                except (TypeError, ValueError, OverflowError):
                    raise ValueError(f"models.als.{name} must be an integer") from None
            for name in ("reg_param", "alpha"):
                if isinstance(als_cfg.get(name), bool):
                    raise ValueError(f"models.als.{name} must be a finite number")
            try:
                reg_param = float(als_cfg.get("reg_param", 0.1))
                alpha = float(als_cfg.get("alpha", 10.0))
            except (TypeError, ValueError, OverflowError):
                raise ValueError("models.als reg_param and alpha must be finite numbers") from None
            custom_params = ALSParams(
                **integer_params, reg_param=reg_param, alpha=alpha,
                strength_col=str(als_cfg.get("strength_col", "strength_raw")),
            )

    candidates = [
        custom_params or ALSParams(),
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
    trial_params = _trial_grid(max_trials)
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
            save_redacted_json(evidence_path, state)

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
        for trial_id, params in enumerate(trial_params, 1):
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
                trial["error"] = safe_error_text(exc)
                trial["traceback"] = redact_sensitive_text(traceback.format_exc())
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
                trial["error"] = safe_error_text(exc)
                trial["traceback"] = redact_sensitive_text(traceback.format_exc())
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
            reason=safe_error_text(exc),
            traceback=redact_sensitive_text(traceback.format_exc()),
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
