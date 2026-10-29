"""Deterministic contracts for the pilot/full internal ALS experiment."""

from dataclasses import asdict
from itertools import count
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from src.module4 import tuning
from src.module4.als import ALSParams
from src.module4.config import Config


def test_trial_grid_preserves_order_parameters_and_prefixes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    grid = tuning._trial_grid()
    assert [(p.rank, p.reg_param, p.alpha, p.max_iter, p.strength_col) for p in grid] == [
        (32, 0.1, 10.0, 10, "strength_raw"),
        (16, 0.01, 10.0, 5, "strength_raw"),
        (16, 0.05, 10.0, 10, "strength_raw"),
        (32, 0.05, 10.0, 10, "strength_raw"),
        (32, 0.1, 40.0, 10, "strength_raw"),
        (64, 0.1, 10.0, 10, "strength_raw"),
        (16, 0.1, 10.0, 15, "strength_log"),
        (32, 0.05, 40.0, 10, "strength_log"),
        (64, 0.1, 40.0, 10, "strength_log"),
        (16, 0.01, 40.0, 10, "strength_log"),
        (32, 0.1, 10.0, 5, "strength_raw"),
        (64, 0.05, 10.0, 5, "strength_raw"),
    ]
    for limit in (1, 3, 12):
        assert tuning._trial_grid(limit) == grid[:limit]


def test_yaml_trial_is_first_and_duplicate_candidates_are_removed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config/pipeline.yaml").write_text(
        "models:\n  als:\n    rank: 16\n    max_iter: 5\n    reg_param: 0.01\n    alpha: 10\n"
    )
    grid = tuning._trial_grid()
    assert grid[0] == ALSParams(rank=16, max_iter=5, reg_param=0.01)
    assert len(grid) == 11
    assert len({tuple(asdict(params).items()) for params in grid}) == 11
    assert grid[1] == ALSParams(rank=16, reg_param=0.05)


def test_trial_order_prioritizes_recall_precision_runtime_then_input_order():
    trials = [
        {"id": 1, "metrics": {"recall_at_k": 0.2, "precision_at_k": 0.9}, "total_runtime_ms": 1},
        {"id": 2, "metrics": {"recall_at_k": 0.3, "precision_at_k": 0.1}, "total_runtime_ms": 2},
        {"id": 3, "metrics": {"recall_at_k": 0.3, "precision_at_k": 0.1}, "total_runtime_ms": 1},
        {"id": 4, "metrics": {"recall_at_k": 0.3, "precision_at_k": 0.1}, "total_runtime_ms": 1},
        {"id": 5, "metrics": None},
    ]
    assert [trial["id"] for trial in sorted(trials, key=tuning._trial_order, reverse=True)] == [3, 4, 2, 1, 5]
    assert tuning._trial_order({}) == (-1.0, -1.0, float("-inf"))


@pytest.fixture
def experiment(monkeypatch, tmp_path):
    cleanup = []

    def frame(name, rows):
        result = MagicMock(name=name)
        for method in ("persist", "cache", "select", "distinct"):
            getattr(result, method).return_value = result
        result.count.return_value = rows
        result.unpersist.side_effect = lambda **kwargs: cleanup.append(name)
        return result

    training = frame("training", 20)
    truth = frame("truth", 4)
    users = frame("users", 2)
    pilot_training = frame("pilot_training", 10)
    pilot_truth = frame("pilot_truth", 2)
    full_users = frame("full_users", 4)
    training.join.return_value = pilot_training
    truth.join.return_value = pilot_truth
    truth.select.return_value = full_users
    params = [ALSParams(rank=rank) for rank in (16, 32, 64, 8)]
    builds = MagicMock(return_value=training)
    monkeypatch.setattr(tuning, "build_prior_interactions", builds)
    sampling = MagicMock(return_value=users)
    monkeypatch.setattr(tuning, "_sample_users", sampling)
    monkeypatch.setattr(tuning, "_trial_grid", lambda limit: params[:limit])
    ticks = count()
    monkeypatch.setattr(tuning, "time", SimpleNamespace(perf_counter=lambda: next(ticks) / 1000))
    models, predictions = [], []

    def fit(source, selected, seed, blocks):
        model = MagicMock()
        model.source = source
        model.params = selected
        model.userFactors = frame(f"user_factors:{len(models)}", 2)
        model.itemFactors = frame(f"item_factors:{len(models)}", 4)
        models.append(model)
        return model

    fit_model = MagicMock(side_effect=fit)
    monkeypatch.setattr(tuning, "fit_model", fit_model)

    def recommend(model, requested, k):
        assert k == 10
        assert requested is (users if model.source is pilot_training else full_users)
        prediction = frame(f"prediction:{len(predictions)}", 4)
        scores = (
            {16: (0.1, 0.9), 32: (0.4, 0.2), 64: (0.4, 0.3), 8: (0.4, 0.2)}
            if model.source is pilot_training else {64: (0.3, 0.2), 32: (0.5, 0.1), 8: (0.5, 0.4)}
        )
        recall, precision = scores[model.params.rank]
        prediction.metrics = {"recall_at_k": recall, "precision_at_k": precision}
        predictions.append(prediction)
        return prediction

    recommendations = MagicMock(side_effect=recommend)
    monkeypatch.setattr(tuning, "recommend_top_k", recommendations)
    baseline = frame("baseline", 4)
    baseline.metrics = {"recall_at_k": 0.05, "precision_at_k": 0.01}
    global_baseline = MagicMock(return_value=baseline)
    monkeypatch.setattr(tuning, "global_popularity", global_baseline)
    monkeypatch.setattr(tuning, "personal_frequency", MagicMock(return_value=baseline))

    def metrics(prediction, actual_truth, k):
        assert actual_truth is (pilot_truth if len(models) <= 4 else truth) or prediction is baseline
        assert k == 10
        return prediction.metrics

    ranking = MagicMock(side_effect=metrics)
    monkeypatch.setattr(tuning, "ranking_metrics", ranking)
    return SimpleNamespace(
        split={"training": object(), "validation_truth": truth}, config=Config(seed=7),
        path=tmp_path / "trials.json", params=params, training=training, truth=truth, users=users,
        pilot_training=pilot_training, pilot_truth=pilot_truth, full_users=full_users,
        models=models, predictions=predictions, fit=fit_model, recommend=recommendations,
        metrics=ranking, baseline=baseline, global_baseline=global_baseline,
        builds=builds, sampling=sampling, cleanup=cleanup,
    )


def run(experiment):
    return tuning.tune_als(experiment.split, experiment.config, 2, 4, evidence_path=experiment.path)


def test_tuning_selects_full_holdout_winner_and_preserves_evidence_and_cleanup(experiment):
    result = run(experiment)
    assert result["status"] == "passed"
    assert result["shortlist_trial_ids"] == [3, 2, 4]
    assert result["selected_trial_id"] == 3
    assert result["selected_pilot_trial_id"] == 4
    assert result["selected_params"] == asdict(experiment.params[3])
    assert result["selection_metric"] == "full internal Recall@10, then Precision@10, then runtime"
    assert result["official_train_used"] is False
    assert result["pilot_users_actual"] == 2
    assert result["internal_training_interactions"] == 20
    assert result["pilot_training_interactions"] == 10
    assert result["pilot_sampling"] == {"method": "xxhash64(user_id, seed), then user_id", "seed": 7}
    assert json.loads(experiment.path.read_text()) == result
    experiment.builds.assert_called_once_with(experiment.split["training"])
    experiment.sampling.assert_called_once_with(experiment.truth, 2, 7)
    assert experiment.fit.call_args_list == [
        call(experiment.pilot_training, params, 7, 4) for params in experiment.params
    ] + [call(experiment.training, experiment.params[index], 7, 4) for index in (2, 1, 3)]
    context = experiment.pilot_training.sparkSession.sparkContext
    assert [args.args[0] for args in context.setJobGroup.call_args_list] == [
        "module4.als.pilot_baseline", *[f"module4.als.pilot.{i}" for i in range(1, 5)],
        "module4.als.full_baseline", *[f"module4.als.full_validation.{i}" for i in range(1, 4)],
    ]
    assert context.setLocalProperty.call_args_list == [
        call(name, None)
        for _ in range(9)
        for name in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
    ]
    for index, prediction in enumerate(experiment.predictions):
        prediction.unpersist.assert_called_once_with(blocking=False)
        experiment.models[index].userFactors.unpersist.assert_called_once_with(blocking=False)
        experiment.models[index].itemFactors.unpersist.assert_called_once_with(blocking=False)
    assert experiment.cleanup == [
        name for index in range(7)
        for name in (f"prediction:{index}", f"user_factors:{index}", f"item_factors:{index}")
    ] + ["full_users", "users", "pilot_training", "pilot_truth", "truth", "training"]
    experiment.training.sparkSession.stop.assert_not_called()


@pytest.mark.parametrize("scenario,status,reason", [
    ("empty", "not_performed", "no user has at least two prior orders; internal holdout is empty"),
    ("pilot", "failed", "all pilot ALS trials failed"),
    ("full", "failed", "all full-size internal validation trials failed"),
    ("incomplete", "incomplete", None),
])
def test_tuning_failure_and_empty_states_preserve_selection_and_owned_cache_cleanup(experiment, scenario, status, reason):
    original_fit = experiment.fit.side_effect
    if scenario == "empty":
        experiment.users.count.return_value = 0
    else:
        def fit(source, params, seed, blocks):
            if scenario == "pilot" or source is experiment.training and (
                scenario == "full" or params.rank == 32
            ):
                raise RuntimeError("synthetic fit failure")
            return original_fit(source, params, seed, blocks)
        experiment.fit.side_effect = fit
    result = run(experiment)
    assert result["status"] == status
    if reason is not None:
        assert result["reason"] == reason
        assert result["selected_params"] is None
    else:
        assert result["selected_params"] == asdict(experiment.params[3])
        assert (result["shortlist_requested"], result["shortlist_passed"]) == (3, 2)
    assert json.loads(experiment.path.read_text()) == result
    assert experiment.cleanup[-5:] == ["users", "pilot_training", "pilot_truth", "truth", "training"]
    if scenario in {"empty", "pilot"}:
        experiment.full_users.unpersist.assert_not_called()
    else:
        experiment.full_users.unpersist.assert_called_once_with(blocking=False)
    if scenario == "empty":
        experiment.fit.assert_not_called()


@pytest.mark.parametrize("phase", ["pilot", "full"])
def test_fatal_baseline_error_preserves_exception_and_releases_experiment_frames(experiment, phase):
    primary = RuntimeError("synthetic baseline failure")
    experiment.global_baseline.side_effect = (
        primary if phase == "pilot" else [experiment.baseline, primary]
    )
    with pytest.raises(RuntimeError) as error:
        run(experiment)
    assert error.value is primary
    evidence = json.loads(experiment.path.read_text())
    assert evidence["status"] == "failed"
    assert evidence["reason"] == "RuntimeError: synthetic baseline failure"
    assert experiment.cleanup[-5:] == ["users", "pilot_training", "pilot_truth", "truth", "training"]
    if phase == "full":
        experiment.full_users.unpersist.assert_called_once_with(blocking=False)


@pytest.mark.parametrize("options,message", [
    ({"pilot_users": 0}, "pilot_users must be positive"),
    ({"max_trials": 0}, "max_trials must be between 1 and 12"),
])
def test_invalid_experiment_limits_fail_before_data_processing(experiment, options, message):
    with pytest.raises(ValueError) as error:
        tuning.tune_als(experiment.split, experiment.config, **options)
    assert str(error.value) == message
    experiment.builds.assert_not_called()
