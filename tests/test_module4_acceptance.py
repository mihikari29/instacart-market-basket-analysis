"""Ensure the full-data operational checker rejects incomplete and changed replays."""

from copy import deepcopy

import pytest

from scripts.check_module4_run import compare_replay, snapshot_digest, validate_report


def test_snapshot_digest_ignores_only_run_metadata_and_tolerated_float_noise():
    first = [{"_id": "m__1__2", "als_rank": 1, "als_score": 0.12345678, "run_id": "a"}]
    replay = [{**first[0], "run_id": "b", "generated_at": "later", "als_score": 0.12345679}]
    assert snapshot_digest(first) == snapshot_digest(replay)
    assert snapshot_digest(first) != snapshot_digest([{**first[0], "als_rank": 2}])
    assert snapshot_digest(first) != snapshot_digest([{**first[0], "_id": "m__1__3"}])
    with pytest.raises(ValueError, match="non-finite"):
        snapshot_digest([{**first[0], "als_score": float("inf")}])


@pytest.mark.parametrize("scope,status,serving", [
    ("staged_fixture", "passed", "published_and_validated"),
    ("full_data", "compute_passed", "skipped_by_request"),
    ("full_data", "failed", "pending"),
])
def test_acceptance_rejects_fixture_compute_only_and_failed_run(scope, status, serving):
    with pytest.raises(ValueError):
        validate_report({"scope": scope, "status": status, "serving_status": serving})


def test_acceptance_requires_source_receipt_stability_readiness_and_real_graphframes():
    report = dict(status="passed", scope="full_data", acceptance_status="full_data_pair_validated",
                  serving_status="published_and_validated", pair_readiness={"ready": True},
                  source_stability_check="passed", input_source_receipt={"sha256": "source"},
                  graph={"validation": {"backend": "graphframes"}})
    validate_report(report)
    for field, changed in (("input_source_receipt", {}), ("source_stability_check", "pending"),
                           ("pair_readiness", {"ready": False}),
                           ("graph", {"validation": {"backend": "dataframe"}})):
        with pytest.raises(ValueError):
            validate_report({**report, field: changed})


def test_replay_requires_same_versions_source_config_and_logical_snapshot():
    first = dict(source_code_sha256="code", source_receipt_sha256="input", config={"seed": 42},
                 model_version="m", graph_version="g", run_id="first", reused_existing_model=False,
                 snapshots={"recs": {"logical_sha256": "rows", "documents": 20}})
    replay = {**first, "run_id": "replay", "reused_existing_model": True}
    compare_replay(replay, first)
    for field, changed in (("model_version", "other"), ("source_code_sha256", "other"),
                           ("config", {"seed": 17}), ("reused_existing_model", False),
                           ("snapshots", {}), ("run_id", "first")):
        invalid = deepcopy(replay)
        invalid[field] = changed
        with pytest.raises(ValueError):
            compare_replay(invalid, first)
