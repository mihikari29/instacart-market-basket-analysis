"""Offline contracts for the descriptive Module 2 user-feature consumer."""

import pytest

from src.module4.feature_reader import (
    USER_FEATURE_COLUMNS,
    inspect_user_feature_rows,
    validate_user_feature_columns,
)


def _feature_row(**overrides):
    row = {
        "user_id": 71,
        "order_count": 12,
        "purchase_count": 54,
        "unique_products": 27,
        "reorder_rate": 0.63,
        "avg_basket_size": 4.5,
        "module2_run_id": "20261017T100000_ab12cd34",
        "feature_scope": "prior_only",
        "generated_at_utc": "2026-10-17T10:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_user_feature_contract_is_exact_and_prior_only():
    validate_user_feature_columns(sorted(USER_FEATURE_COLUMNS))
    with pytest.raises(ValueError, match="unexpected fields.*train_label"):
        validate_user_feature_columns([*USER_FEATURE_COLUMNS, "train_label"])
    with pytest.raises(ValueError, match="missing fields.*module2_run_id"):
        validate_user_feature_columns(sorted(USER_FEATURE_COLUMNS - {"module2_run_id"}))


def test_descriptive_reader_returns_lineage_without_creating_model_input():
    result = inspect_user_feature_rows([_feature_row()], 71)
    assert result["status"] == "available"
    assert result["scope"] == "prior_only descriptive features; not ALS model input"
    assert result["source"]["module2_run_id"] == "20261017T100000_ab12cd34"
    assert result["features"]["purchase_count"] == 54


def test_descriptive_reader_distinguishes_missing_prior_activity_from_invalid_rows():
    missing = inspect_user_feature_rows([], 71)
    assert missing["status"] == "no_prior_activity"
    assert missing["features"] is None
    with pytest.raises(ValueError, match="duplicate rows"):
        inspect_user_feature_rows([_feature_row(), _feature_row()], 71)
    with pytest.raises(ValueError, match="different user_id"):
        inspect_user_feature_rows([_feature_row(user_id=72)], 71)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("feature_scope", "mixed_prior_and_train"),
        ("reorder_rate", 1.1),
        ("avg_basket_size", 0),
        ("purchase_count", 0),
        ("generated_at_utc", "2026-10-17T10:00:00"),
    ],
)
def test_descriptive_reader_fails_closed_on_leakage_or_invalid_values(field, value):
    with pytest.raises(ValueError):
        inspect_user_feature_rows([_feature_row(**{field: value})], 71)
