"""Byte and error contracts for JSON used in publication fingerprints."""

from datetime import datetime, timezone

import pytest

from src.common.runtime import canonical_json, strict_canonical_json
from src.module4.hybrid.sink_mongo import HybridSnapshotContractError, _canonical_json


def test_strict_json_preserves_unicode_ordering_and_numeric_tokens():
    first = {"z": [True, None, -0.0, 1.5], "a": "cà phê"}
    second = {"a": "cà phê", "z": [True, None, -0.0, 1.5]}
    expected = '{"a":"cà phê","z":[true,null,-0.0,1.5]}'
    assert strict_canonical_json(first) == strict_canonical_json(second) == expected
    assert _canonical_json(first) == expected


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_strict_json_rejects_nonfinite_values_and_preserves_publisher_error(value):
    with pytest.raises(ValueError, match="Out of range float values"):
        strict_canonical_json({"score": value})
    with pytest.raises(HybridSnapshotContractError, match="^fingerprint payload is not canonical JSON$") as error:
        _canonical_json({"score": value})
    assert isinstance(error.value.__cause__, ValueError)


def test_strict_json_rejects_unsupported_values_without_changing_report_json():
    value = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(TypeError, match="datetime is not JSON serializable"):
        strict_canonical_json(value)
    with pytest.raises(HybridSnapshotContractError, match="^fingerprint payload is not canonical JSON$") as error:
        _canonical_json(value)
    assert isinstance(error.value.__cause__, TypeError)
    assert canonical_json({"time": value}) == '{"time":"2024-01-01 00:00:00+00:00"}'
