from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.module1.config import SyntheticConfig
from src.module1.event_reader import SCHEMA_FIELDS, event_sort_key, iter_ordered_events
from src.module1.generate import expand_events, finalize, prepare_orders
from src.module1.producer import main as producer_main, publish_rows


def _generation_fixture():
    orders = pd.DataFrame(
        {
            "order_id": [11, 12, 21, 22, 31, 32],
            "user_id": [1, 1, 2, 2, 3, 3],
            "eval_set": ["prior"] * 6,
            "order_number": [1, 2] * 3,
            "order_dow": [0, 2, 1, 4, 3, 5],
            "order_hour_of_day": [8, 9, 10, 11, 12, 13],
            "days_since_prior_order": [0, 2, 0, 3, 0, 2],
        }
    )
    products = pd.DataFrame(
        {"product_id": [1, 2], "aisle_id": [10, 20], "department_id": [1, 2]}
    )
    event_rows = []
    for order_id in orders.order_id:
        event_rows.extend(
            [
                (order_id, 1, 1, False),
                (order_id, 2, 2, True),
            ]
        )
    order_products = pd.DataFrame(
        event_rows,
        columns=["order_id", "product_id", "add_to_cart_order", "reordered"],
    )
    return orders, order_products, products


def _generate_in_batches(batch_users: int) -> pd.DataFrame:
    orders, order_products, products = _generation_fixture()
    config = SyntheticConfig(seed=987, time_mode="uniform")
    outputs = []
    users = sorted(orders.user_id.unique())
    for start in range(0, len(users), batch_users):
        selected = users[start : start + batch_users]
        stream = prepare_orders(orders[orders.user_id.isin(selected)], config)
        facts = order_products[order_products.order_id.isin(stream.order_id)]
        outputs.append(finalize(expand_events(stream, facts, products, config)))
    return pd.concat(outputs).sort_values("event_id").reset_index(drop=True)


def test_generation_is_invariant_to_user_batch_size():
    expected = _generate_in_batches(1)
    pd.testing.assert_frame_equal(expected, _generate_in_batches(2))
    pd.testing.assert_frame_equal(expected, _generate_in_batches(20))


def _event_rows():
    rows = []
    for index, timestamp in enumerate([3000, 1000, 2000, 1000, 4000]):
        rows.append(
            {
                "event_id": f"e{index}",
                "order_id": index + 1,
                "user_id": 10 - index,
                "product_id": 100 + index,
                "add_to_cart_order": 1,
                "reordered": False,
                "aisle_id": 1,
                "department_id": 1,
                "order_dow": 0,
                "order_hour_of_day": 0,
                "event_time_epoch_ms": timestamp,
                "event_time_iso": "2024-01-01T00:00:00.000000",
            }
        )
    return rows


def _write_events(path: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows).select(SCHEMA_FIELDS), path, row_group_size=2)


def test_external_merge_orders_limits_and_bounds_chunks(tmp_path):
    path = tmp_path / "events.parquet"
    rows = _event_rows()
    _write_events(path, rows)
    stats = {}
    actual = list(
        iter_ordered_events(
            path,
            limit=4,
            chunk_rows=2,
            merge_fan_in=2,
            reader_batch_rows=1,
            stats=stats,
        )
    )
    assert actual == sorted(rows, key=event_sort_key)[:4]
    assert stats["source_rows"] == 5
    assert stats["emitted_rows"] == 4
    assert stats["max_chunk_rows"] <= 2
    assert stats["initial_runs"] == 3
    assert stats["merge_passes"] >= 1


def test_sorted_input_limit_stops_source_iteration(tmp_path):
    path = tmp_path / "events.parquet"
    rows = sorted(_event_rows(), key=event_sort_key)
    _write_events(path, rows)
    stats = {}
    actual = list(iter_ordered_events(path, limit=2, input_sorted=True, stats=stats))
    assert actual == rows[:2]
    assert stats["source_rows"] == stats["emitted_rows"] == 2


class _Future:
    def __init__(self, error=None):
        self.error = error

    def add_callback(self, callback):
        if self.error is None:
            callback(object())
        return self

    def add_errback(self, callback):
        if self.error is not None:
            callback(self.error)
        return self


class _Producer:
    def __init__(self, fail_index=None):
        self.fail_index = fail_index
        self.sent = []
        self.closed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        error = RuntimeError("delivery failed") if len(self.sent) - 1 == self.fail_index else None
        return _Future(error)

    def flush(self, timeout):
        assert timeout == 30

    def close(self, timeout):
        assert timeout == 10
        self.closed = True


def test_delivery_acknowledgement_accounting_success_and_failure():
    rows = _event_rows()[:3]
    success, _ = publish_rows(
        _Producer(),
        rows,
        topic="events",
        replay_speed=0,
        target_events_per_second=0,
        start_delay_seconds=0,
        injections={},
    )
    assert (success["attempted_events"], success["acked_events"], success["failed_events"]) == (
        3,
        3,
        0,
    )

    failed, _ = publish_rows(
        _Producer(fail_index=1),
        rows,
        topic="events",
        replay_speed=0,
        target_events_per_second=0,
        start_delay_seconds=0,
        injections={},
    )
    assert (failed["attempted_events"], failed["acked_events"], failed["failed_events"]) == (
        3,
        2,
        1,
    )
    assert "delivery failed" in failed["failure_samples"][0]


def _publish_with_injections(injections):
    producer = _Producer()
    publish_rows(
        producer,
        _event_rows()[2:3],
        topic="events",
        replay_speed=0,
        target_events_per_second=0,
        start_delay_seconds=0,
        injections=injections,
    )
    return [value for _topic, _key, value in producer.sent]


def test_duplicate_injection_reuses_source_event_id():
    messages = _publish_with_injections({"dup": 1.0})
    assert len(messages) == 2
    assert messages[0]["event_id"] == messages[1]["event_id"] == "e2"


def test_late_injection_has_older_time_and_deterministic_distinct_id():
    first = _publish_with_injections({"late": 1.0})
    second = _publish_with_injections({"late": 1.0})
    assert len(first) == len(second) == 2
    assert first[1]["event_id"] == second[1]["event_id"] == "e2__late_1"
    assert first[1]["event_id"] != first[0]["event_id"]
    assert first[1]["event_time_epoch_ms"] < first[0]["event_time_epoch_ms"]


def test_duplicate_and_late_injections_remain_distinguishable():
    original, duplicate, late = _publish_with_injections({"dup": 1.0, "late": 1.0})
    assert duplicate["event_id"] == original["event_id"]
    assert late["event_id"] == "e2__late_1"
    assert late["event_id"] != duplicate["event_id"]


def test_producer_returns_nonzero_and_writes_receipt_on_delivery_failure(tmp_path):
    receipt = tmp_path / "receipt.json"
    with (
        patch("src.module1.producer.iter_ordered_events", return_value=iter(_event_rows()[:1])),
        patch("src.module1.producer.build_producer", return_value=_Producer(fail_index=0)),
    ):
        code = producer_main(
            [
                "--feed",
                str(tmp_path),
                "--limit-events",
                "1",
                "--replay-speed",
                "0",
                "--receipt",
                str(receipt),
            ]
        )
    assert code == 1
    payload = __import__("json").loads(receipt.read_text())
    assert payload["attempted_events"] == payload["failed_events"] == 1
    assert payload["acked_events"] == 0
