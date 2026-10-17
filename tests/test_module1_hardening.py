from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.module1.config import SyntheticConfig
from src.module1 import clean as raw_clean
from src.module1.event_reader import SCHEMA_FIELDS, event_sort_key, iter_ordered_events
from src.module1.generate import expand_events, finalize, prepare_orders
from src.module1.producer import main as producer_main, publish_rows


class _ParquetFixture:
    def __init__(self, value: bytes = b"staged"):
        self.value = value

    def to_parquet(self, path, index=False):
        assert index is False
        Path(path).write_bytes(self.value)


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


def _write_raw_fixture(raw_dir: Path, *, reordered: str = "0", bad_train_order: str = "12") -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    tables = {
        "orders": (
            ["order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order"],
            [["11", "1", "prior", "1", "0", "8", ""], ["12", "1", "train", "2", "2", "9", "7"], ["21", "2", "test", "1", "3", "10", ""]],
        ),
        "order_products__prior": (
            ["order_id", "product_id", "add_to_cart_order", "reordered"],
            [["11", "1", "1", reordered]],
        ),
        "order_products__train": (
            ["order_id", "product_id", "add_to_cart_order", "reordered"],
            [[bad_train_order, "1", "1", "1"]],
        ),
        "products": (["product_id", "product_name", "aisle_id", "department_id"], [["1", "Product one", "10", "2"]]),
        "aisles": (["aisle_id", "aisle"], [["10", "Aisle one"]]),
        "departments": (["department_id", "department"], [["2", "Department two"]]),
    }
    for table, (headers, rows) in tables.items():
        pd.DataFrame(rows, columns=headers).to_csv(raw_dir / f"{table}.csv", index=False)


def test_raw_preflight_requires_all_six_tables_before_output_creation(tmp_path, monkeypatch, capsys):
    raw_dir = tmp_path / "data" / "raw"
    _write_raw_fixture(raw_dir)
    (raw_dir / "aisles.csv").unlink()
    output = tmp_path / "clean"
    monkeypatch.setattr(
        "sys.argv", ["clean", "--data-dir", str(tmp_path / "data"), "--output-dir", str(output)]
    )

    assert raw_clean.main() == 2
    assert "aisles.csv: required source file is missing" in capsys.readouterr().err
    assert not output.exists()


def test_raw_preflight_rejects_lossy_reordered_flag_before_writing_outputs(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "data"
    _write_raw_fixture(data_dir / "raw", reordered="2")
    output = tmp_path / "clean"
    monkeypatch.setattr(
        "sys.argv", ["clean", "--data-dir", str(data_dir), "--output-dir", str(output)]
    )

    assert raw_clean.main() == 2
    error = capsys.readouterr().err
    assert "order_products__prior.csv row 2: reordered must be an integer in [0, 1]" in error
    assert not output.exists()


def test_raw_preflight_rejects_order_hour_above_documented_domain(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    _write_raw_fixture(raw_dir)
    orders_path = raw_dir / "orders.csv"
    orders = pd.read_csv(orders_path, dtype="string", keep_default_na=False)
    orders.loc[0, "order_hour_of_day"] = "24"
    orders.to_csv(orders_path, index=False)
    output = tmp_path / "clean"
    monkeypatch.setattr(
        "sys.argv", ["clean", "--data-dir", str(data_dir), "--output-dir", str(output)]
    )

    assert raw_clean.main() == 2
    assert "orders.csv row 2: order_hour_of_day must be an integer in [0, 23]" in capsys.readouterr().err
    assert not output.exists()


def test_raw_preflight_rejects_incompatible_foreign_key_before_writing_outputs(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "data"
    _write_raw_fixture(data_dir / "raw", bad_train_order="999")
    output = tmp_path / "clean"
    monkeypatch.setattr(
        "sys.argv", ["clean", "--data-dir", str(data_dir), "--output-dir", str(output)]
    )

    assert raw_clean.main() == 2
    assert "order_products__train.csv row 2: order_id does not exist in orders.csv" in capsys.readouterr().err
    assert not output.exists()


def test_raw_preflight_rejects_keys_that_would_alias_after_integer_coercion(tmp_path, monkeypatch, capsys):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    _write_raw_fixture(raw_dir)
    prior_path = raw_dir / "order_products__prior.csv"
    prior = pd.read_csv(prior_path, dtype="string", keep_default_na=False)
    prior.loc[0, "order_id"] = "011"
    prior.to_csv(prior_path, index=False)
    output = tmp_path / "clean"
    monkeypatch.setattr(
        "sys.argv", ["clean", "--data-dir", str(data_dir), "--output-dir", str(output)]
    )

    assert raw_clean.main() == 2
    assert "order_products__prior.csv row 2: order_id must be an integer" in capsys.readouterr().err
    assert not output.exists()


def test_raw_preflight_keeps_first_order_null_and_accepts_reordered_columns(tmp_path):
    raw_dir = tmp_path / "data" / "raw"
    _write_raw_fixture(raw_dir)
    path = raw_dir / "orders.csv"
    frame = pd.read_csv(path, dtype="string", keep_default_na=False)
    frame = frame[list(reversed(frame.columns))]
    frame.to_csv(path, index=False)

    tables = raw_clean.load_raw(tmp_path / "data")

    assert len(tables) == 6
    assert pd.isna(tables["orders"].loc[0, "days_since_prior_order"])
    assert tables["order_products__prior"].loc[0, "reordered"] == 0


def _clean_output_frames(value: bytes = b"new"):
    return {name: _ParquetFixture(value) for name in raw_clean._CLEAN_OUTPUT_FILES}


def _mark_owned_output(path: Path, value: bytes = b"old") -> None:
    path.mkdir()
    for name in raw_clean._CLEAN_OUTPUT_FILES:
        (path / name).write_bytes(value)
    (path / raw_clean._CLEAN_OUTPUT_MANIFEST).write_text(
        __import__("json").dumps({"schema_version": 1, "files": sorted(raw_clean._CLEAN_OUTPUT_FILES)}),
        encoding="utf-8",
    )


def test_clean_publication_rejects_raw_input_overlap_without_touching_sources(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    source = raw_dir / "orders.csv"
    source.write_bytes(b"raw sentinel")

    for output in (raw_dir, data_dir):
        try:
            raw_clean._validate_publication_paths(data_dir, output)
        except ValueError as exc:
            assert "overlap" in str(exc)
        else:
            raise AssertionError(f"unsafe output path was accepted: {output}")
    assert source.read_bytes() == b"raw sentinel"


def test_clean_publication_rejects_symlink_aliases(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(data_dir, target_is_directory=True)

    try:
        raw_clean._validate_publication_paths(data_dir, alias / "raw" / "clean")
    except ValueError as exc:
        assert "symlink" in str(exc)
    else:
        raise AssertionError("symlinked output parent was accepted")


def test_clean_publication_replaces_only_owned_outputs_and_preserves_raw(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    source = raw_dir / "orders.csv"
    source.write_bytes(b"raw sentinel")
    output = data_dir / "clean"

    raw_clean._validate_publication_paths(data_dir, output)
    raw_clean._write_outputs_atomically(output, _clean_output_frames())
    raw_clean._write_outputs_atomically(output, _clean_output_frames(b"replacement"))

    assert source.read_bytes() == b"raw sentinel"
    assert {path.name for path in output.iterdir()} == raw_clean._CLEAN_OUTPUT_FILES | {
        raw_clean._CLEAN_OUTPUT_MANIFEST
    }
    assert all((output / name).read_bytes() == b"replacement" for name in raw_clean._CLEAN_OUTPUT_FILES)


def test_clean_publication_rejects_foreign_files_and_keeps_output_and_input(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    source = raw_dir / "orders.csv"
    source.write_bytes(b"raw sentinel")
    output = data_dir / "clean"
    output.mkdir()
    foreign = output / "keep-me.txt"
    foreign.write_text("foreign sentinel", encoding="utf-8")

    try:
        raw_clean._write_outputs_atomically(output, _clean_output_frames())
    except ValueError as exc:
        assert "unrecognized" in str(exc)
    else:
        raise AssertionError("unrecognized output content was accepted")

    assert source.read_bytes() == b"raw sentinel"
    assert foreign.read_text(encoding="utf-8") == "foreign sentinel"
    assert sorted(path.name for path in output.iterdir()) == ["keep-me.txt"]


def test_clean_publication_staging_failure_preserves_prior_output(tmp_path):
    data_dir = tmp_path / "data"
    raw_dir = data_dir / "raw"
    raw_dir.mkdir(parents=True)
    source = raw_dir / "orders.csv"
    source.write_bytes(b"raw sentinel")
    output = data_dir / "clean"
    _mark_owned_output(output)
    previous = {name: (output / name).read_bytes() for name in raw_clean._CLEAN_OUTPUT_FILES}

    class _FailingFixture(_ParquetFixture):
        def to_parquet(self, path, index=False):
            raise OSError("synthetic staging failure")

    frames = _clean_output_frames()
    frames[next(iter(frames))] = _FailingFixture()
    try:
        raw_clean._write_outputs_atomically(output, frames)
    except OSError as exc:
        assert "synthetic staging failure" in str(exc)
    else:
        raise AssertionError("staging failure was not propagated")

    assert source.read_bytes() == b"raw sentinel"
    assert {name: (output / name).read_bytes() for name in raw_clean._CLEAN_OUTPUT_FILES} == previous


def test_clean_order_products_preserves_large_valid_cart_positions():
    values = pd.DataFrame({"reordered": [0, 1], "add_to_cart_order": [32768, 70000]})

    result = raw_clean.clean_order_products(values)

    assert result["add_to_cart_order"].tolist() == [32768, 70000]
    assert str(result["add_to_cart_order"].dtype) == "int32"


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
