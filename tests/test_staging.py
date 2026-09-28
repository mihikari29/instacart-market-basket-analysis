import json
from unittest.mock import Mock, patch
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
import numpy as np
from src.module1.partition_events import partition_events
from src.module1.clean import clean_orders
from src.module1.stage_hdfs import stage_interactions


def test_generator_widens_narrow_time_columns():
    from src.module1.generate import prepare_orders, order_time_parts, BASE_EPOCH_MS, DAY_MS
    from src.module1.config import SyntheticConfig

    orders = pd.DataFrame(
        {
            "order_id": pd.Series([3421083], dtype="int32"),
            "user_id": [1],
            "eval_set": ["prior"],
            "order_number": [1],
            "order_dow": pd.Series([0], dtype="int16"),
            "order_hour_of_day": pd.Series([23], dtype="int16"),
            "days_since_prior_order": [0.0],
        }
    )
    result = prepare_orders(orders, SyntheticConfig(time_mode="hash"), np.random.default_rng(42))
    minute = (3421083 * 10**6 + 1) % 60
    assert (int(result.iloc[0].base_ms) - BASE_EPOCH_MS) % DAY_MS == 23 * 3600000 + minute * 60000
    assert order_time_parts(None, orders.order_id, "hash")[0].tolist() == [minute]


def source(path, dates=("2024-01-01", "2024-01-02") * 3):
    table = pa.table({"event_id": [str(i) for i in range(len(dates))], "synthetic_date": dates})
    pq.write_table(table, path, row_group_size=2)
    return table


def test_old_writer_loses_overlapping_rows(tmp_path):
    # Executable evidence of the exact original defect, not a hypothetical mock.
    original = source(tmp_path / "events.parquet")
    parquet = pq.ParquetFile(tmp_path / "events.parquet")
    for i in range(parquet.num_row_groups):
        ds.write_dataset(
            parquet.read_row_group(i),
            tmp_path / "old",
            format="parquet",
            partitioning=["synthetic_date"],
            partitioning_flavor="hive",
            existing_data_behavior="overwrite_or_ignore",
        )
    assert ds.dataset(tmp_path / "old", format="parquet", partitioning="hive").count_rows() == 2
    assert original.num_rows == 6


def test_lossless_partition_and_bounded_files(tmp_path):
    expected = source(tmp_path / "events.parquet")
    receipt = partition_events(tmp_path / "events.parquet", tmp_path / "new")
    actual = ds.dataset(tmp_path / "new", format="parquet", partitioning="hive").to_table()
    assert actual.select(expected.column_names).sort_by("event_id").equals(expected.sort_by("event_id"))
    assert receipt["source_rows"] == receipt["local_rows"] == 6
    assert receipt["files"] == receipt["partitions"] == 2
    assert actual["synthetic_year"].to_pylist() == [2024] * 6
    assert actual["synthetic_month"].to_pylist() == [1] * 6
    assert sorted(actual["synthetic_day"].to_pylist()) == [1, 1, 1, 2, 2, 2]
    with pytest.raises(pa.ArrowInvalid):
        partition_events(tmp_path / "events.parquet", tmp_path / "new")


def test_invalid_date_or_manifest_fails(tmp_path):
    source(tmp_path / "events.parquet", ["not-a-date"])
    with pytest.raises(pa.ArrowInvalid):
        partition_events(tmp_path / "events.parquet", tmp_path / "bad")
    source(tmp_path / "events.parquet")
    (tmp_path / "manifest.json").write_text(json.dumps({"events": 999}))
    with pytest.raises(ValueError, match="manifest"):
        partition_events(tmp_path / "events.parquet", tmp_path / "mismatch")


def test_staging_refresh_scope_and_failed_upload(tmp_path):
    source(tmp_path / "events.parquet")
    client = Mock()
    with patch("src.module1.stage_hdfs.webhdfs_upload") as upload:
        stage_interactions(client, tmp_path, "http://localhost:9870", "root")
        assert upload.call_count == 3
    client.delete.assert_called_once_with("/instacart/curated/interactions", recursive=True)
    assert client.rename.call_args.args[1] == "/instacart/curated/interactions"
    client.reset_mock()
    with patch("src.module1.stage_hdfs.webhdfs_upload", side_effect=RuntimeError("network")):
        with pytest.raises(RuntimeError):
            stage_interactions(client, tmp_path, "http://localhost:9870", "root")
    client.delete.assert_not_called()
    client.rename.assert_not_called()


def test_cap30_mask_tracks_rows_after_sort():
    orders = pd.DataFrame(
        {
            "order_id": [2, 1, 4, 3],
            "user_id": [1, 1, 2, 2],
            "order_number": [2, 1, 2, 1],
            "order_dow": [3, 0, 1, 0],
            "days_since_prior_order": [30.0, float("nan"), 1.0, float("nan")],
            "eval_set": ["prior"] * 4,
        }
    )
    cleaned, stats = clean_orders(orders)
    assert cleaned.set_index("order_id").loc[2, "days_since_prior_order"] == 31
    assert cleaned.set_index("order_id").loc[1, "days_since_prior_order"] == 0
    assert stats["dow_mismatch_after_recovery"] == 0


def test_refresh_replaces_stale_dates_and_preserves_other_tables(tmp_path):
    import shutil

    storage = tmp_path / "hdfs"
    storage.mkdir()
    preserved = storage / "instacart/curated/orders/keep.txt"
    preserved.parent.mkdir(parents=True)
    preserved.write_text("untouched")

    class LocalHdfs:
        def location(self, path):
            result = storage / path.lstrip("/")
            assert result.resolve().is_relative_to(storage.resolve())
            return result

        def makedirs(self, path):
            self.location(path).mkdir(parents=True, exist_ok=True)

        def delete(self, path, recursive):
            assert path == "/instacart/curated/interactions"
            if self.location(path).exists():
                shutil.rmtree(self.location(path))

        def rename(self, src, dst):
            self.location(src).rename(self.location(dst))

    client = LocalHdfs()

    def upload(path, local, *_):
        shutil.copyfile(local, client.location(path))

    feed = tmp_path / "feed"
    feed.mkdir()
    with patch("src.module1.stage_hdfs.webhdfs_upload", side_effect=upload):
        source(feed / "events.parquet")
        stage_interactions(client, feed, "unused", "root")
        source(feed / "events.parquet", ["2025-02-03"] * 4)
        stage_interactions(client, feed, "unused", "root")
    result = ds.dataset(
        storage / "instacart/curated/interactions", format="parquet", partitioning="hive"
    ).to_table()
    assert result.num_rows == 4
    assert set(result["synthetic_date"].to_pylist()) == {"2025-02-03"}
    assert preserved.read_text() == "untouched"


def test_webhdfs_must_not_accept_empty_single_hop_create(tmp_path):
    from src.module1.stage_hdfs import webhdfs_upload

    response = Mock(status_code=201, text="")
    with patch("src.module1.stage_hdfs.requests.put", return_value=response):
        with pytest.raises(RuntimeError, match="CREATE"):
            webhdfs_upload("/instacart/test", tmp_path / "not_uploaded")


def test_monotonic_violations_detects_order_level_inversion():
    from src.module1.generate import monotonic_violations

    df = pd.DataFrame({
        "user_id": [1, 1],
        "order_number": [1, 2],
        "add_to_cart_order": [1, 1],
        "event_time_epoch_ms": [5000, 2000],  # Order 2 has smaller timestamp than Order 1
    })
    assert monotonic_violations(df) == 1


def test_monotonic_violations_detects_item_level_inversion():
    from src.module1.generate import monotonic_violations

    df = pd.DataFrame({
        "user_id": [1, 1],
        "order_number": [1, 1],
        "add_to_cart_order": [1, 2],
        "event_time_epoch_ms": [5000, 4000],  # Item 2 has smaller timestamp than Item 1
    })
    assert monotonic_violations(df) == 1


def test_monotonic_violations_detects_timestamp_collisions():
    from src.module1.generate import monotonic_violations

    df = pd.DataFrame({
        "user_id": [1, 1],
        "order_number": [1, 2],
        "add_to_cart_order": [1, 1],
        "event_time_epoch_ms": [5000, 5000],  # Equal timestamp collision
    })
    assert monotonic_violations(df) == 1


def test_monotonic_violations_multi_user_and_clean_sequence():
    from src.module1.generate import monotonic_violations

    df_clean = pd.DataFrame({
        "user_id": [1, 1, 1, 2, 2],
        "order_number": [1, 1, 2, 1, 2],
        "add_to_cart_order": [1, 2, 1, 1, 1],
        "event_time_epoch_ms": [1000, 1030, 2000, 500, 1500],
    })
    assert monotonic_violations(df_clean) == 0

    df_multi = pd.DataFrame({
        "user_id": [1, 1, 2, 2],
        "order_number": [1, 2, 1, 2],
        "add_to_cart_order": [1, 1, 1, 1],
        "event_time_epoch_ms": [5000, 2000, 100, 200],  # user 1 has violation, user 2 is clean
    })
    assert monotonic_violations(df_multi) == 1


def test_generator_enforces_and_validates_monotonicity():
    from src.module1.generate import prepare_orders, expand_events, finalize, monotonic_violations
    from src.module1.config import SyntheticConfig

    orders = pd.DataFrame({
        "order_id": [101, 102],
        "user_id": [1, 1],
        "eval_set": ["prior", "prior"],
        "order_number": [1, 2],
        "order_dow": [0, 0],
        "order_hour_of_day": [10, 10],  # Same day and hour to test monotonicity guard
        "days_since_prior_order": [0.0, 0.0],
    })
    ops = pd.DataFrame({
        "order_id": [101, 101, 102, 102],
        "product_id": [1, 2, 1, 3],
        "add_to_cart_order": [1, 2, 1, 2],
        "reordered": [0, 0, 1, 0],
    })
    products = pd.DataFrame({
        "product_id": [1, 2, 3],
        "aisle_id": [10, 20, 30],
        "department_id": [1, 2, 3],
    })

    conf = SyntheticConfig(time_mode="uniform")
    rng = np.random.default_rng(42)
    stream = prepare_orders(orders, conf, rng)
    ev = expand_events(stream, ops, products, conf, rng)
    final = finalize(ev)

    assert monotonic_violations(final) == 0
    assert len(final) == 4
    # Ensure order 2 starts strictly after order 1 ends
    o1_max = final[final["order_id"] == 101]["event_time_epoch_ms"].max()
    o2_min = final[final["order_id"] == 102]["event_time_epoch_ms"].min()
    assert o2_min >= o1_max + 1000
