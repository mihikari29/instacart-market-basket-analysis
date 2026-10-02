"""Integration tests for Module 3 speed layer.

Mirrors tests/test_spark.py: pytest.importorskip guard + module-scoped Spark
fixture + plan tests covering schema, trending window alignment, and the
poison-pill guard. Kafka/Mongo live integration is gated behind the
`integration` marker (CI may skip when services unavailable).
"""

# ruff: noqa: E402 -- imports intentionally follow pytest.importorskip.

from datetime import datetime, timezone
import json
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql.types import StringType, StructField, StructType

from src.module3.config import Config
from src.module3.checkpoint import ensure_checkpoint
from src.module3.spark import session
from src.module3.fixtures import make_events_dataframe
from src.module3.parse import parse_json_events
from src.module3.schemas import KAFKA_EVENT_SCHEMA
from src.module3.trending import (
    _window_agg,
    build_trending,
    build_window_counts,
    rank_trending_batch,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory):
    directory = tmp_path_factory.mktemp("module3")
    config = Config(root=(directory / "fixture").as_uri(), master="local[2]", shuffle_partitions=2)
    spark = session(config, directory / "events")
    yield spark
    spark.stop()


def test_kafka_event_schema_round_trip(spark_session):
    """Schema parses valid JSON-equivalent rows without loss."""
    base = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    rows = [
        {
            "event_id": "x_1_1", "order_id": 1, "user_id": 7, "product_id": 42,
            "add_to_cart_order": 1, "reordered": True, "aisle_id": 3,
            "department_id": 4, "order_dow": 2, "order_hour_of_day": 17,
            "event_time_epoch_ms": base, "event_time_iso": "2024-01-01T00:00:00Z",
            "ingestion_time_epoch_ms": base + 1000,
        }
    ]
    df = spark_session.createDataFrame(rows, schema=KAFKA_EVENT_SCHEMA)
    collected = df.collect()[0].asDict()
    assert collected["product_id"] == 42
    assert collected["reordered"] is True
    assert collected["event_time_epoch_ms"] == base


def test_poison_pill_product_id_falls_to_null_and_drops(spark_session):
    """Producer --inject poison corrupts product_id to string 'MALFORMED'.

    Casting to LongType yields NULL; trending aggregation must drop it.
    """
    base = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    rows = [
        {"event_id": "ok_1", "order_id": 1, "user_id": 1, "product_id": 1,
         "add_to_cart_order": 1, "reordered": True, "aisle_id": 1, "department_id": 1,
         "order_dow": 0, "order_hour_of_day": 0, "event_time_epoch_ms": base,
         "event_time_iso": "2024-01-01T00:00:00Z", "ingestion_time_epoch_ms": base},
        {"event_id": "bad_2", "order_id": 2, "user_id": 1, "product_id": "MALFORMED",
         "add_to_cart_order": 1, "reordered": True, "aisle_id": 1, "department_id": 1,
         "order_dow": 0, "order_hour_of_day": 0, "event_time_epoch_ms": base + 60_000,
         "event_time_iso": "2024-01-01T00:01:00Z", "ingestion_time_epoch_ms": base + 60_000},
        {"event_id": "bad_3", "order_id": 3, "user_id": 1, "product_id": 2,
         "add_to_cart_order": 1, "reordered": True, "aisle_id": 1, "department_id": 1,
         "order_dow": 0, "order_hour_of_day": 0, "event_time_epoch_ms": "MALFORMED",
         "event_time_iso": "invalid", "ingestion_time_epoch_ms": base + 60_000},
    ]
    payloads = [json.dumps(row) for row in rows] + ["{not valid json", json.dumps({"product_id": 7})]
    raw = spark_session.createDataFrame(
        [(payload,) for payload in payloads],
        StructType([StructField("json_str", StringType())]),
    )
    parsed = parse_json_events(raw)
    assert parsed.count() == 1
    assert parsed.collect()[0].event_id == "ok_1"


def test_window_short_aggregation_counts(spark_session):
    """30 minute windows at 5 minute slide: assert expected counts on fixture rows."""
    events = make_events_dataframe(spark_session)
    short = _window_agg(events, "30 minutes", "5 minutes", "purchase_count_short")
    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 0, 30)
    matching = {
        row.product_id: row.purchase_count_short
        for row in short.collect()
        if row.window_start == start and row.window_end == end
    }
    assert matching == {1: 2, 2: 1, 3: 2}


def test_fixture_does_not_mutate_global_schema(spark_session):
    original_names = KAFKA_EVENT_SCHEMA.fieldNames()
    make_events_dataframe(spark_session)
    make_events_dataframe(spark_session)
    assert KAFKA_EVENT_SCHEMA.fieldNames() == original_names


def test_single_aggregation_computes_short_and_long_counts(spark_session):
    events = make_events_dataframe(spark_session)
    counts = build_window_counts(events, Config.environment(shuffle_partitions=2))
    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 2, 0)
    matching = {
        row.product_id: (row.purchase_count_30m, row.purchase_count_120m)
        for row in counts.collect()
        if row.window_start == start and row.window_end == end
    }
    assert matching == {1: (1, 3), 2: (0, 2), 3: (0, 2)}


def test_trend_score_normalization_bounded(spark_session):
    """Normalized scores lie in [0, 1] across the fixture."""
    events = make_events_dataframe(spark_session)
    trend = build_trending(events, Config.environment(shuffle_partitions=2))
    rows = trend.collect()
    assert rows, "Trending produced no rows; fixture should yield at least one window"
    for r in rows:
        assert 0.0 <= r.trend_score <= 1.0 + 1e-9
        assert r.trend_rank >= 1


def test_trend_rank_unique_within_window(spark_session):
    """Top rank 1 exists per window_end; ranks 1..N distinct within each window."""
    events = make_events_dataframe(spark_session)
    trend = build_trending(events, Config.environment(shuffle_partitions=2))
    by_window = {}
    for r in trend.collect():
        by_window.setdefault(r.window_end, []).append(r.trend_rank)
    for we, ranks in by_window.items():
        assert sorted(ranks) == list(range(1, len(ranks) + 1)), (
            f"window_end={we}: ranks not 1..N: {sorted(ranks)}"
        )
        assert 1 in ranks


def test_weights_sum_to_one_enforced():
    """Config must reject weight combinations that don't sum to 1.0 (Proposal §28.3)."""
    with pytest.raises(ValueError, match="sum to 1"):
        Config.environment(weight_short=0.6, weight_long=0.5)


def test_degenerate_normalization_and_rank_tie_break(spark_session):
    rows = [
        (datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0), 2, 4, 8),
        (datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 2, 0), 1, 4, 8),
    ]
    counts = spark_session.createDataFrame(
        rows,
        "window_start timestamp, window_end timestamp, product_id long, "
        "purchase_count_30m long, purchase_count_120m long",
    )
    ranked = rank_trending_batch(counts, Config.environment(shuffle_partitions=2)).collect()
    assert [(row.product_id, row.trend_score, row.trend_rank) for row in ranked] == [
        (1, 0.0, 1),
        (2, 0.0, 2),
    ]


def test_checkpoint_identity_is_stable_across_run_ids(tmp_path):
    first = ensure_checkpoint(tmp_path, "recovery-smoke")
    second = ensure_checkpoint(tmp_path, "recovery-smoke")
    other = ensure_checkpoint(tmp_path, "other-run")
    assert first == second
    assert first != other
    assert first.endswith("/checkpoint/recovery-smoke")
