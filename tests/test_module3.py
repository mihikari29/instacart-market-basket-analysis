"""Integration tests for Module 3 speed layer.

Mirrors tests/test_spark.py: pytest.importorskip guard + module-scoped Spark
fixture + plan tests covering schema, trending window alignment, and the
poison-pill guard. Kafka/Mongo live integration is gated behind the
`integration` marker (CI may skip when services unavailable).
"""

from datetime import datetime, timezone
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from src.module3.config import Config
from src.module3.spark import session
from src.module3.fixtures import make_events_dataframe
from src.module3.schemas import KAFKA_EVENT_SCHEMA
from src.module3.trending import _window_agg, build_trending

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
    ]
    # KAFKA_EVENT_SCHEMA declares product_id LongType; createDataFrame coerces to null
    df = spark_session.createDataFrame(rows, schema=KAFKA_EVENT_SCHEMA)
    casted = df.withColumn("product_id", F.col("product_id").cast(LongType()))
    casted = casted.where(F.col("product_id").isNotNull() & F.col("event_time_epoch_ms").isNotNull())
    assert casted.count() == 1
    assert casted.collect()[0].event_id == "ok_1"


def test_window_short_aggregation_counts(spark_session):
    """30 minute windows at 5 minute slide: assert expected counts on fixture rows."""
    events = make_events_dataframe(spark_session)
    short = _window_agg(events, "30 minutes", "5 minutes", "purchase_count_short")
    rows = short.collect()
    # window at base+0..base+30m contains products 1,2,3,1 (4 events, 3 distinct products)
    # Find that window.
    matching = [r for r in rows if r.purchase_count_short == 4]
    assert matching, f"Expected one window with 4 events; got {[r.asDict() for r in rows]}"
    products_in_that_window = sorted({r.product_id for r in matching})
    assert products_in_that_window == [1, 2, 3]


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
