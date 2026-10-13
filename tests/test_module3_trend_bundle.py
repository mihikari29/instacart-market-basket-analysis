"""Contract tests for atomic bundles of finalized Module 3 windows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.module3.config import Config
from src.module3.spark import session
from src.module3.trend_bundle import (
    BUNDLE_COLLECTION_NAME,
    DEFAULT_SOURCE_SCOPE,
    TrendBundleConflictError,
    TrendBundleValidationError,
    write_trend_bundles,
)


WINDOW_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("product_id", LongType(), True),
        StructField("purchase_count_30m", LongType(), True),
        StructField("purchase_count_120m", LongType(), True),
        StructField("trend_score", DoubleType(), True),
        StructField("trend_rank", IntegerType(), True),
    ]
)


class FakeUpdateOne:
    def __init__(self, filter_document, update_document, upsert):
        self.filter = filter_document
        self.update = update_document
        self.upsert = upsert


class FakeBulkResult:
    def __init__(self, upserted=0, matched=0):
        self.upserted_count = upserted
        self.matched_count = matched
        self.modified_count = 0


class FakeCollection:
    def __init__(self, documents=None, indexes=None, name=BUNDLE_COLLECTION_NAME):
        self.name = name
        self.documents = dict(documents or {})
        self.indexes = dict(indexes or {})
        self.find_calls = []
        self.create_index_calls = []
        self.drop_index_calls = []
        self.bulk_calls = []

    def index_information(self):
        return self.indexes

    def create_index(self, keys, **options):
        self.create_index_calls.append((keys, options))
        self.indexes[options["name"]] = {"key": list(keys), **options}
        return options["name"]

    def drop_index(self, name):
        self.drop_index_calls.append(name)
        self.indexes.pop(name, None)

    def find(self, query, projection=None):
        self.find_calls.append((query, projection))
        identifiers = set(query["_id"]["$in"])
        rows = []
        for identity in identifiers:
            document = self.documents.get(identity)
            if document is None:
                continue
            if projection is None:
                rows.append(dict(document))
            else:
                rows.append(
                    {
                        key: document[key]
                        for key, enabled in projection.items()
                        if enabled and key in document
                    }
                )
        return iter(rows)

    def bulk_write(self, operations, ordered):
        self.bulk_calls.append((list(operations), ordered))
        assert ordered is False
        upserted = matched = 0
        for operation in operations:
            assert operation.upsert is True
            identity = operation.filter["_id"]
            expected_hash = operation.filter["content_sha256"]
            existing = self.documents.get(identity)
            if existing is not None:
                assert existing["content_sha256"] == expected_hash
                matched += 1
                continue
            document = operation.update["$setOnInsert"]
            assert document["_id"] == identity
            self.documents[identity] = dict(document)
            upserted += 1
        return FakeBulkResult(upserted=upserted, matched=matched)


@pytest.fixture(scope="module")
def spark_session(tmp_path_factory):
    directory = tmp_path_factory.mktemp("module3-trend-bundle")
    config = Config(
        root=(directory / "fixture").as_uri(),
        master="local[2]",
        shuffle_partitions=2,
        mongo_top_k_per_window=20,
        ttl_seconds=0,
    )
    spark = session(config, directory / "events")
    yield spark
    spark.stop()


@pytest.fixture
def pymongo_update_one(monkeypatch):
    import pymongo

    monkeypatch.setattr(pymongo, "UpdateOne", FakeUpdateOne)


def config(top_k=20, ttl_seconds=0, **overrides):
    return Config(
        mongo_top_k_per_window=top_k,
        ttl_seconds=ttl_seconds,
        **overrides,
    )


def window_rows(start, products, *, scores=None):
    end = start + timedelta(hours=2)
    if scores is None:
        scores = [1.0 - index / max(1, len(products)) for index in range(len(products))]
    return [
        (start, end, product_id, 20 - index, 40 - index, scores[index], index + 1)
        for index, product_id in enumerate(products)
    ]


def frame(spark_session, rows, schema=WINDOW_SCHEMA):
    return spark_session.createDataFrame(rows, schema=schema)


def publish(spark_session, collection, rows, *, top_k=20, ttl_seconds=0, **kwargs):
    return write_trend_bundles(
        frame(spark_session, rows),
        collection,
        config(top_k=top_k, ttl_seconds=ttl_seconds),
        source_namespace="instacart:fixture-a",
        **kwargs,
    )


def test_one_complete_window_produces_one_atomic_bundle(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    collection = FakeCollection()
    report = publish(spark_session, collection, window_rows(start, [11, 12, 13]))

    assert report.bundles_created == 1
    assert report.identical_content_retries == 0
    assert report.windows_processed == 1
    assert report.trend_rows_represented == 3
    assert report.rejected_publications == 0
    assert len(collection.documents) == 1
    document = next(iter(collection.documents.values()))
    assert BUNDLE_COLLECTION_NAME == "realtime_trending_windows"
    assert document["schema_version"] == 1
    assert document["source_namespace"] == "instacart:fixture-a"
    assert document["source_scope"] == DEFAULT_SOURCE_SCOPE
    assert document["window_start"] == start.replace(tzinfo=timezone.utc)
    assert document["window_end"] == (start + timedelta(hours=2)).replace(tzinfo=timezone.utc)
    assert document["top_k"] == 20
    assert document["row_count"] == 3
    assert [item["trend_rank"] for item in document["items"]] == [1, 2, 3]
    assert set(document["items"][0]) == {
        "product_id",
        "purchase_count_30m",
        "purchase_count_120m",
        "trend_score",
        "trend_rank",
    }
    assert document["config_hash"]
    assert len(document["content_sha256"]) == 64
    assert "expires_at" not in document
    operations, ordered = collection.bulk_calls[0]
    assert ordered is False
    assert operations[0].filter == {
        "_id": document["_id"],
        "content_sha256": document["content_sha256"],
    }
    assert operations[0].update == {"$setOnInsert": document}
    assert operations[0].upsert is True


def test_multiple_independent_windows_are_separate_documents(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [1, 2]) + window_rows(start + timedelta(minutes=5), [3, 4])
    collection = FakeCollection()

    report = publish(spark_session, collection, rows, batch_size=1)

    assert report.bundles_created == 2
    assert report.windows_processed == 2
    assert report.trend_rows_represented == 4
    assert len(collection.documents) == 2
    assert len(collection.bulk_calls) == 2


def test_fewer_than_twenty_products_and_exactly_twenty_are_valid(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    fewer = FakeCollection()
    exactly = FakeCollection()

    assert publish(spark_session, fewer, window_rows(start, [1, 2, 3])).trend_rows_represented == 3
    exact_rows = window_rows(start, list(range(1, 21)))
    assert publish(spark_session, exactly, exact_rows).trend_rows_represented == 20
    assert len(next(iter(exactly.documents.values()))["items"]) == 20


def test_more_than_configured_top_k_is_rejected_before_writes(spark_session, pymongo_update_one):
    collection = FakeCollection()
    rows = window_rows(datetime(2024, 1, 1), [1, 2, 3])

    with pytest.raises(TrendBundleValidationError, match="larger than configured Top-2") as raised:
        publish(spark_session, collection, rows, top_k=2)

    assert raised.value.report.rejected_publications == 1
    assert not collection.bulk_calls
    assert not collection.documents


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda rows: rows.__setitem__(1, rows[1][:-1] + (1,)), "duplicate trend ranks"),
        (lambda rows: rows.__setitem__(2, rows[2][:-1] + (4,)), "contiguous"),
        (lambda rows: rows.__setitem__(1, (rows[1][0], rows[1][1], rows[0][2], *rows[1][3:])), "duplicate product IDs"),
    ],
)
def test_rank_and_product_identity_validation(spark_session, pymongo_update_one, mutate, message):
    rows = window_rows(datetime(2024, 1, 1), [10, 11, 12])
    mutate(rows)
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match=message):
        publish(spark_session, collection, rows, top_k=3)

    assert not collection.bulk_calls
    assert collection.documents == {}


@pytest.mark.parametrize("score", [-0.1, None, float("nan"), float("inf"), float("-inf")])
def test_invalid_trend_scores_are_rejected(spark_session, pymongo_update_one, score):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    rows[0] = rows[0][:5] + (score,) + rows[0][6:]
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match="trend_score"):
        publish(spark_session, collection, rows)

    assert not collection.bulk_calls


@pytest.mark.parametrize(
    ("index", "value", "message"),
    [
        (3, -1, "purchase_count_30m"),
        (3, None, "purchase_count_30m"),
        (4, -1, "purchase_count_120m"),
        (4, None, "purchase_count_120m"),
        (3, 50, "cannot exceed"),
    ],
)
def test_invalid_purchase_counts_are_rejected(spark_session, pymongo_update_one, index, value, message):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    row = list(rows[0])
    row[index] = value
    rows[0] = tuple(row)
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match=message):
        publish(spark_session, collection, rows)

    assert not collection.bulk_calls


def test_all_zero_score_window_with_products_is_valid(spark_session, pymongo_update_one):
    rows = window_rows(datetime(2024, 1, 1), [8, 9], scores=[0.0, 0.0])
    collection = FakeCollection()

    report = publish(spark_session, collection, rows)

    assert report.bundles_created == 1
    assert [item["trend_score"] for item in next(iter(collection.documents.values()))["items"]] == [0.0, 0.0]


def test_missing_wrong_type_null_and_reversed_window_times_are_rejected(spark_session, pymongo_update_one):
    collection = FakeCollection()
    rows = window_rows(datetime(2024, 1, 1), [1])
    missing_schema = StructType([field for field in WINDOW_SCHEMA.fields if field.name != "window_end"])
    missing_frame = frame(spark_session, [rows[0][0:1] + rows[0][2:]], schema=missing_schema)
    with pytest.raises(TrendBundleValidationError, match="missing required fields"):
        write_trend_bundles(
            missing_frame,
            collection,
            config(),
            source_namespace="instacart:fixture-a",
        )

    wrong_type_schema = StructType(
        [StructField("window_start", StringType(), True)] + list(WINDOW_SCHEMA.fields[1:])
    )
    wrong_type_row = ("2024-01-01T00:00:00Z",) + rows[0][1:]
    with pytest.raises(TrendBundleValidationError, match="TimestampType"):
        write_trend_bundles(
            frame(spark_session, [wrong_type_row], schema=wrong_type_schema),
            collection,
            config(),
            source_namespace="instacart:fixture-a",
        )

    null_rows = [list(rows[0])]
    null_rows[0][0] = None
    with pytest.raises(TrendBundleValidationError, match="timestamp"):
        publish(spark_session, collection, [tuple(null_rows[0])])

    reversed_rows = window_rows(datetime(2024, 1, 1), [1])
    reversed_rows[0] = (reversed_rows[0][1], reversed_rows[0][0]) + reversed_rows[0][2:]
    with pytest.raises(TrendBundleValidationError, match="later than"):
        publish(spark_session, collection, reversed_rows)

    assert not collection.bulk_calls


def test_same_end_with_different_starts_is_inconsistent(spark_session, pymongo_update_one):
    end = datetime(2024, 1, 1, 2)
    rows = [
        (end - timedelta(hours=2), end, 1, 1, 2, 1.0, 1),
        (end - timedelta(hours=1), end, 2, 1, 2, 0.5, 1),
    ]
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match="multiple window_start"):
        publish(spark_session, collection, rows, top_k=2)

    assert not collection.bulk_calls


def test_content_hash_is_order_independent_and_excludes_publication_time(spark_session, pymongo_update_one):
    rows = window_rows(datetime(2024, 1, 1), [1, 2, 3])
    first = FakeCollection()
    permuted = FakeCollection()
    later = FakeCollection()
    publish(
        spark_session,
        first,
        rows,
        published_at=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
    )
    publish(
        spark_session,
        permuted,
        list(reversed(rows)),
        published_at=datetime(2024, 1, 1, 4, tzinfo=timezone.utc),
    )
    publish(
        spark_session,
        later,
        rows,
        published_at=datetime(2024, 1, 2, 3, tzinfo=timezone.utc),
    )
    digest = next(iter(first.documents.values()))["content_sha256"]
    assert next(iter(permuted.documents.values()))["content_sha256"] == digest
    assert next(iter(later.documents.values()))["content_sha256"] == digest
    assert next(iter(first.documents.values()))["published_at"] != next(iter(later.documents.values()))["published_at"]


def test_config_hash_covers_logical_ranking_configuration(spark_session, pymongo_update_one):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    standard = FakeCollection()
    alternate = FakeCollection()
    write_trend_bundles(
        frame(spark_session, rows),
        standard,
        config(),
        source_namespace="instacart:fixture-a",
    )
    write_trend_bundles(
        frame(spark_session, rows),
        alternate,
        Config(weight_short=0.6, weight_long=0.4, ttl_seconds=0),
        source_namespace="instacart:fixture-a",
    )
    original = next(iter(standard.documents.values()))
    changed = next(iter(alternate.documents.values()))
    assert original["config_hash"] != changed["config_hash"]
    assert original["content_sha256"] != changed["content_sha256"]


def test_same_content_retry_is_idempotent_and_preserves_first_publication(spark_session, pymongo_update_one):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    collection = FakeCollection()
    first_time = datetime(2024, 1, 1, 3, tzinfo=timezone.utc)
    retry_time = datetime(2024, 1, 1, 4, tzinfo=timezone.utc)
    first = publish(
        spark_session,
        collection,
        rows,
        ttl_seconds=60,
        published_at=first_time,
    )
    original_document = dict(next(iter(collection.documents.values())))
    second = publish(
        spark_session,
        collection,
        rows,
        ttl_seconds=60,
        published_at=retry_time,
    )

    assert first.bundles_created == 1
    assert second.bundles_created == 0
    assert second.identical_content_retries == 1
    assert collection.documents[original_document["_id"]] == original_document
    assert original_document["published_at"] == first_time
    assert original_document["expires_at"] == first_time + timedelta(seconds=60)


def test_conflicting_retry_is_rejected_without_replacing_existing_document(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    collection = FakeCollection()
    original_rows = window_rows(start, [1, 2])
    publish(spark_session, collection, original_rows)
    identity = next(iter(collection.documents))
    original_document = dict(collection.documents[identity])
    changed_rows = list(original_rows)
    changed_rows[0] = changed_rows[0][:3] + (18, 40) + changed_rows[0][5:]

    with pytest.raises(TrendBundleConflictError, match="conflicting content") as raised:
        publish(spark_session, collection, changed_rows)

    assert raised.value.report.rejected_publications == 1
    assert collection.documents[identity] == original_document
    assert len(collection.bulk_calls) == 1


def test_validation_failure_in_later_window_prevents_all_publication(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [1, 2]) + window_rows(start + timedelta(minutes=5), [3, 4])
    rows[-1] = rows[-1][:-2] + (float("nan"), rows[-1][-1])
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match="trend_score"):
        publish(spark_session, collection, rows)

    assert collection.documents == {}
    assert not collection.bulk_calls


def test_validation_failure_preserves_previous_complete_bundle(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    collection = FakeCollection()
    valid_rows = window_rows(start, [1, 2])
    publish(spark_session, collection, valid_rows)
    identity = next(iter(collection.documents))
    original_document = dict(collection.documents[identity])
    invalid_rows = list(valid_rows)
    invalid_rows[0] = invalid_rows[0][:5] + (float("nan"),) + invalid_rows[0][6:]

    with pytest.raises(TrendBundleValidationError, match="trend_score"):
        publish(spark_session, collection, invalid_rows)

    assert collection.documents[identity] == original_document
    assert len(collection.bulk_calls) == 1


def test_enabled_and_disabled_ttl_semantics(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    enabled = FakeCollection()
    disabled = FakeCollection(
        indexes={
            "ttl_expires_at": {
                "key": [("expires_at", 1)],
                "expireAfterSeconds": 0,
            }
        }
    )
    publish(spark_session, enabled, window_rows(start, [1]), ttl_seconds=3600)
    publish(spark_session, disabled, window_rows(start, [1]), ttl_seconds=0)

    enabled_doc = next(iter(enabled.documents.values()))
    disabled_doc = next(iter(disabled.documents.values()))
    ttl = enabled.indexes["ttl_expires_at"]
    assert ttl["key"] == [("expires_at", 1)]
    assert ttl["expireAfterSeconds"] == 0
    assert enabled_doc["expires_at"] - enabled_doc["published_at"] == timedelta(seconds=3600)
    assert "expires_at" not in disabled_doc
    assert disabled.drop_index_calls == ["ttl_expires_at"]
    assert "ttl_expires_at" not in disabled.indexes


def test_namespace_isolation_and_scope_default(spark_session, pymongo_update_one):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    collection = FakeCollection()
    report_a = write_trend_bundles(
        frame(spark_session, rows), collection, config(), source_namespace="stream-a"
    )
    report_b = write_trend_bundles(
        frame(spark_session, rows), collection, config(), source_namespace="stream-b"
    )

    assert report_a.bundles_created == report_b.bundles_created == 1
    assert len(collection.documents) == 2
    assert {doc["source_namespace"] for doc in collection.documents.values()} == {"stream-a", "stream-b"}
    assert {doc["source_scope"] for doc in collection.documents.values()} == {DEFAULT_SOURCE_SCOPE}


def test_empty_input_creates_no_bundle_and_does_not_touch_collection(spark_session, pymongo_update_one):
    collection = FakeCollection()
    empty = frame(spark_session, [])

    report = write_trend_bundles(
        empty,
        collection,
        config(),
        source_namespace="instacart:fixture-a",
    )

    assert report.bundles_created == 0
    assert report.identical_content_retries == 0
    assert report.windows_processed == 0
    assert report.trend_rows_represented == 0
    assert report.rejected_publications == 0
    assert collection.documents == {}
    assert collection.find_calls == []
    assert collection.create_index_calls == []


def test_namespace_and_retention_metadata_validation(spark_session, pymongo_update_one):
    data = frame(spark_session, window_rows(datetime(2024, 1, 1), [1]))
    collection = FakeCollection()
    for namespace in ("", " padded ", "bad\x00name"):
        with pytest.raises(TrendBundleValidationError, match="source_namespace"):
            write_trend_bundles(data, collection, config(), source_namespace=namespace)
    with pytest.raises(TrendBundleValidationError, match="nonnegative integer"):
        write_trend_bundles(
            data,
            collection,
            config(),
            source_namespace="valid",
            retention_seconds=-1,
        )
    with pytest.raises(TrendBundleValidationError, match="explicit UTC offset"):
        write_trend_bundles(
            data,
            collection,
            config(),
            source_namespace="valid",
            published_at=datetime(2024, 1, 1),
        )

    assert collection.documents == {}


def test_writer_refuses_to_change_an_existing_collection(spark_session, pymongo_update_one):
    collection = FakeCollection(name="realtime_trending")
    data = frame(spark_session, window_rows(datetime(2024, 1, 1), [1]))

    with pytest.raises(TrendBundleValidationError, match="realtime_trending_windows"):
        write_trend_bundles(
            data,
            collection,
            config(),
            source_namespace="instacart:fixture-a",
        )

    assert collection.documents == {}
    assert collection.create_index_calls == []


def test_window_times_must_match_configured_window_duration(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    end = start + timedelta(hours=1)
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match="configured window_long"):
        publish(spark_session, collection, [(start, end, 1, 1, 2, 1.0, 1)])

    assert not collection.bulk_calls


def test_multi_window_fixture_and_bounded_driver_chunks(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1)
    rows = []
    for offset in range(12):
        rows.extend(window_rows(start + timedelta(minutes=5 * offset), [1, 2, 3]))
    collection = FakeCollection()

    report = publish(spark_session, collection, rows, top_k=3, batch_size=2)

    assert report.bundles_created == 12
    assert report.windows_processed == 12
    assert report.trend_rows_represented == 36
    assert len(collection.documents) == 12
    assert all(len(document["items"]) == 3 for document in collection.documents.values())
    assert all(len(operations) <= 2 for operations, _ordered in collection.bulk_calls)


def test_writer_does_not_call_dataframe_collect(spark_session, pymongo_update_one, monkeypatch):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    collection = FakeCollection()

    def forbidden_collect(_self):
        raise AssertionError("writer must not call DataFrame.collect()")

    monkeypatch.setattr(DataFrame, "collect", forbidden_collect)
    report = publish(spark_session, collection, rows)

    assert report.bundles_created == 1
