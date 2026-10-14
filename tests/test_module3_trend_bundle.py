"""Contract tests for atomic bundles of finalized Module 3 windows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
from bson import BSON
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
    TrendBundlePublicationError,
    TrendBundleValidationError,
    _build_document,
    _config_payload,
    _sha256,
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
_DEFAULT_WINDOW_END = object()


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


class ControlledBulkErrorCollection(FakeCollection):
    """Inject a bulk error after chosen documents have become durable."""

    def __init__(
        self,
        *,
        duplicate_indices=(0,),
        missing_indices=(),
        conflicting_indices=(),
        inserted_indices=(),
        matched_count=0,
        error_code=11000,
        key_pattern=None,
        write_concern_errors=None,
        fail_find_on=None,
    ):
        super().__init__()
        self.duplicate_indices = set(duplicate_indices)
        self.missing_indices = set(missing_indices)
        self.conflicting_indices = set(conflicting_indices)
        self.inserted_indices = set(inserted_indices)
        self.matched_count = matched_count
        self.error_code = error_code
        self.key_pattern = key_pattern or {"_id": 1}
        self.write_concern_errors = write_concern_errors
        self.fail_find_on = fail_find_on

    def find(self, query, projection=None):
        if self.fail_find_on == len(self.find_calls) + 1:
            self.find_calls.append((query, projection))
            raise RuntimeError("injected read-back failure")
        return super().find(query, projection)

    def bulk_write(self, operations, ordered):
        from pymongo.errors import BulkWriteError

        operations = list(operations)
        self.bulk_calls.append((operations, ordered))
        assert ordered is False
        for index, operation in enumerate(operations):
            if index in self.missing_indices:
                continue
            document = dict(operation.update["$setOnInsert"])
            if index in self.conflicting_indices:
                document["content_sha256"] = "f" * 64
            self.documents[document["_id"]] = document

        write_errors = []
        for index in sorted(self.duplicate_indices):
            identity = operations[index].filter["_id"]
            write_errors.append(
                {
                    "index": index,
                    "code": self.error_code,
                    "errmsg": "injected bulk write error",
                    "keyPattern": self.key_pattern,
                    "keyValue": {"_id": identity},
                }
            )
        upserted = [
            {"index": index, "_id": operations[index].filter["_id"]}
            for index in sorted(self.inserted_indices)
        ]
        details = {
            "writeErrors": write_errors,
            "nInserted": 0,
            "nMatched": self.matched_count,
            "nModified": 0,
            "nRemoved": 0,
            "nUpserted": len(upserted),
            "upserted": upserted,
        }
        if self.write_concern_errors is not None:
            details["writeConcernErrors"] = self.write_concern_errors
        raise BulkWriteError(details)


class DirectDuplicateKeyCollection(FakeCollection):
    """Persist a competing document, then raise a single-operation E11000."""

    def bulk_write(self, operations, ordered):
        from pymongo.errors import DuplicateKeyError

        operations = list(operations)
        self.bulk_calls.append((operations, ordered))
        assert ordered is False
        document = operations[0].update["$setOnInsert"]
        self.documents[document["_id"]] = dict(document)
        raise DuplicateKeyError(
            "E11000 duplicate key error",
            code=11000,
            details={
                "keyPattern": {"_id": 1},
                "keyValue": {"_id": document["_id"]},
            },
        )


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


def direct_document(
    start,
    *,
    end=_DEFAULT_WINDOW_END,
    namespace="instacart:fixture-a",
    scope=DEFAULT_SOURCE_SCOPE,
):
    writer_config = config()
    if end is _DEFAULT_WINDOW_END:
        end = start + timedelta(hours=2)
    return _build_document(
        {
            "window_start": start,
            "window_end": end,
            "items": [
                {
                    "product_id": 11,
                    "purchase_count_30m": 20,
                    "purchase_count_120m": 40,
                    "trend_score": 0.95,
                    "trend_rank": 1,
                }
            ],
            "_row_count": 1,
        },
        source_namespace=namespace,
        source_scope=scope,
        top_k=writer_config.mongo_top_k_per_window,
        config_hash=_sha256(_config_payload(writer_config, writer_config.mongo_top_k_per_window)),
        published_at=datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
        retention_seconds=0,
    )


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


@pytest.mark.parametrize("microsecond", [0, 123_000, 999_000])
def test_bson_millisecond_window_boundaries_are_accepted(microsecond):
    start = datetime(2024, 1, 1, 0, 0, 0, microsecond, tzinfo=timezone.utc)

    document = direct_document(start)

    assert document["window_start"] == start
    assert document["window_end"] == start + timedelta(hours=2)
    assert document["_id"].endswith("2024-01-01T02:00:00." + f"{microsecond:06d}Z")


@pytest.mark.parametrize("field", ["window_start", "window_end"])
@pytest.mark.parametrize("microsecond", [1, 123_456, 999_999])
def test_submillisecond_window_bounds_are_rejected_by_document_builder(field, microsecond):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)
    if field == "window_start":
        start = start.replace(microsecond=microsecond)
        end = end.replace(microsecond=microsecond)
    else:
        end = end.replace(microsecond=microsecond)

    with pytest.raises(ValueError, match=f"{field}.*millisecond precision"):
        direct_document(start, end=end)


@pytest.mark.parametrize("microsecond", [0, 123_000])
def test_document_identity_and_digest_survive_bson_round_trip(microsecond):
    start = datetime(2024, 1, 1, 0, 0, 0, microsecond, tzinfo=timezone.utc)
    document = direct_document(start)

    # Default BSON decoding returns naive datetimes whose documented meaning is UTC.
    decoded = BSON.encode(document).decode()
    decoded_start = decoded["window_start"].replace(tzinfo=timezone.utc)
    decoded_end = decoded["window_end"].replace(tzinfo=timezone.utc)
    rebuilt = _build_document(
        {
            "window_start": decoded_start,
            "window_end": decoded_end,
            "items": decoded["items"],
            "_row_count": decoded["row_count"],
        },
        source_namespace=decoded["source_namespace"],
        source_scope=decoded["source_scope"],
        top_k=decoded["top_k"],
        config_hash=decoded["config_hash"],
        published_at=decoded["published_at"].replace(tzinfo=timezone.utc),
        retention_seconds=0,
    )

    assert decoded["window_start"].microsecond == microsecond
    assert decoded["window_end"].microsecond == microsecond
    assert rebuilt["_id"] == decoded["_id"]
    assert rebuilt["content_sha256"] == decoded["content_sha256"]
    assert rebuilt["config_hash"] == decoded["config_hash"] == document["config_hash"]
    assert rebuilt["items"] == decoded["items"] == document["items"]
    assert rebuilt["window_start"] == decoded_start == document["window_start"]
    assert rebuilt["window_end"] == decoded_end == document["window_end"]


def test_utc_equivalent_offsets_preserve_identity_and_hash():
    utc_start = datetime(2024, 1, 1, 0, 0, 0, 123_000, tzinfo=timezone.utc)
    offset_start = datetime(
        2023, 12, 31, 19, 0, 0, 123_000, tzinfo=timezone(timedelta(hours=-5))
    )

    utc_document = direct_document(utc_start)
    offset_document = direct_document(offset_start)

    assert offset_document["window_start"] == utc_document["window_start"]
    assert offset_document["window_end"] == utc_document["window_end"]
    assert offset_document["_id"] == utc_document["_id"]
    assert offset_document["content_sha256"] == utc_document["content_sha256"]


def test_existing_millisecond_fixture_identity_and_hash_remain_unchanged():
    document = direct_document(datetime(2024, 1, 1, tzinfo=timezone.utc))

    assert document["_id"] == (
        "trend-window:e0b059af7cf1417f20691e5ef8ac9c5039622f90e8342a96d307b8ac5b35996a:"
        "2024-01-01T02:00:00.000000Z"
    )
    assert document["config_hash"] == "767f9a84f5c826bbe0705c265c090691039e9823ddd81e853d5346d19f5162ce"
    assert document["content_sha256"] == "942795d87ff1332f9573a0a88ee8133af6b9fb0fcd823ab9ec7429608526a866"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("window_start", None),
        ("window_start", "2024-01-01T00:00:00Z"),
        ("window_end", None),
        ("window_end", "2024-01-01T02:00:00Z"),
    ],
)
def test_document_builder_rejects_missing_or_non_datetime_bounds(field, value):
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)
    if field == "window_start":
        start = value
    else:
        end = value

    with pytest.raises(ValueError, match=f"{field} must be a timestamp"):
        direct_document(start, end=end)


def test_document_builder_rejects_nonincreasing_bounds():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="window_end must be later than window_start"):
        direct_document(start, end=start)


def test_spark_window_on_five_minute_slide_remains_publishable(spark_session, pymongo_update_one):
    start = datetime(2024, 1, 1, 0, 5)
    collection = FakeCollection()

    report = publish(spark_session, collection, window_rows(start, [11, 12]))

    document = next(iter(collection.documents.values()))
    assert report.bundles_created == 1
    assert document["window_start"] == start.replace(tzinfo=timezone.utc)
    assert document["window_end"] == (start + timedelta(minutes=120)).replace(tzinfo=timezone.utc)


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


@pytest.mark.parametrize("field", ["window_start", "window_end"])
def test_writer_rejects_submillisecond_bounds_before_publication_and_preserves_prior_bundle(
    spark_session, pymongo_update_one, field
):
    base = datetime(2024, 1, 1)
    collection = FakeCollection()
    publish(spark_session, collection, window_rows(base, [1]))
    original_documents = {identity: dict(document) for identity, document in collection.documents.items()}
    original_bulk_call_count = len(collection.bulk_calls)

    start = base
    end = base + timedelta(hours=2)
    if field == "window_start":
        start = start.replace(microsecond=123_456)
        end = end.replace(microsecond=123_456)
    else:
        end = end.replace(microsecond=123_456)

    with pytest.raises(TrendBundleValidationError, match=f"{field}.*millisecond precision"):
        publish(spark_session, collection, [(start, end, 2, 20, 40, 0.95, 1)])

    assert collection.documents == original_documents
    assert len(collection.documents) == 1
    assert len(collection.bulk_calls) == original_bulk_call_count


def test_submillisecond_bound_in_later_window_prevents_all_batch_publication(
    spark_session, pymongo_update_one
):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [1])
    later_start = start + timedelta(minutes=5, microseconds=1)
    rows.extend(window_rows(later_start, [2]))
    collection = FakeCollection()

    with pytest.raises(TrendBundleValidationError, match="window_start.*millisecond precision"):
        publish(spark_session, collection, rows, batch_size=1)

    assert collection.documents == {}
    assert collection.bulk_calls == []


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


def test_duplicate_key_with_matching_competing_document_is_an_identical_retry(
    spark_session, pymongo_update_one
):
    rows = window_rows(datetime(2024, 1, 1), [11, 12])
    collection = ControlledBulkErrorCollection()

    report = publish(spark_session, collection, rows)

    stored = next(iter(collection.documents.values()))
    writer_config = config()
    expected = _build_document(
        {
            "window_start": rows[0][0],
            "window_end": rows[0][1],
            "items": [
                {
                    "product_id": row[2],
                    "purchase_count_30m": row[3],
                    "purchase_count_120m": row[4],
                    "trend_score": row[5],
                    "trend_rank": row[6],
                }
                for row in rows
            ],
            "_row_count": len(rows),
        },
        source_namespace="instacart:fixture-a",
        source_scope=DEFAULT_SOURCE_SCOPE,
        top_k=writer_config.mongo_top_k_per_window,
        config_hash=_sha256(_config_payload(writer_config, writer_config.mongo_top_k_per_window)),
        published_at=stored["published_at"],
        retention_seconds=0,
    )
    assert stored["_id"] == expected["_id"]
    assert stored["content_sha256"] == expected["content_sha256"]
    assert report.bundles_created == 0
    assert report.identical_content_retries == 1
    assert report.rejected_publications == 0


def test_direct_duplicate_key_with_matching_document_is_an_identical_retry(
    spark_session, pymongo_update_one
):
    collection = DirectDuplicateKeyCollection()

    report = publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    stored = next(iter(collection.documents.values()))
    assert stored["_id"] in collection.documents
    assert report.bundles_created == 0
    assert report.identical_content_retries == 1
    assert report.rejected_publications == 0


def test_duplicate_key_with_different_stored_digest_remains_a_conflict(
    spark_session, pymongo_update_one
):
    collection = ControlledBulkErrorCollection(conflicting_indices={0})

    with pytest.raises(TrendBundleConflictError, match="conflicting content"):
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    stored = next(iter(collection.documents.values()))
    assert stored["content_sha256"] == "f" * 64
    assert len(collection.documents) == 1


def test_duplicate_key_with_missing_stored_document_is_a_publication_failure(
    spark_session, pymongo_update_one
):
    collection = ControlledBulkErrorCollection(missing_indices={0})

    with pytest.raises(TrendBundlePublicationError, match="missing expected") as raised:
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    assert raised.value.report.bundles_created == 0
    assert collection.documents == {}


def test_partial_duplicate_recovery_does_not_succeed_when_another_window_is_missing(
    spark_session, pymongo_update_one
):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [11]) + window_rows(start + timedelta(minutes=5), [12])
    collection = ControlledBulkErrorCollection(missing_indices={1})

    with pytest.raises(TrendBundlePublicationError, match="missing expected") as raised:
        publish(spark_session, collection, rows)

    assert raised.value.report.windows_processed == 2
    assert raised.value.report.identical_content_retries == 1
    assert len(collection.documents) == 1
    assert next(iter(collection.documents.values()))["content_sha256"]


def test_partial_duplicate_recovery_succeeds_when_all_windows_match_and_accounts_writes(
    spark_session, pymongo_update_one
):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [11]) + window_rows(start + timedelta(minutes=5), [12])
    collection = ControlledBulkErrorCollection(
        inserted_indices={1},
        matched_count=0,
    )

    report = publish(spark_session, collection, rows)

    assert report.windows_processed == 2
    assert report.bundles_created == 1
    assert report.identical_content_retries == 1
    assert report.rejected_publications == 0
    assert len(collection.documents) == 2
    assert {document["content_sha256"] for document in collection.documents.values()}


def test_partial_duplicate_recovery_reports_conflict_even_if_another_window_is_missing(
    spark_session, pymongo_update_one
):
    start = datetime(2024, 1, 1)
    rows = window_rows(start, [11]) + window_rows(start + timedelta(minutes=5), [12])
    collection = ControlledBulkErrorCollection(
        conflicting_indices={0},
        missing_indices={1},
    )

    with pytest.raises(TrendBundleConflictError, match="conflicting content"):
        publish(spark_session, collection, rows)

    assert len(collection.documents) == 1
    assert next(iter(collection.documents.values()))["content_sha256"] == "f" * 64


def test_nonduplicate_bulk_write_error_is_not_suppressed(spark_session, pymongo_update_one):
    collection = ControlledBulkErrorCollection(error_code=121)

    with pytest.raises(TrendBundlePublicationError, match="atomically publish"):
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    assert len(collection.documents) == 1
    assert len(collection.find_calls) == 1


def test_duplicate_on_an_unrelated_unique_index_is_not_suppressed(
    spark_session, pymongo_update_one
):
    collection = ControlledBulkErrorCollection(key_pattern={"another_unique_field": 1})

    with pytest.raises(TrendBundlePublicationError, match="atomically publish"):
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    assert len(collection.documents) == 1
    assert len(collection.find_calls) == 1


def test_duplicate_plus_write_concern_error_is_not_suppressed(spark_session, pymongo_update_one):
    collection = ControlledBulkErrorCollection(
        write_concern_errors=[{"code": 64, "errmsg": "write concern failed"}]
    )

    with pytest.raises(TrendBundlePublicationError, match="atomically publish"):
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    assert len(collection.documents) == 1
    assert len(collection.find_calls) == 1


def test_duplicate_recovery_readback_failure_is_not_suppressed(spark_session, pymongo_update_one):
    collection = ControlledBulkErrorCollection(fail_find_on=2)

    with pytest.raises(TrendBundlePublicationError, match="failed to verify every"):
        publish(spark_session, collection, window_rows(datetime(2024, 1, 1), [11]))

    assert len(collection.documents) == 1
    assert len(collection.find_calls) == 2


def test_matching_digest_with_corrupted_source_metadata_is_rejected(
    spark_session, pymongo_update_one
):
    collection = FakeCollection()
    rows = window_rows(datetime(2024, 1, 1), [11])
    publish(spark_session, collection, rows)
    identity, stored = next(iter(collection.documents.items()))
    corrupted = dict(stored)
    corrupted["source_scope"] = "different-scope"
    collection.documents[identity] = corrupted

    with pytest.raises(TrendBundlePublicationError, match="metadata is corrupted"):
        publish(spark_session, collection, rows)

    assert collection.documents[identity] == corrupted
    assert len(collection.bulk_calls) == 1


def test_writer_does_not_call_dataframe_collect(spark_session, pymongo_update_one, monkeypatch):
    rows = window_rows(datetime(2024, 1, 1), [1, 2])
    collection = FakeCollection()

    def forbidden_collect(_self):
        raise AssertionError("writer must not call DataFrame.collect()")

    monkeypatch.setattr(DataFrame, "collect", forbidden_collect)
    report = publish(spark_session, collection, rows)

    assert report.bundles_created == 1
