"""Dependency-free production Hybrid publisher and serving-reader regression."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def _load_hybrid_test_subjects():
    """Load the pure production publisher and reader without Spark stubs."""
    config_module = importlib.import_module("src.module4.config")
    dashboard_readers = importlib.import_module("src.module4.dashboard.readers")
    hybrid = importlib.import_module("src.module4.hybrid")
    module4_sink = importlib.import_module("src.module4.sink_mongo")
    hybrid_sink = importlib.import_module("src.module4.hybrid.sink_mongo")
    return (
        config_module.Config,
        dashboard_readers.read_hybrid_snapshot,
        hybrid.ALSRecommendation,
        hybrid.CapturedUserSources,
        hybrid.HybridPublicationConflictError,
        hybrid.HybridPublicationPromotionError,
        hybrid.HybridRankingConfig,
        hybrid.ProductMetadata,
        hybrid.TrendRecommendation,
        hybrid.TrendSnapshotReadResult,
        hybrid.VerifiedSourceContext,
        hybrid.prepare_hybrid_snapshot,
        hybrid.publish_hybrid_recommendations,
        hybrid.rank_hybrid_recommendations,
        module4_sink,
        hybrid_sink.FINAL_RECOMMENDATIONS_COLLECTION,
    )


(
    Module4Config,
    read_hybrid_snapshot,
    ALSRecommendation,
    CapturedUserSources,
    HybridPublicationConflictError,
    HybridPublicationPromotionError,
    HybridRankingConfig,
    ProductMetadata,
    TrendRecommendation,
    TrendSnapshotReadResult,
    VerifiedSourceContext,
    prepare_hybrid_snapshot,
    publish_hybrid_recommendations,
    rank_hybrid_recommendations,
    module4_sink,
    FINAL_RECOMMENDATIONS_COLLECTION,
) = _load_hybrid_test_subjects()


USER_ID = 7
AS_OF = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
PROCESSING_TIME = datetime(2026, 10, 17, 8, 0, tzinfo=timezone.utc)
NAMESPACE = "instacart-k8s-demo-v1"
RUN_ID = "module4-run-offline"
MODEL_VERSION = "als-offline-v1"
GRAPH_VERSION = "graph-offline-v1"


class Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    def hint(self, _index):
        return self

    def sort(self, keys):
        field, direction = keys[0]
        self.rows.sort(key=lambda row: row[field], reverse=direction < 0)
        return self

    def limit(self, count):
        self.rows = self.rows[:count]
        return self

    def max_time_ms(self, _value):
        return self

    def close(self):
        return None

    def __iter__(self):
        return iter(self.rows)


class Collection:
    def __init__(self, documents=()):
        self.documents = [dict(document) for document in documents]
        self.indexes = {}
        self.fail_replace_before = False

    def create_index(self, keys, **options):
        self.indexes[options["name"]] = {"key": list(keys), **options}
        return options["name"]

    def index_information(self):
        return self.indexes

    def find(self, query, *, projection=None, **_kwargs):
        rows = [
            dict(document) for document in self.documents
            if all(document.get(key) == value for key, value in query.items())
        ]
        if projection is not None:
            included = {key for key, value in projection.items() if value == 1}
            rows = [{key: value for key, value in row.items() if key in included} for row in rows]
        return Cursor(rows)

    def find_one(self, query, **_kwargs):
        for document in self.documents:
            if all(document.get(key) == value for key, value in query.items()):
                return dict(document)
        return None

    def insert_many(self, documents, *, ordered):
        if ordered is not False:
            raise AssertionError("publisher must use unordered bounded row insertion")
        self.documents.extend(dict(document) for document in documents)

    def insert_one(self, document):
        if self.find_one({"_id": document["_id"]}) is not None:
            raise RuntimeError("duplicate fake _id")
        self.documents.append(dict(document))

    def replace_one(self, query, replacement, *, upsert):
        if upsert is not False:
            raise AssertionError("publisher must use compare-and-set without upsert")
        if self.fail_replace_before:
            raise OSError("injected readiness pointer write failure")
        for index, document in enumerate(self.documents):
            if all(document.get(key) == value for key, value in query.items()):
                self.documents[index] = dict(replacement)
                return SimpleNamespace(matched_count=1)
        return SimpleNamespace(matched_count=0)


class Database:
    def __init__(self):
        active = {
            "_id": "active", "status": "ready", "ready": True,
            "run_id": RUN_ID, "model_version": MODEL_VERSION, "graph_version": GRAPH_VERSION,
            "recommendation_published": True, "graph_published": True,
            "single_writer_policy": "module4_publication_lock",
        }
        self.collections = {
            "module4_snapshot_readiness": Collection([active]),
            FINAL_RECOMMENDATIONS_COLLECTION: Collection(),
        }

    def __getitem__(self, name):
        return self.collections.setdefault(name, Collection())


class Client:
    def __init__(self):
        self.database = Database()

    def __getitem__(self, _name):
        return self.database


def _captured_sources():
    return CapturedUserSources(
        user_id=USER_ID,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=(
            ALSRecommendation(USER_ID, 1, 10.0, 1),
            ALSRecommendation(USER_ID, 2, 4.0, 2),
        ),
        product_catalog=(ProductMetadata(1, "Product one"), ProductMetadata(2, "Product two")),
        missing_prospective_trend_product_ids=(),
        run_id=RUN_ID,
        model_version=MODEL_VERSION,
        graph_version=GRAPH_VERSION,
    )


def _trend(*, scores, as_of, namespace=NAMESPACE, digest):
    end = as_of - timedelta(minutes=1)
    ordered = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    return TrendSnapshotReadResult(
        available=True,
        status="available",
        reason=None,
        source_namespace=namespace,
        source_scope="b4_controlled_trend_sensitivity",
        window_start=end - timedelta(hours=2),
        window_end=end,
        published_at=PROCESSING_TIME,
        expires_at=None,
        schema_version=1,
        config_hash="b" * 64,
        content_sha256=digest,
        trend_recommendations=tuple(
            TrendRecommendation(product_id, score, rank)
            for rank, (product_id, score) in enumerate(ordered, start=1)
        ),
    )


def _prepare(trend, *, as_of):
    captured = _captured_sources()
    config = HybridRankingConfig()
    ranking = rank_hybrid_recommendations(
        USER_ID,
        captured.als_recommendations,
        trend.trend_snapshot,
        captured.product_catalog,
        captured.source_context,
        config,
    )
    return prepare_hybrid_snapshot(captured, trend, ranking, config, as_of_event_time=as_of)


class HybridPublicationGuardTests(unittest.TestCase):
    def test_later_same_source_trend_update_and_all_publication_guards(self):
        client = Client()
        database = client.database

        @contextmanager
        def fake_publication_lock(config, _operation_id, lease_minutes=180):
            self.assertIsInstance(config, Module4Config)
            self.assertEqual(lease_minutes, 180)
            yield client

        base_trend = _trend(scores={1: 0.4, 2: 0.8}, as_of=AS_OF, digest="a" * 64)
        baseline = _prepare(base_trend, as_of=AS_OF)
        config = Module4Config(mongo_database="offline_hybrid_test")
        with patch.object(module4_sink, "publication_lock", fake_publication_lock):
            first = publish_hybrid_recommendations(
                config, baseline, client=client, processing_time_utc=PROCESSING_TIME,
            )
            before = read_hybrid_snapshot(database, USER_ID)
            self.assertEqual(first.status, "published")
            self.assertEqual(before["source"]["trend_source_namespace"], NAMESPACE)

            changed_as_of = AS_OF + timedelta(minutes=2)
            changed_trend = _trend(
                scores={1: 1.0, 2: 0.0}, as_of=changed_as_of, digest="c" * 64,
            )
            changed = _prepare(changed_trend, as_of=changed_as_of)
            published = publish_hybrid_recommendations(
                config, changed, client=client, processing_time_utc=PROCESSING_TIME,
            )
            after = read_hybrid_snapshot(database, USER_ID)
            self.assertEqual(published.status, "published")
            self.assertEqual(changed.trend_source_namespace, NAMESPACE)
            self.assertGreater(changed.as_of_event_time, baseline.as_of_event_time)
            self.assertEqual(after["source"]["trend_source_namespace"], NAMESPACE)
            self.assertEqual(after["serving_version"], published.serving_version)
            before_top, after_top = before["recommendations"][0], after["recommendations"][0]
            self.assertEqual((before_top["product_id"], after_top["product_id"]), (1, 1))
            self.assertNotEqual(before_top["final_score"], after_top["final_score"])

            readiness = database["module4_snapshot_readiness"]
            previous_pointer = readiness.find_one({"_id": f"hybrid:{USER_ID}"})
            same_time = _prepare(
                _trend(scores={1: 0.2, 2: 0.9}, as_of=changed_as_of, digest="d" * 64),
                as_of=changed_as_of,
            )
            with self.assertRaises(HybridPublicationConflictError):
                publish_hybrid_recommendations(
                    config, same_time, client=client, processing_time_utc=PROCESSING_TIME,
                )
            self.assertEqual(readiness.find_one({"_id": f"hybrid:{USER_ID}"}), previous_pointer)

            changed_namespace = _prepare(
                _trend(
                    scores={1: 1.0, 2: 0.0}, as_of=changed_as_of + timedelta(minutes=1),
                    namespace="instacart-k8s-demo-v1-other", digest="e" * 64,
                ),
                as_of=changed_as_of + timedelta(minutes=1),
            )
            with self.assertRaises(HybridPublicationConflictError):
                publish_hybrid_recommendations(
                    config, changed_namespace, client=client, processing_time_utc=PROCESSING_TIME,
                )
            self.assertEqual(readiness.find_one({"_id": f"hybrid:{USER_ID}"}), previous_pointer)

            failed_as_of = changed_as_of + timedelta(minutes=2)
            failed = _prepare(
                _trend(scores={1: 0.7, 2: 0.3}, as_of=failed_as_of, digest="f" * 64),
                as_of=failed_as_of,
            )
            readiness.fail_replace_before = True
            with self.assertRaises(HybridPublicationPromotionError):
                publish_hybrid_recommendations(
                    config, failed, client=client, processing_time_utc=PROCESSING_TIME,
                )
            self.assertEqual(readiness.find_one({"_id": f"hybrid:{USER_ID}"}), previous_pointer)


if __name__ == "__main__":
    unittest.main()
