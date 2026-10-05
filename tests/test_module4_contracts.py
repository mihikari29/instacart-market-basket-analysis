"""Dependency-light Module 4 contract tests."""

import pytest

from src.module4.als import ALSParams
from src.module4.config import Config
from src.module4.evaluation import _score_lists
from src.module4.sink_mongo import (
    _write_snapshot,
    ensure_graph_indexes,
    graph_identity,
    recommendation_identity,
)


def test_module4_config_paths_and_validation():
    config = Config(root="hdfs://namenode:8020/instacart")
    assert config.table("prior").endswith("/curated/order_products_prior")
    assert config.features().endswith("/features/user_features")
    with pytest.raises(ValueError, match="between 0 and 1"):
        Config(graph_reset_probability=1.0)


def test_als_strength_aliases_are_canonicalised():
    assert ALSParams(strength_col="raw").strength_col == "strength_raw"
    assert ALSParams(strength_col="log").strength_col == "strength_log"


def test_ranking_detail_and_snapshot_identities_are_deterministic():
    assert _score_lists([2, 3], [2, 3]) == pytest.approx((1.0, 1.0))
    assert recommendation_identity("m1", 7, 42) == "m1__7__42"
    assert graph_identity("g1", 42) == "g1__42"


def test_short_ranking_uses_k_for_map_and_ideal_dcg():
    average_precision, ndcg = _score_lists([1], [1, 2, 3, 4, 5], 5)
    assert average_precision == pytest.approx(0.2)
    assert ndcg == pytest.approx(0.3391602052736161)


def test_environment_values_are_overridden_only_when_cli_value_is_present(monkeypatch):
    monkeypatch.setenv("MODULE4_ROOT", "hdfs://nn:8020/custom")
    monkeypatch.setenv("MODULE4_SEED", "17")
    monkeypatch.setenv("MODULE4_SHUFFLE_PARTITIONS", "32")
    config = Config.environment(shuffle_partitions=4)
    assert config.root == "hdfs://nn:8020/custom"
    assert config.seed == 17
    assert config.shuffle_partitions == 4


class _FakeCollection:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self.documents = []
        self.indexes = {"_id_": {"key": [("_id", 1)]}}

    def insert_many(self, documents, ordered=False):
        self.documents.extend(documents)

    def create_index(self, keys, unique=False, name=None):
        self.indexes[name] = {"key": keys, "unique": unique}

    def count_documents(self, query):
        return sum(all(document.get(key) == value for key, value in query.items()) for document in self.documents)

    def aggregate(self, pipeline):
        return []

    def index_information(self):
        return self.indexes

    def rename(self, new_name, dropTarget=False):
        if new_name in self.db.collections and not dropTarget:
            raise ValueError("target exists")
        self.db.collections[new_name] = self
        del self.db.collections[self.name]
        self.name = new_name

    def drop(self):
        self.db.collections.pop(self.name, None)


class _FakeDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCollection(self, name)
        return self.collections[name]


def _graph_docs(run_id="r1"):
    return [
        {
            "_id": "g1__1",
            "graph_version": "g1",
            "run_id": run_id,
            "product_id": 1,
            "pagerank": 0.5,
        }
    ]


def test_snapshot_rerun_converges_and_insert_failure_keeps_active_collection():
    db = _FakeDatabase()
    first = _write_snapshot(
        db, "graph", _graph_docs(), ensure_graph_indexes, "graph_version", "g1", "r1", 1
    )
    assert first["validation_after_promote"]["valid"] is True
    original = list(db["graph"].documents)

    rerun = _write_snapshot(
        db, "graph", _graph_docs(), ensure_graph_indexes, "graph_version", "g1", "r1", 1
    )
    assert rerun["validation_after_promote"]["documents"] == 1
    assert db["graph"].documents == original

    with pytest.raises(RuntimeError, match="inserted 0 documents"):
        _write_snapshot(db, "graph", [], ensure_graph_indexes, "graph_version", "g2", "r2", 1)
    assert db["graph"].documents == original
