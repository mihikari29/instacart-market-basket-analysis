"""Offline publication contract test for Module 2 serving indexes."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class Module2ServingIndexTests(unittest.TestCase):
    def test_staging_collection_has_exact_compound_indexes_before_rename(self):
        events = []

        class Column:
            pass

        class DataFrame:
            pass

        class SparkSession:
            pass

        class Window:
            pass

        pyspark = types.ModuleType("pyspark")
        sql = types.ModuleType("pyspark.sql")
        sql.Column = Column
        sql.DataFrame = DataFrame
        sql.SparkSession = SparkSession
        sql.Window = Window
        sql.functions = types.ModuleType("pyspark.sql.functions")
        pyspark.sql = sql

        class Collection:
            def __init__(self, name):
                self.name = name

            def drop(self):
                events.append(("drop", self.name))

            def insert_many(self, documents):
                events.append(("insert", self.name, len(documents)))

            def create_index(self, keys, **options):
                events.append(("index", self.name, list(keys), options))

            def rename(self, name, dropTarget):
                events.append(("rename", self.name, name, dropTarget))

        class Database:
            def __init__(self):
                self.collections = {}

            def __getitem__(self, name):
                return self.collections.setdefault(name, Collection(name))

            def create_collection(self, name):
                self.collections[name] = Collection(name)

        class Client:
            def __init__(self, *args, **kwargs):
                self.database = Database()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def get_default_database(self, default):
                return self.database

        pymongo = types.ModuleType("pymongo")
        pymongo.MongoClient = Client

        class Row:
            def __init__(self, values):
                self.values = values

            def asDict(self):
                return dict(self.values)

        class Rows:
            def __init__(self, values):
                self.values = values

            def toLocalIterator(self):
                return iter(self.values)

        spec = spec_from_file_location("module2_analytics_index_test", ROOT / "src/module2/analytics.py")
        module = module_from_spec(spec)
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql, "pymongo": pymongo}):
            assert spec.loader is not None
            spec.loader.exec_module(module)
            module.publish_mongo(
                {
                    "batch_product_metrics": Rows([Row({"product_id": 3, "purchase_count": 4, "reorder_rate": 0.5})]),
                    "department_metrics": Rows([Row({"department_id": 2, "purchase_count": 4, "reorder_rate": 0.5})]),
                },
                "mongodb://publisher.invalid/database",
                "offline-run",
            )

        product_events = [event for event in events if len(event) > 1 and event[1] == "batch_product_metrics__offline-run"]
        self.assertEqual(
            product_events[-3:],
            [
                ("index", "batch_product_metrics__offline-run", [("purchase_count", -1), ("product_id", 1)], {"name": "idx_batch_product_purchase_count_desc_product_id_asc"}),
                ("index", "batch_product_metrics__offline-run", [("reorder_rate", -1), ("product_id", 1)], {"name": "idx_batch_product_reorder_rate_desc_product_id_asc"}),
                ("rename", "batch_product_metrics__offline-run", "batch_product_metrics", True),
            ],
        )


if __name__ == "__main__":
    unittest.main()
