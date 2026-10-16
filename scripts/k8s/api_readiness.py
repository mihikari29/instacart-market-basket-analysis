"""Read-only authenticated MongoDB and required-index readiness check."""

from __future__ import annotations

import sys

from pymongo import MongoClient

from src.module4.dashboard.batch_readers import PRODUCT_COLLECTION, PRODUCT_INDEXES
from src.module4.dashboard.config import ApiConfig
from src.module4.dashboard.readers import (
    FINAL_RECOMMENDATIONS_COLLECTION,
    TREND_BUNDLE_COLLECTION,
    TREND_SELECTION_INDEX_NAME,
)


def _index_specs() -> dict[str, dict[str, tuple[tuple[str, int], ...] | bool]]:
    expected: dict[str, dict[str, tuple[tuple[str, int], ...] | bool]] = {
        PRODUCT_COLLECTION: {
            index_name: keys for index_name, keys in PRODUCT_INDEXES.values()
        },
        TREND_BUNDLE_COLLECTION: {
            TREND_SELECTION_INDEX_NAME: (("source_namespace", 1), ("window_end", -1)),
        },
        FINAL_RECOMMENDATIONS_COLLECTION: {
            "uq_serving_user_rank": (("serving_version", 1), ("user_id", 1), ("rank", 1)),
            "uq_serving_user_product": (
                ("serving_version", 1), ("user_id", 1), ("product_id", 1)
            ),
        },
    }
    return expected


def _verify(client: MongoClient, database_name: str) -> None:
    database = client[database_name]
    client.admin.command("ping")
    for collection_name, required in _index_specs().items():
        indexes = {item["name"]: item for item in database[collection_name].list_indexes()}
        for name, expected in required.items():
            item = indexes.get(name)
            if item is None:
                raise RuntimeError("required index is missing")
            expected_keys = expected if isinstance(expected, tuple) else None
            if expected_keys is not None and tuple(item["key"].items()) != expected_keys:
                raise RuntimeError("required index has an incompatible key")
            if name.startswith("uq_") and item.get("unique") is not True:
                raise RuntimeError("required unique index is not unique")


def main() -> int:
    client = None
    try:
        config = ApiConfig.from_environment()
        client = MongoClient(
            config.mongodb_uri,
            serverSelectionTimeoutMS=config.server_selection_timeout_ms,
            connectTimeoutMS=config.connect_timeout_ms,
            socketTimeoutMS=config.socket_timeout_ms,
            appname="it4931-k8s-api-readiness",
        )
        _verify(client, config.mongo_database)
    except Exception:
        # Never echo a Mongo URI, credentials, driver exception, or server reply.
        print("API MongoDB readiness check failed.", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.close()
    print("API MongoDB connection and required indexes are ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
