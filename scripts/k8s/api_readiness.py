"""Read-only authenticated MongoDB and required-index readiness check."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Mapping
import sys
from typing import Any
from urllib.parse import parse_qsl, unquote, unquote_plus, urlsplit


@dataclass(frozen=True, slots=True)
class IndexRequirement:
    keys: tuple[tuple[str, int], ...]
    unique: bool = False
    allow_trend_prefix: bool = False
    allow_name_fallback: bool = False
    reject_sparse: bool = True


def _index_specs() -> dict[str, dict[str, IndexRequirement]]:
    """Read required names and keys from the application contracts."""
    from src.module3.trend_bundle import BUNDLE_COLLECTION_NAME
    from src.module4.dashboard.batch_readers import PRODUCT_COLLECTION, PRODUCT_INDEXES
    from src.module4.dashboard.readers import (
        FINAL_RECOMMENDATIONS_COLLECTION,
        TREND_SELECTION_INDEX_NAME,
    )

    expected: dict[str, dict[str, IndexRequirement]] = {
        PRODUCT_COLLECTION: {
            index_name: IndexRequirement(keys)
            for index_name, keys in PRODUCT_INDEXES.values()
        },
        BUNDLE_COLLECTION_NAME: {
            TREND_SELECTION_INDEX_NAME: IndexRequirement(
                (("source_namespace", 1), ("window_end", -1)),
                allow_trend_prefix=True,
                allow_name_fallback=True,
                # The application accepts sparse trend indexes because valid
                # published documents always have source_namespace.
                reject_sparse=False,
            ),
        },
        FINAL_RECOMMENDATIONS_COLLECTION: {
            "uq_serving_user_rank": IndexRequirement(
                (("serving_version", 1), ("user_id", 1), ("rank", 1)), unique=True
            ),
            "uq_serving_user_product": IndexRequirement(
                (("serving_version", 1), ("user_id", 1), ("product_id", 1)), unique=True
            ),
        },
    }
    return expected


def _simple_collation(index: Mapping[str, Any]) -> bool:
    collation = index.get("collation")
    if collation is None:
        return True
    return isinstance(collation, Mapping) and collation.get("locale") == "simple"


def _compatible_index(index: Any, requirement: IndexRequirement) -> bool:
    if not isinstance(index, Mapping):
        return False
    if index.get("hidden") is True or "partialFilterExpression" in index:
        return False
    if requirement.reject_sparse and index.get("sparse", False):
        return False
    if not _simple_collation(index):
        return False

    key = index.get("key")
    if not isinstance(key, Mapping):
        return False
    if requirement.allow_trend_prefix:
        prefix = tuple(key.items())[: len(requirement.keys)]
        if len(prefix) != len(requirement.keys):
            return False
        if tuple(field for field, _ in prefix) != tuple(field for field, _ in requirement.keys):
            return False
        if any(type(direction) is not int or direction not in (-1, 1) for _, direction in prefix):
            return False
    else:
        try:
            actual_keys = tuple((field, int(direction)) for field, direction in key.items())
        except (OverflowError, TypeError, ValueError):
            return False
        if actual_keys != requirement.keys:
            return False

    return not requirement.unique or index.get("unique") is True


def _authenticated_username(uri: str, database_name: str) -> str:
    """Require credentials and an explicit target database/authSource in the URI."""
    try:
        parsed = urlsplit(uri)
        username = unquote_plus(parsed.username or "")
        password = unquote(parsed.password or "")
        hostname = parsed.hostname
        database = unquote(parsed.path.removeprefix("/"))
        query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("MongoDB URI authentication settings are invalid") from exc

    auth_sources = [value for key, value in query if key.lower() == "authsource"]
    if (
        parsed.scheme not in {"mongodb", "mongodb+srv"}
        or not hostname
        or not username
        or not password
        or database != database_name
        or len(auth_sources) != 1
        or unquote(auth_sources[0]) != database_name
    ):
        raise RuntimeError("MongoDB URI must identify credentials and the configured database")
    return username


def _verify_authenticated_identity(status: Any, username: str, database_name: str) -> None:
    if not isinstance(status, Mapping):
        raise RuntimeError("MongoDB authentication metadata is invalid")
    ok = status.get("ok")
    if not isinstance(ok, (int, float)) or isinstance(ok, bool) or ok != 1:
        raise RuntimeError("MongoDB authentication metadata is invalid")
    auth_info = status.get("authInfo")
    if not isinstance(auth_info, Mapping):
        raise RuntimeError("MongoDB authentication metadata is missing")
    users = auth_info.get("authenticatedUsers")
    if not isinstance(users, list) or users != [{"user": username, "db": database_name}]:
        raise RuntimeError("MongoDB did not authenticate the configured database user")
    roles = auth_info.get("authenticatedUserRoles")
    if (
        not isinstance(roles, list)
        or not roles
        or any(not isinstance(role, Mapping) for role in roles)
        or any(role.get("role") != "read" or role.get("db") != database_name for role in roles)
    ):
        raise RuntimeError("MongoDB user does not have only the configured database read role")


def _verify(client: Any, database_name: str, username: str) -> None:
    database = client[database_name]
    auth_status = database.command({"connectionStatus": 1})
    _verify_authenticated_identity(auth_status, username, database_name)
    client.admin.command("ping")

    for collection_name, required in _index_specs().items():
        indexes = list(database[collection_name].list_indexes())
        for name, requirement in required.items():
            candidates = [item for item in indexes if isinstance(item, Mapping) and item.get("name") == name]
            if requirement.allow_name_fallback:
                candidates.extend(
                    item for item in indexes if isinstance(item, Mapping) and item.get("name") != name
                )
            if not any(_compatible_index(item, requirement) for item in candidates):
                raise RuntimeError("required index is missing or incompatible")


def main() -> int:
    client = None
    try:
        from pymongo import MongoClient
        from src.module4.dashboard.config import ApiConfig

        config = ApiConfig.from_environment()
        username = _authenticated_username(config.mongodb_uri, config.mongo_database)
        client = MongoClient(
            config.mongodb_uri,
            serverSelectionTimeoutMS=config.server_selection_timeout_ms,
            connectTimeoutMS=config.connect_timeout_ms,
            socketTimeoutMS=config.socket_timeout_ms,
            appname="it4931-k8s-api-readiness",
        )
        _verify(client, config.mongo_database, username)
    except Exception:
        # Never echo a Mongo URI, credentials, driver exception, or server reply.
        print("API MongoDB readiness check failed.", file=sys.stderr)
        return 1
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
    print("API MongoDB connection and required indexes are ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
