"""Bounded MongoDB capture of a verified Module 4A/4B source pair."""

from __future__ import annotations
from ...common.runtime import positive_int

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import math
import re
from numbers import Real
from typing import Any
import uuid

from src.module4.config import Config as Module4Config
from src.module4.sink_mongo import publication_lock

from .config import ALSRecommendation, ProductMetadata, VerifiedSourceContext


MAX_SOURCE_RECOMMENDATIONS = 20
MAX_PROSPECTIVE_TREND_PRODUCTS = 20
READINESS_COLLECTION = "module4_snapshot_readiness"
READINESS_ID = "active"
READINESS_LOCK_POLICY = "module4_publication_lock"

ALS_PROJECTION = {
    "_id": 0,
    "user_id": 1,
    "product_id": 1,
    "als_score": 1,
    "als_rank": 1,
    "model_version": 1,
    "run_id": 1,
}
GRAPH_PROJECTION = {
    "_id": 0,
    "product_id": 1,
    "product_name": 1,
    "pagerank": 1,
    "graph_version": 1,
    "run_id": 1,
}


class SourceCaptureError(RuntimeError):
    """Base error for failed or invalid Module 4 source captures."""


class ReadinessContractError(SourceCaptureError):
    """The active A/B readiness pointer is missing or inconsistent."""


class SourceSnapshotError(SourceCaptureError):
    """A captured ALS or graph row violates its published source contract."""


class SourceQueryError(SourceCaptureError):
    """A bounded MongoDB source query failed."""


class SourceLockError(SourceCaptureError):
    """The accepted Module 4 publication lock could not be held safely."""


@dataclass(frozen=True, slots=True)
class CapturedUserSources:
    """Detached data for one user, ready to pass into the C1-C2 ranker."""

    user_id: int
    source_context: VerifiedSourceContext
    als_recommendations: tuple[ALSRecommendation, ...]
    product_catalog: tuple[ProductMetadata, ...]
    missing_prospective_trend_product_ids: tuple[int, ...]
    run_id: str
    model_version: str
    graph_version: str


@dataclass(frozen=True, slots=True)
class _SnapshotIdentity:
    run_id: str
    model_version: str
    graph_version: str



def _finite_score(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise SourceSnapshotError("ALS als_score must be a finite number")
    try:
        score = float(value)
    except (OverflowError, ValueError) as exc:
        raise SourceSnapshotError("ALS als_score must be a finite number") from exc
    if not math.isfinite(score):
        raise SourceSnapshotError("ALS als_score must be a finite number")
    return score


def _version(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", value)
        or ".." in value
    ):
        raise ReadinessContractError(f"readiness {field} is missing or invalid")
    return value


def readiness_identity(document: Any) -> _SnapshotIdentity:
    if not isinstance(document, Mapping):
        raise ReadinessContractError("active A/B readiness document is missing")
    if document.get("_id") != READINESS_ID:
        raise ReadinessContractError("readiness document does not have _id='active'")
    if document.get("status") != "ready":
        raise ReadinessContractError("active A/B readiness status is not 'ready'")
    if document.get("ready") is not True:
        raise ReadinessContractError("active A/B readiness ready field must be boolean true")

    run_id = _version(document.get("run_id"), "run_id")
    model_version = _version(document.get("model_version"), "model_version")
    graph_version = _version(document.get("graph_version"), "graph_version")

    if document.get("recommendation_published") is not True:
        raise ReadinessContractError("active A/B readiness has no complete ALS publication")
    if document.get("graph_published") is not True:
        raise ReadinessContractError("active A/B readiness has no complete graph publication")
    if document.get("single_writer_policy") != READINESS_LOCK_POLICY:
        raise ReadinessContractError("active A/B readiness has an unexpected single-writer policy")
    if document.get("error"):
        raise ReadinessContractError("ready A/B readiness document contains an error")

    return _SnapshotIdentity(run_id, model_version, graph_version)


def _read_active_identity(database: Any) -> _SnapshotIdentity:
    try:
        document = database[READINESS_COLLECTION].find_one({"_id": READINESS_ID})
    except Exception as exc:
        raise SourceQueryError("failed to read the active A/B readiness pointer") from exc
    return readiness_identity(document)


def _bounded_documents(
    collection: Any,
    query: dict[str, Any],
    projection: dict[str, int],
    sort: list[tuple[str, int]],
    limit: int,
    source_name: str,
) -> list[Mapping[str, Any]]:
    try:
        cursor = collection.find(query, projection).sort(sort).limit(limit)
        documents = list(cursor)
    except Exception as exc:
        raise SourceQueryError(f"bounded {source_name} source query failed") from exc
    if any(not isinstance(document, Mapping) for document in documents):
        raise SourceSnapshotError(f"{source_name} query returned a non-document row")
    return documents


def _read_als_recommendations(
    database: Any,
    config: Module4Config,
    identity: _SnapshotIdentity,
    user_id: int,
) -> tuple[ALSRecommendation, ...]:
    query = {
        "model_version": identity.model_version,
        "run_id": identity.run_id,
        "user_id": user_id,
    }
    rows = _bounded_documents(
        database[config.mongo_recommendation_collection],
        query,
        ALS_PROJECTION,
        [("als_rank", 1), ("product_id", 1)],
        MAX_SOURCE_RECOMMENDATIONS + 1,
        "ALS",
    )
    if len(rows) > MAX_SOURCE_RECOMMENDATIONS:
        raise SourceSnapshotError("ALS recommendations exceed the published Top-20 bound")

    catalog_sample = _bounded_documents(
        database[config.mongo_graph_collection],
        {"graph_version": identity.graph_version, "run_id": identity.run_id},
        {"_id": 0, "product_id": 1},
        [("product_id", 1)],
        config.recommendation_k,
        "graph/catalog",
    )
    expected_count = min(config.recommendation_k, len(catalog_sample)) if catalog_sample else config.recommendation_k
    if rows and len(rows) != expected_count:
        raise SourceSnapshotError(
            f"ALS recommendations are truncated: found {len(rows)} rows; expected {expected_count}"
        )

    recommendations = []
    product_ids: set[int] = set()
    ranks: set[int] = set()
    for row in rows:
        if row.get("model_version") != identity.model_version:
            raise SourceSnapshotError("ALS row has a different model_version than the active pointer")
        if row.get("run_id") != identity.run_id:
            raise SourceSnapshotError("ALS row has a different run_id than the active pointer")
        row_user_id = positive_int(row.get("user_id"), "ALS user_id", error=SourceSnapshotError)
        if row_user_id != user_id:
            raise SourceSnapshotError("ALS row user_id does not match the requested user")
        product_id = positive_int(row.get("product_id"), "ALS product_id", error=SourceSnapshotError)
        rank = positive_int(row.get("als_rank"), "ALS als_rank", error=SourceSnapshotError)
        if rank > MAX_SOURCE_RECOMMENDATIONS:
            raise SourceSnapshotError("ALS als_rank exceeds the published Top-20 bound")
        score = _finite_score(row.get("als_score"))
        if product_id in product_ids:
            raise SourceSnapshotError(f"ALS recommendations contain duplicate product_id {product_id}")
        if rank in ranks:
            raise SourceSnapshotError(f"ALS recommendations contain duplicate als_rank {rank}")
        product_ids.add(product_id)
        ranks.add(rank)
        recommendations.append(ALSRecommendation(user_id, product_id, score, rank))

    if ranks and ranks != set(range(1, len(ranks) + 1)):
        raise SourceSnapshotError("ALS recommendation ranks are not contiguous from 1")
    recommendations.sort(key=lambda row: (row.als_rank, row.product_id))
    return tuple(recommendations)


def _prospective_ids(product_ids: Iterable[int]) -> tuple[int, ...]:
    if isinstance(product_ids, (str, bytes, Mapping)):
        raise SourceSnapshotError("prospective trend product IDs must be an iterable of integers")
    try:
        iterator = iter(product_ids)
    except TypeError as exc:
        raise SourceSnapshotError("prospective trend product IDs must be iterable") from exc

    values = []
    for _ in range(MAX_PROSPECTIVE_TREND_PRODUCTS + 1):
        try:
            values.append(next(iterator))
        except StopIteration:
            break
        except Exception as exc:
            raise SourceSnapshotError("could not read prospective trend product IDs") from exc
    if len(values) > MAX_PROSPECTIVE_TREND_PRODUCTS:
        raise SourceSnapshotError("prospective trend product IDs exceed the bounded Top-20 input")
    validated = tuple(positive_int(value, "prospective trend product_id", error=SourceSnapshotError) for value in values)
    if len(set(validated)) != len(validated):
        raise SourceSnapshotError("prospective trend product IDs contain duplicates")
    return tuple(sorted(validated))


def _read_graph_catalog(
    database: Any,
    config: Module4Config,
    identity: _SnapshotIdentity,
    als_product_ids: tuple[int, ...],
    prospective_trend_product_ids: tuple[int, ...],
) -> tuple[tuple[ProductMetadata, ...], tuple[int, ...]]:
    requested_ids = tuple(sorted(set(als_product_ids).union(prospective_trend_product_ids)))
    if not requested_ids:
        return (), ()

    query = {
        "graph_version": identity.graph_version,
        "run_id": identity.run_id,
        "product_id": {"$in": list(requested_ids)},
    }
    rows = _bounded_documents(
        database[config.mongo_graph_collection],
        query,
        GRAPH_PROJECTION,
        [("product_id", 1)],
        len(requested_ids) + 1,
        "graph/catalog",
    )
    if len(rows) > len(requested_ids):
        raise SourceSnapshotError("graph query returned more rows than the bounded product ID set")

    products: dict[int, str] = {}
    pageranks: dict[int, float] = {}
    requested_set = set(requested_ids)
    for row in rows:
        if row.get("graph_version") != identity.graph_version:
            raise SourceSnapshotError("graph row has a different graph_version than the active pointer")
        if row.get("run_id") != identity.run_id:
            raise SourceSnapshotError("graph row has a different run_id than the active pointer")
        product_id = positive_int(row.get("product_id"), "graph product_id", error=SourceSnapshotError)
        if product_id not in requested_set:
            raise SourceSnapshotError("graph row product_id was not requested")
        product_name = row.get("product_name")
        if not isinstance(product_name, str) or not product_name.strip():
            raise SourceSnapshotError(f"graph product {product_id} has an invalid product_name")
        if product_id in products:
            raise SourceSnapshotError(f"graph catalog contains duplicate product_id {product_id}")
        products[product_id] = product_name
        pr_val = row.get("pagerank")
        if isinstance(pr_val, (int, float)):
            pageranks[product_id] = float(pr_val)

    missing_als = tuple(sorted(set(als_product_ids).difference(products)))
    if missing_als:
        raise SourceSnapshotError(f"ALS candidates are missing graph/catalog metadata: {list(missing_als)}")

    missing_prospective = tuple(
        sorted(set(prospective_trend_product_ids).difference(products).difference(als_product_ids))
    )
    catalog = tuple(
        ProductMetadata(product_id, products[product_id], pageranks.get(product_id))
        for product_id in sorted(products)
    )
    return catalog, missing_prospective


def capture_verified_user_sources(
    config: Module4Config,
    user_id: int,
    prospective_trend_product_ids: Iterable[int] = (),
) -> CapturedUserSources:
    """Capture a bounded user-specific ALS/catalog view from the active A/B pair.

    The accepted publication lock covers readiness reads and the bounded
    collection queries only. No trend rows are read, and no ranking work is
    performed while holding the lock. ``config.recommendation_k`` must match
    the accepted publication; the readiness pointer does not store that K.
    """
    requested_user_id = positive_int(user_id, "requested user_id", error=SourceSnapshotError)
    if (
        type(config.recommendation_k) is not int
        or config.recommendation_k < 1
        or config.recommendation_k > MAX_SOURCE_RECOMMENDATIONS
    ):
        raise ValueError("Module 4 recommendation_k must be an integer between 1 and 20")
    trend_ids = _prospective_ids(prospective_trend_product_ids)
    operation_id = f"module4c-read-{uuid.uuid4().hex}"

    try:
        with publication_lock(config, operation_id) as client:
            database = client[config.mongo_database]
            identity = _read_active_identity(database)
            recommendations = _read_als_recommendations(database, config, identity, requested_user_id)
            als_product_ids = tuple(row.product_id for row in recommendations)
            product_catalog, missing_trend_ids = _read_graph_catalog(
                database,
                config,
                identity,
                als_product_ids,
                trend_ids,
            )
            confirmed_identity = _read_active_identity(database)
            if confirmed_identity != identity:
                raise ReadinessContractError("active A/B source identity changed during bounded capture")
    except SourceCaptureError:
        raise
    except Exception as exc:
        raise SourceLockError("failed during the accepted Module 4 publication lock scope") from exc

    return CapturedUserSources(
        user_id=requested_user_id,
        source_context=VerifiedSourceContext(als_available=True, graph_available=True),
        als_recommendations=recommendations,
        product_catalog=product_catalog,
        missing_prospective_trend_product_ids=missing_trend_ids,
        run_id=identity.run_id,
        model_version=identity.model_version,
        graph_version=identity.graph_version,
    )
