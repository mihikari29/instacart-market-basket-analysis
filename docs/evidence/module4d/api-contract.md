# Module 4D Read API (D1A and D1B)

The read-only FastAPI service provides the D1A verified per-user recommendation
and finalized-trend views, plus D1B Module 2 batch analytics and curated
archived evidence. It does not publish recommendations, modify MongoDB, or read
the legacy mutable `realtime_trending` collection.

## Backend configuration

Set these values in the API process environment. Keep the MongoDB URI and
credentials on the backend; do not put them in browser configuration, source
control, or request URLs.

```text
MODULE4D_MONGODB_URI=<MongoDB URI for a read-only account>
MODULE4D_MONGO_DATABASE=<explicit database name>
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated exact namespace allowlist>
MODULE4D_APP_ROOT=<trusted application root; defaults to the packaged API root>
```

Startup fails if any value is missing. There is no project database fallback.
The account needs read access to `module4_snapshot_readiness`,
`final_recommendations`, `realtime_trending_windows`, `batch_product_metrics`,
and `department_metrics`, including permission to inspect index metadata. The
API does not create collections or indexes. It expects the C6
`uq_serving_user_rank` index, the ordinary `_id_` index on trend bundles, the
trend selection index below, and the two D1B product-ranking indexes below.
Grant no write, create-index, rename, or drop privileges to the API account.

## Local demonstration

Install the pinned dependencies into the API environment, then run this
localhost-only command from the repository root:

```bash
python3 -m uvicorn src.module4.dashboard.api:app --host 127.0.0.1 --port 8000
```

The service exposes `GET /health`, the D1A routes
`GET /api/v1/recommendations/{user_id}` and `GET /api/v1/trends`, and the D1B
routes `GET /api/v1/batch/products`, `GET /api/v1/batch/departments`,
`GET /api/v1/evidence`, `GET /api/v1/evidence/{evidence_id}`,
`GET /api/v1/analytics/orders-distribution`, and `GET /api/v1/evaluation/als`.
The health route is process liveness and does not ping MongoDB. The application
OpenAPI schema describes all D1A and D1B routes.
For the consolidated package paths and Docker commands, see the
[Module 4 package migration guide](../../module4-plan.md).

Recommendation reads return `404` when no per-user pointer is published,
`503` when the pointer is not ready or MongoDB is unavailable, `409` when the
pointer changes during a read, and `502` when persisted rows fail validation.
The pointer/rows/pointer protocol detects races; it is not a MongoDB
multi-document transaction. The Hybrid reader never consults the A/B `_id="active"`
pointer, because the stored Hybrid snapshot can validly reference an older A/B
generation.

The trend implementation delegates candidate selection and integrity checks
to the accepted C5 verifier. Its collection adapter passes C5's exact
`source_namespace` and `window_end <= query_cutoff` predicate, descending
`window_end` sort, and one-document limit through unchanged. It only adds
`maxTimeMS`, closes the cursor, and hints a compatible index selected from
read-only index metadata. This preserves which document C5 selects: a newest
malformed or corrupted bundle returns `502 invalid_source_document`, and the
reader never falls back to an older bundle.

### Required trend query index

The Module 3 bundle writer maintains the optional `expires_at` TTL index, but
does not create the index needed for namespace/time selection. An administrator
must provision the following index in the configured database before starting
the API:

```javascript
db.realtime_trending_windows.createIndex(
  { source_namespace: 1, window_end: -1 },
  { name: "idx_source_namespace_window_end_desc" }
)
```

For a local academic demo, connect `mongosh` with an administrator URI scoped
to the same database configured by `MODULE4D_MONGO_DATABASE`, run the command
above, then start the API with its database-scoped read-only account. Do not
grant index-management rights to the API account. The API checks index metadata
on each trend request, accepts only a compatible non-hidden, non-partial index
with `source_namespace` and `window_end` as its first two keys, and hints that
index. If it cannot inspect indexes, the request returns a sanitized
`503 service_unavailable`. If no suitable index exists, it returns
`503 trend_index_unavailable` before issuing a data query; it never falls back
to a collection scan or the former `_id` range.

## Batch analytics

`GET /api/v1/batch/products` defaults to `order=purchase_count&limit=20`; the
only order values are `purchase_count` and `reorder_rate`, both descending, and
the hard page limit is 50. Equal metric values are ordered by `product_id`
ascending. Each query uses a matching explicit projection, index hint,
`maxTimeMS`, and bounded cursor. Product names are omitted because the Module 2
Mongo collection does not publish them. Product `reorder_rate` is
`reorder_count / purchase_count` over prior order-product rows.

An administrator must create both indexes in the API's configured database:

```javascript
db.batch_product_metrics.createIndex(
  { purchase_count: -1, product_id: 1 },
  { name: "idx_batch_product_purchase_count_desc_product_id_asc" }
)
db.batch_product_metrics.createIndex(
  { reorder_rate: -1, product_id: 1 },
  { name: "idx_batch_product_reorder_rate_desc_product_id_asc" }
)
```

Module 2 promotes a temporary collection that only has its single-field metric
indexes. Its rename replaces the previous collection and its indexes, so run
these administrative commands after every Module 2 publication and before
serving product rankings. The API checks index metadata and hints the exact
named compound index. If it is missing, hidden, sparse, partial, or incompatible,
the API returns `503 batch_index_unavailable` before querying rows. It never
creates an index or silently sorts an unbounded source.

Example item (other items in the response use the same schema):

```json
{
  "product_id": 24852,
  "purchase_count": 472565,
  "unique_users": 73956,
  "reorder_count": 398609,
  "reorder_rate": 0.8435008940569022,
  "run_id": "<source run>",
  "last_batch_time": "<UTC timestamp>"
}
```

`GET /api/v1/batch/departments` reads at most 22 documents, ordered by the
default `_id` index, validates the 1–21 department ID contract, then orders at
most 21 results by purchase count descending and department ID ascending. More
than 21 documents or invalid counts/rates/publication metadata return
`502 invalid_source_document`; the endpoint does not return a truncated global
ranking. Both batch routes use prior-only purchase metrics and carry that
collection's own `run_id` and `last_batch_time`. They do not join the two
collections or claim that their publications were atomic. Product rows return
`200 []` for a valid empty collection; empty sources have no fabricated run ID
or publication time.

## Curated evidence and report endpoints

The API accepts only five stable evidence IDs and fixed repository-relative
artifacts:

| Evidence ID | Registered artifact | Classification |
|---|---|---|
| `module2-full-analytics` | `docs/evidence/full/analytics.json` | Full public dataset analytics |
| `module2-full-hardening-benchmarks` | `docs/evidence/full/current-hardening-2026-10-03.json` | One dated full-data Module 2 benchmark run and environment |
| `module4a-full-als-evaluation` | `docs/evidence/module4/full-ab-20261010-fixed-first/evaluation.json` | Official-train ALS and baseline evaluation |
| `module4c-c8a-responsiveness` | `docs/evidence/module4c/c8a-receipt.json` | Controlled synthetic responsiveness experiment |
| `module4c-c8b-streaming-demo` | `docs/evidence/module4c/c8b-receipt.json` | Bounded synthetic Kafka/Spark demonstration |

`GET /api/v1/evidence` returns the five registry entries, availability state,
classification, safe source label, and content digest. Detail responses expose
curated fields only; they do not return arbitrary raw JSON or artifact paths.
The root is the configured `MODULE4D_APP_ROOT` (by default, the source or
packaged runtime root); resolved allowlist files must remain beneath it. Each
artifact is capped at 256 KiB and schema-validated on read. The Dockerfile and
reproducible validation context copy only these five evidence files, not
`results/` or `.git`.

`GET /api/v1/analytics/orders-distribution` returns day-of-week bins 0–6 and
hour bins 0–23 from the Module 2 archived report. Counts are validated as
nonnegative integers and each distribution must total the archived order
count. These are **all-order counts**, not prior-only item purchase counts.
Department purchase and product metrics remain prior-only.

`GET /api/v1/evaluation/als` keeps each recorded cohort nested by model and K,
with Precision@K, Recall@K, MAP@K, NDCG@K, coverage and evaluated-user counts
where present. The accepted full-data all-user official-train Recall@10 is
0.121472 for ALS and 0.319232 for personal-frequency. The report states that
official test truth is unavailable. The allowlisted evaluation does not record
a separate ALS training-method field; the API labels it as unrecorded. No
metrics are recalculated or represented as Hybrid scores.

Benchmark evidence identifies one run and environment. Broadcast-hash and
sort-merge join medians remain separate; full scan, date filter and partition
filter results keep their recorded scopes; cache materialization and cached
reuse are different metrics. The selected artifact contains no structured
shuffle statistics, so shuffle is `available: false`. C8A does not measure
recommendation accuracy. C8B is archived synthetic demonstration evidence,
not live streaming data; its named acknowledgement-to-bundle-visible interval
is not Kafka lag or generic end-to-end latency.

Unknown IDs return `404 unknown_evidence_id`. A registered but missing packaged
file returns `503 evidence_unavailable`; malformed JSON, oversized files,
path escapes or schema-invalid values return sanitized `502 invalid_evidence`.
The registry reports `available`, `missing` or `invalid` without exposing
absolute paths. Batch Mongo failures return `503 service_unavailable`; invalid
batch documents return `502 invalid_source_document`.

The D1B API does not provide product-by-hour/day aggregates, live department
hour/day charts, Kafka lag, live Module 3 progress, generic event-to-serving
latency, or Hybrid recommendation-quality evaluation. The evidence source has
an archived department-hour pivot, but D1B does not expose it as a live chart.
These should remain labeled unavailable until a bounded, registered source and
contract are added.

The future Streamlit frontend should call only these HTTP routes, show each
source's scope/run/time labels, and never connect to MongoDB or accept caller
filters, sort definitions, collection names, or filesystem paths.

Missing, expired, and explicitly stale snapshots keep their C5 status and
reason. Event-time staleness is evaluated only when `max_event_age_seconds` is
provided. Processing-time `expires_at` is a separate expiration condition; an
expired newest bundle is reported as expired even when an older bundle remains.

The unchanged C5 import graph imports PySpark modules. The canonical root
`Dockerfile` therefore builds `api` from its internal `spark` stage, which
already contains PySpark. Build it with `docker build --target api`; no
separately tagged Spark image is required. API startup and
requests do not create a Spark session, launch Spark jobs, or start Kafka or
HDFS. This keeps the accepted C5 implementation unchanged.

The shared context allowlist and separate runtime targets are documented in
[the Docker runbook](../../docker.md). Compose continues to configure the API with a
database-scoped read identity; administrators provision its indexes separately.

## Reproducible validation

Run the network-free D1A/D1B API and selected Module 4C regression tests,
repository-wide Ruff, runtime import/OpenAPI smoke, and the isolated MongoDB
integration suites with:

```bash
python3 scripts/dashboard/api_validation.py --mode all
```

The script builds the pinned Spark/PyMongo API runtime with the exact evidence
allowlist. MongoDB 7.0.16 runs on a dedicated task network with no host ports
and tmpfs storage. The D1B fixture provisions both product indexes as
administrator, seeds disposable snapshots, and serves requests with a
database-scoped read-only account. It checks actual winning plans for `IXSCAN`
with no `COLLSCAN` or blocking `SORT`, verifies a missing index fails before
`find`, and attempts denied writes/index creation. The D1A integration test
continues to exercise recommendation/trend compatibility. The script removes
its labeled containers, network and temporary image, then verifies cleanup.
