# Module 4D D1A Read API

D1A provides a read-only API for the immutable per-user Hybrid snapshot and one
verified finalized trend bundle. It does not publish recommendations or read
the legacy mutable `realtime_trending` collection.

## Backend configuration

Set these values in the API process environment. Keep the MongoDB URI and
credentials on the backend; do not put them in browser configuration, source
control, or request URLs.

```text
MODULE4D_MONGODB_URI=<MongoDB URI for a read-only account>
MODULE4D_MONGO_DATABASE=<explicit database name>
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated exact namespace allowlist>
```

Startup fails if any value is missing. There is no project database fallback.
The account needs read access to `module4_snapshot_readiness`,
`final_recommendations`, and `realtime_trending_windows`, including permission
to inspect index metadata. The API does not create collections or indexes. It
expects the C6 `uq_serving_user_rank` index and the ordinary `_id_` index on
trend bundles. Trend selection also requires the administrator-provisioned
compound index described below.

## Local demonstration

Install the pinned dependencies into the API environment, then run this
localhost-only command from the repository root:

```bash
python3 -m uvicorn src.module4d.api:app --host 127.0.0.1 --port 8000
```

The service exposes `GET /health`,
`GET /api/v1/recommendations/{user_id}`, and
`GET /api/v1/trends?source_namespace=...&as_of_event_time=...`. A trend age
limit is optional and applies only when sent explicitly. The health route is
process liveness and does not ping MongoDB.

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

Missing, expired, and explicitly stale snapshots keep their C5 status and
reason. Event-time staleness is evaluated only when `max_event_age_seconds` is
provided. Processing-time `expires_at` is a separate expiration condition; an
expired newest bundle is reported as expired even when an older bundle remains.

The unchanged C5 import graph imports PySpark modules. The API image therefore
uses the existing project image that already contains PySpark. API startup and
requests do not create a Spark session, launch Spark jobs, or start Kafka or
HDFS. This keeps the accepted C5 implementation unchanged.

## Reproducible validation

Run all network-free API and selected Module 4C regression tests, Ruff, and
then the isolated MongoDB integration suite with:

```bash
python3 scripts/module4d_api_validation.py --mode all
```

The script builds a temporary context containing only source, selected tests,
the API dependency pins, and test tooling. Its MongoDB service is version
7.0.16, uses a dedicated network and tmpfs storage, publishes no host port, and
is removed with the test network after the run. The live fixture seeds data
through the accepted C4A and C6 writer contracts, then uses a database-scoped
read-only account for HTTP requests. The administrative fixture client
provisions the required trend selection index. Live tests compare C5 and HTTP
behavior for a malformed newest `_id`, exercise similar namespaces and BSON
millisecond boundaries, inspect an `IXSCAN` explain plan, and verify that a
missing index fails before any trend `find` command.
