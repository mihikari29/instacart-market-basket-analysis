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
`final_recommendations`, and `realtime_trending_windows`. The API does not
create collections or indexes. It expects the C6 `uq_serving_user_rank` index
and the ordinary `_id_` index on trend bundles.

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

The trend implementation calls the accepted C5 verifier through an adapter
that translates its namespace/event-time selection into the deterministic
`_id` range documented in D0. The API returns C5's validated metadata and
scores. Missing, expired, and explicitly stale snapshots keep their C5 status
and reason; an integrity error fails closed.

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
read-only account for HTTP requests.
