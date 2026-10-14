# Module 4D Streamlit Dashboard (D2B)

## Layout and boundary

The Streamlit UI is under `src/module4/dashboard/ui/`. Its HTTP client exposes
only the fixed Module 4D GET routes used by the MVP. The browser talks to
Streamlit; Streamlit calls FastAPI; only FastAPI reads MongoDB or registered
evidence files. The UI package has no PyMongo, Spark, Kafka, HDFS, database
credential, report-file reader, or recommendation publisher dependency.

The dashboard has four pages:

- **Overview** checks `/health` for API process liveness and checks each batch,
  trend, and evidence source independently. It shows each source's own run,
  window, publication and API-observation metadata. Health does not imply that
  MongoDB or another data source is available.
- **Realtime & Batch Analytics** loads a single selected finalized trend
  snapshot, prior-only batch products and departments, and the archived
  all-order day/hour distributions.
- **Hybrid Recommendations** reads one verified per-user Hybrid snapshot and
  displays the persisted ranking, mode, reason, serving identities, source
  timestamps, inclusion flags, and separate raw, normalized and final scores.
- **Evaluation & Performance** reads the archived ALS evaluation, benchmark
  report, and C8A/C8B receipts through the existing FastAPI client. It has
  independent section errors and successful-response caching with an explicit
  page refresh. It does not start a benchmark, evaluation, publisher, Spark,
  Kafka, HDFS, or MongoDB client.

The original D0 plan proposed multi-window trend history and per-user ALS
comparison. D1A/D1B do not expose those capabilities: the implemented trends
route returns one selected window, and the recommendation route returns only
the verified Hybrid snapshot. The UI does not infer missing rows or scores.
Direct ALS-versus-Hybrid comparison needs a separate backend endpoint and
milestone. A full Evaluation & Performance page belongs to D2B; full API/UI
integration and release packaging belong to D3.

## Dependencies and environment

Runtime direct dependencies are pinned in
[`requirements-module4d-ui.txt`](../requirements-module4d-ui.txt):

- Streamlit `1.65.0`
- Requests `2.34.2`

The test environment is defined by
[`requirements-module4d-ui-dev.txt`](../requirements-module4d-ui-dev.txt),
which also pins pytest `8.4.2`. Streamlit's built-in tables and bar charts are
used; no chart library is added directly.

Create an isolated environment and install the UI runtime:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-module4d-ui.txt
```

The isolated runtime can also be built without the Spark image or repository
data artifacts:

```bash
docker build -f Dockerfile.module4d-ui -t instacart-module4d-ui:local .
docker run --rm --network module4-dashboard \
  -p 127.0.0.1:8501:8501 \
  -e MODULE4D_API_BASE_URL=http://module4d-api:8000 \
  -e MODULE4D_TREND_SOURCE_NAMESPACES='your-configured-namespace' \
  instacart-module4d-ui:local
```

The API container must join the same task-scoped network with the DNS name
`module4d-api`. The host UI port remains bound to loopback. The Docker build
context is restricted by `Dockerfile.module4d-ui.dockerignore` to the pinned
requirements and UI package; it excludes datasets, `results/`, and `.git/`.

The UI requires these environment settings:

```text
MODULE4D_API_BASE_URL=http://127.0.0.1:8000
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated namespaces also allowlisted by the API>
```

`MODULE4D_API_BASE_URL` accepts localhost/loopback or the explicit Docker
service hostname `module4d-api`. It rejects credentials, URL paths, query
strings, fragments and external hosts. The UI does not read or accept a MongoDB
URI. Keep backend MongoDB settings in the FastAPI process only.

## Startup and presentation workflow

Start the existing API with its separately configured backend environment. The
API requirements and database/index setup are documented in
[`module4d-api.md`](module4d-api.md). Keep API and UI host bindings on
`127.0.0.1` for a local academic demo.

Then run the UI from the repository root:

```bash
source .venv/bin/activate
export MODULE4D_API_BASE_URL=http://127.0.0.1:8000
export MODULE4D_TREND_SOURCE_NAMESPACES='your-configured-namespace'
streamlit run src/module4/dashboard/ui/app.py \
  --server.address 127.0.0.1 --server.port 8501
```

For a presentation, open the Overview first and review each source state. On
Realtime & Batch Analytics, select an allowed namespace and explicit UTC
as-of time, then submit the form. On Hybrid Recommendations, enter a known
positive user ID with a published snapshot. Use **Refresh dashboard data** to
clear this browser session's cached successful reads; submit the page form to
explicitly load its analytics or user snapshot again. Requests are manual;
widget interaction does not continuously poll the API.

## API routes used

| UI view | Implemented route | Source and scope |
|---|---|---|
| API liveness | `GET /health` | Process liveness only |
| Overview batch cards | `GET /api/v1/batch/products?order=purchase_count&limit=1`, `GET /api/v1/batch/departments` | Separate prior-only item-purchase collections; each has its own run and publication time |
| Overview and analytics trends | `GET /api/v1/trends?source_namespace=…&as_of_event_time=…&max_event_age_seconds=…` | One finalized event-time window; namespace must be configured by the API |
| Overview evidence panel | `GET /api/v1/evidence` | Fixed curated registry; archived classification and availability |
| Product analytics | `GET /api/v1/batch/products?order=purchase_count\|reorder_rate&limit=1..50` | Product IDs and metrics; product names are not supplied |
| Department analytics | `GET /api/v1/batch/departments` | At most 21 department IDs and prior-only metrics; names are not supplied |
| Order distributions | `GET /api/v1/analytics/orders-distribution` | Archived all-order count by source bin 0–6 and hour 0–23 |
| Hybrid recommendation | `GET /api/v1/recommendations/{user_id}` | Verified per-user Hybrid rows selected by the serving readiness pointer |

The Evaluation & Performance page calls these fixed routes:

| Panel | Implemented route | Archived source and interpretation |
|---|---|---|
| ALS evaluation | `GET /api/v1/evaluation/als` | Module 4A official-train evaluation and recorded global-popularity/personal-frequency baselines |
| Evidence status | `GET /api/v1/evidence` | Fixed registry with availability, classification, source label and digest |
| Spark batch performance | `GET /api/v1/evidence/module2-full-hardening-benchmarks` | Module 2 isolated full-public-dataset benchmark run |
| Hybrid responsiveness | `GET /api/v1/evidence/module4c-c8a-responsiveness` | Controlled synthetic C8A ranking response and CLI wall-time receipt |
| Kafka/Spark integration | `GET /api/v1/evidence/module4c-c8b-streaming-demo` | Bounded synthetic C8B event reconciliation, windows, Spark durations and serving assertions |

The evidence client validates response shapes and fixed IDs; it does not accept
arbitrary routes or filesystem paths. The registry and each report load
independently, so one missing report does not hide the other panels.

### Evaluation and performance page

The ALS controls select one of the two recorded primary cohorts, K=5/10/20, a
Precision@K/Recall@K/MAP@K/NDCG@K metric, and one recorded method (ALS, global
popularity, or personal frequency). The chart compares all three methods for
the selected cohort/K/metric. The table retains all four metrics and method
identity; a separate table shows evaluated users and available user/catalog
coverage. History subgroups use their own K and metric controls and remain in a
separate section. The methodology section shows the cohort definition, official
train truth, cohort exclusion, fallback, truth counts, and the absence of
official test truth. Optional coverage fields are displayed as unavailable
when not recorded. The UI does not recalculate scores or call this a Hybrid
evaluation. For all official-train users at K=10, the recorded ALS and personal
frequency Recall@10 values remain 0.121472 and 0.319232 respectively; the UI
states that ALS did not outperform personal frequency on that metric.

The Module 2 panel presents join strategy medians in seconds, a separate scan
comparison with a different-scope caveat for the full scan, and all three cache
phases including materialization cost. Structured shuffle metrics have an
explicit unavailable state. Provenance shows the recorded run, date, isolated
environment, Spark version, warmups, and repetitions. Timings are historical
observations, not production throughput or cluster-scale scalability.

C8A is labeled controlled synthetic responsiveness evidence. Its table shows
lambda 0.0, 0.5, 0.7, 0.9, and 1.0 with both Top-1 identities, changed-rank
count, mean absolute rank displacement, and recorded Top-20 overlap. The page
highlights that the selected shift changed Top-1 at 0.0 and 0.5 but not at the
default 0.7. Baseline and changed-window CLI first-publication medians are
separate and labeled as wall time including process startup and MongoDB serving
but excluding fixture setup. C8A did not measure recommendation accuracy.

C8B is labeled bounded synthetic integration evidence. It shows planned,
acknowledged, consumed, invalid, and finalized-bundle counts as historical
receipt values; the two windows retain event-time bounds, watermark observation,
microbatch, row count, Top-1 product, and full bundle identity in a details
view. Spark `add_batch` and `trigger_execution` sequences are shown in
milliseconds. Serving pointer, prior-row immutability, and repeated-publish
checks are presented as recorded assertions. The report's limitation that
Spark checkpoint restart was not exercised remains visible. No interval is
renamed Kafka lag or generic end-to-end latency.

## Availability, provenance and errors

Batch product and department routes require the named D1B compound product
indexes. If an index is missing, the API returns
`503 batch_index_unavailable`; the UI displays the source as a read failure and
does not show an empty chart. Finalized trend requests require the named trend
selection index and return `503 trend_index_unavailable` if it is absent.
The API also distinguishes no matching trends (`200` with `available: false`
and `status: missing`), expired snapshots, and explicitly stale snapshots.
Event-time age is only checked when the optional maximum age is provided.

The UI preserves empty `200` responses as empty states. It keeps API failures
separate from empty data and displays the HTTP status, stable error code, and
request ID when present. The client enforces connect/read timeouts, a 1 MiB
response cap, JSON content types, response-shape checks and disabled redirects.
Connection details and response bodies are not shown for transport or format
failures. A `409 snapshot_changed` offers a manual retry; no automatic retry
can hide a moving pointer.

Charts name their metric units and source. Trend charts use `trend_score` from
the selected finalized window, with product ID and rank. Batch product and
department charts use prior-only counts or reorder rate, and table rows carry
the endpoint's own run ID and last batch time. Day-of-week labels retain the
source's numeric `order_dow` domain 0–6; no weekday names are inferred. Hour
bins stay ordered 0–23. Order charts count all orders, not prior-only product
events. Evidence registry rows are static historical artifacts; C8B is labeled
as archived synthetic Kafka/Spark evidence and never as live ingestion.

Hybrid charts show only persisted `final_score` on its normalized 0–1 scale.
Raw ALS and trend scores, normalized components, and source flags remain
separate table columns. Product names come only from the Hybrid API response.
The displayed `source_scope`, `source_namespace`, run/model/graph versions,
trend window, event cutoff, generation time, pointer update, and API observation
time come from the verified response. Product and department run IDs are never
combined into a cross-collection generation claim.

## Refresh and session behavior

Successful response bodies are cached only in the current Streamlit session,
keyed by API base URL, fixed method and request arguments. Per-user requests
include the user ID in the cache key. HTTP errors and invalid responses are not
cached; the cache is capped at 32 keys. Form submissions bypass cache for the
requested analytics or user snapshot. The global refresh button clears the
session cache. There is no timed or continuous polling.

## Tests and remaining milestones

Run frontend unit and AppTest cases with:

```bash
python -m pip install -r requirements-module4d-ui-dev.txt
python -m pytest -q tests/test_module4d_ui_client.py tests/test_module4d_ui_app.py tests/test_module4d_ui_evaluation.py
```

For a clean Docker test image, build with
`--build-arg MODULE4D_UI_DEV=1`, then mount the repository read-only and run
the same pytest command. This keeps pytest out of the normal UI runtime image.

D2B delivered the Evaluation & Performance evidence page, including controlled
interpretation of persisted ALS, Module 2 benchmarks, and C8A/C8B reports.
Archived evidence remains separate from Mongo-backed overview and recommendation
data. Missing or invalid reports render as section-level errors and are not
cached as successful payloads. Use **Refresh evaluation and evidence** or the
global refresh control to bypass the per-session successful-response cache.

Remaining D3 work includes live integration with the configured Mongo-backed
API, release packaging, scoped network and volume setup, and a reproducible
browser-accessible demonstration. D2B smoke results do not establish D3
acceptance.
