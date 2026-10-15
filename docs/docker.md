# Docker builds and validation

The root `Dockerfile` is the canonical build definition. Explicit targets share
reviewed dependency declarations and produce separate runtime images. MongoDB,
Kafka, and HDFS remain external services configured by Compose.
See the [repository layout guide](repository-layout.md) for the configuration
and script directory map.

| Target | Base | Runtime and copied repository resources |
|---|---|---|
| `spark` | Spark 3.5.5 / Java 17 / Python 3.10 | Pipeline and Spark tests; dependency manifests and verified Kafka/GraphFrames jars |
| `api` | Internal `spark` stage | FastAPI/C5/C6 and fixture validation; Python sources/tests, selected scripts, five curated reports |
| `ui` | Python 3.14 slim | Streamlit 1.65.0; only UI Python code and its two manifests; UID/GID 10001 |
| `browser` | Playwright Python 1.63.0 noble | Separate Chromium E2E runtime; browser manifest and accepted E2E script; `pwuser` |

The external base digests are unchanged:

- Spark: `sha256:96dc2bada923824300e1ece564389258934655ba6f463d6daff8753ff0c639ce`.
- UI: `sha256:a2b82f3c48559aa0a8446d9af49826b6e2b2016f4cd2afabfe6013ec53729170`.
- Browser: `sha256:72bd171a9ffc2b4b59532aaa6210e21014d07093120dc25528870c0b840da1f0`.
- Demo MongoDB 7.0.16: `sha256:c630c59342c1493d50345136df2af14a76b9e827dd5316bfabee07a0880a5f3a`.

API inherits Spark for the accepted C5 imports and fixture validation. Starting
FastAPI does not create a Spark session. UI and browser build independently of
Spark. `MODULE4D_UI_DEV=1` adds the existing pinned pytest dependency to `ui`
for tests; the default runtime retains its accepted dependencies and command.

## Build each image

Always select a target explicitly from the repository root:

```bash
docker build -f Dockerfile --target spark -t instacart-spark:3.5.5 .
docker build -f Dockerfile --target api -t instacart-module4d-api:local .
docker build -f Dockerfile --target ui -t instacart-module4d-ui:local .
docker build -f Dockerfile --target browser -t instacart-module4d-browser:local .
docker build -f Dockerfile --target ui --build-arg MODULE4D_UI_DEV=1 \
  -t instacart-module4d-ui-dev:local .
```

Main `compose.yaml` selects `spark` for master and worker and retains the
pipeline service topology and volumes. Build it with
`docker compose build spark-master spark-worker`.

The API validation script builds `api` in a narrow temporary context from the
same Dockerfile. It supplies the Spark dependency manifests itself and does
not require a pre-existing `instacart-spark` image.

## Start and stop the synthetic dashboard

The isolated `deploy/module4/compose.demo.yaml` selects `api` and `ui`. Its
build context is `../..`, resolved from the Compose file directory to the
repository root. It does not mount main pipeline volumes or the repository:

```bash
bash scripts/module4/module4d_demo.sh up
# Open http://127.0.0.1:18501 and retain the printed run ID.
bash scripts/module4/module4d_demo.sh down <12-hex-run-id>
```

Startup builds images, creates a unique MongoDB database, publishes valid
synthetic snapshots, starts FastAPI and Streamlit, and compares HTTP responses
with persisted data and curated evidence. One small `local[1]` Spark session
derives synthetic batch metrics during setup.

Credentials are generated in a mode-0600 file under `/tmp`. The fixture
administrator provisions C6, finalized-trend, and batch-ranking indexes and
creates a database-scoped `read` user. API and validator receive only that
reader; Streamlit receives API settings. MongoDB uses tmpfs with no host port;
API and UI bind to loopback. Separate database and frontend networks keep
Streamlit isolated from MongoDB.

Shutdown removes only that named demo project, its private environment file,
and its tagged API/UI images. Startup failure invokes the same scoped cleanup;
failed shutdown retains the private file for retry. The demo is synthetic and
uses disposable tmpfs; it does not use the default project Compose services or
their persistent volumes.

## Run the API and dashboard against existing data

The API requires explicit backend configuration and a database-scoped MongoDB
read account. Provision required indexes with an administrator before starting
it; do not grant index-management or write privileges to the API account. The
URI's database, when present, must match `MODULE4D_MONGO_DATABASE`.

```bash
export MODULE4D_MONGODB_URI='mongodb://<read-user>:<password>@<host>:27017/<database>?authSource=<auth-database>'
export MODULE4D_MONGO_DATABASE='<database>'
export MODULE4D_TREND_SOURCE_NAMESPACES='<exact-namespace-allowlist>'
python3 -m pip install -r deploy/module4/requirements/api.txt
python3 -m uvicorn src.module4.dashboard.api:app \
  --host 127.0.0.1 --port 8000
```

The API process may also set `MODULE4D_APP_ROOT` to the application or
repository root containing `docs/evidence/`; the default is the packaged source
root and is normally `/workspace` in the API image. Do not set it to the
`docs/evidence/` directory itself. `/health` is process liveness only and does
not establish that MongoDB or any source collection is available. Never put
the MongoDB URI in the UI process, browser configuration, request URL, or
source control.

For an existing-data dashboard, install the separately pinned UI runtime and
point it only at the loopback API:

```bash
python3 -m pip install -r deploy/module4/requirements/ui.txt
export MODULE4D_API_BASE_URL=http://127.0.0.1:8000
export MODULE4D_TREND_SOURCE_NAMESPACES='<same-exact-namespace-allowlist>'
streamlit run src/module4/dashboard/ui/app.py \
  --server.address 127.0.0.1 --server.port 8501
```

For a local academic demonstration, keep both host bindings on `127.0.0.1`.
Network-accessible deployment requires authentication/authorization, TLS, and
rate limiting; those controls are outside this project configuration.

### Required dashboard indexes

The API checks and hints these administrator-provisioned indexes. It does not
create or repair them:

```javascript
db.realtime_trending_windows.createIndex(
  { source_namespace: 1, window_end: -1 },
  { name: "idx_source_namespace_window_end_desc" }
)
db.batch_product_metrics.createIndex(
  { purchase_count: -1, product_id: 1 },
  { name: "idx_batch_product_purchase_count_desc_product_id_asc" }
)
db.batch_product_metrics.createIndex(
  { reorder_rate: -1, product_id: 1 },
  { name: "idx_batch_product_reorder_rate_desc_product_id_asc" }
)
```

Keep the accepted `final_recommendations` indexes
`uq_serving_user_rank` (unique `(serving_version, user_id, rank)`) and
`uq_serving_user_product` (unique `(serving_version, user_id, product_id)`).
The API also requires the ordinary MongoDB `_id_` indexes and permission to
inspect index metadata. Module 2 replaces `batch_product_metrics` and its
indexes during publication; recreate both D1B product indexes afterward.
Missing or incompatible indexes fail closed with an explicit API error rather
than triggering an unbounded collection scan.

The route schemas, bounds, provenance and error contracts live in
[Module 4 plan §20](module4-plan.md#module4d-spec).
The retired API and Streamlit documents remain in the
[Module 4D archive](evidence/module4d/README.md).

## Module 4C controlled streaming demo

The archived [C8B procedure](evidence/module4c/c8b-streaming-demo.md) is a
bounded synthetic experiment, not a production or full-data workflow. It
requires the pinned Kafka 3.7.0, MongoDB 7.0.16, and `instacart-spark:3.5.5`
images to be present locally and refuses to pull or build them. It creates an
internal task-labeled network, a temporary KRaft broker, tmpfs-only MongoDB,
and a unique database; it publishes no ports, mounts no project volumes, and
does not connect to the default Compose project. The repository bind mount is
read-only; only that run's unique `results/module4c-streaming-demo/<run-id>/`
directory is writable.

The cleanup verifier inspects exact task/run labels, container mounts, network,
and the Kafka image's anonymous volumes before removing run-owned resources.
If cleanup fails, inspect the run's `docker-cleanup.json` and listed IDs; do
not use a global prune or remove shared/named volumes. The historical C8B
report predates the anonymous-volume correction, so its cleanup flag is not
proof that those volumes were removed. Current test commands and all historical
observations are in the archived procedure and [Module 4C evidence index](evidence/module4c/README.md).

## Build context and image safety

The root `.dockerignore` excludes every path by default, then admits the
Dockerfile, `pyproject.toml`, six requirement manifests, Python sources/tests,
six explicitly named validation scripts, and exactly five registered reports.
Directory-wide exceptions are avoided. Final exclusions protect environment
files, private keys, credentials, datasets, `results/`, checkpoints, Spark
state, caches, logs, and temporary files. Each target's explicit `COPY`
instructions restrict which of those allowed resources enter its image.

Historical evidence and raw experiments remain unchanged. Module 4's runtime
provenance now reads the canonical Dockerfile path, so new runs identify the
updated build declaration. Bundle/snapshot hash algorithms, ranking,
timestamps, HTTP schemas, and browser expectations retain their accepted code.

## Validation commands

Repository tests run in Spark; the API and UI files excluded here are owned by
and exercised in their respective runtimes below:

```bash
docker run --rm -v "$PWD:/workspace:ro" -w /workspace \
  -e PYTHONDONTWRITEBYTECODE=1 -e PYTEST_ADDOPTS="-p no:cacheprovider" \
  instacart-spark:3.5.5 python3 -m pytest -q \
  --ignore=tests/test_module4d_api_mongo.py \
  --ignore=tests/test_module4d_batch_api_mongo.py \
  --ignore=tests/test_module4d_ui_app.py \
  --ignore=tests/test_module4d_ui_client.py \
  --ignore=tests/test_module4d_ui_evaluation.py
python3 scripts/module4/module4d_api_validation.py --mode all
docker run --rm -v "$PWD:/workspace:ro" -w /workspace \
  -e PYTHONPATH=/workspace -e PYTHONDONTWRITEBYTECODE=1 \
  -e PYTEST_ADDOPTS="-p no:cacheprovider" \
  instacart-module4d-ui-dev:local python -m pytest -q \
  tests/test_module4d_ui_app.py tests/test_module4d_ui_client.py \
  tests/test_module4d_ui_evaluation.py
docker run --rm -v "$PWD:/workspace:ro" -w /workspace \
  -e RUFF_CACHE_DIR=/tmp/ruff instacart-spark:3.5.5 ruff check .
docker compose config --quiet
git diff --check
```

Run the supplementary Module 3/4/4C Mongo suites with their accepted isolated
aliases. These tests require a credential-free test service and create their
own databases. This service has no host port or persistent storage; it is
independent of the authenticated dashboard fixture:

```bash
(
  set -euo pipefail
  validation_id="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
  validation_network="instacart-validation-${validation_id}"
  validation_mongo="instacart-validation-mongo-${validation_id}"
  cleanup() {
    docker rm --force --volumes "$validation_mongo" >/dev/null 2>&1 || true
    docker network rm "$validation_network" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT
  docker network create --label com.codex.task=docker-validation "$validation_network"
  docker run -d --name "$validation_mongo" --network "$validation_network" \
    --network-alias i1-mongo --network-alias i2-mongo --network-alias i3-mongo \
    --label com.codex.task=docker-validation \
    --tmpfs /data/db:rw,noexec,nosuid,size=768m \
    --tmpfs /data/configdb:rw,noexec,nosuid,size=64m \
    mongo:7.0.16@sha256:c630c59342c1493d50345136df2af14a76b9e827dd5316bfabee07a0880a5f3a \
    mongod --setParameter enableTestCommands=1 --wiredTigerCacheSizeGB 0.25
  docker run --rm --network "$validation_network" \
    --label com.codex.task=docker-validation \
    -v "$PWD:/workspace:ro" -w /workspace \
    -e PYTHONDONTWRITEBYTECODE=1 -e PYTEST_ADDOPTS="-p no:cacheprovider" \
    -e MODULE4_MONGO_TEST_URI=mongodb://i1-mongo:27017 \
    -e I1_MONGODB_TEST_URI=mongodb://i1-mongo:27017 \
    -e I2_MONGODB_TEST_URI=mongodb://i2-mongo:27017 \
    -e I3_MONGODB_TEST_URI=mongodb://i3-mongo:27017 \
    instacart-spark:3.5.5 python3 -m pytest -q \
    tests/test_module3_trend_bundle_mongo.py tests/test_module4_mongo.py \
    tests/test_module4c_data_mongo.py tests/test_module4c_trend_reader_mongo.py \
    tests/test_module4c_sink_mongo_mongo.py tests/test_module4c_cli_mongo.py
)
```

Start the isolated demo above, then run Chromium through host loopback:

```bash
docker run --rm --network host --ipc=host \
  -e MODULE4D_BROWSER_UI_URL=http://127.0.0.1:18501 \
  -e MODULE4D_BROWSER_API_URL=http://127.0.0.1:18000 \
  -e MODULE4D_DEMO_TREND_NAMESPACE=module4d-demo \
  instacart-module4d-browser:local
```

The required `fixtures` CI job retains repository tests, lint, OpenAPI/startup,
API, and isolated live Mongo tests. Its dependent job runs UI tests, publishers,
the route validator, and Chromium. Module 3 and Module 4 runtime workflows add
streaming, live Mongo, staged-flow, and publication-replay checks. Full-data
workflows retain separate UI test ownership. Every browser expectation and
curated evidence artifact remains unchanged.

## Consolidation validation — 2026-10-14

Validation started from PR #7's merged main commit
`d44bb14615f57e78b1611a64954b9f2ead6709ad` on
`refactor/unified-docker-build`. All four targets and the UI development variant
built separately with exit code 0. Every target passed `pip check`; the external
base digests, pinned requirements, and all jar SHA-512 values match the accepted
Dockerfiles.

Docker's context exporter admitted 127 files totaling 1,568,612 bytes, including
exactly five evidence files and six scripts. Seventeen exclusion markers for
credentials, datasets, state, caches, and unregistered files were rejected.
Copied file names and SHA-256 hashes matched each stage's manifest: Spark has
no repository source under `/workspace`, API has 119 files, UI has 15, and
browser has two. UI has no PyMongo, PySpark, or Playwright; API has no Streamlit
or Playwright. Both Compose topologies are unchanged after excluding their
build-definition fields from the comparison.

| Check | Result | Exit |
|---|---|---:|
| Repository Spark suite, command above | 690 passed, 69 service-backed skips, 5 warnings; 154.56 s | 0 |
| D2A/D2B UI client/AppTest/evaluation suites | 64 passed; 7.81 s | 0 |
| API/Hybrid unit suites through the API harness | 497 passed; 3.65 s | 0 |
| D1A/D1B live API Mongo suites | 26 passed; 39.92 s | 0 |
| Supplementary Module 3/4/4C Mongo suites | 93 passed; 44.47 s | 0 |
| Demo accepted publishers and read-only route validator | 21 HTTP cases and all source/index/evidence assertions passed | 0 |
| Unchanged Chromium E2E runner | Four pages, five evaluation charts, all selectors, refresh, fallback, error recovery; no JavaScript exceptions | 0 |
| Ruff, compileall, shell syntax, Compose and workflow YAML, Docker build check, diff check | Passed; Docker build check reported no warnings | 0 |

The 69 broad-suite skips are service-backed cases; isolated Mongo suites run
separately and streaming workflows exercise Kafka paths. The initial
supplementary harness used an alias and credentials rejected by the existing
URI guards (28 passed, 65 setup errors); the documented dedicated-alias run
passed all 93 without changing tests. One initial browser capture lacked the
last C8B duration heading; the next run of the unchanged runner passed every
assertion. Both attempts retain the original expectations.

Runtime observations were Python 3.10.12 / Java 17.0.14 / Spark 3.5.5 /
GraphFrames Python 0.6 / FastAPI 0.143.0 / PyMongo 4.11.3 for Spark/API;
Python 3.14.8 / Streamlit 1.65.0 for UI; and Python 3.12.3 / Playwright 1.63.0 /
Chromium 153.0.8010.12 for browser. GraphFrames import and functional graph tests,
OpenAPI/Uvicorn startup, Streamlit host-loopback health, and Chromium execution
passed. The demo run `83ea6e42419a` used its own tmpfs database and retained
the authenticated fixture administrator, database-scoped reader, exact indexes,
loopback ports, and separate networks.

Cleanup removed the task's demo, validation services, networks, five uniquely
tagged images, and private temporary files. No task resources remain. Existing
pipeline containers, volumes, shared images, ignored `results/`, and local data
were preserved; no global Docker prune was used.
