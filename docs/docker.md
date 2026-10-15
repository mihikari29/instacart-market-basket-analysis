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

## Run the persistent pipeline stack

The root `compose.yaml` is the persistent development project. Its pipeline
services are `namenode` and `datanode` (HDFS), `kafka`, `spark-master` and
`spark-worker`, and `mongodb`. Module 2, 3, and 4 are opt-in tools-profile
containers. The default project uses the named volumes `namenode_data`,
`datanode_data`, and `mongodb_data`, and binds the repository into tool
containers. Treat HDFS tables, Kafka topics, MongoDB collections, local
`data/`, `results/`, and checkpoints as real data.

Host commands use Kafka `localhost:9092`, WebHDFS `http://localhost:9870`, HDFS
IPC `hdfs://localhost:8020`, and Spark UI `http://127.0.0.1:8080`. The root
Compose file does not publish a MongoDB host port; tool containers use service
names such as `kafka:29092`, `namenode:8020`, `spark-master:7077`, and
`mongodb:27017` on the project network.

Before starting it, inspect `docker compose ps -a`, `docker volume ls`, the
current project name, the local inputs, and the HDFS/MongoDB targets that the
planned command may replace or publish. Confirm that the expected ports are
free and that writing to the selected project and database is intended. Starting
services creates or reuses containers and persistent volumes; it does not
provide a disposable test environment. From the repository root, start only
the services required for the task and wait for their health checks. Build the
Spark targets first if the pinned image is not already available:

```bash
docker compose build spark-master spark-worker
docker compose up -d --wait namenode datanode kafka spark-master spark-worker mongodb
```

Compose health checks do not establish that HDFS is writable: the NameNode
HTTP check only confirms that its web endpoint responds, and it can still be in
Safe Mode. After the NameNode is running and before staging, replacing, or
writing HDFS data, wait for HDFS to leave Safe Mode:

```bash
docker compose exec -T namenode hdfs dfsadmin -safemode wait
```

This command blocks until Safe Mode ends. If it hangs, diagnose NameNode and
DataNode readiness and logs; do not bypass the wait or attempt the write.

The isolated synthetic dashboard below is a separate demo: it uses a unique
database on disposable tmpfs, loopback-only ports, and does not join or mount
this pipeline project. Use it for a first UI demonstration. Routine cleanup of
the persistent project must never use `docker compose down -v`, `docker volume
prune`, or global container/image cleanup. Keep its HDFS and MongoDB volumes,
Kafka topics, and Module 3 checkpoints unless an owner has reviewed a precise
backup and deletion plan.

## Module 1 ingestion, generation, staging, and Kafka replay

Run from the repository root with the project dependencies installed. The six
source files must be present under `data/raw/`: `orders.csv`,
`order_products__prior.csv`, `order_products__train.csv`, `products.csv`,
`aisles.csv`, and `departments.csv`. `python3 scripts/fetch_instacart.py`
downloads the recorded public mirror, checks the expected row counts, writes
those files, and creates `data/raw/source-receipt.json`; it refuses to overwrite
existing source CSVs. Keep and inspect any existing files before fetching.

Clean first, then generate one feed at a time. The cleaning command writes or
overwrites Parquet files under `data/clean/`. Its `--validate` option prints
audit statistics after writing; it does not fail closed on every missing file
or raw-domain problem. Check that all six inputs exist and review the output
before staging. The cleaner's missing-input and pre-coercion validation gap is
recorded in [current progress](progress.md#module-1--ingestion--storage).
Generation reuses its selected output directory and replaces the feed file and
manifest there; retain any existing feed before selecting the same path.

```bash
python3 -m src.module1.clean --validate

# Historical feed presets: 1 week, 1 month (4 weeks), or 3 months (13 weeks).
python3 -m src.module1.generate --scenario default --scatter-weeks 1 \
  --out-dir data/synthesized/scatter_1w
python3 -m src.module1.generate --scenario default --scatter-weeks 4 \
  --out-dir data/synthesized/scatter_1m
python3 -m src.module1.generate --scenario default --scatter-weeks 13 \
  --out-dir data/synthesized/scatter_3m
```

Generation reads `data/clean/`, defaults to seed 42 and 20,000 users per
processing batch, and writes `events.parquet` and `manifest.json` in each
selected output directory. The manifest records the seed/configuration, span,
daily counts, and monotonicity result. Generation is deterministic for the
same source and seed regardless of `--batch-users`. `--list-scenarios` prints
the supported presets:

| Preset | Per-item gap model | Order clock |
|---|---|---|
| `default` | Exponential, mean 30 s, clipped to 5–120 s | `uniform` |
| `mobile-fast` | Exponential, mean 20 s, clipped to 3–90 s | `uniform` |
| `desktop-browse` | Exponential, mean 40 s, clipped to 10–240 s | `uniform` |
| `uniform` | Uniform 15–50 s | `uniform` |
| `deterministic` | Exponential, mean 30 s, clipped to 5–120 s | `hash` |

`--scatter-weeks` accepts 1, 4, 13, 26, or 52. The 1-week, 4-week and
13-week outputs correspond to the named historical `scatter_1w`, `scatter_1m`
and `scatter_3m` presets.

After inspecting the existing target and completing the shared HDFS Safe Mode
readiness gate above, stage the selected feed to the default HDFS `/instacart`
tree through WebHDFS at `http://localhost:9870`:

```bash
python3 -m src.module1.stage_hdfs --feed data/synthesized/scatter_3m
# Read-only listing of the existing HDFS layout:
python3 -m src.module1.stage_hdfs --tree-only
```

Full staging uploads local raw CSVs, cleaned curated tables, and the selected
partitioned event feed. It replaces raw/curated files and their `_sources.json`
receipt; the interactions handoff is written through a staging/backup/rename
sequence with `_handoff.json`. Use `--interactions-only` only when intentionally
replacing just the interactions handoff. Staging writes into persistent HDFS;
`--tree-only` only reads it. Do not stage into an active handoff without checking
its receipt and downstream use. Before a batch consumer reads it, use the
[Module 2 HDFS contract validator](#module-2-batch-processing-and-benchmarks);
it checks the recorded receipt and data contract but does not hash every staged
table against the raw source.

Optional producer replay reads the chosen feed's `events.parquet`, publishes to
Kafka (default topic `instacart-purchase-events`, host broker
`localhost:9092`), and adds send-time ingestion timestamps. It performs a
disk-backed chronological merge when the file is not declared sorted. Bound
the number of records and set a pacing mode for controlled tests. `--replay-speed`
controls synthetic-time playback (0 means unpaced); alternatively,
`--target-events-per-second` sets a fixed rate. `--receipt` writes attempted,
acknowledged, failed, and replay metadata to a local JSON file. Replay appends
to the selected topic; it does not reset existing Kafka records.

```bash
python3 -m src.module1.producer --feed data/synthesized/scatter_3m \
  --limit-events 50000 --replay-speed 0 \
  --receipt results/module1/scatter_3m-50k-receipt.json

# Alternative fixed-rate test:
python3 -m src.module1.producer --feed data/synthesized/scatter_3m \
  --limit-events 20000 --replay-speed 0 --target-events-per-second 500 \
  --receipt results/module1/scatter_3m-20k-500s-receipt.json

# Fault injection belongs only on a controlled test topic/feed:
python3 -m src.module1.producer --feed data/synthesized/scatter_3m \
  --topic instacart-module1-fault-test --limit-events 20000 --replay-speed 0 \
  --inject "late:0.05,dup:0.02,burst:2,poison:1" \
  --receipt results/module1/fault-test-receipt.json
```

`late` and `dup` values are probabilities; `burst` is the number of extra sends
at each 500-record interval; `poison` is a count of malformed initial records.
Duplicate sends retain an event ID, while injected late records get distinct
IDs and older synthetic event times. Replay writes to the selected Kafka topic;
the optional receipt is local. Do not inject faults into a shared production or
acceptance topic. `scripts/module1.py all` also exists, but its staging step
starts HDFS with `--remove-orphans`; the direct commands above make each write
boundary explicit.

## Module 2 batch processing and benchmarks

Module 2 requires the curated HDFS tables and receipts under `/instacart/curated`
and reachable Spark and MongoDB services. Check the handoff and selected Compose
project first. The direct tool-service form below does not start dependencies;
start the required services using the stack section above. Keep `--use-aliases`
so Spark workers can resolve the driver's advertised `module2` hostname.

```bash
# HDFS receipt, counts, nulls, keys, relationships, domains and multiset checks:
docker compose --profile tools run --rm --use-aliases --no-deps module2 validate

# Analytics, user features, HDFS run artifacts and MongoDB batch publication:
docker compose --profile tools run --rm --use-aliases --no-deps module2 stats

# Each benchmark validates inputs and records its own measurements:
docker compose --profile tools run --rm --use-aliases --no-deps module2 benchmark-joins
docker compose --profile tools run --rm --use-aliases --no-deps module2 benchmark-partitions
docker compose --profile tools run --rm --use-aliases --no-deps module2 benchmark-cache

# Analytics/publication and all three benchmarks:
docker compose --profile tools run --rm --use-aliases --no-deps module2 all
```

Every command writes a unique `results/module2/<run-id>/summary.json` and Spark
event log. `stats` and `all` also write `analytics.json` and per-run Parquet
outputs, and overwrite HDFS `/instacart/features/user_features`; unless `--skip-serving` is
passed, they also publish to MongoDB `batch_product_metrics` and
`department_metrics` using the configured `MONGODB_URI`. The three benchmark
commands write plans, checksums, trials, and Spark event-log metrics under their
run directory and do not publish analytics to MongoDB. Defaults are one warm-up
and three measured repetitions; benchmark outputs are workload-specific
measurements. Module 2 validates receipt structure, counts, nulls, keys,
relationships, domains, and relevant multisets. It does not independently hash
every staged table; `--source-events <local-file>` adds only the documented
local-source comparison. The `scripts/module2.py` wrapper starts services with
`up --remove-orphans` before forwarding arguments, so its arguments and startup
effects are not equivalent to a harmless help/query command.

## Module 3 streaming operations

The current Module 3 event-time, ranking, sink, and checkpoint contracts are in
the [Module 3 specification](module3-contracts.md). The commands here use the
`module3` tools service directly and assume the selected persistent Compose
project already has healthy Kafka, Spark master/worker, and MongoDB services.
The one-off Spark service command includes `--use-aliases` because Compose
advertises the driver hostname as `module3`.

```bash
# Kafka and MongoDB connectivity; creates a local run summary, no stream:
docker compose --profile tools run --rm --use-aliases --no-deps module3 validate

# Bounded stream, then an unbounded foreground run (Ctrl+C to stop):
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  run --duration-seconds 60
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  run --duration-seconds 0

# Set an explicit one-day retention instead of the seven-day default:
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  run --duration-seconds 60 --ttl-seconds 86400

# Offset loss fails by default; allow skipping only as an explicit decision:
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  run --duration-seconds 60 --allow-data-loss

# Deterministic late-data/watermark demo; uses and resets its dedicated topic:
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  late-demo --duration-seconds 60

# Bounded benchmarks against a generated feed; choose the actual feed path:
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  benchmark-throughput --feed data/synthesized/scatter_3m \
  --duration-seconds 15 --benchmark-events 100
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  benchmark-partitions --feed data/synthesized/scatter_3m \
  --duration-seconds 15 --benchmark-events 100

# Run both benchmark groups using their default 100/s, 500/s, and bulk arms:
docker compose --profile tools run --rm --use-aliases --no-deps module3 \
  all --feed data/synthesized/scatter_3m --duration-seconds 60 \
  --benchmark-events 100
```

All CLI commands create a unique `results/module3/<run-id>/` with a summary;
streaming runs also write checkpoint/state and progress/event-log files and
publish finalized rows into the configured MongoDB collection. The default
checkpoint identity is the run ID and checkpoint data lives under
`results/module3/checkpoint/<checkpoint-id>`. Before running, inspect the
selected topic and offset range: default `startingOffsets=earliest` can replay
existing topic records when no checkpoint is present. The late-data demo
resets `<configured-topic>-late-demo` and writes to that collection. Keep
the default seven-day `--ttl-seconds 604800` unless a deliberate alternative is
approved; `--ttl-seconds 0` disables expiry. Kafka offset loss is strict by
default (`failOnDataLoss=true`). Relaxed behavior requires explicit
`--allow-data-loss` or `MODULE3_FAIL_ON_DATA_LOSS=false`.
Advanced CLI overrides include `--window-short`, `--window-long`, `--slide`,
`--watermark`, `--weight-short`, `--weight-long`, `--trigger-interval`, and
`--mongo-top-k-per-window`; their defaults and meanings are in the
[Module 3 specification](module3-contracts.md).

The repository smoke/recovery helpers exercise the default Compose project and
are stateful. Only run them after inspecting that project's topic, collection,
checkpoint, and volume ownership:

```bash
test -s data/synthesized/scatter_3m/events.parquet
test -s data/synthesized/scatter_3m/manifest.json
python3 scripts/module3_smoke.py --feed data/synthesized/scatter_3m \
  --events 50000 --duration-seconds 120
python3 scripts/module3_recovery.py --duration-seconds 25
```

The smoke command depends on the selected generated feed already existing; the
Module 1 generation procedure above writes both checked files into that feed
directory. The smoke helper resets `instacart-module3-live-smoke`, archives a
previous `results/module3/smoke/checkpoint/live-smoke`, starts Compose services,
and writes to MongoDB; recovery resets
`instacart-module3-recovery-smoke` and archives a previous
`results/module3/recovery/checkpoint/recovery-smoke`. Both start services in
the default Compose project and write to its `realtime_trending` collection.
They are bounded integration checks, not production-capacity tests. The
benchmarks create/reset trial topics and write bounded output to the configured
Mongo collection; retain their run artifacts when comparing results.

## Module 4A/4B operation and full-data acceptance

Module 4A/4B reads the curated HDFS handoff, fits prior-only ALS, evaluates
against the official train truth, builds the prior-basket graph, and can publish
versioned recommendations and graph snapshots. Commands create
`results/module4/<run-id>/`; publication changes the selected MongoDB versions
and active readiness pointer. Confirm the selected project, source receipt,
HDFS handoff, Mongo database, and version names before running. The wrapper
starts HDFS and Spark services (and MongoDB unless serving is skipped) and uses
the driver network alias. Choose unique run/model/graph version IDs in the
selected database; `--skip-serving` omits snapshot publication.

The CLI commands are `validate`, `als`, `graph`, and `all`. `all` performs both
model/graph work and, by default, versioned publication; separate `als` and
`graph` runs are useful for diagnosis. A run with `--skip-serving` is compute-only
and does not pass the full A/B publication gate.

Use `validate` to check the selected HDFS handoff. The following diagnostic
commands compute ALS or graph outputs without publishing a one-sided snapshot;
each run still writes artifacts and checkpoints under `/instacart` and
`results/module4/`:

```bash
run_tag="diagnostic-$(date -u +%Y%m%dT%H%M%SZ)"
python3 scripts/module4.py validate --scope staged_curated
python3 scripts/module4.py als --scope staged_curated \
  --run-id "$run_tag-als" --skip-serving
python3 scripts/module4.py graph --scope staged_curated \
  --run-id "$run_tag-graph" --skip-serving
```

For normal paired recommendation and graph publication, use `all` with unique
IDs in the selected MongoDB database:

```bash
publish_tag="pair-$(date -u +%Y%m%dT%H%M%SZ)"
python3 scripts/module4.py all --scope staged_curated \
  --run-id "$publish_tag" --model-version "als-$publish_tag" \
  --graph-version "graph-$publish_tag"
```

For a bounded fixture flow, use the repository helper. It creates a unique
HDFS fixture root and MongoDB database, runs first publication and same-version
replay, reads the snapshots back, and removes only those run-scoped resources.
It still needs healthy services and the pinned Spark image. The CLI's
`staged_fixture` label alone does not bound data; point it only at the fixture
handoff. `staged_curated` reads the configured curated dataset and may be a
full-data workload. The `full_data` scope requires
`data/raw/source-receipt.json` plus the complete verified HDFS input.

```bash
docker compose --profile tools run --rm --use-aliases --no-deps --entrypoint python3 module4 \
  scripts/module4_flow_smoke.py
```

For a new Linux full-data rehearsal, first verify the source receipt and
`data/clean/` tables. Use an explicitly named Compose project so its persistent
HDFS and MongoDB volumes are distinct from the normal development project. Check
that required host ports are free; do not start another project against the same
ports. Keep the same project name through the first run and replay. The portable
resource override below is the one used by the manual
[Module 4 full-data workflow](../.github/workflows/module4-full.yml); it is not
the machine-specific WSL image/storage mount used by the October 10 acceptance.
The workflow downloads and verifies source data, stages the curated handoff,
runs repository and live-integration checks, and performs same-version
publication replay. It is a manual, resource-intensive workflow.
Its local override provides a 2 GB worker executor, four worker/total cores,
and a 1 GB driver. The accepted historical host had about 16 GiB RAM; that
measurement is environment context, not a guaranteed minimum. Plan disk space
for the source, cleaned Parquet, Spark temporary data, models, and run artifacts.

The following local sequence is provided for an operator who has deliberately
chosen that resource-intensive run. Starting the project creates or reuses its
named volumes; staging and `all` write HDFS tables/models, local run artifacts,
and the project MongoDB database. Retain the Compose project and volumes between
first run and replay:

```bash
# Use a fresh suffix and keep these exports for the entire procedure.
run_suffix="$(date -u +%Y%m%dT%H%M%SZ)"
export COMPOSE_FILE=compose.yaml:.github/module4-full.override.yml
export COMPOSE_PROJECT_NAME="instacart-module4-full-$run_suffix"

# In a fresh checkout, create the Python environment used by the host-side
# fetcher and WebHDFS staging helper:
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Fetch only when data/raw/ has no source CSVs; the fetcher refuses overwrite.
python3 scripts/fetch_instacart.py
python3 -m src.module1.clean --validate
docker compose build spark-master spark-worker
docker compose up -d --wait namenode datanode mongodb spark-master spark-worker
docker compose exec -T namenode hdfs dfsadmin -safemode wait

# Stage the verified curated tables used by A/B, without replacing the
# interactions feed. This matches the staging boundary in the manual workflow.
python3 - <<'PY'
from hdfs import InsecureClient
from src.module1.stage_hdfs import stage_curated, stage_places

client = InsecureClient("http://localhost:9870", user="root")
stage_curated(client, "http://localhost:9870", "root")
stage_places(client)
PY

python3 scripts/module4.py validate --scope full_data \
  --source-receipt data/raw/source-receipt.json --shuffle-partitions 32

# New run IDs and unused model/graph versions in the selected Mongo database.
first_run_id="full-first-$run_suffix"
replay_run_id="full-replay-$run_suffix"
model_version="als-full-$run_suffix"
graph_version="graph-full-$run_suffix"

python3 scripts/module4.py all --scope full_data \
  --source-receipt data/raw/source-receipt.json --run-id "$first_run_id" \
  --model-version "$model_version" --graph-version "$graph_version" \
  --shuffle-partitions 32 --executor-memory 2g --executor-cores 4 --total-cores 4 \
  --als-blocks 8 --pilot-users 5000 --max-trials 1 --graph-min-edge-weight 10
docker compose --profile tools run --rm --use-aliases --no-deps --entrypoint python3 module4 \
  scripts/check_module4_run.py --run-dir "results/module4/$first_run_id" \
  --output "results/module4/$first_run_id/snapshot-check.json"

# Same source, input, config, and model/graph versions; use a new run ID.
python3 scripts/module4.py all --scope full_data \
  --source-receipt data/raw/source-receipt.json --run-id "$replay_run_id" \
  --model-version "$model_version" --graph-version "$graph_version" \
  --shuffle-partitions 32 --executor-memory 2g --executor-cores 4 --total-cores 4 \
  --als-blocks 8 --pilot-users 5000 --max-trials 1 --graph-min-edge-weight 10
docker compose --profile tools run --rm --no-deps --entrypoint python3 module4 \
  scripts/check_module4_run.py --run-dir "results/module4/$replay_run_id" \
  --compare-to "results/module4/$first_run_id/snapshot-check.json" \
  --output "results/module4/$replay_run_id/snapshot-check.json"
```

The checker requires a passed full-data A/B publication and verifies the active
readiness pointer, counts, versions, indexes, and logical snapshot digests;
replay also checks the same source/config/version and rounds floating values to
six decimal places. See the [dated October 10 acceptance record](evidence/module4/acceptance-20261010/README.md)
and its [exact environment-specific commands](evidence/module4/acceptance-20261010/run-acceptance.sh)
for the historical WSL storage mount and accepted measurements. That record is
not a portable Linux recipe. Never use `docker compose down -v` or remove these
volumes as routine cleanup.

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

## Module 3 streaming and Module 3 to Module 4C enablement

This is an example for an operator who intends to publish into the existing
default `instacart` Compose project's Kafka and MongoDB. It was checked against
the CLI parser and Compose service definition; it was not run as part of this
documentation change. The `instacart-spark:3.5.5` image must be available
locally. The project must already have healthy Kafka, Spark master/worker, and
MongoDB services on its network, and the topic must contain
the intended replay or have an active producer. Module 4C also needs a ready
Module 4A/4B snapshot and compatible catalog in that MongoDB database. The
command below does not start dependencies, but it writes local output and
checkpoint files under `results/module3/` and writes to the project's MongoDB.
Do not use it unless those writes are intended. The default project has
persistent services and named volumes; do not use `down -v` or global cleanup.

Run the module entry point directly. Do not use `scripts/module3.py`, which
starts Compose before forwarding arguments. The options follow the `run`
subcommand in the parser:

```bash
docker compose --profile tools run --rm --use-aliases --no-deps \
  --entrypoint python3 module3 \
  -m src.module3.cli run \
  --duration-seconds 0 \
  --publish-trend-bundles \
  --trend-source-namespace instacart-replay-v1 \
  --trend-source-scope mixed-prior-train-synthetic \
  --checkpoint-id instacart-replay-v1
```

`--duration-seconds 0` runs until interrupted. A new checkpoint starts at
`--starting-offsets` (default `earliest`); on restart, Spark restores offsets
and state from `results/module3/checkpoint/<checkpoint-id>`. Keep the same
checkpoint identity and compatible topic/query settings to resume. Use a new
checkpoint identity for a genuinely new stream. Keep the namespace stable for
the same logical source and replay, but choose a new namespace for changed
source/window content because bundles are immutable per namespace and window.
Never use a process run ID as the source namespace. `source_scope` is descriptive
metadata; `mixed-prior-train-synthetic` does not establish train exclusion.
The ordinary generated Kafka feed includes prior and train orders, so a stable
cutoff alone does not make it safe for official-train quality evaluation.

Module 4C selects the same namespace explicitly and requires a timezone-aware,
BSON-millisecond-aligned event-time cutoff. For example, its `serve-user`
interface takes `--source-namespace instacart-replay-v1` and an explicit
`--as-of-event-time <UTC timestamp>`; it selects the newest valid finalized
window ending at or before that cutoff. Module 4C also requires ready A/B inputs.
For Module 4D, configure the API's exact allowlist to include the same namespace
in `MODULE4D_TREND_SOURCE_NAMESPACES`; the UI's choices and API's backend check
must match. These are three related settings: Module 3 publication namespace,
Module 4C request namespace, and Module 4D API allowlist entry.

The legacy writer provisions its own indexes, and the bundle writer creates its
`expires_at` TTL index when publishing bundles. The Module 4D API selection
index on `(source_namespace, window_end descending)` is different: an
administrator provisions it before starting the API. See [Required dashboard
indexes](#required-dashboard-indexes). TTL removes documents by processing-time
expiry; it does not implement the event-time cutoff or Module 4C's optional
maximum-event-age policy.

On shutdown, retain the checkpoint if recovery may be needed. Remove only the
exact local run-output directory after preserving any required evidence. Bundle
documents expire by their configured TTL; if manual deletion is necessary,
scope it to the exact source namespace and obtain the database owner's approval.
Do not remove the shared MongoDB database, Kafka topic, HDFS data, or Compose
volumes as routine cleanup.

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
