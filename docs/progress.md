# Project progress and requirements coverage

This report describes the current implementation in the reviewed source revision and separates it from historical measurements and runtime work still pending. The approved [Proposal](Proposal.md) remains the requirements baseline. Detailed command ownership belongs to the [Docker guide](docker.md) and [Kubernetes guide](kubernetes.md); contracts belong to the [Module 3 guide](module3-contracts.md), [Module 4 plan](module4-plan.md), and [implementation ledger](implementation-ledger.md).

## Current status

**Code complete; external runtime acceptance is pending.** The final independent source-code review passed at `a5fe5003134efb26f2a8826ce51e98bc291a2ff9`. Historical offline results below are attributed to that source revision. No tests, service runs, fresh measurements, or acceptance checks are claimed for this documentation revision.

| Area | Implemented scope | Recorded development verification | Still requires external validation |
|---|---|---|---|
| Module 1 | Six-file fail-closed preflight, cleaning, deterministic replay event generation, HDFS handoff and Kafka producer. | Broad offline suite included bounded fixture cleaning and source-package checks; historical full-data processing is separately recorded. | Pinned runtime integration, exact six-file full-data execution and fresh receipts. |
| Module 2 | Prior-history Spark analytics, descriptive user features, Mongo publication, local thread comparison and standalone executor benchmark tooling. | Offline suite and source review; dated full-data analytics and benchmark results retain their original environment and code identity. | Pinned Spark/HDFS execution and new performance/executor experiments. |
| Module 3 | Kafka validation, event-time aggregation, bounded replay reconciliation, versioned checkpoints, optional immutable trend bundles and executed-batch telemetry. | Focused telemetry/BSON and API/UI checks; historical live Kafka/Spark/Mongo measurements are linked below. | Pinned service integration, restart/recovery, full-feed execution and fresh latency evidence. |
| Modules 4A–4B | Prior-only implicit ALS, catalog-wide product graph, evaluation and versioned publication. | Offline ranking, publication and contract checks; historical 21/21 full-data gates from 10 October 2026. | Pinned Spark/GraphFrames/Mongo integration and a fresh full-data run. |
| Module 4C | Readiness-aware Hybrid source selection, deterministic ranking, versioned publication and prior-only evaluation tooling. | Offline reader/ranking/publication checks; synthetic C8A/C8B integration evidence remains bounded. | Runtime integration and a leakage-safe Hybrid quality result based on an evaluation design that is valid for the chosen trend source. |
| Module 4D | Read-only FastAPI routes, bounded Mongo readers and four-page Streamlit dashboard. | API, UI client and AppTest checks; browser job records are historical and keep their own source identity. | Browser checks against pinned runtime and live services. |
| Kubernetes | Isolated B2 serving, B3 processing, B4 acceptance, one-session runner, private receipts and separately gated teardown. | Offline fake-client/static checks, local rendering, runner and packaging/source checks. | Image build/import verification and a dedicated isolated B2/B3/B4 runtime. |

The broad recorded non-service suite reported **803 passed, 7 skipped**. The seven skips were Spark-dependent modules; eight live MongoDB modules and two PySpark-importing integration suites were excluded by selection. They were not seven skipped individual test cases. The exact selection and exclusions are preserved in the [acceptance handoff](acceptance.md#delivered-scope-and-evidence-boundaries). Focused checks reported **59 passed** for telemetry/BSON regressions and **134 passed** for the telemetry, API, and UI selection. Repository Ruff passed and Python compilation covered 190 files. Offline Kubernetes fake-client, static and packaging checks also passed. These are development checks on host Python with temporary lightweight dependencies, not pinned Spark/Java or live-service acceptance.

## Module 1 — ingestion and storage

<a id="module-1--ingestion--storage"></a>
<a id="22-cleaning-and-reconstruction-results"></a>

**Objective.** Admit the six related source tables as a coherent dataset, produce typed curated tables, reconstruct usable replay time, and stage input and output with enough provenance to validate later processing.

**Flow and contracts.** `orders.csv`, `order_products__prior.csv`, `order_products__train.csv`, `products.csv`, `aisles.csv`, and `departments.csv` are required. The cleaner checks exact column sets and duplicate headers, parses rows initially as strings, then validates canonical integer spellings, ranges, evaluation-set labels, primary/foreign keys, order-level consistency, unique product/cart positions, and contiguous `1..N` cart order before coercion. Empty, unreadable, missing, malformed or inconsistent input fails with sanitized diagnostics. First-order missing inter-order gaps become zero; capped 30-day values are reconstructed to the minimum weekday-compatible value in `[30, 36]`. Product names replace nonbreaking spaces and collapse surrounding/repeated whitespace.

The cleaner emits six typed Parquet files and `.it4931-clean-output.json`. It stages every result, then replaces only a directory bearing the expected ownership manifest and file set. Unsafe symlink/path overlap and unknown output entries fail closed. If validation or staging fails, the existing recognized publication remains. This procedure assumes one writer; it is not a multi-process transaction.

The event generator selects `prior` and `train` orders and excludes the official `test` set. Stable hash-derived values use the seed and entity keys for user anchor weeks, order minute/second and within-cart gaps; they do not use Python's salted hash. The 2024-01-01 Monday base aligns `order_dow` with calendar offsets. It adds cleaned inter-order gaps, recorded hour and generated within-order item intervals, then enforces strictly increasing event time along each user's order/cart sequence. `--batch-users` bounds processing memory and does not change generated values for the same source and seed. Parquet carries `order_number` and `synthetic_date`; Kafka messages do not.

HDFS staging writes `_sources.json` for the curated tables and `_handoff.json` for the partitioned interaction feed. The six-file source receipt is retained as separate input/run evidence. Curated order and dimension tables are staged separately from the interaction handoff. The producer emits one denormalized JSON message per product event, keyed by `user_id`, with stable `event_id`, order/product/basket attributes, synthetic event time and send-time ingestion time. It omits `eval_set`, so the ordinary stream combines prior and train orders and is unsuitable for official-train temporal evaluation.

**Inputs and outputs.** Six raw CSV tables lead to six cleaned Parquet tables; generation produces Parquet or JSONL replay files and a feed manifest; HDFS receives raw/curated data, receipts and the interactions handoff; Kafka receives the generated product-event records.

**Verification and historical findings.** A bounded synthetic fixture was included in the offline suite. The 3 October 2026 full-data record reconciled 33,819,106 events across source facts, local feed and HDFS readback, with 456 daily files/partitions and zero recorded per-user monotonicity violations. The 8 October source-preparation receipt recorded 206,209 first-order gaps filled, 369,323 capped gaps reconstructed, zero weekday mismatches and 16 normalized product names. These records remain historical and are tied to their archived inputs and runtime.

**Limitations and remaining acceptance.** The generated dates describe replay only. The Kafka event schema does not preserve evaluation-set membership. Runtime HDFS/Kafka integration, fresh six-file full-data processing and identity-bound output receipts remain acceptance tasks. See the [Module 1 guide](docker.md#module-1-ingestion-generation-staging-and-kafka-replay), [generation reference](docker.md#synthetic-event-generation) and [full-data evidence](evidence/full/README.md).

## Module 2 — batch layer

<a id="3-module-2--batch-layer"></a>

**Objective.** Produce reproducible historical aggregates and descriptive user features from curated HDFS data, then make validated outputs available to read-only consumers.

**Flow and interfaces.** Module 2 verifies handoff receipts, row counts, nulls, keys, foreign keys, domains, relevant partition values and exact fact/event multisets before computing. Historical analytics use prior interactions. The resulting `user_features` schema has `user_id`, `order_count`, `purchase_count`, `unique_products`, `reorder_rate`, and `avg_basket_size`, with `module2_run_id`, `feature_scope=prior_only` and `generated_at_utc` metadata. The Module 4 reader checks the expected schema, bounds the lookup to a user, and detects duplicate rows; the returned features are descriptive and are not inputs to ALS fitting. The optional local source-event hash check is separate and narrower than verifying a hash for every staged input. Module 4 staged-Parquet hash checks are another independent boundary.

Spark analytics include product and department aggregates, user features, order weekday/hour distributions and department-hour pivot/unpivot totals. Mongo publication writes bounded batches to temporary collections, validates output and indexes, then promotes each collection. Each collection has a publication boundary, but a set of collections is not a cross-collection transaction. Readers must validate run identity instead of assuming every collection changed as one unit.

**Optimization experiments.** The benchmark suite measures requested join plans, date-column versus partition-column filters, cached aggregate reuse and uncached recomputation. It records the cache materialization cost separately. A local `local[1]`/`local[2]` comparison tests thread configurations; a distinct standalone mode attaches to an existing Spark master and records actual executor allocation and task/shuffle data. Neither configuration knobs alone nor a local-thread comparison proves executor-layout speedup.

**Verification and historical findings.** The 3 October full-data run verified exact fact/event reconciliation, prior-only aggregates, pivot/unpivot totals, equal outputs across join strategies and filter strategies, and Mongo publication of 49,677 product and 21 department documents. Eleven catalog products had no prior purchases. Recorded prior-only reorder rate was 58.9697%, with average basket size 10.0889. Median times included Sort-Merge Join 17.581934 s and Broadcast Hash Join 2.304046 s; all-date scan 3.497363 s, date-filtered 1.666808 s and partition-filtered 0.074906 s; uncached aggregate 2.110284 s, cache materialization 2.175583 s and cached reuse 0.088085 s. These were one-host, one-executor measurements with warm OS/HDFS caches; the all-date scan had a different scope. They do not establish cluster-scale performance.

**Limitations and remaining acceptance.** Current runtime Spark/HDFS execution and fresh publication checks remain pending. The standalone executor-layout experiment has tooling but no new measured result. See the [full-data report](evidence/full/README.md), [benchmark guide](docker.md#spark-executor-layout-experiment), and [Module 2 Docker procedure](docker.md#module-2-batch-processing-and-benchmarks).

## Module 3 — speed layer and Structured Streaming

<a id="module3-status"></a>
<a id="41-final-streaming-architecture"></a>
<a id="42-event-time-watermark-and-trending"></a>
<a id="43-mongodb-serving-and-idempotency"></a>
<a id="44-end-to-end-streaming-validation"></a>
<a id="45-checkpoint-recovery"></a>
<a id="46-late-data-and-watermark-validation"></a>
<a id="47-throughput-measurements"></a>
<a id="48-validation-results"></a>
<a id="49-current-technical-status-and-known-limitations"></a>

**Objective and flow.** Module 3 consumes the keyed Kafka event feed, validates JSON schema and field domains, accounts for invalid messages and deduplicates by `event_id` while state remains within the configured watermark. One 120-minute sliding stateful aggregation produces the short 30-minute and long 120-minute windows. Windows slide every five minutes, the watermark is ten minutes, and the configured short/long weights are 0.7/0.3. After a window is complete, the full product population is normalized and deterministically ranked, then limited to Top-20.

**Outputs and operations.** The legacy `realtime_trending` collection stores mutable product/window rows and replaces a complete window before upserting its rows; those operations are not a transaction for readers. Optional `realtime_trending_windows` stores one complete immutable ranked document per finalized window and source namespace. Its identity combines a digest of `source_namespace` with the UTC `window_end`; the document contains schema version, window bounds, Top-K and row counts, an `items` array of ranked products, configuration and content hashes, publication time, and optional `expires_at`. An identical retry is accepted, while conflicting content under the same identity fails. Publication is atomic for one document, not across several windows. TTL expiry metadata is distinct from event-time filtering. Module 4C and the trends API consume these bundles when explicitly enabled; neither path uses the legacy mutable rows. Per-product/window latency samples describe finalized output rows and do not change the bundle's one-document-per-window granularity. No open-window leaderboard is implied.

Checkpoint continuity is bound to a versioned path such as `checkpoint/<id>/ingestion-latency-v1`. Unversioned legacy telemetry state is rejected without mutation. A checkpoint is reusable only with matching query/topic/config identity. The Kubernetes runner disables automatic resume and requires a fresh run identity.

Finite replay completion requires successful producer delivery, consistent generated/attempted/acknowledged counts, and consumed terminal offsets across all partitions. A completed Kafka read may leave an event-time tail in checkpoint state because the pipeline does not fabricate future events. Telemetry pairs each executed Spark batch (identified by the `durationMs.addBatch` marker) with its same-batch sink receipt. Idle progress does not count as an executed batch; zero-input cleanup batches do. The bounded receipt queue applies backpressure instead of dropping records, and missing receipts or gaps fail visibly. Shutdown stops the query before signaling the progress logger to perform its final drain.

For each finalized product/window output row, the latency source is the latest contributing ingestion timestamp. The summary reports ingestion-to-output-completion delay after enabled publishers; it is constant-memory and records min/mean/max and skew status. Producer and output host clocks may be unsynchronized, values may be negative from skew, and the metric includes watermark delay. It is not acknowledgement, trigger, sink-only, Kafka lag, or dashboard latency. The metrics document is bounded and read-only at the API boundary; malformed BSON integer values fail closed.

**Historical verification.** The 3 October 2026 bounded integration ran one Kafka broker with four partitions and Spark 3.5.5/Java 17/Python 3.10.12 on a two-core setup. A 5,000-event smoke acknowledged and consumed all events, reported 79,970 Mongo documents, no duplicate window/product or window/rank keys, and 374.20 processed rows/s. In a separate 50,000-event run, all 50,000 events were acknowledged and valid; observed processed rate was 3,203.90 rows/s, 35,701 finalized windows produced 580,342 documents, aggregate state reached 1,174,010 rows, and final sink batch time was 62,021.821 ms. Kafka was preloaded, so input rows/s was not meaningful; each event could fan out to as many as 24 five-minute windows.

The same evidence records bounded late-data and watermark behavior, two-process checkpoint continuation (three records in the first process and two new records in the second), plus throughput arms at 100/s, 500/s and bulk and a partition comparison. Two partitions performed best in that small test; this does not imply a general optimum. See the [Module 3 evidence report](evidence/module3/README.md).

**Limitations and remaining acceptance.** Watermark deduplication is not permanent uniqueness. One broker does not demonstrate high availability. The legacy mutable snapshot can be transiently empty/partial to concurrent readers. Bounded rates do not establish production throughput, and the historic trigger/addBatch/sink durations are not the defined ingestion-to-finalized-output latency. Pinned runtime integration, checkpoint restart at runtime, full-feed execution and new latency evidence remain pending. See the [Module 3 contract](module3-contracts.md) and [streaming operations](docker.md#module-3-streaming-operations).

## Modules 4A and 4B — prior-only recommendations and product graph

**Objective.** Module 4A produces personalized implicit-feedback recommendations from prior history and evaluates ranking against held-out data. Module 4B produces catalog-wide co-purchase and graph metrics from prior baskets. Both publish versioned outputs with a readiness identity so downstream readers can avoid mixing incompatible generations.

**Processing and data boundaries.** ALS uses prior user/product purchase counts as implicit preference input. Its internal holdout supports model checks among eligible prior-history users; official train baskets are not used to fit ALS or select its fit parameters. Evaluation reports ALS and baseline results across K values and warm-user cohorts. A separate global-popularity and personal-frequency baseline supports comparisons. Module 4B deduplicates products inside each basket, canonicalizes unordered pairs, applies declared co-purchase thresholds, preserves all catalog vertices and calculates neighbors, weighted degree and PageRank over symmetric topology. The PageRank run uses graph topology, not co-purchase weights as weighted PageRank.

**Historical verification.** The 10 October 2026 acceptance report records 21/21 historical gates for its exact data and source scope. It records 4,124,180 recommendation documents for 206,209 users, 49,688 graph vertices, 3,492,837 undirected edges, 6,985,674 directed rows and 21,164 isolated catalog products. ALS Recall@10 was 0.121472 versus 0.319232 for personal frequency, so this run does not demonstrate an ALS quality improvement. Threshold-specific graph counts and failed/interrupted attempts remain in the dated report. This result predates later changes and is not fresh full-data acceptance for `a5fe500`.

**Limitations and remaining acceptance.** Full-data Spark/GraphFrames/Mongo verification on the reviewed source revision remains pending. Historical quality measures describe their recorded holdout and cohort; they do not establish general recommendation quality. See the [Module 4 acceptance report](evidence/module4/acceptance-20261010/README.md) and [Module 4 plan](module4-plan.md).

## Module 4C — Hybrid serving and offline evaluation

<a id="progress-module4c"></a>

**Objective.** Combine eligible versioned batch sources and finalized trend bundles while preserving source identity, deterministic ranking and read consistency.

**Processing and inputs/outputs.** The service captures a ready ALS/graph generation, checks bounded source reads and readiness identities, selects the newest eligible immutable trend bundle at an explicit event-time cutoff, normalizes the selected ALS and trend scores, unions candidate sets, and publishes versioned per-user recommendation snapshots. The output generation is immutable; a per-user readiness pointer promotes the serving version. A/B snapshots are separate Mongo promotions and do not form one multi-collection transaction. Endpoint candidate behavior is explicit at lambda boundaries, including cold-user trend fallback, and ties use stable product identity.

The prior-only evaluation command uses a fixed global-frequency Top-20 candidate source and a preregistered lambda sweep (`0`, `0.5`, `0.7`, `0.9`, `1`) at K=5/10/20. It reports all official-train and warm-user truth cohorts and records source hashes. Official train labels are used only for evaluation, not fitting, tuning, candidate creation or trend source generation. This experiment is not equivalent to chronological trend evaluation. The ordinary stream combines prior and train orders, so a leakage-safe trend evaluation needs an appropriately separated source and chronology.

**Recorded checks.** The bounded C8A controlled synthetic shift changed Top-1 at lambda 0.0 and 0.5, but not at default lambda 0.7, 0.9 or 1.0; Top-20 overlap stayed 20/20, and accuracy was not measured. C8B reconciled 48 planned, acknowledged and consumed events with zero invalid records and observed 36 bundle documents; checkpoint restart was not demonstrated. These are integration observations, not quality or capacity results. See the [Module 4C archive](evidence/module4c/README.md).

**Limitations and remaining acceptance.** Runtime publisher/reader integration and an evaluation source that isolates trend chronology remain to be validated. No leakage-safe Hybrid accuracy result or production capacity claim exists. See [Module 4 §19](module4-plan.md#module4c-spec).

## Module 4D — API and dashboard

<a id="progress-module4d"></a>

**Objective and flow.** Module 4D offers a bounded read surface over versioned recommendation and graph outputs, batch metrics, order distributions, finalized trends, streaming metrics and an allowlisted set of historical evidence. Mongo indexes are provisioned by an administrator; API database credentials are read-only. FastAPI validates current readiness/source identities and uses indexed bounded queries. Streamlit calls the API and has no MongoDB client.

The four UI pages cover Overview, Realtime & Batch, Hybrid recommendations, and Evaluation & Performance. ALS and Hybrid provenance remain separate. The API distinguishes unavailable, empty, malformed and error states; it does not silently turn missing values into zero. Health/liveness checks do not establish data readiness. A refresh action only re-reads a snapshot; it does not prove the streaming job is running.

**Recorded checks.** API, client, AppTest and browser checks have their own dated workflow/evidence scope. The independent review's offline suite includes the read contracts and UI selection; the browser/runtime environment is not part of the 803-pass host suite unless explicitly listed in a recorded workflow. The earlier four-chart browser assertion failure remains in historical CI records; later passing runs do not explain its original cause. See the [Module 4D evidence index](evidence/module4d/README.md).

**Limitations and remaining acceptance.** Pinned browser validation against live services and a fresh container build/package check remain pending. Read-only API behavior and fixture tests do not establish production authentication, scale or availability. See [Module 4 §20](module4-plan.md#module4d-spec) and [Docker API/UI operations](docker.md#run-the-api-and-dashboard-against-existing-data).

## Kubernetes demonstration

**Objective and flow.** The demonstration is isolated from the persistent Compose environment. A run has a generated identity, private output directory, dedicated profile/context/namespace, run-owned storage and locked image/toolchain identities. B2 starts serving services; B3 runs the input, Module 1, Module 2, Module 4 and replay pipeline; B4 verifies runtime evidence. Jobs do not retry automatically. The HDFS lifecycle creates only a fresh run root and refuses to reuse it. MongoDB admin, publisher and API-read roles are separate; API/UI access is read-only. Secrets are passed through memory/stdin. Network policy starts from deny-by-default with only declared paths enabled.

The host reserves at least six physical cores, 12 GiB available RAM and 40 GiB free disk for the full workflow. Full-data input is capped at 4 GiB with an explicit storage budget; fixture input is bounded to 50 users and 5,000 events. The runtime lock is a template until actual host, cluster, storage, image and Python inventory identities are measured. Automatic resume is disabled. Success or failure retains resources and evidence; teardown is separate and rechecks ownership.

**Verification and remaining work.** Offline fake-client, static, rendering and runner checks were recorded, but no container build, image load, Minikube profile, Kubernetes workload, B2/B3/B4 acceptance or teardown was run for the reviewed source revision. Source state `runtime_pass` records completion of B4 and retained runtime resources; the separate `verify` command is offline and reports `teardown_performed: false`. Acceptance still requires image/package checks and an isolated live run with fresh identity-bound receipts. See the [Kubernetes guide](kubernetes.md).

## Historical synthetic-feed measurements

<a id="23-synthetic-feed-configurations"></a>

The following values are historical measurements from named feeds in the earlier documentation. They are not the 3 October full-data manifest described below and should not be treated as regenerated results.

| Preset | Scatter window | Span | Peak events/day | Mean events/day | Peak/mean |
|---|---:|---:|---:|---:|---:|
| `scatter_1w` | 1 week | 372 days | 428,965 | 90,912 | 4.72× |
| `scatter_1m` | 1 month (4 weeks) | 393 days | 241,413 | 86,054 | 2.80× |
| `scatter_3m` (default) | 3 months (13 weeks) | 456 days | 208,674 | 74,165 | 2.81× |

The separate 3 October 2026 full-data run used seed 42, 13-week scatter and 2,000 users per batch; its tracked manifest records a 212,714-event daily peak. Older prose used a different peak value, so this report follows the receipt-backed manifest and does not merge the two scopes. The generator's current `--batch-users` default is 20,000; that runtime memory setting is not the historical 2,000 value and does not affect generated events. See [feed generation](docker.md#synthetic-event-generation) and the [full-data manifest](evidence/full/manifest.json).

## Historical benchmark and acceptance findings

The 3 October batch benchmark used Spark 3.5.5, Java 17.0.14 and Python 3.10.12 on one two-core standalone executor, 32 shuffle partitions, seed 42, 13-week scatter and one warm-up plus three measured repetitions. Equivalent arms were checked for plan/row/checksum conditions. The report records the measurements and their cache/scan caveats in [full-data evidence](evidence/full/README.md). These results are not cluster performance claims.

The 3 October streaming report used one broker, four Kafka partitions and a two-core Spark setup. Its bounded throughput and partition measurements are detailed in [Module 3 evidence](evidence/module3/README.md); the small test's two-partition arm was fastest, without establishing a generally optimal partition count. The 10 October Module 4 acceptance report recorded 21/21 gates for its exact historical run, but its ALS metrics trailed the personal-frequency baseline and do not imply model superiority.

## Repository maintenance

A structural audit of the source tree, the scripts, the documentation and the
continuous integration definitions was performed against the reviewed
revision. The audit produced a module dependency graph, a list of duplicated
definition bodies, a list of cross-module private imports and an inventory of
unreferenced symbols. The changes below are behaviour-preserving except where
a measured improvement is stated.

**Shared layer.** A new `src/common/` package owns the building blocks that
every module had re-implemented: environment readers, credential redaction,
run identity and JSON report writing, numeric and record guards, and source
identity in `runtime.py`, and the shared Spark session contract in `spark.py`.
The three Module 2/3/4 Spark session modules now differ only in their
application name and their module-specific settings, and the three
configuration modules share their path and environment handling.

**File count.** The audit started at 426 tracked files and finished at 424,
with 19 genuinely new files and 47 removed or merged. The new files are the
shared layer (3), the Kubernetes primitives (1), the compose runner (1), the
demo entry point (1), the test helpers (1) and the evidence index, checksum
manifest and compact summaries (11). Everything else was merged or removed:

| Change | Before | After |
|---|---|---|
| `src/common/` helper modules | 7 | 3 |
| Kubernetes primitives | 2 | 1 |
| Test helper modules | 2 | 1 |
| Module 3 smoke scripts | 3 | 1 |
| Module 4 smoke scripts | 3 | 1 |
| Dashboard demo tools | 8 | 7 |
| Top-level documents | 18 | 11 |
| Legacy-named test files | 25 | 0 |
| Raw evidence dumps | 8 | 0 |
| `docs/evidence/` | 186 files, 1.55 MB | 188 files, 0.85 MB |
| Tracked files | 426 | 424 |

**Dependency direction.** Every implementation-to-implementation private
import was removed. The helpers that cross module boundaries are now public
(`locked_image`, `validate_pipeline_lock`, `checked`, `json_output`,
`read_receipt`, `build_document`, `hdfs_sha256`, `snapshot_validation`,
`readiness_identity` and the rest). The only remaining private cross-module
references are test-to-implementation ones, which is the normal shape of a
white-box test.

**Duplication.** Nine duplicated definition bodies were unified: the positive
integer, stored-UTC and BSON-millisecond guards, the canonical JSON writer,
the environment boolean parser, the bound-PV capacity reader, the required
environment reader, the JSON report writer, and the five Module 3 Compose
harness helpers, which now live in `scripts/module3_harness.py`. Four
unreferenced functions were deleted.

**Layout.** `scripts/module4/` mixed the live Module 4D dashboard tools with
the archived Module 4C experiment harnesses; the tools are now `scripts/dashboard/`
and the harnesses `scripts/experiments/`. `deploy/module4/` is now
`deploy/dashboard/`. The Kubernetes package gained `scripts/k8s/io.py` for
hashing, atomic receipt writing, redaction and `kubectl` JSON access, and
`scripts/k8s/constants.py` for the ports, claim sizes and name builders that
were previously written out in five places.

**Documentation.** The eighteen top-level documents were reduced to twelve by
merging the Kubernetes runbook into the Kubernetes guide, the external
acceptance handoff into the acceptance tutorial, the synthetic-generation and
executor-benchmark guides into the Docker guide, and the repository layout
guide into the root README. The evidence archive keeps every original byte
and gained a Module 4 run-directory index, compact summaries of the largest
raw artifacts, a checksum manifest and an explicit list of the five files that
live code registers. Every local markdown link resolves.

**Build context.** The `.dockerignore` allow-list carried twenty-five
per-file entries that a single directory rule already admitted; the redundant
lines were removed and the resulting build context was verified to be
unchanged. The tests now assert the admission outcome rather than the
spelling of an allow-list line.

**Measured improvements.**

| Change | Measurement |
|---|---|
| Zstandard Parquet for the generated feed, cleaned tables and staged interactions | 40% smaller than Snappy on the 33.8M event feed, at roughly the same write cost |
| Popularity baselines computed once per run instead of once per cohort | Removes about seven full aggregations of the 32.4M-row interaction table from a Module 4 evaluation |
| Spark task counters accumulated instead of retained | The event-log parser no longer holds every task event in driver memory |
| Kafka log retention configured | The topic no longer keeps every replayed event for the default seven days |
| The build-context allow-list and the CI branch triggers | Dead rules and triggers on branches that no longer exist were removed, and the CI workflows no longer repeat each other's selections |

**Still open.** The `final_recommendations` collection grows by a new
serving version per publication and has no retention rule. The Module 4 HDFS
checkpoint directory is declared as retained for audit and is never cleaned.
The Spark event log is still written uncompressed: Spark emits a compressed
log as a sequence of independent zstd frames and the only decoder available
to the project reads a single frame, so a compressed log cannot be parsed
back into the published benchmark metrics. The Kafka partition and
executor-layout experiments still have to run in the owner environment. These
are recorded rather than changed, because each one alters a published
contract or a recorded acceptance gate.

## Reproduction and operator references

<a id="d3-local-integration-and-release-validation"></a>

- Persistent services, isolated dashboard demo, Module 1–4 operating procedures and current validation command ownership: [Docker guide](docker.md).
- Kubernetes prerequisites, run stages, recovery, evidence capture and separately gated teardown: [Kubernetes guide](kubernetes.md).
- Exact offline test selection, exclusions and runtime work still required: [external acceptance handoff](acceptance.md#delivered-scope-and-evidence-boundaries).
- Contract and algorithm detail: [implementation ledger](implementation-ledger.md), [Module 3 guide](module3-contracts.md), and [Module 4 plan](module4-plan.md).

This page preserves the former reproduction anchors for existing links. It does not duplicate the detailed runbook commands.
