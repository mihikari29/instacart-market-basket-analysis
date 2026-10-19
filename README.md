# IT4931 — Instacart market basket analysis

This project studies product purchasing patterns in the public Instacart market-basket dataset and demonstrates how batch analytics and event-time trends can support recommendations. It combines Python, Apache Spark, HDFS, Kafka, MongoDB, FastAPI, Streamlit, Docker, and a guarded local Kubernetes demonstration.

The architecture follows the **Lambda Architecture** pattern. The batch layer computes stable historical aggregates and recommendation inputs from curated data. The speed layer consumes replayable purchase events and derives time-windowed trends. A serving layer combines versioned batch and streaming results and exposes bounded read-only views. Separating these paths makes their data scope, refresh behavior, and failure modes visible. The stream can advance while batch snapshots change on their own schedule; Module 4C reconciles them through explicit versions and readiness checks.

## Architecture

```mermaid
flowchart LR
  csv["Six source CSV files"] --> m1["Module 1"]
  m1 --> hdfs[("HDFS curated Parquet")]
  hdfs --> m2["Module 2 batch analytics"]
  hdfs -->|"prior interactions"| als["Module 4A ALS"]
  hdfs -->|"prior interactions"| graph["Module 4B product graph"]
  m2 --> batch[("MongoDB batch metrics")]
  batch --> api["Module 4D FastAPI read API"]
  m2 --> features[("HDFS prior-only user_features")]
  features --> featureReader["Bounded Module 4 descriptive reader<br/>(not a Hybrid ranking input)"]
  als --> alsStore[("MongoDB versioned ALS snapshots")]
  alsStore --> hybrid["Module 4C Hybrid"]
  alsStore --> api
  graph --> graphStore[("MongoDB graph snapshots")]
  graphStore --> hybrid
  m1 -->|"synthetic events"| kafka[("Kafka")]
  kafka --> m3["Module 3 event-time processing"]
  m3 --> legacy[("Mutable realtime_trending<br/>legacy output only")]
  m3 -. "OPTIONAL immutable publication" .-> bundles[("realtime_trending_windows<br/>complete finalized-window bundles")]
  bundles --> hybrid
  bundles -->|"trends route"| api
  hybrid --> hybridStore[("MongoDB recommendation snapshots")]
  hybridStore --> api
  api --> ui["Streamlit dashboard"]
```

The bounded Module 4 user-feature reader is a separate descriptive lookup. It does not feed Hybrid ranking. Module 3's legacy mutable rows are not a Hybrid source; only the explicitly enabled immutable finalized bundles serve the Hybrid and trends API paths shown above.

Module 1 creates replay timestamps because the source has weekday and hour fields, but no absolute purchase timestamps. Those dates and times are simulated values, not observed customer chronology. The ordinary generated feed includes both `prior` and `train` orders; its Kafka contract does not retain `eval_set`, so an event-time cutoff alone cannot make that feed safe for official-train evaluation. Module 4A/4B fit from prior-history data. Official train baskets are held out as evaluation truth. Module 4D reads through FastAPI; the dashboard does not connect directly to MongoDB.

| Module | Responsibility and principal output |
|---|---|
| **1 — Ingestion and storage** | Validate and clean six source tables, create deterministic replay events, stage curated Parquet and event feeds. |
| **2 — Batch layer** | Compute historical product, department, user and time-pattern analytics; publish prior-only descriptive user features. |
| **3 — Speed layer** | Consume Kafka, validate and deduplicate events within watermark state, aggregate event-time trends, and publish finalized results and telemetry. |
| **4A — ALS recommendations** | Fit implicit-feedback ALS from prior-history user/product counts, evaluate against held-out data, and publish versioned recommendations. |
| **4B — Product graph** | Build prior-basket product co-purchase relationships and graph metrics for the full catalog. |
| **4C — Hybrid serving** | Validate eligible source generations, combine ALS and trend candidates deterministically, evaluate prior-only inputs, and publish a versioned snapshot. |
| **4D — Read interface** | Serve bounded, read-only API views for recommendations, metrics, trends and registered evidence; render them in Streamlit. |

## Dataset and source relationships

The public release contains six CSV tables. Full-data evidence from 3 October 2026 recorded 3,421,083 orders, 32,434,489 prior order-product facts, 1,384,617 train facts, 49,688 catalog products, 206,209 users, 134 aisles and 21 departments. These values describe that dated run and its verified input, not a new measurement from this documentation revision. The detailed receipt and environment are in the [full-data evidence report](docs/evidence/full/README.md).

| Input file | Key fields and relationship | Purpose |
|---|---|---|
| `orders.csv` | `order_id` identifies an order; `user_id` groups a customer's order sequence. `order_id` is referenced by both order-product tables. | Evaluation-set label, order sequence number, weekday, hour and capped inter-order gap. |
| `order_products__prior.csv` | Each `(order_id, product_id)` pair is unique; references `orders.order_id` and `products.product_id`. | Historical interactions used for batch analysis, ALS fitting and graph construction. |
| `order_products__train.csv` | Same interaction keys and foreign keys as prior. | Held-out official train baskets used as recommendation evaluation truth; not ALS fit data. |
| `products.csv` | `product_id` is the product key; `aisle_id` and `department_id` reference their dimension tables. | Product names and catalog classifications. |
| `aisles.csv` | `aisle_id` is the dimension key. | Human-readable aisle names. |
| `departments.csv` | `department_id` is the dimension key. | Human-readable department names. |

```text
orders.order_id 1 ── N order_products__prior.order_id
orders.order_id 1 ── N order_products__train.order_id
products.product_id 1 ── N order_products__prior.product_id
products.product_id 1 ── N order_products__train.product_id
aisles.aisle_id 1 ── N products.aisle_id
departments.department_id 1 ── N products.department_id
```

Module 1 requires all six files and checks their headers, integer spellings and domains, evaluation-set values, keys, foreign keys, order-level consistency, unique basket items, and cart positions before coercing types. Each basket's `add_to_cart_order` must be the contiguous sequence `1..N`. This fail-closed order matters: a partially present or malformed table cannot silently produce plausible-looking joins. Cleaning uses compact integer and category types, maps a first-order missing gap to zero, reconstructs weekday-consistent values where the source's 30-day cap hides a longer gap, normalizes whitespace and nonbreaking spaces in product names, and retains catalog products even when they have no prior facts.

The capped gap reconstruction chooses the minimum weekday-consistent gap from 30 through 36 days and fails if the result does not agree with weekday progression. A historical 8 October cleaning record counted 206,209 first-order gaps filled, 369,323 capped gaps reconstructed, zero weekday mismatches and 16 normalized product names; see the [source-cleaning receipt](docs/evidence/module4/validation-20261008/source-clean.txt). The cleaner stages six Parquet outputs and an ownership manifest before replacing a previously recognized Module 1 publication. It refuses symlinked or overlapping raw/output paths and preserves the prior publication when validation or staging fails. This is a single-writer directory publication procedure, not a general multi-writer transaction.

Curated Parquet gives Spark typed, columnar inputs and supports date partition pruning for the generated event feed. CSV remains the source-cleaning format. Kafka messages are JSON records denormalized with product, aisle and department fields, so trend aggregation does not need to join dimensions.

## Module 1 — Replay event generation and staging

The source has weekday, hour and order-sequence information but no date. `SyntheticConfig` makes the simulation reproducible: its default seed is `42`, scatter window is 13 weeks, and within-basket item gaps use an exponential distribution with a 30-second mean clipped to 5–120 seconds. Other presets are `mobile-fast` (20 seconds, 3–90), `desktop-browse` (40 seconds, 10–240), `uniform` (15–50 seconds), and `deterministic` (hash-derived timing). Scatter windows are 1, 4, 13, 26 or 52 weeks. See [synthetic event generation](docs/generate.md) for the algorithm and [feed scenarios](docs/progress.md#23-synthetic-feed-configurations) for historical measurements.

Generation chooses a stable per-user anchor week, reconstructs order dates from cleaned gaps, and preserves the recorded weekday and hour. Stable hash-derived values keyed by user, order and cart position make results invariant to row order and the `--batch-users` processing-memory setting for the same source and seed. Within an order, item events advance according to the configured gap distribution. A final monotonicity pass shifts a later order forward when needed so that event times increase strictly by cart position and order sequence for each user. The event identifier is `order_id_add_to_cart_order`.

Parquet output also includes `order_number` and `synthetic_date` for analysis and partitioning. The Kafka event schema contains 13 fields: `event_id`, `order_id`, `user_id`, `product_id`, `add_to_cart_order`, `reordered`, `aisle_id`, `department_id`, `order_dow`, `order_hour_of_day`, `event_time_epoch_ms`, `event_time_iso`, and `ingestion_time_epoch_ms`. It uses `user_id` as the Kafka key. It does not include `eval_set` or `synthetic_date`; therefore the ordinary replay combines prior and train orders and is not a chronology-preserving evaluation feed.

Retain the six-file source receipt with the input/run evidence. HDFS stores the six raw CSVs separately from the curated Parquet tables; the curated upload writes `_sources.json`, and the partitioned interaction feed is promoted with its own `_handoff.json` receipt. Kafka replay is a separate step and has bounded completion checks in the current pipeline. The [Docker guide](docs/docker.md#module-1-ingestion-generation-staging-and-kafka-replay) owns the verified command sequence and paths.

## Module 2 — Spark batch layer

Module 2 reads the curated HDFS tables, verifies handoff receipts and structural relationships, and computes analytics from `order_products__prior`. Checks cover row counts, nulls, keys, foreign keys, domains, date partitions and exact fact/event multisets. An optional local event-source hash check is narrower than rehashing every staged input; Module 4 has a separate staged-Parquet hash path.

The batch outputs include product and department metrics, user-level descriptive features, weekday/hour distributions and pivoted time summaries. `user_features` records `user_id`, order and purchase counts, unique products, reorder rate and average basket size, with run ID, `prior_only` feature scope and UTC generation metadata. Module 4 has a bounded reader for one descriptive feature row. These features are not ALS model inputs. Batch publication uses run-owned temporary MongoDB collections and promotes each collection after validation. Individual collection promotion is atomic at the rename boundary, but a set of multiple collections is not one cross-collection transaction. Readers use run/version metadata to avoid mixing generations.

The benchmark tooling includes explicit join strategies, date and partition filters, cached versus uncached aggregation, local thread configurations, and a separate existing-standalone-master executor-layout experiment. The full-data evidence records a Sort-Merge versus Broadcast Hash join comparison, equivalent filtered outputs, and cache materialization separately from reuse. It cautions that the host's caches were warm and the all-date arm had a different scan scope. See [Module 2 benchmark design](docs/module2-executor-benchmark.md) and the [dated measurements](docs/evidence/full/README.md).

## Module 3 — Kafka and Spark Structured Streaming

The producer sends one JSON record per product event. Event time comes from the deterministic simulator; ingestion time is assigned when publishing. Kafka delivery, event time and ingestion time are distinct clocks. The consumer validates schema and domains, counts invalid records, deduplicates by `event_id` within watermark-managed state and computes the 30-minute and 120-minute trend windows from one 120-minute sliding stateful aggregation. The configured slide is five minutes and the event-time watermark is ten minutes.

Append mode publishes only finalized windows. For each finalized window, the complete product population is normalized before Top-20 selection; equal scores are ordered by `product_id`. The short and long window scores combine with weights 0.7 and 0.3. The mutable legacy `realtime_trending` output preserves its older replace/update contract. Optional `realtime_trending_windows` bundles are immutable, versioned inputs for Module 4C and the Module 4D trends view. Bundle mode is opt-in and its publication and index requirements are documented in the [Module 3 contract](docs/module3-contracts.md).

Watermark deduplication is bounded state, not permanent event-ID uniqueness. The legacy delete-and-upsert sequence is not a read transaction, and each immutable bundle document is individually atomic rather than all windows being one transaction. Checkpoint identity and schema version are explicit; incompatible legacy state fails without mutation, and Kubernetes resume is disabled until run ownership and offset continuity are established.

Finite replay is considered complete only when the producer reports success, receipt counts reconcile, and consumed terminal offsets match across Kafka partitions. A replay may end with an event-time tail that is not yet finalized; the pipeline does not invent future events to force windows closed. Completed-batch telemetry pairs each executed Spark batch with the receipt for that batch, including zero-input state-cleanup batches. It tracks bounded input/processing rates and, for finalized output, ingestion-to-publication-completion latency. That latency uses producer and output-host wall clocks, includes watermark delay, and may expose clock skew; it is not Kafka acknowledgement latency or UI response time. See the [streaming contract](docs/module3-contracts.md) and [historical bounded evidence](docs/evidence/module3/README.md).

## Module 4 — Recommendations, graph, Hybrid and dashboard

**Module 4A, ALS.** The model fits implicit-feedback ALS from prior user-product purchase counts. Its internal holdout is drawn from eligible prior-history users; official train baskets are kept for evaluation and do not select ALS fitting parameters. Evaluation compares ALS with global popularity and personal frequency at multiple cutoffs and reports macro ranking metrics and coverage. A historical full-data run recorded ALS Recall@10 below personal frequency; no claim of recommendation-quality improvement is made. The [acceptance record](docs/evidence/module4/acceptance-20261010/README.md) preserves its exact scope and run identity.

**Module 4B, product graph.** The graph uses prior baskets and retains the full catalog, including inactive products as isolated vertices. It counts co-occurring product pairs, applies configured edge thresholds, derives neighbor and weighted-degree metrics, and runs GraphFrames PageRank on symmetric directed topology. Co-purchase weights inform weighted degree; PageRank uses the graph topology rather than weighted PageRank. The recorded 10 October run is historical evidence, not a fresh acceptance for the current source revision.

**Module 4C, Hybrid.** Serving reads ready versioned ALS/graph snapshots and the newest eligible finalized trend bundle at an explicit cutoff. It normalizes the selected ALS and trend candidate scores, unions their candidates, ranks a bounded endpoint candidate set deterministically and publishes immutable per-user results behind a readiness pointer. Cold users can use trend candidates. Prior-only evaluation uses a fixed prior global-frequency candidate source and a preregistered lambda sweep against official train truth; it does not claim temporal trend quality because the generated stream mixes prior and train and drops `eval_set` from the message schema. Synthetic C8A/C8B experiments show integration behavior only, not accuracy or capacity.

**Module 4D, API and Streamlit.** FastAPI provides bounded, indexed, read-only views for Hybrid recommendations, ALS outputs where available, batch metrics, order distributions, trends, streaming metrics and registered evidence. API readiness checks are distinct from liveness; the dashboard accesses data only through the API. The four dashboard pages cover overview, real-time and batch views, Hybrid recommendations, and evaluation/performance. Missing, empty, unavailable and malformed snapshots have distinct responses; unavailable scores remain blank instead of being presented as zero. Exact ranking, read and route contracts are in the [Module 4 plan](docs/module4-plan.md#module4c-spec).

## Environments and where to work

The root `compose.yaml` is the persistent development stack for Kafka, Spark, HDFS and MongoDB. Its named volumes preserve state. The isolated synthetic dashboard environment in `deploy/module4/compose.demo.yaml` uses disposable MongoDB storage and loopback-only API/UI ports; it does not exercise production capacity. The [Docker guide](docs/docker.md) owns verified image targets, prerequisites, operating commands, and cleanup boundaries.

Use the approved [Proposal](docs/Proposal.md) for the assignment objectives and requirements, and the [documentation index](docs/README.md) to navigate current contracts, procedures and historical evidence.

The Kubernetes demonstration uses a fresh run identity, profile, context, namespace and storage. It separates serving (B2), processing (B3) and acceptance (B4), records private receipts, disables automatic retry/resume, and retains resources on failure. Teardown is a separate operation that rechecks run ownership. Its resource bounds, runtime identity lock, exact commands and limitations are in the [Kubernetes runbook](docs/kubernetes-demo-runbook.md). Kubernetes Spark runs in local mode inside run-owned jobs; this is not Spark executor scaling across Kubernetes workers.

For an executable procedure, use the canonical runbooks rather than copying a command into this overview: [Module 1](docs/docker.md#module-1-ingestion-generation-staging-and-kafka-replay), [Module 2](docs/docker.md#module-2-batch-processing-and-benchmarks), [Module 3](docs/docker.md#module-3-streaming-operations), [Module 4A/4B](docs/docker.md#module-4a4b-operation-and-full-data-acceptance), and [Kubernetes](docs/kubernetes-demo-runbook.md). Test selection and service-backed exclusions are recorded in the [acceptance handoff](docs/external-acceptance-handoff.md); implementation files are mapped in the [repository layout](docs/repository-layout.md).

## Results and limits

Historical full-data, streaming and Module 4 acceptance evidence is linked above and indexed in [docs/evidence](docs/evidence/README.md). Those documents retain their original dates, code/environment identities and experiment boundaries. The project status page reports offline development checks associated with the reviewed source revision: 803 passed and 7 Spark-dependent modules skipped in the broad non-service selection, plus focused selections of 59 and 134 passes, repository Ruff, and compilation of 190 files. The independent source-code review is a separate outcome and does not imply runtime behavior. None of these are new results from this documentation update.

Selected historical findings help show what the evidence means:

| Experiment | Recorded observation | Scope limit |
|---|---|---|
| Full-data batch joins | Median Sort-Merge Join 17.581934 s; Broadcast Hash Join 2.304046 s. | One warm, two-core executor; not a cluster comparison. |
| Full-data date filtering | All-date scan 3.497363 s; date filter 1.666808 s; partition filter 0.074906 s for the same 177,739-event filtered result. | The all-date scan reads a different amount of data; OS/HDFS caches were not flushed. |
| Cached aggregation | Uncached 2.110284 s; cache materialization 2.175583 s; cached reuse 0.088085 s. | Include materialization cost when comparing total work. |
| Bounded streaming | The 50k-event run processed 3,203.90 rows/s and finalized 35,701 windows into 580,342 Mongo documents. | One broker and a small Spark runtime; sliding windows amplified state to 1,174,010 aggregate rows. |
| ALS quality baseline | Historical Recall@10 was 0.121472 for ALS versus 0.319232 for personal frequency. | Dated Module 4 acceptance; no quality-improvement claim. |

Full trial context, environment identity and limitations are in the [full-data report](docs/evidence/full/README.md), [Module 3 evidence](docs/evidence/module3/README.md), and [Module 4 acceptance record](docs/evidence/module4/acceptance-20261010/README.md).

External runtime acceptance remains pending. That work includes pinned Spark/MongoDB integration, browser and container-package checks, isolated Kubernetes B2/B3/B4 execution, runtime checkpoint recovery, full-data processing, actual performance/executor experiments, and fresh identity-bound evidence. Historical results, offline fixture checks, source review and runtime acceptance are different evidence scopes. See [current project progress](docs/progress.md) and the [external acceptance handoff](docs/external-acceptance-handoff.md) for the exact boundaries.

<a id="lambda-modules-spec-7--verify-against-assignment"></a>
