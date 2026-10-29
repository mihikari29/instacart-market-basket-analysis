# Implementation and design ledger

This ledger records the system's interfaces, important implementation choices and the reasons behind them. It describes current source behavior and known limits, rather than replaying review milestones. The repository [Proposal](Proposal.md) is retained as the requirements baseline; [available historical evidence](README.md#historical-evidence-available-here) lists the artifacts actually included in this checkout.

## 1. Architecture and module boundaries

The project separates data acquisition and preparation (Module 1), historical processing (Module 2), event-time processing (Module 3), recommendation and graph computation (Modules 4A/4B), their combined serving snapshot (Module 4C), and read-only presentation (Module 4D). HDFS stores curated Parquet and staged feed artifacts. Kafka provides replayable product events. Spark performs batch and streaming work. MongoDB stores versioned serving outputs and trend data. FastAPI is the only database boundary used by Streamlit.

```mermaid
flowchart LR
  csv["Six source CSV files"] --> m1["Module 1"]
  m1 --> hdfs[("HDFS curated Parquet")]
  hdfs --> m2["Module 2 batch analytics"]
  hdfs -->|"prior interactions"| als["Module 4A ALS"]
  hdfs -->|"prior interactions"| productGraph["Module 4B product graph"]
  m2 --> batch[("MongoDB batch metrics")]
  batch --> api["Module 4D FastAPI read API"]
  m2 --> features[("HDFS prior-only user_features")]
  features --> featureReader["Bounded Module 4 descriptive reader<br/>(not a Hybrid ranking input)"]
  als --> alsStore[("MongoDB versioned ALS snapshots")]
  alsStore --> hybrid["Module 4C Hybrid"]
  alsStore --> api
  productGraph --> graphStore[("MongoDB graph snapshots")]
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

The Module 2 feature reader is a bounded descriptive lookup, separate from Hybrid ranking and the API's Mongo-backed routes. The API reads batch metrics and ALS snapshots directly; Hybrid consumes ready ALS/graph snapshots and, when enabled, immutable finalized trend bundles. The trends API also reads those optional bundles. The legacy mutable trend collection is a separate output and is not a Hybrid source. These interfaces retain distinct source scope and version identities. A/B publication does not become a single transaction merely because both snapshots are required by readiness. The API returns the source identities used by each view so a caller can distinguish a current snapshot from an unavailable one.

## 2. Module 1 — raw contracts, cleaning and publication

### 2.1 Source tables and validation order

The input contract is exactly six CSV tables:

| Table | Primary/unique identity | Relationships |
|---|---|---|
| `orders` | `order_id` | `user_id` groups order sequences. |
| `order_products__prior` | unique `(order_id, product_id)` | `order_id` → `orders`; `product_id` → `products`. |
| `order_products__train` | unique `(order_id, product_id)` | Same keys as prior, but held out for recommendation evaluation. |
| `products` | `product_id` | `aisle_id` → `aisles`; `department_id` → `departments`. |
| `aisles` | `aisle_id` | Product classification dimension. |
| `departments` | `department_id` | Product classification dimension. |

The cleaner requires every table, rejects duplicate header names and any missing or unexpected column, and initially reads values as strings. It validates canonical positive integer text (allowing zero-padded hour text only for `order_hour_of_day`), numeric ranges, allowed `eval_set` values, unique identifiers, order/user relationships, prior/train correspondence, foreign keys, and raw-value domains before numeric coercion. Diagnostics identify table, row and field without echoing arbitrary source values. Validation errors are bounded to a small set of messages.

`add_to_cart_order` values must be positive, unique within an order, and exactly contiguous from 1 through N. This is stronger than checking only uniqueness or maximum position; gaps would invalidate event sequence and basket interpretation. The cleaner rejects malformed or empty source files before creating a new publication.

### 2.2 Cleaning decisions

- A missing `days_since_prior_order` is accepted only for the first order and becomes `0.0`.
- A source value of 30 is a cap, not necessarily an exact interval. For a later order, the cleaner chooses `30 + ((current_dow - previous_dow - 2) % 7)`, the minimum compatible gap in `[30, 36]`, and then checks the gap modulo seven against weekday progression.
- Product names replace nonbreaking spaces, collapse whitespace and trim outer whitespace.
- Integer, boolean, category and string columns are converted to compact dtypes only after raw validation. The full catalog remains present, including products without prior interactions.
- `eval_set=test` rows remain represented in cleaned order metadata but are outside generated prior/train purchase events.

The weekday-consistent reconstruction is a simulation aid; it cannot recover the true uncapped interval. Generated calendar dates are not source facts.

### 2.3 Safe local publication

The cleaner writes the six expected Parquet files and `.it4931-clean-output.json` into a sibling staging directory. Before replacement it rejects symlink traversal, raw/output overlap, non-directory output paths, unknown entries and invalid ownership manifests. It renames a previously recognized publication to a temporary backup, renames the complete staged directory into place, restores the backup if promotion fails, and removes only the expected files from a validated backup. It does not recursively delete arbitrary paths. The sequence assumes one writer; it is not a distributed or concurrent-writer transaction.

### 2.4 Synthetic event-time algorithm

The source tables contain order sequence number, weekday and hour, not absolute timestamps. The generator starts from Monday 2024-01-01 and computes a per-user anchor using a stable hash of `(seed, "user-anchor-week", user_id)` within the selected scatter window. It accumulates cleaned inter-order gaps, adds the recorded order hour, and derives minute and second from stable order-keyed namespaces in uniform mode. Deterministic/hash mode derives order time from order identity. A stable namespace hash and SplitMix64 values select within-cart intervals keyed by `(seed, "event-item-gap", order_id, add_to_cart_order)`.

`SyntheticConfig` defaults to seed 42, 13 scatter weeks, uniform order-time sampling and exponential within-cart gaps with mean 30 seconds clipped to 5–120. Other declared scenarios are `mobile-fast` (20 seconds, 3–90), `desktop-browse` (40 seconds, 10–240), `uniform` (15–50) and `deterministic` (hash time mode). Supported scatter windows are 1, 4, 13, 26 and 52 weeks. `--batch-users` limits processing batch size (default 20,000), not the event population or generated values.

The first item in a basket has zero within-order delay; subsequent item timestamps accumulate sampled intervals. A per-user chronological pass shifts an order forward when its first event would not exceed the previous order's final event by at least one second. This establishes strict monotonic event time along `(user_id, order_number, add_to_cart_order)` while preserving the relative sequence. A manifest records configuration, determinism keys, batch-size invariance, counts, date span, daily distribution and monotonicity count.

The Parquet event output also records `order_number` and `synthetic_date`. The Kafka serializer's contract has 13 fields: `event_id`, `order_id`, `user_id`, `product_id`, `add_to_cart_order`, `reordered`, `aisle_id`, `department_id`, `order_dow`, `order_hour_of_day`, `event_time_epoch_ms`, `event_time_iso` and `ingestion_time_epoch_ms`. The producer uses `user_id` as message key and assigns ingestion time at send time. It does not send `eval_set` or `synthetic_date`. The normal feed includes prior and train orders, so consumers cannot reconstruct a prior-only stream from that payload.

### 2.5 HDFS and Kafka staging

HDFS staging retains raw inputs and their source receipt separately from curated Parquet. The default root is `/instacart`; isolated runs can supply a run-owned root. The principal paths are:

| Data | HDFS path under the selected root | Publication/ownership note |
|---|---|---|
| Raw input | `raw/<table>/<source-file>.csv` for each of the six source tables. | The six-file source receipt is retained as separate input/run evidence; the raw HDFS area stores the CSVs. |
| Curated orders | `curated/orders/orders.parquet` | Uploaded as an individual curated table. |
| Curated interactions | `curated/order_products_prior/order_products__prior.parquet`; `curated/order_products_train/order_products__train.parquet` | Counted and hashed in `curated/_sources.json`. |
| Curated dimensions | `curated/dimensions/{products,aisles,departments}/<table>.parquet` | Each table is uploaded at its canonical dimension path. |
| Partitioned replay interactions | `curated/interactions/` with date partitions and `_handoff.json`. | Uploaded under a temporary sibling; promoted through a separate staging/backup/rename sequence, with rollback on failure. |
| Descriptive user features | `features/user_features/` | Module 2 writes this prior-only handoff; Module 4 reads a bounded row. |
| ALS inputs and model area | `features/als_interactions/`; `models/als/` | Module 1 creates the expected path placeholders; model jobs own their output content. |

Staging verifies the produced receipt and promotes the interaction handoff through its own staging/backup/rename procedure. This boundary prevents a Module 1 run from claiming that every HDFS location is one atomic publication.

Kafka replay is separate from file generation and staging. Finite replay requires producer success, receipt/count reconciliation, and committed terminal offsets across partitions. A bounded consumer reaching end of current input does not prove the event-time tail has finalized.

## 3. Module 2 — batch analytics, features and performance

### 3.1 Input checks and analytics scope

Module 2 checks source receipts and handoff metadata, counts, nulls, keys, foreign keys, domains, date partitions, and exact fact/event multisets with Spark set operations. Its primary history join matches prior facts to order attributes by `order_id`, then broadcasts the product dimension for the `product_id` enrichment. It does not independently compute SHA-256 equality for every staged table. Its optional `--source-events` check compares a supplied local event feed against its receipt; Module 4 staged-Parquet hashing is a separate check.

Historical analytics use `order_products__prior`. The `user_features` output contains `user_id`, `order_count`, `purchase_count`, `unique_products`, `reorder_rate` and `avg_basket_size`, with `module2_run_id`, `feature_scope=prior_only`, and `generated_at_utc`. The Module 4 feature reader validates the exact contract, performs a bounded user lookup and detects duplicate rows. These descriptive features are not ALS fit inputs; an absent row does not by itself prove an unknown user.

Analytics include product/department aggregates, weekday and hour distributions, user feature extraction and department-hour pivot/unpivot reconciliation. Date partition filters, exact plan choices and result equality are recorded with benchmark evidence.

### 3.2 Mongo publication and reader identity

Writers use bounded bulk batches and run-owned temporary collections, validate the expected document set and indexes, then promote a collection by rename. Product collection indexes are created on the replacement before it is promoted. Promotion is atomic for one same-database collection rename, not for a set of product, department, feature or metric collections. Readers must use run/version IDs and readiness contracts rather than assume a cross-collection transaction.

### 3.3 Benchmark design and rationale

The local comparison runs `local[1]` and `local[2]` in separate Spark processes against the same measured source and records output equality, source identity, configuration, bytes/rows, wall time, plan, event log and shuffle/task information. The standalone executor experiment is separate: it attaches to an existing master, uses declared executor layouts, and verifies actual registered executor IDs/host/ports and allocated resources through Spark REST evidence. Neither mode creates a cluster or assumes the requested memory is the allocated heap. A benchmark result is valid only with its input, output, code/runtime, arm configuration and trial records.

The join benchmark holds data and output constant while exercising Sort-Merge and Broadcast Hash plans; scan comparisons distinguish all-date, date-column and partition-column scope; aggregation reports cache construction separately from cached reuse. Historical timings were recorded on a warm, single-executor machine with no OS/HDFS cache flush. They describe that execution only and do not predict cluster performance.

## 4. Module 3 — event-time processing and telemetry

The speed layer uses one sliding stateful aggregate rather than independent
overlapping state plans. Finalization lets ranking see the complete product
population. Watermark-bounded deduplication controls state; it does not provide
permanent uniqueness. A complete immutable window document gives readers a
stronger consistency boundary than delete/upsert of mutable rows.

The [Module 3 specification](module3-contracts.md) owns parsing, windows,
normalization, bundle identity/expiry, checkpoint compatibility, replay-offset
completion, completed-batch coordination and BSON telemetry validation. These
are separate contracts: consumed terminal offsets do not close an event-time
tail, and a latency sample is not proof of stream liveness or Kafka lag.

## 5. Module 4 — model, graph, Hybrid and read interfaces

The [Module 4 specification](module4-plan.md) owns detailed model, graph,
publication, ranking, API and dashboard contracts. The cross-module decisions
are:

| Boundary | Rationale and authoritative contract |
|---|---|
| Prior versus official train | Fit and select parameters using prior history; reserve official train for final evaluation. [ALS and cohorts](module4-plan.md#module4a-als-recommendation) keep the publication population distinct from evaluation cohorts. |
| Graph versus recommendation | Preserve catalog isolates and distinguish weighted degree from topology PageRank. [Graph contracts](module4-plan.md#module4b-product-co-purchase-graph) govern threshold/scope and runtime semantics. |
| A/B readiness versus Hybrid publication | Source captures and per-user pointer promotion have distinct identities. Separate Mongo promotions do not become a distributed transaction. [Publication](module4-plan.md#8-mongodb-snapshot-publication) and [Hybrid contracts](module4-plan.md#module4c-spec) define validation and race handling. |
| Publishers versus read interfaces | API credentials cannot publish or repair indexes; UI uses HTTP only. [API/UI contracts](module4-plan.md#module4d-spec) define bounded reads, errors and registered evidence. |

An absent ALS row is not enough to classify an unknown/cold user without
verified source context. A missing readiness pointer is not proof that a user
does not exist. Refreshing a view re-reads a snapshot; it does not demonstrate
that batch or streaming processing is currently running.

## 6. Kubernetes isolation, security and lifecycle

### 6.1 Run ownership and storage

Every run has a 12-hex ID, profile name `it4931-k8s-<id>`, namespace identity and private output outside all worktrees with restrictive permissions. Colliding output or existing run identity fails. Commands use an explicit profile/context. A shared B2/B3 lock binds selected runtime versions to actual host/toolchain/cluster/storage/image identities; the committed lock remains incomplete until those values are measured. No identity is fabricated from a template.

After HDFS readiness, a checked bootstrap creates only the fresh `/instacart/<run-id>` root, assigns it to `k8spipeline` with mode 750 and refuses to reuse an existing root. It does not recursively change ownership of unrelated paths. HDFS staging, Spark writes and readback use this run-scoped identity.

The host admission reserves six physical CPU cores, 12 GiB available RAM and 40 GiB disk. Full-data raw inputs are capped at 4 GiB; pipeline storage budget is 16 GiB claim plus transfer/work headroom, with an explicit storage-class requirement. Fixture mode is capped at 50 users/5,000 events. WSL backing-disk capacity must be supplied as measured operator evidence.

### 6.2 B2/B3/B4 sequencing and runtime boundary

- **B2** provisions serving services, MongoDB users/indexes, API/UI and readiness receipts.
- **B3** runs isolated input preparation, Module 1, Module 2, Module 4, replay/streaming and Hybrid work in ordered run-owned Jobs.
- **B4** compares live ownership receipts and verifies the run's acceptance matrix, including data receipts, Kafka delivery/consumption, checkpoint, Mongo publication, API/UI and allowed/denied network paths.

Pipeline Jobs use `backoffLimit: 0`; failures retain resources for inspection. The coordinator does not auto-resume; container-step receipt reuse follows `execution.resume_policy` and is not an end-to-end recovery guarantee. B3 Spark uses local mode inside run-scoped Kubernetes jobs; Kubernetes does not allocate Spark executors across worker nodes. Mongo admin, publisher and API-read credentials are separate. Secrets are passed through memory/stdin and are not stored in receipts. Network policy defaults to deny and expresses only declared paths, but live CNI probes are needed to prove enforcement.

A `runtime_pass` coordinator receipt means B4 returned a complete passing receipt for the declared fixture or full-data scope. It does not mean resources were torn down. The separate `verify` operation checks saved receipts offline, contacts no cluster and reports `teardown_performed: false`. Teardown is a separate command with explicit execution, live identity rechecks and exact run-scoped targets. This separation retains evidence and prevents automatic cleanup from hiding a failed run.

### 6.3 Image and package provenance

The root Dockerfile has explicit `spark`, `api`, `ui`, `browser`, `k8s-runner` and `pipeline` targets. API inherits the Spark stage for accepted imports/fixtures; UI and browser are separate dependency environments. The manual Kubernetes packaging workflow imports the runner, UI and pipeline package surfaces with networking disabled and no repository source mount. A configured workflow is not evidence that it has passed; it must be dispatched and its result retained. Base image, runtime, dependency, jar, copied-file and final image identities remain tied to their recorded revisions.

## 7. Failure behavior and deliberately limited guarantees

- Raw validation occurs before coercion and publication; invalid input leaves the current recognized clean output in place.
- The cleaner's directory swap is safe for one writer, not a cross-filesystem or multi-writer transaction.
- HDFS table and interaction handoffs use separate receipts and promotion boundaries.
- Module 2 validates multiset/count contracts and scoped source receipts, not universal hashes of every staged file.
- Mongo collection rename is atomic for that collection only. Multi-collection outputs require run/readiness identity checks.
- Legacy trend delete/upsert is not atomic for readers; immutable bundle identity is the stricter path.
- Watermark dedupe expires from state; it does not permanently reject every future duplicate.
- Kafka terminal offsets prove bounded input consumption, not event-time closure of the final window.
- Checkpoint compatibility and run ownership are explicit; the Kubernetes coordinator does not auto-resume.
- API health is not data readiness. Dashboard refresh is not stream liveness.
- Fixture, offline source checks, historical full-data evidence, runtime acceptance and production capacity are separate claims.

## 8. Verification and provenance

[Project status](progress.md) owns current capabilities, unfinished work and
verification limits. The [acceptance appendix](acceptance.md#historical-development-record)
preserves the historical source-review identities, scoped host-check results
and missing-artifact qualifications once. They do not validate the current
source's live-service, full-data or deployment behavior.

New evidence must identify source/dirty state, runtime, input, configuration,
commands and outcomes for its actual scope. Historical observations remain tied
to their original provenance; unavailable artifacts must be reported as missing,
not replaced by a current runbook or inferred from a configured workflow.
