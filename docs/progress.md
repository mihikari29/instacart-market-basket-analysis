# Project Progress & Technical Runbook

> **A Lambda-based Big Data system for purchase behavior analytics and trend-aware product recommendation**
>
> **Course:** Big Data Storage and Processing (IT4931)
>
> **Reference architecture:** [Proposal Document](Proposal.md)

---

## 1. Architecture overview and module status

| Module | Primary responsibility | Technical components | Status |
|---|---|---|---|
| **Module 1** | Ingestion, cleaning, synthetic timestamps, HDFS staging, Kafka replay | Python, PyArrow, HDFS WebHDFS, Kafka Producer | **Complete and validated** |
| **Module 2** | Batch Layer, historical analytics, MongoDB serving, join/pruning/cache benchmarks | Apache Spark 3.5.5, PySpark, Standalone Cluster, MongoDB 7.0 | **Complete and validated on the full dataset** |
| **Module 3** | Speed Layer, event-time stream processing, watermark, realtime trending | Spark Structured Streaming, Kafka Consumer, MongoDB Sink | **Complete and validated** |
| **Module 4** | ML & Serving, collaborative filtering, GraphFrames, API, and dashboard | Spark MLlib (ALS), GraphFrames, FastAPI, Streamlit/React | *Ready for the next stage* |

---

## 2. Module 1 — Ingestion & Storage

### 2.1. Processing flow

```text
data/raw/*.csv (6 Instacart files)
  │
  ├──> python -m src.module1.clean
  │      - Narrow dtypes to reduce memory use
  │      - NaN days_since_prior_order (order_number=1) -> 0
  │      - Cap-30 synthetic reconstruction [30, 36] consistent with Day-Of-Week (DOW)
  │      - Normalize text by removing the '\xa0' character from product_name
  │      - Write data/clean/*.parquet
  │
  ├──> python -m src.module1.generate
  │      - Entity-keyed SplitMix64 by user/order/cart position
  │      - Identical logical output for the same source and seed for every --batch-users value
  │      - Cumulative days and session-level exponential item deltas
  │      - Monotonicity guard that preserves per-user chronological order
  │      - Write data/synthesized/scatter_{1w,1m,3m}/events.parquet
  │
  ├──> python -m src.module1.stage_hdfs
  │      - Upload the 6 raw CSV files to /instacart/raw/
  │      - Upload curated Parquet files to /instacart/curated/
  │      - Partition interactions into 456 daily partitions (/instacart/curated/interactions/synthetic_year=.../synthetic_month=.../synthetic_day=...)
  │      - Write metadata and validation receipts: _sources.json, _handoff.json
  │      - Staging plus backup/restore preserves the previous target after upload or promotion failure
  │
  └──> python -m src.module1.producer
         - Disk-backed external merge sort with memory bounded by chunk and fan-in sizes
         - Replay events to Kafka topic 'instacart-purchase-events' (Key = user_id)
         - Receipt separates attempted / broker-acknowledged / failed deliveries
         - Assign ingestion_time_epoch_ms at send time
         - Duplicates retain event_id; late copies use distinct deterministic IDs and older timestamps
         - Support fault simulation: late data, duplicates, bursts, and poison pills
```

### 2.2. Cleaning and reconstruction results

- **First-order NaN gap:** 206,209 rows $\rightarrow 0.0$
- **Cap-30 reconstruction:** 369,323 rows with gap $= 30$ reconstructed as the minimum weekday-consistent synthetic gap in $[30, 36]$ $\rightarrow$ **0 DOW mismatches**
- **Normalized `\xa0` characters:** 16 products
- **Test orders:** 75,000 orders isolated from the streaming/prior interaction path
- **Monotonicity check:** **0 violations** across all 33,819,106 events when ordered by the business sequence `user_id` $\rightarrow$ `order_number` $\rightarrow$ `add_to_cart_order`.

### 2.3. Synthetic feed configurations

| Feed | Scatter Window | Span | Peak / Mean Daily Events | Manifest |
|---|---|---|---|---|
| `scatter_1w` | 1 week | 372 days | 428,965 / 90,912 | `data/synthesized/scatter_1w/manifest.json` |
| `scatter_1m` | 1 month | 393 days | 241,413 / 86,054 | `data/synthesized/scatter_1m/manifest.json` |
| `scatter_3m` (default) | 3 months | 456 days | 208,674 / 74,165 | `data/synthesized/scatter_3m/manifest.json` |

---

## 3. Module 2 — Batch Layer

### 3.1. Handoff contracts and HDFS validation

Module 2 reads from HDFS (`/instacart/curated/`) and performs strict validation before execution:

- Validate `_sources.json` and `_handoff.json`, including exact SHA-256 matches.
- Reconcile fact and event multisets: 3,421,083 orders; 32,434,489 prior facts; 1,384,617 train facts; 49,688 products; 134 aisles; 21 departments; 206,209 users; and 33,819,106 events across 456 partitions.
- Enforce separation: `prior` is used for historical metrics and feature extraction, while `train` is preserved as ground truth for Module 4 recommendation evaluation.

### 3.2. Spark SQL analytics and MongoDB serving

The pipeline performs the following advanced transformations and analyses:

1. **User features:** Compute `order_count`, `purchase_count`, `unique_products`, `avg_basket_size`, and `reorder_rate` for each user, then write them to `/instacart/features/user_features`.
2. **Product and department metrics:** Compute support-controlled reorder rates (`min_support`), total purchases, day-of-week distributions, and hour-of-day distributions.
3. **Department-hour pivot/unpivot:** Create a 21-department × 24-hour matrix, convert it back to long form with Spark `DataFrame.unpivot`, and fail fast if the two representations have different `purchase_count` totals.
4. **MongoDB publication:** Publish batch results to the `batch_product_metrics` and `department_metrics` collections for the API and dashboard.

### 3.3. Performance optimization benchmarks

Each experiment was measured independently with one warm-up and three runs, using metrics extracted directly from the Spark event log:

1. **Join strategy (Broadcast Hash Join vs. Sort-Merge Join):**
   - Compare a join between the large interaction table and the small product/department dimensions.
   - Result: Broadcast Hash Join eliminated Shuffle Write and Shuffle Read for the dimension table and substantially reduced query latency.
2. **Partition pruning:**
   - Compare scanning all 456 days with a query containing a date-range predicate.
   - Result: Spark pushed the filter into the file scan (`PartitionFilters`), read only the required daily directories, and reduced I/O by more than 90% for short date ranges.
3. **In-memory caching:**
   - Compare a `MEMORY_AND_DISK` cached DataFrame with repeated scans and computation from HDFS Parquet.
   - Result: Caching substantially accelerated repeated work across multiple aggregation stages.

See the execution plan, measurements, and logs in the [full-data evidence](evidence/full/README.md).

Full-data validation **passed**: 51 tests; 33,819,106 events in each source, local, and HDFS representation; 456 partitions; equal pivot/unpivot totals of 32,434,489; and successful benchmark and MongoDB publication checks. The configuration, measurements, and limitations are recorded in the [full-data evidence](evidence/full/README.md).

---

## 4. Module 3 — Speed Layer / Structured Streaming

### 4.1. Final streaming architecture

```text
Module 1 producer
  -> Kafka topic instacart-purchase-events
  -> Spark Structured Streaming
  -> schema/domain validation + record_quality metrics
  -> event-time watermark
  -> event_id deduplication within watermark
  -> one stateful 120m sliding aggregation + exact conditional C30
  -> append-mode finalized window
  -> foreachBatch full-population normalization + deterministic ranking
  -> configurable Top-K
  -> replace complete finalized snapshot + idempotent MongoDB upsert
```

The runtime uses Spark 3.5.5 and `spark-sql-kafka-0-10_2.12:3.5.5`. Kafka is available at `localhost:9092` from the host and `kafka:29092` in Docker. Min-max normalization and the analytical ranking window operate on the static micro-batch DataFrame supplied by `foreachBatch`, rather than directly on a streaming DataFrame. This implements **finalized-window trending**, not an open-window ranking recalculated at every processing trigger.

The Kafka source is strict by default with `failOnDataLoss=true`; missing offsets fail the query instead of being skipped silently. Relaxed mode requires the explicit `MODULE3_FAIL_ON_DATA_LOSS=false` setting or the `--allow-data-loss` CLI flag. The `startingOffsets` and checkpoint semantics remain unchanged.

### 4.2. Event time, watermark, and trending

The pipeline uses a **10-minute** event-time watermark, a **120-minute** long window, a **30-minute** short window, and a **5-minute** slide. In the same stateful aggregation, `C120` counts all purchases in the window, while `C30` conditionally counts events in its final 30 minutes. The trend score is:

`trend_score = 0.7 × norm(C30) + 0.3 × norm(C120)`

Results are sorted by descending `trend_score`; ascending `product_id` is the tie-break, which makes ranking deterministic.

### 4.3. MongoDB serving and idempotency

The `realtime_trending` collection stores `window_start`, `window_end`, `product_id`, `purchase_count_30m`, `purchase_count_120m`, `trend_score`, `trend_rank`, and `updated_at`. Each document has a deterministic `_id` derived from `window_end + product_id` and is written with an idempotent upsert. MongoDB maintains unique indexes on `(window_end, product_id)` and `(window_end, trend_rank)`. The default publication retains Top-20 after normalization and ranking over the complete population and applies a seven-day TTL (`ttl_seconds = 604800`; only `0` disables retention).

Each complete finalized-window snapshot is deleted by `window_end` before the Top-K upsert. Replaying a bounded feed therefore removes stale non-Top-K rows and avoids collisions with old ranks. Source events with duplicate `event_id` values are deduplicated within the watermark before aggregation. Invalid and poison rows are counted in `record_quality` but do not enter stateful aggregation.

Snapshot replacement is logically idempotent and converges correctly after a Spark retry, but the delete-and-upsert sequence is **not transactionally atomic** for concurrent readers. A dashboard may briefly observe an empty or incomplete window during replacement. A production design could use immutable snapshot/version IDs with an active-version pointer or a MongoDB transaction where appropriate. This project does not add that complexity or claim transactional exactly-once serving.

### 4.4. End-to-end streaming validation

| Metric | Result |
|---|---:|
| Attempted / acknowledged / failed | 50,000 / 50,000 / 0 |
| Kafka partitions | 4 |
| Producer rate | 10,821.22 events/s |
| Spark input / processed | 50,000 / 50,000 |
| Spark inputRowsPerSecond | 0.0 (feed was present before query start) |
| Spark processedRowsPerSecond | 3,203.90 rows/s |
| Input trigger execution | 15,605 ms |
| Peak aggregation / dedup state rows | 1,174,010 / 50,000 |
| Rows dropped by watermark in this on-time run | 0 |
| Finalized Mongo documents / windows | 580,342 / 35,701 |
| Maximum rows per window | 20 (configured Top-K = 20) |
| Missing required fields | 0 |
| Invalid trend scores | 0 |
| Duplicate `(window_end, product_id)` keys | 0 |
| Duplicate ranks within a window | 0 |

The finalized-window sink batch measured **62,021.821 ms**; the input batch `addBatch` measured **14,098 ms**. Output fan-out is large because one source event can participate in multiple five-minute sliding windows. This is a bounded development measurement on WSL2 with one Kafka broker and two Spark cores, not a production-capacity claim. The source digest is `3fb1c0240e873cb0700320a236caef73761b9f9ccbfbf2713d8a1fae264da8b4`.

### 4.5. Checkpoint recovery

The `recovery-smoke` scenario verified recovery across **two separate Spark processes**. Process 1 consumed three records, after which two new records were written to Kafka. Process 2 reused the checkpoint and read only the two new records: the query ID remained unchanged, the batch number continued, and Kafka did not replay from earliest. The scenario produced 109 documents and zero duplicate keys; the document count depends on the finalized sliding windows and does not affect the recovery assertion.

### 4.6. Late-data and watermark validation

The first batch received three Kafka records: two valid and one poison/invalid. One of the valid records duplicated an existing `event_id`, so Spark reported `numDroppedDuplicateRows = 1`. The event at `00:15` was accepted while the watermark was `00:10`. After the watermark advanced to `02:50`, a subsequently submitted event with event time `00:18` was dropped from deduplication state (`numRowsDroppedByWatermark = 1`). The selected finalized result remained `C30 = 2`, `C120 = 2`.

### 4.7. Throughput measurements

Each workload used one warm-up and three measured trials.

| Producer target | Achieved producer rate | Spark processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 100/s | 100.74/s | 40.55 | 2,383 |
| 500/s | 497.97/s | 46.86 | 2,123 |
| unbounded bulk | 5,695.47/s | 47.48 | 2,106 |

Each partition arm used a real temporary Kafka topic, verified broker metadata, one warm-up, and three measured trials.

| Kafka partitions | Spark observed | Processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 1 | 1 | 51.57 | 1,939 |
| 2 | 2 | 60.86 | 1,643 |
| 4 | 4 | 47.73 | 2,095 |
| 8 | 8 | 31.26 | 3,199 |

Two partitions produced the best result in this small bounded workload, with performance decreasing at four and eight partitions. The result does not imply that additional partitions always improve throughput; scheduling and state-management overhead can dominate a small workload. The single-broker configuration measures consumer partition parallelism, not broker high availability.

### 4.8. Validation results

- Complete automated suite: **51 passed**.
- Ruff, Docker Compose configuration, and whitespace checks: **passed**.
- Bounded Module 1 feed generation and pinned Spark runtime: **passed**.
- Kafka → Spark → MongoDB end-to-end smoke: **passed**.
- Two-process checkpoint recovery: **passed**; the second process read only new records.
- Duplicate, poison, within-watermark, and beyond-watermark scenarios: **passed**.
- Finalized-window normalization, deterministic ranking, Top-K, and MongoDB snapshot idempotency: **passed**.
- Short throughput smoke and evidence capture: **passed**.

Validation used an isolated Ubuntu environment with a pinned runtime, temporary services, and bounded streaming data. Separate full-data validation reconciled 33,819,106 events for Modules 1–2; bounded measurements are not used to claim production capacity.

### 4.9. Current technical status and known limitations

Module 3 is **COMPLETE AND VALIDATED**.

- Benchmarks are bounded development measurements, not production-capacity claims.
- Deployment uses one Kafka broker; the partition benchmark does not verify broker high availability.
- Output uses finalized-window semantics; the system does not provide an open-window ranking that changes at every trigger.
- Sliding windows create substantial state and output fan-out; the 50k result cannot be extrapolated to production throughput or capacity.
- The MongoDB sink uses bounded driver-side `toLocalIterator` plus bulk batches. Top-K bounds the output, but the sink is not a distributed MongoDB writer.
- Delete plus upsert during finalized-window replacement is not transactionally atomic; a concurrent reader may briefly observe an empty or partial snapshot.

---

## 5. Reproduction guide

### 5.1. Start the service cluster with Docker Compose

Docker Compose manages the complete environment:

```bash
docker compose up -d namenode datanode kafka mongodb spark-master spark-worker
```

- **Hadoop NameNode & WebHDFS:** `http://localhost:9870` (IPC: `hdfs://localhost:8020`)
- **Kafka Broker:** `localhost:9092`
- **Spark Master UI:** `http://localhost:8080` (Worker: 2 cores, 2 GB RAM)
- **MongoDB:** `mongodb://localhost:27017`

### 5.2. Run the ingestion and staging pipeline (Module 1)

Run the complete pipeline through the runner or execute each step separately:

```bash
# Option 1: Run the complete Module 1 pipeline (Clean -> Generate -> Stage)
python scripts/module1.py all

# Option 2: Run each step separately
python -m src.module1.clean --validate
python -m src.module1.generate --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
python -m src.module1.stage_hdfs

# Optional Kafka replay experiment
python -m src.module1.producer --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0
```

### 5.3. Run Spark batch analytics and benchmarks (Module 2)

Run all Module 2 work or individual tasks through the standard container driver:

```bash
# Run everything (handoff validation + analytics + MongoDB export + 3 benchmarks)
python scripts/module2.py all

# Or run individual tasks
python scripts/module2.py validate
python scripts/module2.py stats
python scripts/module2.py benchmark-joins
python scripts/module2.py benchmark-partitions
python scripts/module2.py benchmark-cache
```

### 5.4. Run Spark Structured Streaming (Module 3)

Module 3 consumes the `instacart-purchase-events` Kafka topic, applies a 10-minute watermark to `event_time_epoch_ms`, observes and excludes invalid rows, deduplicates `event_id`, and uses one stateful 120-minute aggregate with a five-minute slide and conditional 30-minute count. Append mode emits finalized windows only. Full-population normalization and ranking run on the static `foreachBatch` DataFrame before Top-K filtering, snapshot replacement, and idempotent upsert into MongoDB `realtime_trending` (Proposal §17–22):

```bash
# Validate Kafka + Mongo connectivity without streaming
python scripts/module3.py validate

# Run streaming in the foreground (Ctrl+C to stop; --duration-seconds 0 = forever)
python scripts/module3.py run --duration-seconds 0

# Deterministic late-data demo; create a topic and publish each phase automatically
python scripts/module3.py late-demo --duration-seconds 60

# Live smoke and checkpoint recovery without a second terminal
python scripts/module3_smoke.py --events 50000 --duration-seconds 120
python scripts/module3_recovery.py --duration-seconds 25

# Benchmarks (Proposal §15.4, §15.5)
python scripts/module3.py benchmark-throughput --duration-seconds 15 --benchmark-events 100
python scripts/module3.py benchmark-partitions --duration-seconds 15 --benchmark-events 100
python scripts/module3.py all --duration-seconds 60

# Default Top-20 and seven-day TTL; explicitly change or disable TTL
python scripts/module3.py run --mongo-top-k-per-window 50 --ttl-seconds 86400
python scripts/module3.py run --ttl-seconds 0

# Kafka offset loss fails by default; relaxed mode requires explicit opt-in
MODULE3_FAIL_ON_DATA_LOSS=false python scripts/module3.py run --duration-seconds 60
python scripts/module3.py run --allow-data-loss --duration-seconds 60
```

Window, watermark, and weight settings can be overridden with `--window-short`, `--window-long`, `--slide`, `--watermark`, `--weight-short`, `--weight-long`, and `--trigger-interval`.

### 5.5. Run the tests

```bash
# Run the test suite in the development environment
python -m pytest

# Run only Module 3 tests (requires PySpark; live Kafka/MongoDB tests require the cluster)
docker compose run --rm --no-deps --entrypoint python3 module3 -m pytest -q tests/test_module3.py
```

---

## 6. Deliverables and technical acceptance checklist

- [x] **Module 1 — Ingestion & Storage:**
  - [x] Read and clean the 6 source CSV files with explicit schemas.
  - [x] Replace NaN values and reconstruct capped 30-day gaps while preserving day-of-week consistency.
  - [x] Use entity-keyed deterministic generation that is invariant to `--batch-users`, with strictly monotonic per-user timestamps.
  - [x] Partition interactions into 456 daily HDFS partitions.
  - [x] Preserve the previous HDFS target with staging backup/restore after upload or promotion failure.
  - [x] Use a bounded-memory external-merge Kafka producer with broker acknowledgement receipts, pacing, and fault injection.
- [x] **Module 2 — Batch Layer:**
  - [x] Start the Spark 3.5.5 container with HDFS and MongoDB connectivity.
  - [x] Validate input integrity and staging receipts (`_sources.json`, `_handoff.json`).
  - [x] Compute user features and write them to `/instacart/features/user_features`.
  - [x] Compute batch metrics and publish them to MongoDB.
  - [x] Run Spark pivot plus explicit unpivot with equal-total validation.
  - [x] Benchmark joins (BHJ vs. SMJ), partition pruning, and in-memory caching.
  - [x] Complete automated tests and retain measured evidence.
- [x] **Module 3 — Speed Layer / Structured Streaming:**
  - [x] Parse JSON from Kafka source `instacart-purchase-events` with a poison-pill guard.
  - [x] Use Kafka `failOnDataLoss=true` by default with explicit environment/CLI opt-out.
  - [x] Apply domain validation and quality metrics plus source `event_id` deduplication.
  - [x] Apply a 10-minute event-time watermark to `event_time_epoch_ms`.
  - [x] Use one stateful 120m sliding aggregate (5m slide), exact conditional C30m, and `trend_score = 0.7·N(C30m) + 0.3·N(C120m)`.
  - [x] Normalize and rank the full population before Top-K, then replace the snapshot and perform idempotent MongoDB upserts.
  - [x] Maintain unique indexes by window/product and window/rank, with a seven-day default TTL.
  - [x] Use an explicit checkpoint ID and verify two-process recovery without replay from earliest.
  - [x] Measure watermark acceptance and dropped state rows in the deterministic late-data demo.
  - [x] Measure throughput with real producer-rate control and a real Kafka 1/2/4/8 topic sweep.
  - [x] Pass the final hardening Docker suite: `51 passed`; Ruff, Compose, and whitespace checks passed.
  - [x] Store measured evidence in `docs/evidence/module3/`.
- [ ] **Module 4 — ML & Serving:** Ready to read features from HDFS and use `order_products__train` as ground truth.

---

## 7. Next stage — Module 4

Module 4 will use `order_products__prior` and HDFS user features as training history, retain `order_products__train` as evaluation ground truth, build ALS recommendations and a GraphFrames co-purchase graph, and blend recommendations with `realtime_trending`. Module 4 is not implemented in the current baseline.
