# IT4931 — Big Data Project · Lambda Architecture

Replay of the **Instacart Market Basket** dataset as a live real-time feed: cleaned
parquet facts + synthesized absolute timestamps → Kafka → Spark Streaming, with a
Spark batch layer for historical metrics, features and execution benchmarks.
ALS/recommendation belongs to Module 4.

## Repo layout

```text
src/module1/             data cleaning, synthetic timestamp generator, staging, producer
src/module2/             Spark batch analytics, feature extraction, benchmarks, CLI
src/module3/             Spark Structured Streaming, watermarking, deduplication,
                         finalized-window trending, Mongo serving, recovery and benchmarks
data/raw/*.csv           original Kaggle files (archive — only read by clean.py)
data/clean/*.parquet     batch source of truth (facts + dims)
data/synthesized/        reproducible synthetic feeds (scatter_1w, scatter_1m, scatter_3m)
scripts/                 canonical Docker runners and orchestration utilities
tests/                   unit, staging, and Spark integration test suites
```

## Dataset (originals → cleaned)

| file | rows |
|---|---|
| `orders` | 3,421,083 (206,209 users) |
| `order_products__prior` | 32,434,489 |
| `order_products__train` | 1,384,617 |
| `products` | 49,688 |
| `aisles` / `departments` | 134 / 21 |

Stream = prior + train = **3,346,083 orders · 33,819,106 events**.

## What to use, and for which module

| file | reads | feeds |
|---|---|---|
| `data/clean/orders.parquet`, `order_products__*.parquet`, `products/aisles/departments.parquet` | batch/ML | Module 2 (historical analytics), Module 4 (ALS) |
| `data/synthesized/scatter_*/events.parquet` + `manifest.json` | producer | Module 1 → Kafka (Module 3) |
| `data/raw/*.csv` | only `clean.py` | archive — never read by modules |

## Why parquet (and not CSV)

It doesn't *have* to be parquet — CSV would work. Parquet wins because:

- **~70–80% smaller on disk** (dictionary + compression over repeated strings/ints),
  which matters at 700 MB raw / 34 M rows.
- **Columnar reads**: consumers load only the columns they need.
- **Native in Spark/pyarrow** with dtype preserved (`int8/int16/int32/string`) — CSV
  re-infers dtypes on every load and is 2–3× the I/O.
- Format choice is orthogonal to Kafka: the producer serializes each row to **JSON**
  regardless; parquet is just the on-disk storage.

## Run

```bash
# Run full Module 1 pipeline (clean -> generate -> stage)
python scripts/module1.py all

# Or run individual steps:
python -m src.module1.clean --validate
python -m src.module1.generate --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
python -m src.module1.generate --scenario default --scatter-weeks 1  --out-dir data/synthesized/scatter_1w
python -m src.module1.generate --scenario default --scatter-weeks 4  --out-dir data/synthesized/scatter_1m
python -m src.module1.generate --list-scenarios
```

## Cleaning (validated)

- Narrow dtypes at load (`int8–int32`, `string`).
- 206,209 first-order NaN gaps → 0.
- 369,323 cap-30 gaps recovered to [30,36] (Strategy B) → DOW mismatches remain 0.
- 16 product names with `\xa0` normalized.
- `test` eval_set (75,000 orders) excluded from the stream.

## SyntheticConfig

- **Absolute-time axis**: `scatter_window_weeks` (1/4/13/26/52) spreads users' first orders.
- **Session axis**: `delta` seconds between item-adds — `exponential` (Poisson arrivals,
  mean 30 s, clip [5,120]; median session ≈ 3.5 min, matches ContentSquare mobile)
  or `uniform [15,50]`; `time_mode` `uniform` (U(0,59)) or `hash` (RNG-free).
- **Presets**: `default`, `mobile-fast` (20 s), `desktop-browse` (40 s), `uniform`, `deterministic`.
- **Reproducibility**: random-looking values use entity-keyed SplitMix64 inputs
  (`user_id`, `order_id`, and `order_id + cart position`), so the logical feed is
  identical for the same source and seed regardless of `--batch-users`.
- **Invariant**: per-user monotonicity always enforced → **0 violations** across all 33.8 M events.

## Scenario feeds (historical runs)

The current hardening full-validation feed uses batch_users=2000 and has a peak of
210,195 events/day across 456 days; seed, batching and manifest are recorded in
[full evidence](docs/evidence/full/README.md). The earlier figures below describe
the previous generated feeds.

| folder | scatter | span | peak/mean daily | crowding |
|---|---|---|---|---|
| `scatter_1w` | 1 week | 372 d | 428,965 / 90,912 | 4.72× |
| `scatter_1m` | 1 month | 393 d | 241,413 / 86,054 | 2.80× |
| `scatter_3m` (default) | 3 months | 456 d | 208,674 / 74,165 | 2.81× |

## Event schema (one row = one product event)

`event_id` `<order_id>_<add_to_cart_order>`, `order_id`, `user_id`, `product_id`,
`add_to_cart_order`, `reordered`, `aisle_id`, `department_id`, `order_hour_of_day`,
`order_dow`, `event_time_epoch_ms`, `event_time_iso`, `synthetic_date`.
The per-item gaps (exp/uniform deltas) are folded into `event_time_epoch_ms` —
recover by `diff()` within `order_id`.

## Data relationships (join keys)

```
orders.order_id ──→ order_products__prior/train.order_id   (1:N)
order_products.product_id ──→ products.product_id          (N:1)
products.aisle_id ──→ aisles.aisle_id                      (N:1)
products.department_id ──→ departments.department_id        (N:1)
```

`events.parquet` is **denormalized**: it already carries `order_id/user_id/product_id/
aisle_id/department_id/order_hour_of_day/order_dow`, so the streaming path needs no
runtime joins. Join `products/aisles/departments` (tiny, broadcast) only for names.

## Lambda modules (spec §7 — verify against assignment)

| # | module | reads | status |
|---|---|---|---|
| 1 | Ingestion & transfer: clean → synthesize timestamps → Kafka → HDFS | `data/raw/*`, `data/clean/*`, `data/synthesized/scatter_*/events.parquet` | Implemented; corrected full-data HDFS handoff validated |
| 2 | Batch layer (Spark): stats, SparkSQL/join benchmarks + optimization | `data/clean/*.parquet`, `/instacart/curated/` | Complete; full-data Docker/Spark/HDFS run passed |
| 3 | Stream processing + serving data: Kafka → Spark Structured Streaming → `realtime_trending` MongoDB output | Kafka topic `instacart-purchase-events` | Complete; validated locally and on GitHub-hosted CI; PR #2 pending merge |
| 4 | ML/ALS recommendation + product graph + visualization | `order_products__prior/train`, `orders.eval_set` split | Pending |

## Team Onboarding & Environment Setup

Đồng đội khi nhận repository có thể chạy ngay toàn bộ môi trường mà **không cần cài đặt thủ công Java, Hadoop hay Kafka**:

1. **Khởi động dịch vụ hạ tầng:**
   ```bash
   docker compose up -d
   ```
   *(Sử dụng trực tiếp các official images có sẵn trên Docker Hub: `apache/kafka:3.7.0`, `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`, `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` — **không cần publish image riêng**).*

2. **Dựng HDFS Data Lake (cho Module 2 & 4):**
   ```bash
   python -m src.module1.stage_hdfs
   ```
   *(Kiểm tra cây thư mục HDFS: `python -m src.module1.stage_hdfs --tree-only`)*.

3. **Phát dữ liệu lên Kafka (cho Module 3 Streaming):**
   ```bash
   # Phát thử 50k events nhanh
   python -m src.module1.producer --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0

   # Hoặc phát mô phỏng có lỗi trễ để test watermark
   python -m src.module1.producer --feed data/synthesized/scatter_3m --limit-events 20000 --replay-speed 50000 --inject "late:0.05,dup:0.02"
   ```

   Unsorted Parquet input is replayed chronologically through a disk-backed
   external merge. Memory is bounded by the configured chunk/fan-in sizes, and
   `--limit-events` stops output without materializing the full feed in memory.
   The JSON receipt separates attempted, broker-acknowledged, and failed sends;
   any delivery failure produces a normal non-zero process exit.

   Fault modes have separate identities: `dup` republishes the same logical
   event with the same `event_id`, while `late` emits a separate event with an
   older timestamp and a deterministic `<source_id>__late_<sequence>` ID. This
   lets source deduplication remove duplicates without masking late-data and
   watermark behavior. Poison and burst behavior are unchanged.

Each HDFS interaction refresh uploads to a unique staging tree, moves the old
target to a backup, and promotes the complete staging tree with an atomic HDFS
rename. Upload/promotion failure removes staging and restores the prior target;
a successful promotion removes the backup.

4. **Các cổng dịch vụ đã ánh xạ sẵn:**
   - **Kafka Broker:** `localhost:9092` (Topic: `instacart-purchase-events`)
   - **HDFS NameNode WebHDFS:** `http://localhost:9870`
   - **HDFS IPC (Spark defaultFS):** `hdfs://localhost:8020` (hoặc `hdfs://namenode:8020` trong container)
   - **HDFS DataNode WebHDFS:** `http://localhost:9864`

## Module 2 — Spark batch layer

Module 2 now includes handoff validation, prior-only historical analytics and user
features, MongoDB batch snapshots, an explicit department/hour pivot and Spark
`unpivot` with equal-total validation, forced Sort-Merge/Broadcast Hash Join
trials, partition pruning and aggregate caching experiments. All benchmark arms
retain plans, checksums, repetitions, medians and Spark input/shuffle metrics.

After regenerating and staging data with the corrected Module 1 pipeline:

```bash
python scripts/module2.py all
```

The runner builds a pinned Spark 3.5.5 / Java 17 image and starts the Compose
master, worker and MongoDB. See [Progress Runbook](docs/progress.md) for the complete
setup, schemas, individual commands, methodology and limitations. Outputs go to
`results/module2/<run-id>/`, HDFS `/instacart/features/user_features`, and MongoDB
`batch_product_metrics` / `department_metrics`. Compact measured fixture evidence
is in [docs/evidence](docs/evidence).

New source layout:

```text
src/module1/             data cleaning, synthetic timestamp generator, staging, producer
src/module2/             Spark batch analytics, feature extraction, benchmarks, CLI
src/module3/             Structured Streaming, watermark/dedup, finalized trends,
                         Mongo serving, recovery and benchmarks
scripts/module2.py       canonical Docker runner
scripts/module3*.py      Module 3 runner, smoke, and recovery orchestration
Dockerfile.spark         pinned Spark runtime
tests/                   staging/unit/Spark integration regressions
```

**Migration:** the old interaction writer could overwrite overlapping date
partitions. A cap-30 sorting defect also affected regenerated order gaps. Regenerate
cleaned data and feeds, then restage using the corrected code. Existing uploads
without validation receipts are not accepted as a valid Module 2 handoff.

Current full execution passed in GitHub Actions run **37107843181** on hardening
revision `a35170f`, using real Docker, standalone Spark, HDFS and MongoDB:
**33,819,106 source/local/HDFS events**, 456 daily partitions, and **51 passing
tests**. See [full measured evidence](docs/evidence/full/README.md)
and [execution guide](docs/progress.md). The hosted services are
temporary; this validation does not install a permanent cluster on your computer.

## Module 3 — Speed layer (Spark Structured Streaming)

Consumes `instacart-purchase-events` (Module 1 producer), applies a 10-minute
event-time watermark, rejects malformed/domain-invalid rows with observable
quality counters, deduplicates `event_id` within the watermark, and uses one
stateful 120-minute sliding aggregation (5-minute slide). The 30-minute count is
calculated conditionally inside that same window. Append mode emits only
finalized windows; the complete product population is then normalized and
deterministically ranked on the static `foreachBatch` DataFrame. Configurable
Top-K filtering happens only after ranking, followed by snapshot replacement and
idempotent MongoDB upserts into `realtime_trending` (Proposal §17–22).

```text
Kafka events
  -> schema/domain validation + quality metrics
  -> event-time watermark
  -> event-ID deduplication
  -> one 120-minute sliding aggregation (C30 + C120)
  -> finalized window (append mode)
  -> full-population normalization + deterministic ranking
  -> Top-K
  -> MongoDB finalized trend snapshot
```

This is finalized-window trending, not an open-window ranking recomputed every
processing trigger.

The Kafka source defaults to `failOnDataLoss=true`: unavailable requested
offsets fail the query instead of being skipped silently. Relaxed behavior
requires an explicit `--allow-data-loss` flag or
`MODULE3_FAIL_ON_DATA_LOSS=false`; checkpoint and `startingOffsets` semantics
are otherwise unchanged.

```bash
# Validate Kafka + Mongo connectivity (no streaming)
python scripts/module3.py validate

# Run streaming query in the foreground (Ctrl+C to stop)
python scripts/module3.py run --duration-seconds 0

# Trending blend with default 0.7/0.3 weights; configurable via flags
# --window-short 30 minutes --window-long 120 minutes --slide 5 minutes
# --watermark 10 minutes --weight-short 0.7 --weight-long 0.3

# Deterministic on-time / within-watermark / beyond-watermark scenario
python scripts/module3.py late-demo --duration-seconds 60

# Bounded real Module 1 producer -> Kafka -> Spark -> Mongo smoke
python scripts/module3_smoke.py --events 50000 --duration-seconds 120

# Two separate Spark processes reuse checkpoint ID "recovery-smoke"
python scripts/module3_recovery.py --duration-seconds 25

# Throughput + Kafka partition sweep (Proposal §15.4, §15.5)
python scripts/module3.py benchmark-throughput --duration-seconds 15 --benchmark-events 100
python scripts/module3.py benchmark-partitions --duration-seconds 15 --benchmark-events 100

# All benchmarks in one go
python scripts/module3.py all --duration-seconds 60
```

Current final-hardening local validation reports **51 passed**; Ruff, Compose
validation and whitespace checks pass. Fast validation run **37107841305** and
Module 3 streaming run **37107841330** succeeded for source revision `a35170f`.
Historically, the suite reported 43
tests after the HDFS rollback regression. On implementation commit `ff06239`, GitHub Actions
fast-validation run **37094705692** and streaming run **37094705601** succeeded with **42 tests**,
a live Kafka → Spark → MongoDB path, two-process checkpoint recovery, and the
duplicate/poison/late-data scenario. The rollback follow-up is commit `61ae930`;
its fast run **37103618599** and streaming run **37103621207** also succeeded.
PR #2 targets `main` and remains open and unmerged. See the
[detailed progress and measurements](docs/progress.md) and [Module 3
evidence](docs/evidence/module3/README.md).

Output goes to `results/module3/<run_id>/` (summary.json, analytics
streaming_progress.jsonl, event log) and into MongoDB collection
`realtime_trending` with indexes `(window_end, product_id)` unique and
`(window_end, trend_rank)` unique. The default retention is seven days
(`ttl_seconds = 604800`); pass `--ttl-seconds 0` to explicitly disable TTL.
The default serving bound is Top-20 per finalized window and can be changed with
`--mongo-top-k-per-window`. Before replay-safe upserts, each complete finalized
window snapshot is replaced, removing stale rows and preventing rank conflicts
when a bounded feed is recomputed. Evidence lives in
[docs/evidence/module3](docs/evidence/module3/README.md) and is uploaded by
[module3-full.yml](.github/workflows/module3-full.yml).

Finalized-window replacement is logically idempotent and converges correctly
under Spark retry, but delete + upsert is not transactionally atomic for
concurrent readers. A dashboard may briefly observe an empty or partial snapshot
during replacement. A production design could publish immutable snapshot/version
IDs and atomically switch an active-version pointer, or use a Mongo transaction
where appropriate.

The producer's `--replay-speed` remains a synthetic event-time acceleration
multiplier. Benchmarks use the separate `--target-events-per-second` control and
record achieved wall-clock send rate. Repeated Kafka records with the same
`event_id` contribute once while retained by the event-time deduplication state.
Invalid records are counted in `record_quality` and do not enter aggregation.

`tests/test_module3.py` covers JSON/domain failures, poison records,
deduplication, fixture immutability, 30/120-minute counts, full-population
normalization, deterministic rank ties, post-ranking Top-K, Mongo snapshot
idempotency/index/TTL behavior, weights, and checkpoint identity. It is marked
`integration` because it requires PySpark.

## CI strategy

- `Fast validation` runs Compose validation, Ruff and the complete fixture/Spark
  test suite on pull requests and pushes to active module branches and `main`.
- `Module 2 full-data validation` remains manually dispatchable, runs weekly,
  and retains its canonical full-data path.
- `Module 3 streaming validation` runs bounded live Kafka/Spark/Mongo,
  checkpoint-recovery, late-data and throughput checks for relevant changes.

## Next step: Module 4

**Module 4 (ML & Serving)**: ALS recommendation from `order_products__prior`
(history) with `order_products__train` as ground truth, GraphFrames
co-purchase graph (weighted degree + PageRank), and Serving Layer
blending `λ·norm(ALS) + (1-λ)·norm(Trend)` into `final_recommendations`.
