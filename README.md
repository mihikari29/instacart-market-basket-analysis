# IT4931 — Big Data Project · Lambda Architecture

Replay of the **Instacart Market Basket** dataset as a live real-time feed: cleaned
parquet facts + synthesized absolute timestamps → Kafka → Spark Streaming, with a
Spark batch layer for historical metrics, features and execution benchmarks.
ALS/recommendation belongs to Module 4.

## Repo layout

```text
src/module1/             data cleaning, synthetic timestamp generator, staging, producer
src/module2/             Spark batch analytics, feature extraction, benchmarks, CLI
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
- **Invariant**: per-user monotonicity always enforced → **0 violations** across all 33.8 M events.

## Scenario feeds (historical runs)

The regenerated full-validation feed uses batch_users=2000 and has a peak of
212,714 events/day across 456 days; seed, batching and manifest are recorded in
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
| 3 | Real-time streaming: Kafka → windowed trending → dashboard | Kafka topic `instacart-purchase-events` | Complete; validated locally and on GitHub-hosted CI; PR #2 pending merge |
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

4. **Các cổng dịch vụ đã ánh xạ sẵn:**
   - **Kafka Broker:** `localhost:9092` (Topic: `instacart-purchase-events`)
   - **HDFS NameNode WebHDFS:** `http://localhost:9870`
   - **HDFS IPC (Spark defaultFS):** `hdfs://localhost:8020` (hoặc `hdfs://namenode:8020` trong container)
   - **HDFS DataNode WebHDFS:** `http://localhost:9864`

## Module 2 — Spark batch layer

Module 2 now includes handoff validation, prior-only historical analytics and user
features, MongoDB batch snapshots, forced Sort-Merge/Broadcast Hash Join trials,
partition pruning and aggregate caching experiments. All benchmark arms retain
plans, checksums, repetitions, medians and Spark input/shuffle metrics.

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
scripts/module2.py       canonical Docker runner
Dockerfile.spark         pinned Spark runtime
tests/                   staging/unit/Spark integration regressions
```

**Migration:** the old interaction writer could overwrite overlapping date
partitions. A cap-30 sorting defect also affected regenerated order gaps. Regenerate
cleaned data and feeds, then restage using the corrected code. Existing uploads
without validation receipts are not accepted as a valid Module 2 handoff.

Full execution passed on a GitHub-hosted Linux runner using real Docker, standalone
Spark, HDFS and MongoDB: **33,819,106 source/local/HDFS events**, 456 daily
partitions, and **15 passing tests**. See [full measured evidence](docs/evidence/full/README.md)
and [execution guide](docs/progress.md). The hosted services are
temporary; this validation does not install a permanent cluster on your computer.

## Module 3 — Speed layer (Spark Structured Streaming)

Consumes `instacart-purchase-events` (Module 1 producer), applies a 10-minute
event-time watermark, and uses one stateful 120-minute sliding aggregation
(5-minute slide). The 30-minute count is calculated conditionally inside that
same window. Finalized rows are normalized and ranked on the static
`foreachBatch` DataFrame, then idempotently upserted into MongoDB
`realtime_trending` (Proposal §17–22).

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

Validation status: local Docker passed; Module 3 tests reported **10 passed**
and the complete repository suite reported **30 passed**. GitHub Actions
`Module 3 streaming validation` run **37079034707** completed the `streaming`
job with **SUCCESS**, including a live 50k Kafka → Spark → MongoDB path.
The two-process checkpoint restart and deterministic late-data test also
passed. PR #2 remains open and unmerged. See the [detailed progress and
measurements](docs/progress.md) and [Module 3 evidence](docs/evidence/module3/README.md).

Output goes to `results/module3/<run_id>/` (summary.json, analytics
streaming_progress.jsonl, event log) and into MongoDB collection
`realtime_trending` with indexes `(window_end, product_id)` unique and
`(window_end, trend_rank)`. TTL on `updated_at` is **disabled by default**
(`ttl_seconds = 0`); pass `--ttl-seconds 86400` to demo W4 automatic
window cleanup. Evidence lives in [docs/evidence/module3](docs/evidence/module3/README.md)
and is uploaded by [module3-full.yml](.github/workflows/module3-full.yml).

The producer's `--replay-speed` remains a synthetic event-time acceleration
multiplier. Benchmarks use the separate `--target-events-per-second` control and
record achieved wall-clock send rate. Kafka duplicate injection is not input
deduplication: repeated source events count repeatedly; Mongo idempotency only
prevents duplicate output documents for the same window/product.

`tests/test_module3.py` covers JSON/schema failures, fixture immutability,
30/120-minute counts, normalization, deterministic rank ties, weights, and
checkpoint identity. It is marked `integration` because it requires PySpark.

## Next step: Module 4

**Module 4 (ML & Serving)**: ALS recommendation from `order_products__prior`
(history) with `order_products__train` as ground truth, GraphFrames
co-purchase graph (weighted degree + PageRank), and Serving Layer
blending `λ·norm(ALS) + (1-λ)·norm(Trend)` into `final_recommendations`.
