# IT4931 — Big Data Project · Lambda Architecture

Replay of the **Instacart Market Basket** dataset as a live real-time feed: cleaned
parquet facts + synthesized absolute timestamps → Kafka → Spark Streaming, with a
batch layer for ALS recommendation and trending.

## Repo layout

```
src/clean.py      data/raw/*.csv → data/clean/*.parquet (cleaned facts + dims)
src/config.py     SyntheticConfig + scenario presets
src/generate.py   data/clean/ → data/synthesized/<scenario>/events.parquet + manifest.json
data/raw/*.csv                   original Kaggle files (archive — only read by clean.py)
data/clean/*.parquet             batch source of truth (facts + dims)
data/synthesized/scatter_{1w,1m,3m}   three demo feeds, pick ONE
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
| `data/clean/orders.parquet`, `order_products__*.parquet`, `products/aisles/departments.parquet` | batch/ML | Module 2 (trending, ALS), Module 4 |
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
python src/clean.py --validate
python src/generate.py --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
python src/generate.py --scenario default --scatter-weeks 1  --out-dir data/synthesized/scatter_1w
python src/generate.py --scenario default --scatter-weeks 4  --out-dir data/synthesized/scatter_1m
python src/generate.py --list-scenarios
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

## Scenario feeds (realized)

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
| 1 | Ingestion & transfer: clean → synthesize timestamps → Kafka → HDFS | `data/raw/*`, `data/clean/*`, `data/synthesized/scatter_*/events.parquet` | **Done** (Validated, HDFS Staged, Producer Verified) |
| 2 | Batch layer (Spark): stats, SparkSQL/join benchmarks + optimization | `data/clean/*.parquet`, `/instacart/curated/` | Next |
| 3 | Real-time streaming: Kafka → windowed trending → dashboard | Kafka topic `instacart-purchase-events` | Next |
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
   python src/stage_hdfs.py
   ```
   *(Kiểm tra cây thư mục HDFS: `python src/stage_hdfs.py --tree-only`)*.

3. **Phát dữ liệu lên Kafka (cho Module 3 Streaming):**
   ```bash
   # Phát thử 50k events nhanh
   python src/producer.py --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0

   # Hoặc phát mô phỏng có lỗi trễ để test watermark
   python src/producer.py --feed data/synthesized/scatter_3m --limit-events 20000 --replay-speed 50000 --inject "late:0.05,dup:0.02"
   ```

4. **Các cổng dịch vụ đã ánh xạ sẵn:**
   - **Kafka Broker:** `localhost:9092` (Topic: `instacart-purchase-events`)
   - **HDFS NameNode WebHDFS:** `http://localhost:9870`
   - **HDFS IPC (Spark defaultFS):** `hdfs://localhost:8020` (hoặc `hdfs://namenode:8020` trong container)
   - **HDFS DataNode WebHDFS:** `http://localhost:9864`

## Next steps (Module 2 & Module 3)

1. **Module 2 (Batch Layer)**: Spark batch metrics on `/instacart/curated/`, join benchmarks (Broadcast vs Sort-Merge), and partition pruning experiment.
2. **Module 3 (Speed Layer)**: Spark Structured Streaming consuming `instacart-purchase-events`, watermark trên `event_time_epoch_ms`, và tính realtime trending window.