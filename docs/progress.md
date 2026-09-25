# Module 1 — Progress (Data Ingestion & Storage Foundation)

Mục tiêu Module 1: đọc 6 CSV → validate → cleaning → tạo synthetic timestamp → freeze event schema → sẵn sàng HDFS layout + Kafka producer.

---

## 1. Quy trình đã hoàn thành

```
data/raw/*.csv
  → src/clean.py   (narrow dtypes, NaN gap→0, cap-30 recovery, xa0 cleanup)
  → data/clean/*.parquet
  → src/generate.py (seeded anchor, cumulative days, per-item exponential gaps, monotonicity guard)
  → data/synthesized/scatter_{1w,1m,3m}/events.parquet + manifest.json
```

### Cleaning (xong, validated)

| Kiểm tra | Kết quả |
|---|---|
| first-order NaN gaps → 0 | 206,209 |
| cap-30 recovered [30,36] | 369,323 |
| DOW mismatches sau recovery | 0 |
| Tên sản phẩm chứa `\xa0` → cleaned | 16 |
| test orders (eval_set=test) loại khỏi stream | 75,000 |

---

## 2. SyntheticConfig (đã triển khai)

| Knob | Giá trị / Chọn | Ghi chú |
|---|---|---|
| `seed` | 42 | deterministic |
| `scatter_window_weeks` | 1 / 4 / **13** (default) / 26 / 52 | anchor week uniform trong cửa sổ |
| `time_mode` | `uniform` \| `hash` | uniform: U(0,59); hash: `(order_id·10⁶+1)%60` |
| `delta` | `exponential(mean=30, clip=[5,120])` \| `uniform([15,50])` | giây giữa các lần add-to-cart |
| `limit_users` | null (all) | |
| `batch_users` | 20000 | chunk size cho write, cũng ảnh hưởng RNG anchor |

5 preset scenarios: `default` · `mobile-fast` · `desktop-browse` · `uniform` · `deterministic` (xem `python src/generate.py --list-scenarios`).

---

## 3. Các feed đã tạo

| Folder | scatter | span | peak / mean daily | crowding | manifest |
|---|---|---|---|---|---|
| `scatter_1w` | 1 week | 372 d | 428,965 / 90,912 | 4.72× | [link](../data/synthesized/scatter_1w/manifest.json) |
| `scatter_1m` | 1 month | 393 d | 241,413 / 86,054 | 2.80× | [link](../data/synthesized/scatter_1m/manifest.json) |
| `scatter_3m` (default) | 3 months | 456 d | 208,674 / 74,165 | 2.81× | [link](../data/synthesized/scatter_3m/manifest.json) |

Mỗi folder chứa `events.parquet` (14 cột) + `manifest.json`. Tất cả: **33,819,106 events · 206,209 users · 3,346,083 orders · 0 monotonic violations**.

---

## 4. Event schema (`events.parquet`, 14 cột)

| Field | Kiểu | Nguồn |
|---|---|---|
| `event_id` | string | `<order_id>_<add_to_cart_order>` |
| `order_id` | int64 | orders / order_products |
| `user_id` | int64 | orders |
| `product_id` | int64 | order_products |
| `add_to_cart_order` | int16 | order_products |
| `reordered` | bool | order_products |
| `aisle_id` | int16 | products (denormalized vào event) |
| `department_id` | int8 | products (denormalized vào event) |
| `order_hour_of_day` | int8 | orders |
| `order_dow` | int8 | orders |
| `event_time_epoch_ms` | int64 | synthetic (anchor + cumulative + gaps) |
| `event_time_iso` | string | derived from epoch_ms |
| `order_number` | int16 | orders (user's order rank 1..N) |
| `synthetic_date` | string | derived, cho partition/grouping |

- Không có `event_type` — dataset chỉ chứa purchase events, không có browse/scroll/cart_add.
- `ingestion_time_epoch_ms` chỉ tồn tại ở producer time (không lưu trong file), sẽ được producer stamp khi gửi lên Kafka.

---

## 5. Cách tái tạo (replication)

Deterministic với cùng `seed` và `batch_users` (và cùng phiên bản numpy/RNG). `manifest.json` ghi lại config + RNG protocol.

```bash
# canonical feed (13-week scatter, default)
python src/generate.py --scenario default --scatter-weeks 13 \
  --out-dir data/synthesized/scatter_3m

# extras
python src/generate.py --scenario default --scatter-weeks 1  --out-dir data/synthesized/scatter_1w
python src/generate.py --scenario default --scatter-weeks 4  --out-dir data/synthesized/scatter_1m
```

**RNG protocol:** mỗi batch (20k users) dùng `default_rng(SeedSequence([seed, batch_index]))`. Anchor days phụ thuộc `batch_users` — muốn tái tạo chính xác phải giữ nguyên `batch_users=20000` (mặc định).

---

## 6. Cách sử dụng (module tiếp theo)

| Module | Đọc từ | Ghi chú |
|---|---|---|
| **M1 Producer (next)** | `events.parquet` (hoặc `.json.gz`) | stream JSON → Kafka topic `instacart-purchase-events`, key = `user_id`; stamp `ingestion_time_epoch_ms` tại send-time |
| **M2 Batch** | `data/clean/*.parquet` | batch analytics, SparkSQL join benchmarks; join dims broadcast (products/aisles/departments) cho tên |
| **M3 Streaming** | Kafka topic | watermark trên `event_time_epoch_ms`; windowed trending |
| **M4 ML / graph** | `data/clean/order_products__*.parquet`, `products.parquet` | ALS + co-purchase graph |

`events.parquet` đã **denormalize** aisle_id/department_id vào event (không cần join runtime). Muốn lấy tên aisle/department → join broadcast 134 + 21 rows.

---

## 7. Definition of Done (§42) — Module 1

- [x] đọc 6 file raw CSV
- [x] explicit schema hoạt động
- [x] validation chạy (0 NaN gap, 0 DOW mismatch, 0 nbsp error)
- [x] timestamp deterministic (seed 42, reproducible)
- [x] weekday consistency đạt yêu cầu (DOW mismatches = 0)
- [x] timestamp monotonic theo user (violations = 0)
- [x] Parquet ghi HDFS theo chuẩn Proposal §10 (raw, curated, dimensions, interactions 456 ngày, features, models)
- [x] Kafka producer replay được (hỗ trợ pacing, partition theo `user_id`, stamp `ingestion_time_epoch_ms`, fault injections `late/dup/burst/poison`)
- [x] event schema được freeze (13 trường theo Proposal §9.4 / §11.6)

---

## 8. Các quyết định / thay đổi so với Proposal

| Đoạn | Proposal gốc | Đã triển khai | Lý do |
|---|---|---|---|
| §8.2 anchor | `hash(user_id, SEED) mod SCATTER_WEEKS` | `rng.integers(0, SCATTER_WEEKS)` seeded | Deterministic; reproducible bằng cách chạy lại cùng seed+batch_users; thực tế cho kết quả giống hệt |
| §9.4 `event_type` | `"purchase"` constant | **không có** | Dataset chỉ có purchase; giữ schema gọn, producer có thể thêm nếu cần |
| §11.5 `order_number` | có | **có** (đã thêm sau backfill) | Tránh M2/M4 phải rank lại |
| `event_time_iso` | `%Y-%m-%dT%H:%M:%SZ` | `%Y-%m-%dT%H:%M:%S.%f` (microsecond) | Epoch_ms chứa đủ precision; ISO dạng `.f` nhất quán với pandas/pyarrow output |

---

## 9. Kết quả kiểm thử Module 1

### 9.1. HDFS Staging (`src/stage_hdfs.py`)
- Đã tải 6 CSV gốc lên `/instacart/raw/{orders,order_products_prior,order_products_train,products,aisles,departments}/`.
- Đã tải Parquet chuẩn hóa lên `/instacart/curated/` và `/instacart/curated/dimensions/`.
- Đã phân vùng 33,819,106 dòng sự kiện thành **456 partition ngày** (`synthetic_year=.../synthetic_month=.../synthetic_day=...`) trên `/instacart/curated/interactions/`.
- Đã tạo sẵn thư mục `/instacart/features/{user_features,als_interactions}` và `/instacart/models/als`.

### 9.2. Kafka Producer (`src/producer.py`)
- Đã kiểm thử thành công trên cả 3 feed (`scatter_1w`, `scatter_1m`, `scatter_3m`).
- Tốc độ phát đạt ~5,000 – 6,000 events/giây ở local mode.
- Đã kiểm thử fault injection (`late:0.05,dup:0.02,burst:200,poison:1`) sẵn sàng phục vụ kiểm thử Module 3.

---

## 10. Hướng dẫn bàn giao cho thành viên nhóm (Team Onboarding)

1. **Khởi động dịch vụ:** `docker compose up -d` (Docker tự pull official public images từ Docker Hub: Kafka 3.7, Hadoop NameNode & DataNode 3.2.1 — **không cần publish image riêng**).
2. **Khởi tạo HDFS:** `python src/stage_hdfs.py` (tải raw/curated/interactions lên Data Lake).
3. **Phát stream:** `python src/producer.py --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0` (hoặc cấu hình pacing tùy chọn cho Module 3).
4. **Kết nối hạ tầng:**
   - Kafka: `localhost:9092`
   - HDFS WebHDFS: `http://localhost:9870`
   - HDFS IPC: `hdfs://localhost:8020`


## Module 2 handoff audit (2026-09-25)

The earlier §9.1 staging statement records uploads, not verified Spark row counts.
A regression reproduction confirmed that the original per-row-group dataset
writer overwrote files for overlapping dates. Those prior full HDFS counts must
be revalidated after regenerating/restaging with the corrected writer. Module 2
now requires source receipts and exact fact/event checks. The generator also
restores the documented order_number column and records batch_users. The cap-30
mask now follows sorted rows correctly. See [Module 2](module2.md) and its evidence
for executed fixture checks and the remaining Docker/full-data gates.


## Full Module 2 revalidation (2026-09-26, Asia/Saigon)

The remaining gates above are now closed. Public source acquisition, corrected
cleaning/generation, Docker build, 15 tests, HDFS staging and all Module 2
analytics/experiments passed on GitHub Actions. Exact source/local/HDFS counts
are 33,819,106 with 456 daily partitions and 456 files. Real execution also
exposed and fixed NumPy 2 narrow-integer timestamp multiplication overflow.
MongoDB verification found 49,677 product and 21 department documents.
See [durable full evidence](evidence/full/README.md) for code SHA, source
receipts, physical plans, repeated measurements and runtime limitations.
