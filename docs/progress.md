# Project Progress & Technical Runbook

> **Hệ thống Big Data phân tích hành vi mua sắm và gợi ý sản phẩm thích ứng xu hướng theo kiến trúc Lambda**  
> **Course:** Lưu trữ và Xử lý Dữ liệu lớn (IT4931)  
> **Reference Architecture:** [Proposal Document](proposal.md)

---

## 1. Tổng quan kiến trúc & Trạng thái các Module

| Module | Chức năng chính | Thành phần kỹ thuật | Trạng thái |
|---|---|---|---|
| **Module 1** | Data Ingestion, Cleaning, Synthetic Timestamps, HDFS Staging, Kafka Replay | Python, PyArrow, HDFS WebHDFS, Kafka Producer | **Hoàn thành & Đã kiểm chứng** |
| **Module 2** | Spark Batch Layer, Historical Analytics, MongoDB Serving, Join/Pruning/Cache Benchmarks | Apache Spark 3.5.5, PySpark, Standalone Cluster, MongoDB 7.0 | **Hoàn thành & Đã merge vào `main`** |
| **Module 3** | Speed Layer, Event-Time Stream Processing, Watermark, Realtime Trending | Spark Structured Streaming, Kafka Consumer, MongoDB Sink | **Hoàn thành, đã kiểm chứng và đã merge vào `main`** |
| **Module 4** | Machine Learning & Graph, Collaborative Filtering, GraphFrames, API & Dashboard | Spark MLlib (ALS), GraphFrames, FastAPI, Streamlit/React | *Sẵn sàng triển khai tiếp theo* |

---

## 2. Module 1 — Data Ingestion & Storage Foundation

### 2.1. Quy trình xử lý
```text
data/raw/*.csv (6 files Instacart)
  │
  ├──> python -m src.module1.clean
  │      - Narrow dtypes (tối ưu bộ nhớ)
  │      - NaN days_since_prior_order (order_number=1) -> 0
  │      - Cap-30 synthetic reconstruction [30, 36] khớp Day-Of-Week (DOW)
  │      - Chuẩn hóa text: xóa ký tự lạ '\xa0' trong product_name
  │      - Ghi ra data/clean/*.parquet
  │
  ├──> python -m src.module1.generate
  │      - Entity-keyed SplitMix64 theo user/order/cart position
  │      - Cùng source + seed cho output logic giống nhau với mọi --batch-users
  │      - Cumulative days & session-level exponential item deltas
  │      - Monotonicity guard: bảo toàn thứ tự thời gian cho từng user
  │      - Ghi ra data/synthesized/scatter_{1w,1m,3m}/events.parquet
  │
  ├──> python -m src.module1.stage_hdfs
  │      - Tải 6 CSV raw lên /instacart/raw/
  │      - Tải curated Parquet lên /instacart/curated/
  │      - Phân vùng tương tác thành 456 partition ngày (/instacart/curated/interactions/synthetic_year=.../synthetic_month=.../synthetic_day=...)
  │      - Xuất metadata và hóa đơn kiểm chứng: _sources.json, _handoff.json
  │      - Staging + backup/restore: lỗi upload/promotion giữ nguyên target cũ
  │
  └──> python -m src.module1.producer
         - External merge sort trên disk, giới hạn bộ nhớ theo chunk/fan-in
         - Replay sự kiện lên Kafka topic 'instacart-purchase-events' (Key = user_id)
         - Receipt tách attempted / broker-acknowledged / failed
         - Gán ingestion_time_epoch_ms tại send-time
         - Duplicate giữ nguyên event_id; late dùng ID deterministic riêng và timestamp cũ hơn
         - Hỗ trợ mô phỏng lỗi: late data, duplicate, burst, poison pill
```

### 2.2. Kết quả Làm sạch & Tái tạo dữ liệu
- **First-order NaN gap:** 206,209 dòng $\rightarrow 0.0$
- **Cap-30 reconstruction:** 369,323 dòng gap $= 30$ được tái cấu trúc thành minimum weekday-consistent synthetic gap trong $[30, 36]$ $\rightarrow$ **0 DOW mismatches**
- **Ký tự `\xa0` được chuẩn hóa:** 16 sản phẩm
- **Test orders:** 75,000 orders tách riêng khỏi luồng streaming/prior interaction
- **Kiểm tra thứ tự Monotonic:** **0 violations** trên toàn bộ 33,819,106 events khi kiểm tra theo đúng thứ tự nghiệp vụ: `user_id` $\rightarrow$ `order_number` $\rightarrow$ `add_to_cart_order`.

### 2.3. Cấu hình Synthetic Feeds
| Feed | Scatter Window | Span | Peak / Mean Daily Events | Manifest |
|---|---|---|---|---|
| `scatter_1w` | 1 tuần | 372 ngày | 428,965 / 90,912 | [manifest.json](../data/synthesized/scatter_1w/manifest.json) |
| `scatter_1m` | 1 tháng | 393 ngày | 241,413 / 86,054 | [manifest.json](../data/synthesized/scatter_1m/manifest.json) |
| `scatter_3m` (chuẩn) | 3 tháng | 456 ngày | 208,674 / 74,165 | [manifest.json](../data/synthesized/scatter_3m/manifest.json) |

---

## 3. Module 2 — Spark Batch Layer & Performance Optimization

### 3.1. Handoff Contracts & HDFS Validation
Module 2 đọc dữ liệu từ HDFS (`/instacart/curated/`) và kiểm tra nghiêm ngặt trước khi thực thi:
- Xác thực `_sources.json` và `_handoff.json` (SHA-256 khớp tuyệt đối).
- So khớp Multiset giữa fact và events: 3,421,083 orders; 32,434,489 prior; 1,384,617 train; 49,688 products; 134 aisles; 21 departments; 206,209 users; 33,819,106 events trên 456 partitions.
- Tách bạch rõ ràng: tập `prior` dùng cho tính toán chỉ số lịch sử và trích xuất đặc trưng; tập `train` được bảo toàn làm Ground Truth cho đánh giá Recommendation ở Module 4.

### 3.2. Spark SQL Analytics & MongoDB Serving
Thực thi các phép biến đổi và phân tích nâng cao:
1. **User Features:** Tính toán `order_count`, `purchase_count`, `unique_products`, `avg_basket_size`, `reorder_rate` cho từng user và lưu vào `/instacart/features/user_features`.
2. **Product & Department Metrics:** Tính tỷ lệ mua lại (reorder rate) có kiểm soát ngưỡng hỗ trợ (`min_support`), tổng số lượt mua, phân phối theo ngày trong tuần và khung giờ trong ngày.
3. **Department-Hour Pivot/Unpivot:** Tạo ma trận rộng 21 phòng ban × 24 giờ,
   chuyển lại dạng dài bằng Spark `DataFrame.unpivot`, và fail-fast nếu tổng
   `purchase_count` giữa hai biểu diễn không bằng nhau.
4. **Xuất kết quả sang MongoDB:** Đẩy kết quả batch xuống 2 collection: `batch_product_metrics` và `department_metrics` (phục vụ API và Dashboard).

### 3.3. Thử nghiệm Tối ưu hóa Hiệu năng (Performance Benchmarks)
Được đo lường độc lập, lặp lại nhiều lần (warmup + 3 runs), trích xuất chỉ số trực tiếp từ Spark EventLog:
1. **Join Strategy (Broadcast Hash Join vs. Sort-Merge Join):**
   - So sánh việc join bảng tương tác lớn với bảng danh mục sản phẩm / phòng ban nhỏ.
   - Kết quả: Broadcast Join loại bỏ hoàn toàn Shuffle Write / Shuffle Read trên bảng dimension, giảm đáng kể độ trễ truy vấn.
2. **Partition Pruning:**
   - So sánh việc quét toàn bộ 456 ngày so với truy vấn có vị từ lọc theo khoảng thời gian (date range filter).
   - Kết quả: Spark đẩy bộ lọc xuống File Scan (`PartitionFilters`), chỉ đọc đúng các thư mục ngày cần thiết, giảm I/O hơn 90% trên các khoảng truy vấn ngắn hạn.
3. **In-Memory Caching:**
   - So sánh giữa DataFrame được lưu bộ đệm `MEMORY_AND_DISK` và việc quét/tính toán lại từ HDFS Parquet.
   - Kết quả: Tăng tốc rõ rệt cho các tác vụ lặp qua nhiều tầng aggregation.

*Xem chi tiết kế hoạch thực thi, số liệu đo lường và logs tại:* [`docs/evidence/full/`](evidence/full/README.md).

Current hardening full-data workflow
[`37107843181`](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37107843181)
đã **SUCCESS** trên revision `a35170f`: 51 tests, 33,819,106 source/local/HDFS
events, 456 partitions, pivot/unpivot cùng tổng 32,434,489, toàn bộ benchmark và
Mongo publication đều pass. Số benchmark hiện tại được ghi trong full evidence;
historical run cũ vẫn được giữ và gắn nhãn riêng.

---

## 4. Module 3 — Spark Structured Streaming Speed Layer

### 4.1. Kiến trúc Streaming cuối cùng

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

Runtime dùng Spark 3.5.5 và connector
`spark-sql-kafka-0-10_2.12:3.5.5`. Kafka kết nối qua `localhost:9092` từ
host và `kafka:29092` trong Docker. Min-max normalization và analytical
ranking Window được thực hiện trên static micro-batch DataFrame do
`foreachBatch` cung cấp, thay vì áp dụng trực tiếp lên streaming
DataFrame. Đây là **finalized-window trending**, không phải open-window ranking
được tính lại ở mỗi processing trigger.

Kafka source mặc định strict với `failOnDataLoss=true`, do đó missing offsets
làm query fail thay vì bị bỏ qua âm thầm. Chỉ cấu hình rõ ràng
`MODULE3_FAIL_ON_DATA_LOSS=false` hoặc CLI `--allow-data-loss` mới bật relaxed
mode; `startingOffsets` và checkpoint semantics không đổi.

### 4.2. Event-time, Watermark và Trending

Pipeline dùng watermark event-time **10 phút**, cửa sổ dài **120 phút**
và slide **5 phút**. Trong cùng một stateful aggregation, `C120` là
tổng lượt mua trong cửa sổ, còn `C30` đếm có điều kiện các sự kiện
thuộc 30 phút cuối. Điểm xu hướng là:

`trend_score = 0.7 × norm(C30) + 0.3 × norm(C120)`

Kết quả được xếp theo `trend_score` giảm dần; khi bằng điểm,
`product_id` tăng dần là tie-break để rank luôn deterministic.

### 4.3. MongoDB Serving & Idempotency

Collection `realtime_trending` lưu contract gồm `window_start`, `window_end`,
`product_id`, `purchase_count_30m`, `purchase_count_120m`, `trend_score`,
`trend_rank` và `updated_at`. Mỗi document có `_id` deterministic từ
`window_end + product_id` và được upsert idempotent. MongoDB duy trì unique
index `(window_end, product_id)` và `(window_end, trend_rank)`. Mặc định giữ
Top-20 sau khi normalize/rank toàn bộ population và áp dụng TTL 7 ngày
(`ttl_seconds = 604800`; giá trị `0` mới tắt retention).

Mỗi complete finalized-window snapshot được xóa theo `window_end` trước khi
Top-K upsert. Vì vậy rerun một bounded feed loại bỏ stale non-Top-K rows và
không va chạm rank cũ. Source event trùng `event_id` được deduplicate trong
watermark trước aggregation; invalid/poison rows được đếm qua
`record_quality` nhưng không đi vào stateful aggregation.

Snapshot replacement này logically idempotent và hội tụ đúng khi Spark retry,
nhưng chuỗi delete + upsert **không transactionally atomic** đối với reader đồng
thời. Dashboard có thể thoáng thấy window rỗng hoặc chưa đầy đủ trong lúc thay
thế. Thiết kế production tương lai có thể dùng immutable snapshot/version ID với
active-version pointer, hoặc Mongo transaction khi phù hợp; project hiện tại
không triển khai thêm độ phức tạp đó và không tuyên bố transactional exactly-once.

### 4.4. Live End-to-End Validation

| Chỉ số | Kết quả |
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

Finalized-window sink batch được đo **62,021.821 ms** (`addBatch` của input
batch là **14,098 ms**). Output fan-out lớn vì mỗi source event có thể
tham gia nhiều cửa sổ trượt 5 phút. Đây là phép đo development có
giới hạn trên WSL2, một Kafka broker và hai Spark cores, không phải tuyên bố
production capacity. Artifact mang commit chính xác
`61ae930c883293fd006eb6d27f7da70aab376d8b` và source digest
`3fb1c0240e873cb0700320a236caef73761b9f9ccbfbf2713d8a1fae264da8b4`.

### 4.5. Checkpoint & Fault-Tolerance Validation

Kịch bản dùng checkpoint ID `recovery-smoke` đã chứng minh recovery qua
**hai Spark process tách biệt**. Process thứ nhất đọc 3 records; sau đó
2 records mới được ghi thêm vào Kafka. Process thứ hai dùng đúng
checkpoint đã chỉ đọc 2 records mới: query ID không đổi, batch
number tiếp tục và Kafka không replay từ earliest. CI exact-source run
`37103621207` có 109 scenario documents và 0 duplicate keys; số document phụ
thuộc các finalized sliding windows, không ảnh hưởng recovery assertion.

### 4.6. Late Data & Watermark Validation

Batch đầu nhận 3 Kafka records: 2 valid, 1 poison/invalid; trong 2 valid
records có 1 duplicate `event_id`, nên Spark báo `numDroppedDuplicateRows = 1`.
Sự kiện lúc `00:15` được chấp nhận khi watermark là `00:10`. Sau khi watermark
tiến đến `02:50`, sự kiện gửi sau đó với event-time `00:18` bị loại tại dedup
state (`numRowsDroppedByWatermark = 1`). Kết quả finalized được chọn vẫn là
`C30 = 2`, `C120 = 2`.

### 4.7. Performance Benchmarks

Mỗi workload có 1 warm-up và 3 measured trials.

| Producer target | Achieved producer rate | Spark processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 100/s | 100.74/s | 40.55 | 2,383 |
| 500/s | 497.97/s | 46.86 | 2,123 |
| unbounded bulk | 5,695.47/s | 47.48 | 2,106 |

Mỗi arm partition dùng topic Kafka tạm thời thực, xác minh broker metadata,
1 warm-up và 3 measured trials.

| Kafka partitions | Spark observed | Processed rows/s | Trigger ms |
|---:|---:|---:|---:|
| 1 | 1 | 51.57 | 1,939 |
| 2 | 2 | 60.86 | 1,643 |
| 4 | 4 | 47.73 | 2,095 |
| 8 | 8 | 31.26 | 3,199 |

Trong bounded workload nhỏ này, kết quả cao nhất ở 2 partitions và giảm
ở 4/8 partitions; không thể suy ra rằng thêm partition luôn tăng hiệu năng.
Scheduling và state-management overhead có thể chi phối workload nhỏ. Cấu
hình chỉ có một Kafka broker nên thử nghiệm đo consumer partition
parallelism, không kiểm chứng broker high availability.

### 4.8. Automated Validation / CI

- Final-hardening local Docker suite: **51 passed**; Ruff, Compose và whitespace
  đều **passed**.
- Complete repository suite on implementation commit `ff06239`: **42 passed**.
- Complete repository suite after rollback regression `61ae930`: **43 passed**.
- Ruff: **passed**; Docker Compose configuration và GitHub workflow YAML:
  **valid**.
- GitHub Actions `Fast validation` run `37094705692`: **SUCCESS** on `ff06239`.
- GitHub Actions `Module 3 streaming validation` run `37094705601`, job
  `streaming`: **SUCCESS** on `ff06239`.
- GitHub Actions `Fast validation` run `37103618599`: **SUCCESS** on `61ae930`.
- GitHub Actions `Module 3 streaming validation` run `37103621207`: **SUCCESS**
  on `61ae930`.
- Final-source `Fast validation` run `37107841305`: **SUCCESS**, 51 tests.
- Final-source `Module 3 streaming validation` run `37107841330`: **SUCCESS**
  trên `a35170f`; Kafka → Spark → Mongo, recovery, duplicate/poison/late và
  throughput smoke đều pass.
- Final-PR-head `Fast validation` run `37109270743`: **SUCCESS** trên `c462618`.
- Final-PR-head `Module 3 streaming validation` run `37109270688`: **SUCCESS**
  trên `c462618`.
- Post-merge `Fast validation` run `37119416919`: **SUCCESS** trên merge revision
  `0622b14309b3ca379752cc827cdc7a7a45919551`.
- Post-merge `Module 3 streaming validation` run `37119416925`: **SUCCESS** trên
  merge revision `0622b14309b3ca379752cc827cdc7a7a45919551`.

CI đã tái lập bounded Kafka → Spark → MongoDB path trên một
GitHub-hosted runner sạch, bao gồm bounded Module 1 feed generation, pinned
Spark runtime build, tests, service connectivity, recovery, late-data/watermark
validation, throughput smoke, evidence upload và cleanup. PR #2 đã merge vào
`main` ngày 2026-10-03. Lineage kiểm chứng chính xác là:

- `a35170ff0f98729ad2b64b8df4c96c4bf6321201`: source hardening đã chạy full-data
  33,819,106 events và các validation Module 1–3.
- `c462618048b3c0e2462486a8f145858762878feb`: final PR head, gồm đồng bộ tài liệu
  và evidence; cả hai final pre-merge workflows đều thành công.
- `0622b14309b3ca379752cc827cdc7a7a45919551`: merge commit thực tế trên `main`;
  cả Fast validation và Module 3 streaming validation post-merge đều thành công.

Full-data workflow không được chạy lại trên hai revision tài liệu/merge về sau.

Lần chạy PR đầu tiên chỉ phát hiện timing issue: startup batch khoảng
8.66 giây cộng producer delay 5 giây vượt trial allowance 12 giây. Allowance
được điều chỉnh từ **12 lên 30 giây** cho CI; không thay đổi
architecture, streaming semantics, business logic, window/watermark, scoring hay
MongoDB behavior.

### 4.9. Current Status & Limitations

Module 3 có trạng thái **COMPLETE, VALIDATED AND MERGED**. PR #2 đã merge vào
`main` tại `0622b14309b3ca379752cc827cdc7a7a45919551`.

- Các benchmark là bounded development measurements, không phải production
  capacity claims.
- Deployment dùng một Kafka broker; partition benchmark không chứng minh
  broker high availability.
- Đây là finalized-window output; hệ thống không cung cấp open-window ranking
  thay đổi ở mỗi trigger.
- Sliding windows tạo state/output fan-out lớn; kết quả 50k không ngoại suy
  thành production throughput hoặc capacity.
- Mongo sink dùng bounded `toLocalIterator` + bulk batches trên driver; Top-K
  giới hạn output nhưng sink chưa phải distributed Mongo writer.
- Delete + upsert khi thay finalized-window snapshot không transactionally atomic;
  concurrent reader có thể thoáng thấy snapshot rỗng hoặc một phần.

---

## 5. Hướng dẫn Thực thi & Tái lập (Runbook)

### 5.1. Khởi động Cụm dịch vụ qua Docker Compose
Docker Compose quản lý toàn bộ hệ sinh thái:
```bash
docker compose up -d namenode datanode kafka mongodb spark-master spark-worker
```
- **Hadoop NameNode & WebHDFS:** `http://localhost:9870` (IPC: `hdfs://localhost:8020`)
- **Kafka Broker:** `localhost:9092`
- **Spark Master UI:** `http://localhost:8080` (Worker: 2 cores, 2 GB RAM)
- **MongoDB:** `mongodb://localhost:27017`

### 5.2. Chạy Pipeline Ingestion & Staging (Module 1)
Có thể chạy nhanh toàn bộ pipeline qua script runner hoặc chạy từng bước riêng biệt:
```bash
# Cách 1: Chạy trọn gói toàn bộ pipeline Module 1 (Clean -> Generate -> Stage)
python scripts/module1.py all

# Cách 2: Chạy chi tiết từng bước
python -m src.module1.clean --validate
python -m src.module1.generate --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
python -m src.module1.stage_hdfs

# Thử nghiệm phát stream Kafka (tuỳ chọn)
python -m src.module1.producer --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0
```

### 5.3. Chạy Spark Batch Analytics & Benchmark (Module 2)
Chạy toàn bộ hoặc từng phần của Module 2 qua container driver chuẩn:
```bash
# Chạy toàn bộ (Handoff validation + Analytics + MongoDB Export + 3 Benchmarks)
python scripts/module2.py all

# Hoặc chạy riêng từng tác vụ
python scripts/module2.py validate
python scripts/module2.py stats
python scripts/module2.py benchmark-joins
python scripts/module2.py benchmark-partitions
python scripts/module2.py benchmark-cache
```

### 5.4. Chạy Spark Structured Streaming (Module 3)
Module 3 tiêu thụ Kafka topic `instacart-purchase-events`, áp dụng watermark
10 phút trên `event_time_epoch_ms`, quan sát và loại invalid rows, deduplicate
`event_id`, rồi dùng một stateful aggregate 120 phút (slide 5 phút) với count 30
phút được tính có điều kiện. Append mode chỉ phát finalized windows;
normalize/rank full population chạy trên static DataFrame trong `foreachBatch`,
sau đó mới Top-K, replace snapshot và upsert idempotent vào MongoDB
`realtime_trending` (Proposal §17–22):
```bash
# Validate Kafka + Mongo connectivity (không stream)
python scripts/module3.py validate

# Run streaming foreground (Ctrl+C để dừng; --duration-seconds 0 = forever)
python scripts/module3.py run --duration-seconds 0

# Deterministic late-data demo; tự tạo topic và phát từng phase
python scripts/module3.py late-demo --duration-seconds 60

# Live smoke và checkpoint recovery (không cần terminal thứ hai)
python scripts/module3_smoke.py --events 50000 --duration-seconds 120
python scripts/module3_recovery.py --duration-seconds 25

# Benchmarks (Proposal §15.4, §15.5)
python scripts/module3.py benchmark-throughput --duration-seconds 15 --benchmark-events 100
python scripts/module3.py benchmark-partitions --duration-seconds 15 --benchmark-events 100
python scripts/module3.py all --duration-seconds 60

# Mặc định Top-20 và TTL 7 ngày; có thể đổi hoặc tắt TTL rõ ràng
python scripts/module3.py run --mongo-top-k-per-window 50 --ttl-seconds 86400
python scripts/module3.py run --ttl-seconds 0

# Kafka offset loss là lỗi mặc định; relaxed mode phải opt-in rõ ràng
MODULE3_FAIL_ON_DATA_LOSS=false python scripts/module3.py run --duration-seconds 60
python scripts/module3.py run --allow-data-loss --duration-seconds 60
```
Cấu hình windows/watermark/weights có thể override qua CLI flags:
`--window-short`, `--window-long`, `--slide`, `--watermark`,
`--weight-short`, `--weight-long`, `--trigger-interval`.

### 5.5. Kiểm thử Đơn vị (Unit Tests)
```bash
# Chạy bộ unit test trên môi trường phát triển
python -m pytest

# Chỉ test Module 3 (cần pyspark; Kafka/Mongo live test cần cluster)
docker compose run --rm --no-deps --entrypoint python3 module3 -m pytest -q tests/test_module3.py
```

---

## 6. Danh mục Bàn giao & Checklist Nghiệm thu

- [x] **Module 1 - Ingestion & Storage:**
  - [x] Đọc và làm sạch 6 file CSV gốc với explicit schema.
  - [x] Khử NaN, tái cấu trúc gap 30 ngày bảo toàn Day-Of-Week.
  - [x] Entity-keyed deterministic generation, bất biến theo `--batch-users`, và timestamp đơn điệu chặt chẽ theo user.
  - [x] Phân vùng tương tác thành 456 partition ngày trên HDFS.
  - [x] HDFS staging backup/restore giữ target cũ khi upload/promotion lỗi.
  - [x] Kafka producer external-merge bounded-memory, broker-ack receipt, pacing và fault injection.
- [x] **Module 2 - Spark Batch Processing:**
  - [x] Khởi tạo container Spark 3.5.5 kết nối HDFS và MongoDB.
  - [x] Xác thực toàn vẹn dữ liệu đầu vào và hóa đơn staging (`_sources.json`, `_handoff.json`).
  - [x] Tính toán User Features và lưu trữ tại `/instacart/features/user_features`.
  - [x] Tính toán Batch Metrics và xuất bản sang MongoDB.
  - [x] Spark pivot + explicit unpivot với kiểm tra tổng nhất quán.
  - [x] Benchmark Join (BHJ vs SMJ), Partition Pruning, In-Memory Caching.
  - [x] Kiểm thử tự động trên CI/CD GitHub Actions và lưu trữ Evidence đầy đủ.
- [x] **Module 3 - Structured Streaming** *(implementation and validation complete; PR #2 merged into `main`)*:
  - [x] Kafka source `instacart-purchase-events` parse JSON + poison-pill guard.
  - [x] Kafka `failOnDataLoss=true` mặc định; environment/CLI opt-out rõ ràng.
  - [x] Domain validation/quality metrics + source `event_id` deduplication.
  - [x] Event-time watermark 10 phút trên `event_time_epoch_ms`.
  - [x] Một stateful 120m sliding aggregate (slide 5m) + exact conditional C30m + trend_score = 0.7·N(C30m) + 0.3·N(C120m).
  - [x] Full-population normalize/rank, sau đó Top-K; replace snapshot + idempotent upsert MongoDB.
  - [x] Unique indexes theo window/product và window/rank; TTL mặc định 7 ngày.
  - [x] Explicit checkpoint ID; two-process recovery measured without replay from earliest.
  - [x] Deterministic late-data demo measured watermark acceptance and dropped state rows.
  - [x] Measured throughput with real producer rate control + real Kafka 1/2/4/8 topic sweep.
  - [x] Final-hardening Docker complete suite: `51 passed`; Ruff + Compose + whitespace PASS.
  - [x] CI workflow `.github/workflows/module3-full.yml` + evidence folder `docs/evidence/module3/`.
  - [x] GitHub Actions `Module 3 streaming validation` run `37103621207`: job `streaming` **SUCCESS** on `61ae930`.
- [ ] **Module 4 - ML & Serving:** Sẵn sàng đọc features từ HDFS và ground truth `order_products__train`.
