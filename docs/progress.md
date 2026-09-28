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
| **Module 3** | Speed Layer, Event-Time Stream Processing, Watermark, Realtime Trending | Spark Structured Streaming, Kafka Consumer, MongoDB Sink | *Sẵn sàng triển khai tiếp theo* |
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
  │      - Seeded RNG anchor week (deterministic & reproducible)
  │      - Cumulative days & session-level exponential item deltas
  │      - Monotonicity guard: bảo toàn thứ tự thời gian cho từng user
  │      - Ghi ra data/synthesized/scatter_{1w,1m,3m}/events.parquet
  │
  ├──> python -m src.module1.stage_hdfs
  │      - Tải 6 CSV raw lên /instacart/raw/
  │      - Tải curated Parquet lên /instacart/curated/
  │      - Phân vùng tương tác thành 456 partition ngày (/instacart/curated/interactions/synthetic_year=.../synthetic_month=.../synthetic_day=...)
  │      - Xuất metadata và hóa đơn kiểm chứng: _sources.json, _handoff.json
  │
  └──> python -m src.module1.producer
         - Đọc events.parquet, sort theo event_time_epoch_ms
         - Replay sự kiện lên Kafka topic 'instacart-purchase-events' (Key = user_id)
         - Gán ingestion_time_epoch_ms tại send-time
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
3. **Department-Hour Pivot:** Phân tích ma trận mật độ mua sắm giữa 21 phòng ban và 24 khung giờ.
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

---

## 4. Hướng dẫn Thực thi & Tái lập (Runbook)

### 4.1. Khởi động Cụm dịch vụ qua Docker Compose
Docker Compose quản lý toàn bộ hệ sinh thái:
```bash
docker compose up -d namenode datanode kafka mongodb spark-master spark-worker
```
- **Hadoop NameNode & WebHDFS:** `http://localhost:9870` (IPC: `hdfs://localhost:8020`)
- **Kafka Broker:** `localhost:9092`
- **Spark Master UI:** `http://localhost:8080` (Worker: 2 cores, 2 GB RAM)
- **MongoDB:** `mongodb://localhost:27017`

### 4.2. Chạy Pipeline Ingestion & Staging (Module 1)
```bash
# 1. Làm sạch dữ liệu gốc
python -m src.module1.clean --validate

# 2. Tạo synthetic event stream (canonical 3-month scatter)
python -m src.module1.generate --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m

# 3. Phân vùng và đẩy lên HDFS
python -m src.module1.stage_hdfs

# 4. Thử nghiệm phát stream Kafka (tuỳ chọn)
python -m src.module1.producer --feed data/synthesized/scatter_3m --limit-events 50000 --replay-speed 0
```

### 4.3. Chạy Spark Batch Analytics & Benchmark (Module 2)
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

### 4.4. Kiểm thử Đơn vị (Unit Tests)
```bash
# Chạy bộ unit test trên môi trường phát triển
python -m pytest
```

---

## 5. Danh mục Bàn giao & Checklist Nghiệm thu

- [x] **Module 1 - Ingestion & Storage:**
  - [x] Đọc và làm sạch 6 file CSV gốc với explicit schema.
  - [x] Khử NaN, tái cấu trúc gap 30 ngày bảo toàn Day-Of-Week.
  - [x] Tạo synthetic timestamp có tính đơn điệu chặt chẽ theo user (`0 violations`).
  - [x] Phân vùng tương tác thành 456 partition ngày trên HDFS.
  - [x] Kafka producer hỗ trợ pacing và mô phỏng lỗi (fault injection).
- [x] **Module 2 - Spark Batch Processing:**
  - [x] Khởi tạo container Spark 3.5.5 kết nối HDFS và MongoDB.
  - [x] Xác thực toàn vẹn dữ liệu đầu vào và hóa đơn staging (`_sources.json`, `_handoff.json`).
  - [x] Tính toán User Features và lưu trữ tại `/instacart/features/user_features`.
  - [x] Tính toán Batch Metrics và xuất bản sang MongoDB.
  - [x] Benchmark Join (BHJ vs SMJ), Partition Pruning, In-Memory Caching.
  - [x] Kiểm thử tự động trên CI/CD GitHub Actions và lưu trữ Evidence đầy đủ.
- [ ] **Module 3 - Structured Streaming:** Sẵn sàng kết nối Kafka topic `instacart-purchase-events`.
- [ ] **Module 4 - ML & Serving:** Sẵn sàng đọc features từ HDFS và ground truth `order_products__train`.
