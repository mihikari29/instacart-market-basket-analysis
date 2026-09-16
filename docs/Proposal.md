# PROPOSAL DỰ ÁN
## Môn: Lưu trữ và Xử lý Dữ liệu lớn (IT4931)

# Xây dựng hệ thống Big Data phân tích hành vi mua sắm và gợi ý sản phẩm thích ứng xu hướng theo kiến trúc Lambda sử dụng Apache Kafka, Spark, HDFS và NoSQL

**Tên tiếng Anh:**  
**A Lambda-Based Big Data System for Purchase Behavior Analytics and Trend-Aware Product Recommendation**

---

# 1. Bối cảnh và vấn đề

Trong các hệ thống thương mại điện tử, gợi ý sản phẩm thường dựa trên hành vi lịch sử dài hạn của người dùng. Cách tiếp cận này có thể tạo ra các gợi ý ổn định theo sở thích cá nhân, nhưng có một hạn chế quan trọng: mô hình lịch sử phản ứng chậm trước những thay đổi ngắn hạn như sản phẩm đang tăng nhanh về mức độ phổ biến hoặc xu hướng mua mới xuất hiện.

Dự án đề xuất xây dựng một hệ thống Big Data end-to-end theo **Lambda Architecture**, kết hợp hai loại tín hiệu:

- **Long-term preference:** sở thích dài hạn của người dùng được học từ lịch sử mua hàng bằng mô hình ALS trong Spark MLlib.
- **Short-term trend:** xu hướng sản phẩm gần thời gian thực được tính từ luồng purchase event được replay qua Kafka và xử lý bằng Spark Structured Streaming.

Kết quả của hai nhánh được hợp nhất tại Serving Layer để tạo ra danh sách gợi ý cuối cùng vừa phản ánh sở thích cá nhân, vừa thích ứng với xu hướng ngắn hạn.

Dữ liệu sử dụng là **Instacart Market Basket Analysis**. Dataset là dữ liệu lịch sử, không chứa timestamp tuyệt đối cho từng purchase event. Vì vậy, dự án sẽ tạo **synthetic timestamp** theo cách deterministic và reproducible để mô phỏng event stream. Timestamp này chỉ phục vụ mục đích replay và streaming, không được xem là timestamp thực tế của Instacart.

---

# 2. Mục tiêu dự án

Dự án hướng tới xây dựng một pipeline Big Data hoàn chỉnh từ ingestion đến serving, đồng thời chứng minh đầy đủ các nội dung kỹ thuật của học phần.

Các mục tiêu chính:

1. Lưu trữ và quản lý dữ liệu lớn trên HDFS.
2. Thiết kế pipeline ingestion từ dataset gốc sang HDFS và Kafka.
3. Thực hiện batch processing bằng Spark SQL/DataFrame với:
   - complex aggregations;
   - advanced transformations;
   - window functions;
   - pivot/unpivot;
   - broadcast join;
   - sort-merge join.
4. Thực hiện performance optimization và benchmark:
   - partition pruning;
   - caching;
   - broadcast join;
   - bucketing nếu phù hợp;
   - phân tích physical execution plan.
5. Xây dựng stream processing bằng Spark Structured Streaming:
   - event-time processing;
   - window aggregation;
   - watermark;
   - late data;
   - checkpoint;
   - idempotent sink.
6. Xây dựng mô hình ALS recommendation bằng Spark MLlib.
7. Đánh giá recommendation bằng Precision@K và Recall@K.
8. Xây dựng Product Co-purchase Graph bằng GraphFrames.
9. Thiết kế Serving Layer kết hợp ALS recommendation và realtime trending.
10. Lưu kết quả phục vụ truy vấn bằng NoSQL.
11. Xây dựng dashboard để trình bày:
   - batch analytics;
   - realtime trending;
   - recommendation;
   - performance benchmark.
12. Container hóa và triển khai các thành phần chính bằng Docker/Kubernetes ở mức phù hợp với đồ án sinh viên.

---

# 3. Phạm vi dự án

## 3.1. Must-have

Các thành phần bắt buộc phải hoàn thành:

- HDFS raw/curated storage.
- Kafka producer và topic purchase event.
- Spark batch pipeline.
- Spark Structured Streaming.
- Event-time window và watermark.
- Batch analytics.
- Performance experiments.
- ALS recommendation.
- Precision@K và Recall@K.
- MongoDB serving store.
- Trend-aware final ranking.
- Dashboard.
- Docker.
- Kubernetes demo.

## 3.2. Should-have

- Product Co-purchase Graph.
- Weighted degree.
- PageRank.
- REST API bằng FastAPI.
- Demo late data.
- Benchmark Kafka partitions.
- Thêm MAP@K hoặc NDCG@K.

## 3.3. Optional

- Connected Components.
- Bucketing benchmark nếu môi trường hỗ trợ ổn định.
- Cloud deployment.
- Dashboard nâng cao.
- Graph score được đưa vào FinalScore.

---

# 4. Dataset

## 4.1. Dataset được lựa chọn

**Instacart Market Basket Analysis**

Các đặc điểm chính:

- khoảng 3,4 triệu đơn hàng;
- khoảng 206.000 người dùng;
- hơn 32 triệu lượt mua sản phẩm;
- khoảng 50.000 sản phẩm.

Dataset phù hợp với dự án vì:

- quy mô đủ lớn để các kỹ thuật Spark optimization có ý nghĩa;
- có cấu trúc fact/dimension rõ ràng;
- có nhiều tương tác lặp lại theo user-product;
- có `reordered` để hỗ trợ phân tích hành vi mua lại;
- phù hợp với collaborative filtering;
- có thể xây dựng product co-purchase graph.

## 4.2. Các file sử dụng

### `orders.csv`

| Field | Spark Type | Ý nghĩa |
|---|---|---|
| `order_id` | Long | Định danh đơn hàng |
| `user_id` | Long | Định danh người dùng |
| `eval_set` | String | Tập prior/train/test |
| `order_number` | Integer | Thứ tự đơn hàng của user |
| `order_dow` | Integer | Ngày trong tuần |
| `order_hour_of_day` | Integer | Giờ đặt hàng |
| `days_since_prior_order` | Double | Khoảng cách với đơn trước |

### `order_products__prior.csv`

| Field | Spark Type | Ý nghĩa |
|---|---|---|
| `order_id` | Long | Định danh order |
| `product_id` | Integer | Định danh sản phẩm |
| `add_to_cart_order` | Integer | Thứ tự thêm vào basket |
| `reordered` | Integer/Boolean | Đã từng mua trước đó hay chưa |

### `order_products__train.csv`

Schema giống `order_products__prior.csv`, được dùng làm ground truth để đánh giá mô hình recommendation.

### `products.csv`

| Field | Spark Type | Ý nghĩa |
|---|---|---|
| `product_id` | Integer | Định danh sản phẩm |
| `product_name` | String | Tên sản phẩm |
| `aisle_id` | Integer | Nhóm aisle |
| `department_id` | Integer | Nhóm department |

### `aisles.csv`

| Field | Spark Type | Ý nghĩa |
|---|---|---|
| `aisle_id` | Integer | Định danh aisle |
| `aisle` | String | Tên aisle |

### `departments.csv`

| Field | Spark Type | Ý nghĩa |
|---|---|---|
| `department_id` | Integer | Định danh department |
| `department` | String | Tên department |

## 4.3. Lưu ý dữ liệu

Dataset không cung cấp:

- timestamp tuyệt đối;
- giá sản phẩm;
- doanh thu;
- brand chuẩn hóa;
- click/view event.

Vì vậy dự án không sử dụng các chỉ số cần những trường này, trừ trường hợp dữ liệu được tổng hợp rõ ràng và được ghi chú là synthetic.

---

# 5. Kiến trúc hệ thống

Dự án sử dụng **Lambda Architecture** gồm ba lớp chính:

- **Batch Layer:** xử lý toàn bộ lịch sử.
- **Speed Layer:** xử lý event stream gần thời gian thực.
- **Serving Layer:** hợp nhất output của hai nhánh.

## 5.1. Sơ đồ kiến trúc

```text
                         INSTACART CSV
                              │
                              ▼
                  ┌──────────────────────┐
                  │ MODULE 1             │
                  │ Ingestion & Storage  │
                  └──────────┬───────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
          HDFS Raw/Curated           Synthetic Event Generator
              │                             │
              │                             ▼
              │                           Kafka
              │                             │
              ▼                             ▼
      ┌─────────────────┐          ┌─────────────────────┐
      │ MODULE 2        │          │ MODULE 3            │
      │ Spark Batch     │          │ Structured Streaming│
      └────────┬────────┘          └──────────┬──────────┘
               │                              │
               ▼                              ▼
     Batch Metrics / Features          Realtime Trending
               │                              │
               └──────────────┬───────────────┘
                              ▼
                         MongoDB
                              ▲
                              │
                    ┌─────────┴─────────┐
                    │ MODULE 4          │
                    │ ALS + Graph +     │
                    │ Serving + Dashboard│
                    └─────────┬─────────┘
                              │
                              ▼
                    Final Recommendations
                              │
                              ▼
                           Dashboard
```

---

# 6. Data Flow và Data Lineage

```text
orders.csv
   │
   ├──────────────┐
   │              │
   ▼              ▼
orders_curated   timestamp synthesis
   │              │
   │              └──────────────┐
   │                             │
   ▼                             ▼
order_products_prior_curated   purchase_event
   │                             │
   ├───── join ─────┐            ▼
   │                │           Kafka
   ▼                │            │
curated_interactions│            ▼
   │                │      Spark Structured Streaming
   │                │            │
   ▼                │            ▼
Spark Batch         │      realtime_trending
   │                │            │
   ▼                │            │
batch_metrics       │            │
   │                │            │
   ├──────────────┐ │ ┌──────────┘
   ▼              ▼ ▼ ▼
 MongoDB       ALS / Graph
                  │
                  ▼
         user_recommendations
                  │
                  ▼
             Serving Layer
                  │
                  ▼
        final_recommendations
                  │
                  ▼
             API / Dashboard
```

---

# 7. MODULE 1 — Data Ingestion & Storage Foundation

## 7.1. Input

Module 1 đọc toàn bộ 6 file:

```text
orders.csv
order_products__prior.csv
order_products__train.csv
products.csv
aisles.csv
departments.csv
```

## 7.2. Processing

```text
Raw CSV
→ explicit schema
→ schema validation
→ data quality checks
→ curated normalized tables
→ timestamp synthesis
→ interaction table
→ HDFS output
→ Kafka event generation
```

## 7.3. Data Quality Checks

Kiểm tra tối thiểu:

- `order_id` unique trong `orders`;
- `product_id` unique trong `products`;
- `order_dow ∈ [0,6]`;
- `order_hour_of_day ∈ [0,23]`;
- `add_to_cart_order` tăng từ 1 đến N trong từng order;
- foreign key hợp lệ giữa product, aisle, department;
- không đảo thứ tự order theo user.

---

# 8. Synthetic Timestamp Generation

Dataset không có timestamp tuyệt đối nên cần tạo synthetic timestamp.

## 8.1. Mục tiêu

Phương pháp phải:

- deterministic;
- reproducible;
- bảo toàn thứ tự order;
- giữ consistency với `order_dow`;
- sử dụng `order_hour_of_day`;
- tạo event time riêng cho từng item;
- có thể replay qua Kafka.

## 8.2. Anchor date

Sử dụng seeded RNG (reproducible given same SEED + batch_users):

```text
SCATTER_WEEKS = 13
SEED = 42

anchor_week(user)
= rng.integers(0, SCATTER_WEEKS)
```

anchor_day = anchor_week * 7 + first_order_dow

Ngày anchor được chọn sao cho weekday khớp `order_dow` của order đầu tiên.
Reproducible bằng cách chạy lại cùng SEED và `batch_users` (ghi trong `manifest.json`).

## 8.3. Order date

```text
order_date(i)
= anchor_date
+ cumulative_sum(days_since_prior_order)
```

## 8.4. Recovery cho gap bị cap ở 30

Với các gap bằng 30, dùng:

```text
gap_true
= 30 + ((next_dow - prev_dow - 2) mod 7)
```

nhằm khôi phục gap 30–36 ngày và đảm bảo weekday consistency.

## 8.5. Item-level event time

Item đầu tiên có base time trong `order_hour_of_day`.

Các item tiếp theo:

```text
event_time(k)
= base + Σ delta_i

delta_i ~ Exp(mean ≈ 30s)
delta_i clipped to [5s,120s]
```

## 8.6. Monotonicity Guard

```text
event_time(next)
= max(
    generated_time,
    previous_user_last_event + 1 second
)
```

Điều này đảm bảo chronology theo user không bị đảo.

---

# 9. Kafka Design

## 9.1. Topic

```text
instacart-purchase-events
```

## 9.2. Kafka Key

```text
user_id
```

Lý do:

- bảo toàn ordering cho toàn bộ event của cùng user;
- phù hợp với chronology invariant;
- giúp partition theo user ổn định.

## 9.3. Serialization

```text
UTF-8 JSON
```

## 9.4. Kafka Event Schema

```json
{
  "event_id": "456_3",
  "order_id": 456,
  "user_id": 123,
  "product_id": 789,
  "add_to_cart_order": 3,
  "reordered": true,
  "aisle_id": 12,
  "department_id": 4,
  "order_dow": 2,
  "order_hour_of_day": 17,
  "event_time_epoch_ms": 1799852543123,
  "event_time_iso": "2027-01-13T17:15:43Z",
  "ingestion_time_epoch_ms": 1799852545210
}
```

### Giải thích field

| Field | Ý nghĩa |
|---|---|
| `event_id` | ID duy nhất của item event |
| `order_id` | Order gốc |
| `user_id` | User |
| `product_id` | Product |
| `add_to_cart_order` | Thứ tự item trong basket |
| `reordered` | Có phải mua lại |
| `aisle_id` | Aisle |
| `department_id` | Department |
| `order_dow` | Weekday gốc |
| `order_hour_of_day` | Hour gốc |
| `event_time_epoch_ms` | Synthetic event time |
| `event_time_iso` | Synthetic event time dạng ISO |
| `ingestion_time_epoch_ms` | Thời điểm Kafka producer phát event |

---

# 10. HDFS Design

```text
/instacart/
│
├── raw/
│   ├── orders/
│   ├── order_products_prior/
│   ├── order_products_train/
│   ├── products/
│   ├── aisles/
│   └── departments/
│
├── curated/
│   ├── orders/
│   ├── order_products_prior/
│   ├── order_products_train/
│   ├── dimensions/
│   │   ├── products/
│   │   ├── aisles/
│   │   └── departments/
│   └── interactions/
│       └── synthetic_year=YYYY/
│           └── synthetic_month=MM/
│               └── synthetic_day=DD/
│
├── features/
│   ├── user_features/
│   └── als_interactions/
│
└── models/
    └── als/
```

Partition theo ngày/tháng synthetic giúp hỗ trợ partition pruning và historical filtering.

---

# 11. Output Module 1

## 11.1. `orders_curated`

- Format: Parquet
- Storage: HDFS
- Consumer: Module 2

## 11.2. `order_products_prior_curated`

- Format: Parquet
- Storage: HDFS
- Consumer: Module 2, Module 4

## 11.3. `order_products_train_curated`

- Format: Parquet
- Storage: HDFS
- Consumer: Module 4

## 11.4. `product_dimension`

- Format: Parquet
- Storage: HDFS
- Consumer: Module 2, Module 4

## 11.5. `curated_interactions`

Schema chính:

```text
order_id
user_id
product_id
add_to_cart_order
reordered
aisle_id
department_id
order_number
event_time
synthetic_date
```

- Format: Parquet
- Storage: HDFS
- Consumer: Module 2, Module 4

## 11.6. `purchase_event`

- Format: JSON
- Topic: Kafka `instacart-purchase-events`
- Consumer: Module 3

---

# 12. MODULE 2 — Batch Processing Layer

## 12.1. Input

```text
orders_curated
order_products_prior_curated
curated_interactions
product_dimension
aisles
departments
```

## 12.2. Complex Aggregations

### Window functions

Ví dụ:

```text
purchase_count(product, day)
rolling_7day_purchase_count
```

### Pivot

Ví dụ:

```text
department × order_hour_of_day
```

### Custom aggregation

Tính:

```text
reorder_rate
= sum(reordered) / count(*)
```

### User aggregation

```text
user_id
order_count
purchase_count
unique_products
avg_basket_size
reorder_rate
```

---

# 13. Advanced Transformations

Pipeline ví dụ:

```text
interaction
→ time feature derivation
→ product metadata enrichment
→ user-product frequency
→ reorder feature
→ user/product aggregate features
```

Nếu sử dụng UDF trên `product_name`, chỉ xem đây là text feature engineering, không xem là brand extraction chính thức.

---

# 14. Join Operations

## 14.1. Broadcast Join

```text
curated_interactions
JOIN broadcast(aisles)
JOIN broadcast(departments)
```

Dimension nhỏ hơn fact table nên phù hợp broadcast.

Metric:

- runtime;
- shuffle read;
- shuffle write;
- physical plan.

## 14.2. Sort-Merge Join

```text
orders_curated
JOIN order_products_prior_curated
ON order_id
```

Hai bảng lớn tạo ra trường hợp sort-merge join tự nhiên.

Có thể tắt auto broadcast khi benchmark:

```text
spark.sql.autoBroadcastJoinThreshold = -1
```

Physical plan được kiểm tra bằng:

```text
df.explain("formatted")
```

---

# 15. Performance Optimization

## 15.1. Experiment 1 — Broadcast vs Sort-Merge

- Baseline: Sort-Merge Join
- Optimization: Broadcast small dimension
- Metric:
  - execution time;
  - shuffle bytes;
  - physical plan.

## 15.2. Experiment 2 — Partition Pruning

- Baseline: scan toàn bộ interactions.
- Optimization: filter theo synthetic date partition.
- Metric:
  - files scanned;
  - bytes read;
  - runtime.

## 15.3. Experiment 3 — Caching

- Baseline: recompute DataFrame dùng lại nhiều lần.
- Optimization: persist/cache.
- Metric:
  - first execution;
  - repeated execution.

## 15.4. Experiment 4 — Streaming Throughput

- Input: Kafka event stream.
- Baseline: producer rate thấp.
- Experiment: tăng rate.
- Metric:
  - inputRowsPerSecond;
  - processedRowsPerSecond;
  - trigger duration.

## 15.5. Experiment 5 — Kafka Partitions

- 1, 2, 4, 8 partitions.
- Metric:
  - throughput;
  - consumer parallelism;
  - processing latency.

## 15.6. Experiment 6 — Spark Executor Configuration

So sánh một số cấu hình executor.

Kết quả cụ thể chỉ được ghi sau khi thực nghiệm.

---

# 16. Output Module 2

## 16.1. `batch_product_metrics`

```text
product_id
purchase_count
unique_users
reorder_count
reorder_rate
last_batch_time
```

- Storage: MongoDB
- Primary key: `product_id`
- Consumer: Module 4, Dashboard

## 16.2. `department_metrics`

```text
department_id
purchase_count
unique_users
reorder_rate
```

- Storage: MongoDB
- Consumer: Dashboard

## 16.3. `user_features`

```text
user_id
order_count
purchase_count
unique_products
avg_basket_size
reorder_rate
```

- Format: Parquet
- Storage: HDFS
- Consumer: Module 4

---

# 17. MODULE 3 — Streaming Processing Layer

## 17.1. Input

Kafka topic:

```text
instacart-purchase-events
```

## 17.2. Processing

```text
Kafka
→ Spark Structured Streaming
→ parse JSON
→ schema validation
→ event-time processing
→ watermark
→ window aggregation
→ trending score
→ foreachBatch
→ MongoDB upsert
```

---

# 18. Trending Definition

Dự án chỉ có purchase event, không sử dụng view hoặc click.

Đề xuất Trending Score:

```math
T(p,t)
=
0.7 × N(C_30m(p,t))
+
0.3 × N(C_120m(p,t))
```

Trong đó:

- `C_30m`: số purchase event của product trong 30 phút gần nhất;
- `C_120m`: số purchase event trong 120 phút;
- `N(.)`: normalization trong cùng window.

Ý nghĩa:

- 30 phút giúp phản ứng nhanh;
- 120 phút làm mượt nhiễu;
- trọng số là baseline và có thể điều chỉnh trong thực nghiệm.

---

# 19. Window và Watermark

Baseline:

```text
Recent window: 30 minutes
Long window:   120 minutes
Slide:         5 minutes
Watermark:     10 minutes
```

Các giá trị trên là cấu hình ban đầu, không phải giá trị tối ưu đã được chứng minh.

Event-time sử dụng synthetic event time.

---

# 20. Late Data

Kịch bản demo:

```text
Event A:
arrives on time
→ processed normally

Event B:
late by 4 minutes
→ within watermark
→ aggregation updated

Event C:
late by 20 minutes
→ may arrive after state cleanup
→ does not update closed window
```

---

# 21. Exactly-Once và Idempotency

Không tuyên bố external sink luôn exactly-once.

Spark sử dụng:

- Kafka offsets;
- checkpoint;
- state metadata.

MongoDB sink dùng deterministic key:

```text
(window_start, window_end, product_id)
```

và:

```text
upsert = true
```

Nếu micro-batch bị retry, cùng logical result sẽ ghi đè cùng document thay vì tạo duplicate.

Mức đảm bảo trong dự án được mô tả là:

**effectively exactly-once for deterministic aggregated outputs with idempotent sink design.**

---

# 22. Output Module 3

## `realtime_trending`

```text
window_start
window_end
product_id
purchase_count_30m
purchase_count_120m
trend_score
trend_rank
updated_at
```

- Storage: MongoDB
- Key: `(window_end, product_id)`
- Consumer: Module 4, Dashboard

---

# 23. MODULE 4A — ALS Recommendation

## 23.1. Train/Test Strategy

- `order_products__prior` dùng để train/history.
- `order_products__train` dùng làm ground truth.
- Không dùng train set để xây interaction training matrix.

## 23.2. Input

```text
user_id
product_id
purchase_count
```

## 23.3. Implicit Strength

Baseline:

```math
r_ui = purchaseCount(u,i)
```

Có thể thử:

```math
r_ui = 1 + log(1 + purchaseCount(u,i))
```

như một feature/hyperparameter experiment.

## 23.4. Model

Spark MLlib ALS:

```text
implicitPrefs = true
```

Hyperparameter được tune:

```text
rank
regParam
alpha
maxIter
```

Không đặt giá trị tối ưu trước thực nghiệm.

---

# 24. Recommendation Evaluation

Với user `u`:

```text
R_K(u) = top-K predicted products
G(u)   = products in order_products__train
```

## Precision@K

```math
Precision@K
=
|R_K(u) ∩ G(u)| / K
```

## Recall@K

```math
Recall@K
=
|R_K(u) ∩ G(u)| / |G(u)|
```

Có thể bổ sung MAP@K hoặc NDCG@K nếu đủ thời gian.

---

# 25. Output ALS

## `user_recommendations`

```text
user_id
product_id
als_score
als_rank
model_version
generated_at
```

- Storage: MongoDB
- Consumer: Serving Layer

---

# 26. MODULE 4B — Graph Processing

## 26.1. Product Co-purchase Graph

### Vertex

```text
product_id
```

### Edge

Hai sản phẩm xuất hiện trong cùng một order.

### Edge Weight

```text
co_purchase_count(product_a, product_b)
```

## 26.2. Graph Algorithms

### Weighted Degree

Insight:

- sản phẩm nào thường đồng xuất hiện với nhiều sản phẩm khác;
- mức độ kết nối trong basket.

### PageRank

Insight:

- sản phẩm có tính trung tâm trong mạng lưới co-purchase.

### Connected Components

Optional.

Chỉ sử dụng nếu thực nghiệm tạo ra insight có ý nghĩa.

---

# 27. Output Graph

## `product_graph_metrics`

```text
product_id
neighbor_count
weighted_degree
pagerank
component_id
computed_at
```

`component_id` là optional.

---

# 28. MODULE 4C — Serving Layer

## 28.1. Candidate Set

```text
ALS Top-N(user)
UNION
Current Trending Top-M
```

## 28.2. Normalization

ALS:

```math
A'_{u,p}
=
(A_{u,p} - A_min)
/
(A_max - A_min + epsilon)
```

Trend:

```math
T'_{p,t}
=
T_{p,t}
/
(max_q T_{q,t} + epsilon)
```

## 28.3. FinalScore

```math
FinalScore(u,p,t)
=
lambda × A'_{u,p}
+
(1-lambda) × T'_{p,t}
```

`lambda` là configurable.

Ví dụ benchmark:

```text
0.5
0.7
0.9
```

Không xem giá trị nào là tối ưu trước thực nghiệm.

## 28.4. Cold Start

### User mới

Không có ALS:

```math
FinalScore = TrendScore
```

### Product mới

Nếu chưa có ALS score nhưng có trend score:

```text
NormalizedALS = 0
```

product vẫn có thể xuất hiện trong final ranking.

---

# 29. Output Serving

## `final_recommendations`

```text
user_id
product_id
product_name
als_score
trend_score
final_score
rank
generated_at
```

- Storage: MongoDB
- Consumer: API/Dashboard

---

# 30. MODULE 4D — Visualization

Dashboard chỉ hiển thị các output đã tồn tại trong pipeline.

## 30.1. Realtime

- Top trending products.
- Trend score theo window.
- Kafka/Spark throughput.
- Streaming processing latency.

## 30.2. Batch

- Top products.
- Reorder rate.
- Department statistics.
- Purchase distribution theo giờ/ngày.

## 30.3. ML

- Precision@K.
- Recall@K.
- ALS recommendation cho một user.

## 30.4. Serving

- ALS score.
- Trend score.
- Final score.
- Ranking trước và sau khi trend thay đổi.

## 30.5. Performance

- baseline runtime;
- optimized runtime;
- shuffle metrics;
- partition pruning.

---

# 31. NoSQL Design

Dự án sử dụng **MongoDB**.

Lý do:

- access pattern đơn giản;
- document model phù hợp recommendation/trending;
- dễ upsert;
- phù hợp dashboard/API;
- triển khai đơn giản hơn Cassandra cho đồ án sinh viên.

## 31.1. `batch_product_metrics`

```text
_id
product_id
purchase_count
unique_users
reorder_count
reorder_rate
updated_at
```

Indexes:

```text
purchase_count
reorder_rate
```

## 31.2. `realtime_trending`

```text
_id
window_start
window_end
product_id
purchase_count
trend_score
trend_rank
updated_at
```

Indexes:

```text
(window_end, trend_rank)
(window_end, product_id)
```

## 31.3. `user_recommendations`

```text
_id
user_id
product_id
als_score
als_rank
model_version
```

Index:

```text
(user_id, als_rank)
```

## 31.4. `final_recommendations`

```text
_id
user_id
product_id
als_score
trend_score
final_score
rank
generated_at
```

Index:

```text
(user_id, rank)
```

## 31.5. `product_graph_metrics`

```text
_id
product_id
neighbor_count
weighted_degree
pagerank
component_id
computed_at
```

---

# 32. Data Contract Toàn Hệ Thống

| Producer Module | Output Dataset/Event | Schema chính | Format | Storage/Topic | Consumer |
|---|---|---|---|---|---|
| M1 | `orders_curated` | order/user/time | Parquet | HDFS | M2 |
| M1 | `order_products_prior_curated` | order-product | Parquet | HDFS | M2, M4 |
| M1 | `order_products_train_curated` | evaluation interactions | Parquet | HDFS | M4 |
| M1 | `product_dimension` | product metadata | Parquet | HDFS | M2, M4 |
| M1 | `curated_interactions` | user-order-product-event_time | Parquet | HDFS | M2, M4 |
| M1 | `purchase_event` | Kafka event schema | JSON | Kafka | M3 |
| M2 | `batch_product_metrics` | product historical metrics | BSON | MongoDB | M4, Dashboard |
| M2 | `department_metrics` | department historical metrics | BSON | MongoDB | Dashboard |
| M2 | `user_features` | user historical features | Parquet | HDFS | M4 |
| M3 | `realtime_trending` | window/product/trend | BSON | MongoDB | M4, Dashboard |
| M4A | `user_recommendations` | user/product/ALS score | BSON | MongoDB | M4C |
| M4B | `product_graph_metrics` | graph metrics | BSON | MongoDB | Dashboard/M4C |
| M4C | `final_recommendations` | blended ranking | BSON | MongoDB | API/Dashboard |

---

# 33. Input – Processing – Output của từng Module

| Module | Input | Processing | Output | Output dùng bởi |
|---|---|---|---|---|
| M1 | 6 Instacart CSV | validation, cleaning, timestamp synthesis, event generation | HDFS curated tables + Kafka events | M2, M3, M4 |
| M2 | HDFS curated tables | batch aggregation, transformation, joins, optimization | batch metrics + user features | M4, Dashboard |
| M3 | Kafka purchase events | event-time streaming, watermark, windows, trending | realtime_trending | M4, Dashboard |
| M4 | history + train ground truth + batch metrics + trend | ALS, graph, serving, visualization | user recommendations, graph metrics, final ranking | API/Dashboard |

---

# 34. Sáu nhóm yêu cầu Spark

| Yêu cầu môn học | Project thực hiện ở đâu | Ví dụ | Module |
|---|---|---|---|
| Complex Aggregations | Spark Batch | rolling metrics, pivot, reorder rate | M2 |
| Advanced Transformations | Batch ETL | multi-stage feature pipeline | M2 |
| Join Operations | Spark SQL | broadcast + sort-merge | M2 |
| Performance Optimization | Experiment | pruning, cache, join plan | M2 |
| Streaming Processing | Structured Streaming | watermark, window, late data | M3 |
| Advanced Analytics | MLlib + GraphFrames | ALS + co-purchase graph | M4 |

---

# 35. Công nghệ

| Technology | Vai trò | Input | Output | Vì sao cần |
|---|---|---|---|---|
| HDFS | distributed storage | CSV/Parquet | historical datasets | lưu trữ dữ liệu lớn |
| Kafka | message queue | synthetic events | event stream | replay và decouple |
| Spark SQL | batch processing | HDFS | metrics/features | distributed processing |
| Structured Streaming | speed layer | Kafka | trending | event-time streaming |
| Spark MLlib | machine learning | user-product interactions | ALS recommendations | native distributed ML |
| GraphFrames | graph analytics | co-purchase edges | graph metrics | xử lý graph trên Spark |
| MongoDB | serving store | processed outputs | dashboard/API query | dễ upsert và triển khai |
| FastAPI | API | MongoDB | HTTP JSON | serving |
| Streamlit | dashboard | MongoDB/API | charts | triển khai nhanh |
| Docker | packaging | services | images | reproducibility |
| Kubernetes | orchestration | containers | deployed services | đáp ứng yêu cầu môn học |

---

# 36. Deployment

## 36.1. Development

Dùng Docker Compose cho:

```text
Kafka
Spark
HDFS
MongoDB
API
Dashboard
```

## 36.2. Final Demonstration

Dùng Kubernetes local cluster như Minikube.

Ví dụ:

```text
namespace: instacart-bigdata

├── kafka
├── spark
├── mongodb
├── api
└── dashboard
```

HDFS có thể chạy bằng containerized NameNode/DataNode hoặc được giữ trong môi trường Docker Compose nếu Kubernetes local quá nặng, nhưng phải mô tả rõ deployment thực tế khi demo.

Persistent Volume:

- Kafka;
- MongoDB;
- HDFS DataNode.

Mục tiêu không phải production-grade HA mà là chứng minh có deployment orchestration thực tế.

---

# 37. Non-Functional Requirements

| Metric | Cách đo |
|---|---|
| Kafka throughput | events/s |
| Streaming input rate | inputRowsPerSecond |
| Streaming processing rate | processedRowsPerSecond |
| Micro-batch duration | Spark progress |
| Streaming latency | processing/output time − ingestion time |
| Batch runtime | Spark job duration |
| Shuffle | Spark UI metrics |
| Data correctness | Spark output vs reference sample |
| Recommendation quality | Precision@K, Recall@K |

Không đặt SLA production phi thực tế.

---

# 38. Evaluation

## 38.1. Data Correctness

- kiểm tra row count;
- key uniqueness;
- weekday consistency;
- per-user timestamp monotonicity;
- Spark aggregation so với reference sample.

## 38.2. Batch Performance

- runtime;
- shuffle read/write;
- bytes scanned;
- physical plan.

## 38.3. Streaming

- throughput;
- processing rate;
- late-event handling;
- watermark behavior;
- checkpoint recovery.

## 38.4. Recommendation

- Precision@K;
- Recall@K;
- MAP@K/NDCG@K nếu đủ thời gian.

## 38.5. Serving

Kiểm tra:

- ALS recommendation tồn tại;
- trending thay đổi;
- FinalScore thay đổi;
- ranking cuối thay đổi theo trend.

---

# 39. Kịch bản Demo Cuối kỳ

Thời lượng dự kiến: **8–12 phút**.

```text
0:00–1:00
Giới thiệu problem + architecture.

1:00–2:00
Show HDFS raw/curated data.

2:00–3:00
Chạy Spark batch aggregation.

3:00–4:00
Show physical plan:
sort-merge vs broadcast.

4:00–5:00
Start Kafka replay producer.

5:00–6:00
Show Kafka events + Spark Structured Streaming.

6:00–7:00
Dashboard trending thay đổi.

7:00–8:00
Inject late event để demo watermark.

8:00–9:00
Nhập user_id, show ALS recommendation.

9:00–10:00
Tăng trend của một product,
show FinalScore/rank thay đổi.

10:00–11:00
Show performance benchmark.

11:00–12:00
Show kubectl get pods/services.
```

---

# 40. Timeline

Baseline đề xuất: 8 tuần.

| Tuần | Module 1 | Module 2 | Module 3 | Module 4 | Integration |
|---|---|---|---|---|---|
| 1 | profiling | batch design | stream design | ALS design | freeze contracts |
| 2 | schemas + HDFS | aggregation prototype | Kafka prototype | ALS pipeline | schema freeze |
| 3 | timestamp generator | joins | streaming parse | ALS baseline | end-to-end sample |
| 4 | Kafka producer | optimization | window/watermark | evaluation | MongoDB |
| 5 | finalize | benchmarks | late data | graph | API |
| 6 | support | outputs | outputs | serving/dashboard | full integration |
| 7 | fixes | experiment | experiment | experiment | Kubernetes |
| 8 | report | report | report | report | rehearsal/demo |

Dependency chính:

```text
Module 3 phụ thuộc event schema từ Module 1.
Module 4 serving phụ thuộc output schema từ Module 2 và Module 3.
```

---

# 41. Phân công nhóm

## Member 1 — Module 1

- dataset ingestion;
- schema validation;
- timestamp synthesis;
- HDFS layout;
- Kafka producer.

## Member 2 — Module 2

- batch analytics;
- joins;
- performance optimization;
- batch outputs.

## Member 3 — Module 3

- Structured Streaming;
- watermark;
- late event;
- realtime trending;
- streaming sink.

## Member 4 — Module 4

- ALS;
- graph;
- serving;
- dashboard.

Do Module 4 có khối lượng lớn, nhóm nên hỗ trợ chéo:

- Member 2 hỗ trợ graph preprocessing.
- Member 3 hỗ trợ realtime MongoDB contract.
- Member 1 hỗ trợ integration/dashboard data.

Ownership vẫn giữ nguyên theo 4 module.

---

# 42. Definition of Done

## Module 1 Done khi

- đọc thành công 6 file;
- explicit schema hoạt động;
- validation chạy;
- timestamp deterministic;
- weekday consistency đạt yêu cầu;
- timestamp monotonic theo user;
- Parquet ghi HDFS;
- Kafka producer replay được;
- event schema được freeze.

## Module 2 Done khi

- batch metrics chạy end-to-end;
- có window/pivot/custom aggregation;
- broadcast join được chứng minh;
- sort-merge join được chứng minh;
- có ít nhất 3 optimization experiments;
- output ghi đúng storage.

## Module 3 Done khi

- Kafka → Spark chạy;
- parse đúng schema;
- event-time window đúng;
- watermark hoạt động;
- late-event test thành công;
- trending tính được;
- checkpoint hoạt động;
- sink idempotent;
- throughput/latency đo được.

## Module 4 Done khi

- ALS train được;
- không data leakage;
- Precision@K/Recall@K tính được;
- graph chạy được;
- recommendation lưu MongoDB;
- FinalScore chạy được;
- cold-start có fallback;
- dashboard đọc đúng output.

---

# 43. Rủi ro và phương án xử lý

## 43.1. Kubernetes quá nặng

**Rủi ro:** laptop không đủ tài nguyên.

**Giải pháp:**

- development bằng Docker Compose;
- chỉ deploy các service chính lên Minikube ở final demo;
- không triển khai production HA.

## 43.2. Graph quá lớn

**Rủi ro:** pairwise product co-purchase có thể tạo rất nhiều edge.

**Giải pháp:**

- lọc edge weight tối thiểu;
- giới hạn theo top products;
- coi Connected Components là optional.

## 43.3. ALS tốn tài nguyên

**Giải pháp:**

- tune với subset trước;
- full training sau;
- cache interaction table;
- checkpoint model.

## 43.4. Streaming replay quá nhanh

**Giải pháp:**

- producer có configurable event rate;
- hỗ trợ accelerated replay;
- tách synthetic event_time khỏi ingestion_time.

## 43.5. Timestamp synthetic bị hiểu nhầm

**Giải pháp:**

Trong proposal, code, dashboard và demo luôn ghi rõ:

```text
Synthetic event time for replay simulation
```

không xem đây là timestamp thực của Instacart.

---

# 44. Tính khả thi

Dự án có phạm vi tương đối lớn nhưng khả thi với nhóm 4 người nếu:

- freeze data contract sớm;
- giữ GraphFrames ở mức vừa đủ;
- không mở rộng sang deep learning;
- không thêm công nghệ không cần thiết;
- không yêu cầu production deployment;
- tách development và final deployment.

Kiến trúc được chia theo module độc lập và có hợp đồng dữ liệu rõ ràng nên các thành viên có thể phát triển song song.

---

# 45. Kết quả đầu ra dự kiến

Sau khi hoàn thành, dự án sẽ có:

1. HDFS raw/curated data lake.
2. Synthetic timestamp generator.
3. Kafka replay producer.
4. Spark batch analytics pipeline.
5. Batch optimization report.
6. Structured Streaming pipeline.
7. Realtime trending collection.
8. ALS recommendation model.
9. Recommendation evaluation report.
10. Product co-purchase graph.
11. MongoDB serving database.
12. Final trend-aware recommendation.
13. API.
14. Dashboard.
15. Docker environment.
16. Kubernetes deployment demo.
17. Báo cáo thực nghiệm.
18. Video/demo hoặc slide trình bày.

---

# 46. Câu hỏi phản biện dự kiến

| Câu hỏi | Câu trả lời đề xuất |
|---|---|
| Tại sao đây là Big Data? | Dataset có hàng triệu orders và hàng chục triệu interactions, phù hợp distributed storage/processing. |
| Tại sao Lambda? | ALS cần lịch sử dài hạn; trending cần tín hiệu mới gần realtime. |
| Tại sao không Kappa? | Recompute ALS và historical analytics là workload batch tự nhiên. |
| Realtime data ở đâu? | Historical data được replay thành synthetic event stream qua Kafka. |
| Timestamp có thật không? | Không. Timestamp được sinh deterministic cho mục đích replay. |
| M1 → M2 bằng gì? | HDFS curated Parquet. |
| M1 → M3 bằng gì? | Kafka purchase event JSON. |
| M2/M3 → M4 bằng gì? | MongoDB batch metrics + realtime trending và HDFS features. |
| Recommendation + Trending kết hợp thế nào? | Normalize ALS và Trend rồi linear blend bằng lambda. |
| NoSQL dùng để làm gì? | Serving low-latency outputs cho API/dashboard. |
| HDFS khác MongoDB ở đâu? | HDFS là historical master/batch storage; MongoDB là serving database. |
| Exactly-once đến đâu? | Checkpoint + Kafka offsets + idempotent MongoDB upsert cho deterministic outputs. |
| GraphFrames tạo giá trị gì? | Phân tích sản phẩm có tính kết nối/trung tâm trong co-purchase network. |
| Kubernetes dùng thật không? | Các service chính được container hóa và deploy trong final demo. |
| Có quá nhiều công nghệ không? | Chỉ dùng những công nghệ gắn trực tiếp với yêu cầu môn học và kiến trúc. |
| Nhóm 4 người có làm được không? | Có nếu freeze contract sớm và giữ graph/deployment ở phạm vi vừa đủ. |
| Dataset không có giá thì phân tích doanh thu thế nào? | Không phân tích doanh thu. |
| Có click/view không? | Không. Streaming chỉ mô phỏng purchase event. |
| `train` có dùng train ALS không? | Không. `prior` dùng training/history, `train` dùng ground truth evaluation. |
| Demo chứng minh điều gì? | Batch, streaming, optimization, recommendation và sự thay đổi ranking khi trend thay đổi. |

---

# 47. Kết luận

Dự án xây dựng một hệ thống Big Data hoàn chỉnh dựa trên Instacart Market Basket Analysis và kiến trúc Lambda. Hệ thống tách rõ:

```text
Historical Preference
→ Batch Layer

Recent Purchase Trend
→ Speed Layer

ALS + Trend
→ Serving Layer
```

Cách thiết kế này vừa phù hợp với bản chất bài toán Recommendation + Trending, vừa cho phép nhóm chứng minh đầy đủ các nội dung chính của học phần:

- distributed storage;
- Spark batch processing;
- advanced aggregation;
- transformation;
- join;
- optimization;
- streaming;
- machine learning;
- graph processing;
- NoSQL;
- deployment.

Điểm quan trọng nhất của proposal là **Data Contract** giữa các module. Mỗi module đều có input, processing, output, storage và consumer xác định, giúp các thành viên có thể triển khai độc lập nhưng vẫn ghép nối thành một pipeline end-to-end thống nhất.

---

# 48. Tài liệu tham khảo

1. Apache Spark Documentation.
2. Apache Kafka Documentation.
3. Apache Hadoop HDFS Documentation.
4. Spark MLlib ALS Documentation.
5. GraphFrames Documentation.
6. MongoDB Documentation.
7. Kubernetes Documentation.
8. Instacart Market Basket Analysis Dataset, Kaggle.
9. Tài liệu học phần IT4931 — Lưu trữ và Xử lý Dữ liệu lớn.
