# PROJECT PROPOSAL
## Course: Big Data Storage and Processing (IT4931)

# A Lambda-Based Big Data System for Purchase Behavior Analytics and Trend-Aware Product Recommendation Using Apache Kafka, Spark, HDFS, and NoSQL

---

# 1. Context and Problem

Product recommendations in e-commerce systems commonly depend on users' long-term historical behavior. This approach can produce stable recommendations that reflect individual preferences, but it has an important limitation: a historical model reacts slowly to short-term changes such as rapidly increasing product popularity or emerging purchase trends.

This project proposes an end-to-end Big Data system based on **Lambda Architecture** that combines two signals:

- **Long-term preference:** learned from purchase history with an ALS model in Spark MLlib.
- **Short-term trend:** calculated near real time from a purchase-event stream replayed through Kafka and processed by Spark Structured Streaming.

The two branches are combined in the Serving Layer to produce final recommendations that reflect both personal preferences and short-term trends.

The project uses the **Instacart Market Basket Analysis** dataset. This historical dataset does not contain an absolute timestamp for each purchase event. The project therefore creates **synthetic timestamps** deterministically and reproducibly to simulate an event stream. These timestamps are used only for replay and streaming and must not be interpreted as actual Instacart timestamps.

---

# 2. Project Objectives

The project aims to build a complete Big Data pipeline from ingestion through serving while demonstrating the technical topics required by the course.

Primary objectives:

1. Store and manage large-scale data in HDFS.
2. Design an ingestion pipeline from the source dataset to HDFS and Kafka.
3. Perform batch processing with Spark SQL/DataFrame operations, including:
   - complex aggregations;
   - advanced transformations;
   - window functions;
   - pivot/unpivot;
   - broadcast join;
   - sort-merge join.
4. Perform performance optimization and benchmarking, including:
   - partition pruning;
   - caching;
   - broadcast join;
   - bucketing where appropriate;
   - physical execution-plan analysis.
5. Build stream processing with Spark Structured Streaming, including:
   - event-time processing;
   - window aggregation;
   - watermark;
   - late data;
   - checkpoint;
   - idempotent sink.
6. Build an ALS recommendation model with Spark MLlib.
7. Evaluate recommendations with Precision@K and Recall@K.
8. Build a Product Co-purchase Graph with GraphFrames.
9. Design a Serving Layer that combines ALS recommendations with realtime trending.
10. Store query-serving results in NoSQL.
11. Build a dashboard for:
    - batch analytics;
    - realtime trending;
    - recommendation;
    - performance benchmarks.
12. Containerize and deploy the primary components with Docker/Kubernetes at a scope appropriate for a student project.

---

# 3. Project Scope

## 3.1. Must-have

The following components are required:

- HDFS raw/curated storage.
- Kafka producer and purchase-event topic.
- Spark batch pipeline.
- Spark Structured Streaming.
- Event-time window and watermark.
- Batch analytics.
- Performance experiments.
- ALS recommendation.
- Precision@K and Recall@K.
- MongoDB serving store.
- Trend-aware final ranking.
- Dashboard.
- Docker.
- Kubernetes demo.

## 3.2. Should-have

- Product Co-purchase Graph.
- Weighted degree.
- PageRank.
- REST API with FastAPI.
- Late-data demonstration.
- Kafka partition benchmark.
- MAP@K or NDCG@K.

## 3.3. Optional

- Connected Components.
- Bucketing benchmark if the environment supports it reliably.
- Cloud deployment.
- Advanced dashboard.
- Graph score incorporated into `FinalScore`.

---

# 4. Dataset

## 4.1. Selected dataset

**Instacart Market Basket Analysis**

Primary characteristics:

- approximately 3.4 million orders;
- approximately 206,000 users;
- more than 32 million product purchases;
- approximately 50,000 products.

The dataset is appropriate because:

- its scale is large enough for Spark optimization techniques to be meaningful;
- it has a clear fact/dimension structure;
- it contains repeated user-product interactions;
- the `reordered` field supports repeat-purchase analysis;
- it is suitable for collaborative filtering;
- it supports construction of a product co-purchase graph.

## 4.2. Source files

### `orders.csv`

| Field | Spark Type | Meaning |
|---|---|---|
| `order_id` | Long | Order identifier |
| `user_id` | Long | User identifier |
| `eval_set` | String | Prior/train/test set |
| `order_number` | Integer | User's order sequence number |
| `order_dow` | Integer | Day of week |
| `order_hour_of_day` | Integer | Hour when the order was placed |
| `days_since_prior_order` | Double | Interval since the previous order |

### `order_products__prior.csv`

| Field | Spark Type | Meaning |
|---|---|---|
| `order_id` | Long | Order identifier |
| `product_id` | Integer | Product identifier |
| `add_to_cart_order` | Integer | Position at which the item was added to the basket |
| `reordered` | Integer/Boolean | Whether the product had been purchased previously |

### `order_products__train.csv`

The schema matches `order_products__prior.csv`. This file is used as ground truth for recommendation-model evaluation.

### `products.csv`

| Field | Spark Type | Meaning |
|---|---|---|
| `product_id` | Integer | Product identifier |
| `product_name` | String | Product name |
| `aisle_id` | Integer | Aisle group |
| `department_id` | Integer | Department group |

### `aisles.csv`

| Field | Spark Type | Meaning |
|---|---|---|
| `aisle_id` | Integer | Aisle identifier |
| `aisle` | String | Aisle name |

### `departments.csv`

| Field | Spark Type | Meaning |
|---|---|---|
| `department_id` | Integer | Department identifier |
| `department` | String | Department name |

## 4.3. Data limitations

The dataset does not provide:

- absolute timestamps;
- product prices;
- revenue;
- normalized brands;
- click/view events.

The project therefore does not use metrics that require these fields unless the data is explicitly synthesized and identified as synthetic.

---

# 5. System Architecture

The project uses **Lambda Architecture** with three primary layers:

- **Batch Layer:** processes the complete history.
- **Speed Layer:** processes the event stream near real time.
- **Serving Layer:** combines the outputs of both branches.

## 5.1. Architecture diagram

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

# 6. Data Flow and Data Lineage

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

# 7. MODULE 1 — Ingestion & Storage

## 7.1. Input

Module 1 reads all six files:

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

## 7.3. Data quality checks

At minimum, validate that:

- `order_id` is unique in `orders`;
- `product_id` is unique in `products`;
- `order_dow ∈ [0,6]`;
- `order_hour_of_day ∈ [0,23]`;
- `add_to_cart_order` increases from 1 to N within each order;
- foreign keys between product, aisle, and department are valid;
- per-user order sequence is not reversed.

---

# 8. Synthetic Timestamp Generation

The dataset has no absolute timestamps, so the project must create synthetic timestamps.

## 8.1. Objectives

The method must be:

- deterministic;
- reproducible;
- order-preserving;
- consistent with `order_dow`;
- based on `order_hour_of_day`;
- capable of creating a distinct event time for each item;
- replayable through Kafka.

## 8.2. Anchor date

Use a seeded RNG (reproducible given the same SEED + batch_users):

```text
SCATTER_WEEKS = 13
SEED = 42

anchor_week(user)
= rng.integers(0, SCATTER_WEEKS)
```

```text
anchor_day = anchor_week * 7 + first_order_dow
```

The anchor date is selected so its weekday matches the first order's `order_dow`. Reproduction requires the same SEED and `batch_users`, which are recorded in `manifest.json`.

## 8.3. Order date

```text
order_date(i)
= anchor_date
+ cumulative_sum(days_since_prior_order)
```

## 8.4. Synthetic reconstruction for gaps capped at 30

For gaps capped at 30, where the actual interval may exceed 30 days, reconstruct the minimum synthetic gap in the range 30–36 days that matches the day of week:

```text
gap_synthetic
= 30 + ((next_dow - prev_dow - 2) mod 7)
```

This maintains consistency between synthetic calendar dates and the source weekday (`order_dow`).

## 8.5. Item-level event time

The first item receives a base time within `order_hour_of_day`. For subsequent items:

```text
event_time(k)
= base + Σ delta_i

delta_i ~ Exp(mean ≈ 30s)
delta_i clipped to [5s,120s]
```

## 8.6. Monotonicity guard

```text
event_time(next)
= max(
    generated_time,
    previous_user_last_event + 1 second
)
```

This prevents reversal of per-user chronology.

---

# 9. Kafka Design

## 9.1. Topic

```text
instacart-purchase-events
```

## 9.2. Kafka key

```text
user_id
```

Rationale:

- preserve ordering for all events from one user;
- conform to the chronology invariant;
- provide stable user-based partitioning.

## 9.3. Serialization

```text
UTF-8 JSON
```

## 9.4. Kafka event schema

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

### Field definitions

| Field | Meaning |
|---|---|
| `event_id` | Unique item-event identifier |
| `order_id` | Source order |
| `user_id` | User |
| `product_id` | Product |
| `add_to_cart_order` | Item position in the basket |
| `reordered` | Whether the product was purchased previously |
| `aisle_id` | Aisle |
| `department_id` | Department |
| `order_dow` | Source weekday |
| `order_hour_of_day` | Source hour |
| `event_time_epoch_ms` | Synthetic event time |
| `event_time_iso` | Synthetic event time in ISO format |
| `ingestion_time_epoch_ms` | Time when the Kafka producer published the event |

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

Synthetic day/month partitioning supports partition pruning and historical filtering.

---

# 11. Module 1 Outputs

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

---

# 12. MODULE 2 — Batch Layer

## 12.1. Input

```text
orders_curated
order_products_prior_curated
curated_interactions
product_dimension
aisles
departments
```

## 12.2. Complex aggregations

### Window functions

Example:

```text
purchase_count(product, day)
rolling_7day_purchase_count
```

### Pivot

Example:

```text
department × order_hour_of_day
```

### Custom aggregation

Calculate:

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

Example pipeline:

```text
interaction
→ time feature derivation
→ product metadata enrichment
→ user-product frequency
→ reorder feature
→ user/product aggregate features
```

If a UDF is applied to `product_name`, it is treated only as text feature engineering and not as official brand extraction.

---

# 14. Join Operations

## 14.1. Broadcast Join

```text
curated_interactions
JOIN broadcast(aisles)
JOIN broadcast(departments)
```

The dimensions are smaller than the fact table and are therefore suitable for broadcast.

Metrics:

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

The two large tables naturally produce a sort-merge join case.

Automatic broadcast can be disabled for benchmarking:

```text
spark.sql.autoBroadcastJoinThreshold = -1
```

Inspect the physical plan with:

```text
df.explain("formatted")
```

---

# 15. Performance Optimization

## 15.1. Experiment 1 — Broadcast vs. Sort-Merge

- Baseline: Sort-Merge Join
- Optimization: Broadcast small dimension
- Metrics:
  - execution time;
  - shuffle bytes;
  - physical plan.

## 15.2. Experiment 2 — Partition Pruning

- Baseline: scan all interactions.
- Optimization: filter by synthetic date partition.
- Metrics:
  - files scanned;
  - bytes read;
  - runtime.

## 15.3. Experiment 3 — Caching

- Baseline: recompute a repeatedly used DataFrame.
- Optimization: persist/cache.
- Metrics:
  - first execution;
  - repeated execution.

## 15.4. Experiment 4 — Streaming Throughput

- Input: Kafka event stream.
- Baseline: low producer rate.
- Experiment: increase the rate.
- Metrics:
  - inputRowsPerSecond;
  - processedRowsPerSecond;
  - trigger duration.

## 15.5. Experiment 5 — Kafka Partitions

- 1, 2, 4, 8 partitions.
- Metrics:
  - throughput;
  - consumer parallelism;
  - processing latency.

## 15.6. Experiment 6 — Spark Executor Configuration

Compare multiple executor configurations.

Record concrete results only after experimentation.

---

# 16. Module 2 Outputs

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

# 17. MODULE 3 — Speed Layer / Structured Streaming

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
→ schema + domain validation / quality metrics
→ event-time watermark
→ event_id deduplication within watermark
→ one 120-minute sliding aggregation (C30 + C120)
→ append-mode finalized window
→ foreachBatch full-population normalization + deterministic ranking
→ configurable Top-K
→ replace finalized MongoDB snapshot + idempotent upsert
```

Ranking is calculated only after a window is finalized. The design does not publish an open-window leaderboard at each processing trigger because normalization requires the complete product population for the same `window_end`.

---

# 18. Trending Definition

The project contains purchase events only; it does not use view or click events.

Proposed Trending Score:

```math
T(p,t)
=
0.7 × N(C_30m(p,t))
+
0.3 × N(C_120m(p,t))
```

Where:

- `C_30m`: number of purchase events for the product in the most recent 30 minutes;
- `C_120m`: number of purchase events in 120 minutes;
- `N(.)`: normalization within the same window.

Interpretation:

- 30 minutes provides fast response;
- 120 minutes smooths noise;
- the weights are a baseline and may be adjusted experimentally.

---

# 19. Window and Watermark

Baseline:

```text
Recent window: 30 minutes
Long window:   120 minutes
Slide:         5 minutes
Watermark:     10 minutes
```

These values are initial configurations, not empirically proven optima.

Event time uses synthetic event time.

---

# 20. Late Data

Demonstration scenario:

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

# 21. Exactly-Once and Idempotency

The project does not claim that the external sink is always exactly-once.

Spark uses:

- Kafka offsets;
- checkpoints;
- state metadata.

The MongoDB sink uses the deterministic key:

```text
(window_end, product_id)
```

and:

```text
upsert = true
```

If a micro-batch is retried, the same logical result overwrites the same document instead of creating a duplicate. Before upsert, the complete snapshot for every finalized `window_end` in the batch is replaced so stale non-Top-K rows and old ranks do not remain. Top-K is applied only after normalization and ranking over the full population. The default TTL is seven days and can be disabled explicitly with `ttl_seconds = 0`.

Replacement converges correctly and is logically idempotent under Spark retries, but delete plus upsert is not transactionally atomic for concurrent readers. A dashboard may briefly observe an empty or partial snapshot during replacement. A production design could use immutable snapshot/version IDs, an active-version pointer, or a MongoDB transaction where appropriate; the current project accepts this limitation. The Kafka source defaults to `failOnDataLoss=true`; permission to skip unavailable offsets must be an explicit opt-in.

The project's guarantee is described as:

**deterministic, replay-convergent aggregated output with an idempotent sink design; not transactionally exactly-once serving for concurrent readers.**

---

# 22. Module 3 Output

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

- Use `order_products__prior` for training/history.
- Use `order_products__train` as ground truth.
- Do not use the train set to build the interaction training matrix.

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

An alternative feature/hyperparameter experiment may use:

```math
r_ui = 1 + log(1 + purchaseCount(u,i))
```

## 23.4. Model

Spark MLlib ALS:

```text
implicitPrefs = true
```

Tune the following hyperparameters:

```text
rank
regParam
alpha
maxIter
```

Do not specify an optimum before experimentation.

---

# 24. Recommendation Evaluation

For user `u`:

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

MAP@K or NDCG@K may be added if time permits.

---

# 25. ALS Output

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

Two products appear in the same order.

### Edge weight

```text
co_purchase_count(product_a, product_b)
```

## 26.2. Graph algorithms

### Weighted Degree

Insights:

- which products frequently co-occur with many other products;
- degree of connectivity within baskets.

### PageRank

Insight:

- which products are central in the co-purchase network.

### Connected Components

Optional.

Use it only if the experiment produces meaningful insight.

---

# 27. Graph Output

## `product_graph_metrics`

```text
product_id
neighbor_count
weighted_degree
pagerank
component_id
computed_at
```

`component_id` is optional.

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

`lambda` is configurable.

Example benchmark values:

```text
0.5
0.7
0.9
```

Do not treat any value as optimal before experimentation.

## 28.4. Cold Start

### New user

When no ALS score exists:

```math
FinalScore = TrendScore
```

### New product

When an ALS score is unavailable but a trend score exists:

```text
NormalizedALS = 0
```

The product can still appear in the final ranking.

---

# 29. Serving Output

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

The dashboard displays only outputs that exist in the pipeline.

## 30.1. Realtime

- Top trending products.
- Trend score by window.
- Kafka/Spark throughput.
- Streaming processing latency.

## 30.2. Batch

- Top products.
- Reorder rate.
- Department statistics.
- Purchase distribution by hour/day.

## 30.3. ML

- Precision@K.
- Recall@K.
- ALS recommendations for one user.

## 30.4. Serving

- ALS score.
- Trend score.
- Final score.
- Ranking before and after a trend change.

## 30.5. Performance

- baseline runtime;
- optimized runtime;
- shuffle metrics;
- partition pruning.

---

# 31. NoSQL Design

The project uses **MongoDB**.

Rationale:

- simple access patterns;
- a document model suitable for recommendations and trending data;
- straightforward upserts;
- compatibility with dashboards and APIs;
- simpler student-project deployment than Cassandra.

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
purchase_count_30m
purchase_count_120m
trend_score
trend_rank
updated_at
```

Indexes:

```text
(window_end, trend_rank)
(window_end, product_id)
updated_at TTL (default: 7 days)
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

# 32. System-wide Data Contract

| Producer Module | Output Dataset/Event | Primary Schema | Format | Storage/Topic | Consumer |
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

# 33. Input–Processing–Output by Module

| Module | Input | Processing | Output | Output Consumer |
|---|---|---|---|---|
| M1 | 6 Instacart CSV files | validation, cleaning, timestamp synthesis, event generation | HDFS curated tables + Kafka events | M2, M3, M4 |
| M2 | HDFS curated tables | batch aggregation, transformation, joins, optimization | batch metrics + user features | M4, Dashboard |
| M3 | Kafka purchase events | event-time streaming, watermark, windows, trending | realtime_trending | M4, Dashboard |
| M4 | history + train ground truth + batch metrics + trend | ALS, graph, serving, visualization | user recommendations, graph metrics, final ranking | API/Dashboard |

---

# 34. Six Spark Requirement Categories

| Course Requirement | Project Location | Example | Module |
|---|---|---|---|
| Complex Aggregations | Spark Batch | rolling metrics, pivot, reorder rate | M2 |
| Advanced Transformations | Batch ETL | multi-stage feature pipeline | M2 |
| Join Operations | Spark SQL | broadcast + sort-merge | M2 |
| Performance Optimization | Experiment | pruning, cache, join plan | M2 |
| Streaming Processing | Structured Streaming | watermark, window, late data | M3 |
| Advanced Analytics | MLlib + GraphFrames | ALS + co-purchase graph | M4 |

---

# 35. Technologies

| Technology | Role | Input | Output | Rationale |
|---|---|---|---|---|
| HDFS | distributed storage | CSV/Parquet | historical datasets | large-scale data storage |
| Kafka | message queue | synthetic events | event stream | replay and decoupling |
| Spark SQL | batch processing | HDFS | metrics/features | distributed processing |
| Structured Streaming | Speed Layer | Kafka | trending | event-time streaming |
| Spark MLlib | machine learning | user-product interactions | ALS recommendations | native distributed ML |
| GraphFrames | graph analytics | co-purchase edges | graph metrics | graph processing on Spark |
| MongoDB | serving store | processed outputs | dashboard/API queries | straightforward upserts and deployment |
| FastAPI | API | MongoDB | HTTP JSON | serving |
| Streamlit | dashboard | MongoDB/API | charts | rapid deployment |
| Docker | packaging | services | images | reproducibility |
| Kubernetes | orchestration | containers | deployed services | course deployment requirement |

---

# 36. Deployment

## 36.1. Development

Use Docker Compose for:

```text
Kafka
Spark
HDFS
MongoDB
API
Dashboard
```

## 36.2. Final Demonstration

Use a local Kubernetes cluster such as Minikube.

Example:

```text
namespace: instacart-bigdata

├── kafka
├── spark
├── mongodb
├── api
└── dashboard
```

HDFS may run with containerized NameNode/DataNode services or remain in Docker Compose if local Kubernetes is too resource-intensive. The actual deployment must be documented clearly during the demonstration.

Persistent volumes:

- Kafka;
- MongoDB;
- HDFS DataNode.

The objective is to demonstrate real deployment orchestration, not production-grade high availability.

---

# 37. Non-Functional Requirements

| Metric | Measurement Method |
|---|---|
| Kafka throughput | events/s |
| Streaming input rate | inputRowsPerSecond |
| Streaming processing rate | processedRowsPerSecond |
| Micro-batch duration | Spark progress |
| Streaming latency | processing/output time − ingestion time |
| Batch runtime | Spark job duration |
| Shuffle | Spark UI metrics |
| Data correctness | Spark output vs. reference sample |
| Recommendation quality | Precision@K, Recall@K |

Do not specify an unrealistic production SLA.

---

# 38. Evaluation

## 38.1. Data Correctness

- row-count checks;
- key uniqueness;
- weekday consistency;
- per-user timestamp monotonicity;
- Spark aggregation compared with a reference sample.

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
- MAP@K/NDCG@K if time permits.

## 38.5. Serving

Verify that:

- ALS recommendations exist;
- trending changes;
- `FinalScore` changes;
- final ranking responds to the trend.

---

# 39. Final Demonstration Scenario

Estimated duration: **8–12 minutes**.

```text
0:00–1:00
Introduce the problem and architecture.

1:00–2:00
Show HDFS raw/curated data.

2:00–3:00
Run Spark batch aggregation.

3:00–4:00
Show the physical plan:
sort-merge vs. broadcast.

4:00–5:00
Start the Kafka replay producer.

5:00–6:00
Show Kafka events and Spark Structured Streaming.

6:00–7:00
Show the dashboard responding to a trend change.

7:00–8:00
Inject a late event to demonstrate watermark behavior.

8:00–9:00
Enter user_id and show ALS recommendations.

9:00–10:00
Increase the trend for one product and show the resulting FinalScore/rank change.

10:00–11:00
Show the performance benchmark.

11:00–12:00
Show kubectl get pods/services.
```

---

# 40. Timeline

Proposed baseline: eight weeks.

| Week | Module 1 | Module 2 | Module 3 | Module 4 | Integration |
|---|---|---|---|---|---|
| 1 | profiling | batch design | stream design | ALS design | freeze contracts |
| 2 | schemas + HDFS | aggregation prototype | Kafka prototype | ALS pipeline | schema freeze |
| 3 | timestamp generator | joins | streaming parse | ALS baseline | end-to-end sample |
| 4 | Kafka producer | optimization | window/watermark | evaluation | MongoDB |
| 5 | finalize | benchmarks | late data | graph | API |
| 6 | support | outputs | outputs | serving/dashboard | full integration |
| 7 | fixes | experiment | experiment | experiment | Kubernetes |
| 8 | report | report | report | report | rehearsal/demo |

Primary dependencies:

```text
Module 3 depends on the event schema from Module 1.
Module 4 serving depends on the output schemas from Modules 2 and 3.
```

---

# 41. Team Responsibilities

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
- late events;
- realtime trending;
- streaming sink.

## Member 4 — Module 4

- ALS;
- graph;
- serving;
- dashboard.

Because Module 4 has a large workload, the team should provide cross-module support:

- Member 2 supports graph preprocessing.
- Member 3 supports the realtime MongoDB contract.
- Member 1 supports integration and dashboard data.

Ownership remains aligned with the four modules.

---

# 42. Definition of Done

## Module 1 is complete when

- all six files are read successfully;
- explicit schemas work;
- validation runs;
- timestamps are deterministic;
- weekday consistency meets requirements;
- timestamps are monotonic per user;
- Parquet is written to HDFS;
- the Kafka producer can replay events;
- the event schema is frozen.

## Module 2 is complete when

- batch metrics run end to end;
- window, pivot, and custom aggregations exist;
- Broadcast Hash Join is demonstrated;
- Sort-Merge Join is demonstrated;
- at least three optimization experiments exist;
- outputs are written to the correct storage.

## Module 3 is complete when

- Kafka → Spark operates correctly;
- the schema parses correctly;
- event-time windows are correct;
- watermark behavior works;
- the late-event test passes;
- trending is calculated;
- checkpoint recovery works;
- the sink is idempotent;
- throughput and latency are measured.

## Module 4 is complete when

- ALS training completes;
- there is no data leakage;
- Precision@K/Recall@K are calculated;
- the graph pipeline runs;
- recommendations are stored in MongoDB;
- `FinalScore` is calculated;
- cold start has a fallback;
- the dashboard reads the correct outputs.

---

# 43. Risks and Mitigation

## 43.1. Kubernetes is too resource-intensive

**Risk:** development laptops lack sufficient resources.

**Mitigation:**

- use Docker Compose for development;
- deploy only the primary services to Minikube for the final demonstration;
- do not implement production high availability.

## 43.2. Graph is too large

**Risk:** pairwise product co-purchases may create a very large number of edges.

**Mitigation:**

- apply a minimum edge-weight threshold;
- limit the graph to top products;
- keep Connected Components optional.

## 43.3. ALS is resource-intensive

**Mitigation:**

- tune on a subset first;
- perform full training afterward;
- cache the interaction table;
- checkpoint the model.

## 43.4. Streaming replay is too fast

**Mitigation:**

- provide a configurable producer event rate;
- support accelerated replay;
- separate synthetic `event_time` from `ingestion_time`.

## 43.5. Synthetic timestamps are misinterpreted

**Mitigation:**

Always state the following in the proposal, code, dashboard, and demonstration:

```text
Synthetic event time for replay simulation
```

Do not represent it as an actual Instacart timestamp.

---

# 44. Feasibility

The project has a relatively large scope but is feasible for a four-person team if it:

- freezes the data contract early;
- keeps GraphFrames at an appropriate scope;
- does not expand into deep learning;
- does not add unnecessary technologies;
- does not require production deployment;
- separates development from final deployment.

The architecture is divided into independent modules with clear data contracts, allowing team members to develop in parallel.

---

# 45. Expected Deliverables

At completion, the project will provide:

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
12. Final trend-aware recommendations.
13. API.
14. Dashboard.
15. Docker environment.
16. Kubernetes deployment demonstration.
17. Experimental report.
18. Video/demo or presentation slides.

---

# 46. Expected Defense Questions

| Question | Proposed Answer |
|---|---|
| Why is this Big Data? | The dataset contains millions of orders and tens of millions of interactions, making it suitable for distributed storage and processing. |
| Why Lambda architecture? | ALS requires long-term history, while trending requires new near-real-time signals. |
| Why not Kappa architecture? | Recomputing ALS and historical analytics is naturally a batch workload. |
| Where does realtime data come from? | Historical data is replayed as a synthetic event stream through Kafka. |
| Are the timestamps real? | No. They are generated deterministically for replay purposes. |
| How does M1 feed M2? | HDFS curated Parquet. |
| How does M1 feed M3? | Kafka purchase-event JSON. |
| How do M2/M3 feed M4? | MongoDB batch metrics and realtime trending, plus HDFS features. |
| How are recommendation and trending combined? | Normalize ALS and Trend scores, then apply a linear blend using lambda. |
| What is NoSQL used for? | Low-latency serving of outputs to the API/dashboard. |
| How do HDFS and MongoDB differ? | HDFS is the historical master/batch store; MongoDB is the serving database. |
| What exactly-once guarantee is provided? | Checkpoints, Kafka offsets, and idempotent MongoDB upserts provide deterministic outputs. |
| What value does GraphFrames provide? | It identifies products with connectivity or centrality in the co-purchase network. |
| Is Kubernetes actually used? | The primary services are containerized and deployed for the final demonstration. |
| Are too many technologies being used? | Only technologies directly tied to course requirements and the architecture are included. |
| Can a four-person team complete it? | Yes, if contracts are frozen early and graph/deployment scope remains controlled. |
| How is revenue analyzed without price data? | Revenue is not analyzed. |
| Are click/view events available? | No. Streaming simulates purchase events only. |
| Is `train` used to train ALS? | No. `prior` is used for training/history, and `train` is used as evaluation ground truth. |
| What does the demonstration verify? | Batch processing, streaming, optimization, recommendation, and ranking changes in response to trends. |

---

# 47. Conclusion

The project builds a complete Big Data system using the Instacart Market Basket Analysis dataset and Lambda Architecture. The system clearly separates:

```text
Historical Preference
→ Batch Layer

Recent Purchase Trend
→ Speed Layer

ALS + Trend
→ Serving Layer
```

This design fits the combined recommendation-and-trending problem and allows the team to demonstrate the primary course topics:

- distributed storage;
- Spark batch processing;
- advanced aggregation;
- transformation;
- joins;
- optimization;
- streaming;
- machine learning;
- graph processing;
- NoSQL;
- deployment.

The proposal's most important element is the **Data Contract** between modules. Each module has defined inputs, processing, outputs, storage, and consumers, allowing team members to implement independently while integrating into a consistent end-to-end pipeline.

---

# 48. References

1. Apache Spark Documentation.
2. Apache Kafka Documentation.
3. Apache Hadoop HDFS Documentation.
4. Spark MLlib ALS Documentation.
5. GraphFrames Documentation.
6. MongoDB Documentation.
7. Kubernetes Documentation.
8. Instacart Market Basket Analysis Dataset, Kaggle.
9. IT4931 course materials — Big Data Storage and Processing.

## 11.4. `product_dimension`

- Format: Parquet
- Storage: HDFS
- Consumer: Module 2, Module 4

## 11.5. `curated_interactions`

Primary schema:

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
