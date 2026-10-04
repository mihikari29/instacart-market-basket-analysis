# Kế hoạch triển khai và đánh giá Module 4A–4B

## 1. Phạm vi

Tài liệu này chỉ bao gồm hai phần:

- **Module 4A — ALS Recommendation:** chuẩn bị implicit interactions, huấn luyện
  Spark MLlib ALS, tuning, đánh giá và xuất `user_recommendations`.
- **Module 4B — Product Graph:** dựng đồ thị đồng mua, tính neighbor count,
  weighted degree, PageRank và xuất `product_graph_metrics`.

Serving Layer kết hợp ALS–Trend, FastAPI, dashboard và Kubernetes không thuộc
phạm vi kế hoạch này.

## 2. Mục tiêu nghiệm thu

Module 4A–4B được xem là hoàn thành khi:

1. ALS chỉ học từ `order_products_prior`; `order_products_train` không tham gia
   huấn luyện hoặc tuning.
2. Precision@K và Recall@K được tính trên ground truth `train` và có baseline để
   đối chiếu.
3. Mọi trial ALS có cấu hình, seed, runtime và metric truy xuất được.
4. Co-purchase graph được dựng từ `prior`, không có self-loop hoặc cạnh trùng.
5. Weighted degree và PageRank chạy thành công; Connected Components là tùy chọn.
6. `user_recommendations` và `product_graph_metrics` được ghi vào MongoDB với
   version/index rõ ràng.
7. Có test trên fixture và một lần chạy dữ liệu đầy đủ với evidence cùng `run_id`.

## 3. Hợp đồng dữ liệu

### 3.1. Input

| Dataset | Vị trí | Vai trò |
|---|---|---|
| `orders` | HDFS `curated/orders` | Gắn `user_id`, `order_number`, `eval_set` vào facts |
| `prior` | HDFS `curated/order_products_prior` | Train ALS và dựng graph |
| `train` | HDFS `curated/order_products_train` | Ground truth cuối cùng |
| `products` | HDFS `curated/dimensions/products` | Kiểm tra khóa và bổ sung product metadata |
| `user_features` | HDFS `features/user_features` | Phân tích cohort; không phải input bắt buộc của ALS |

`prior` và `train` không chứa `user_id`, nên Module 4 phải join với `orders` bằng
`order_id`. Đề cương hiện ghi `orders_curated` chỉ phục vụ Module 2; data contract
cần cập nhật Module 4 là consumer.

Theo full-data evidence hiện có, mốc đối soát dự kiến là 32.434.489 prior facts và
1.384.617 train facts. Code không được hard-code các số này; mỗi run phải tính lại
và ghi vào validation report.

### 3.2. Output

`user_recommendations`:

```text
_id
user_id
product_id
als_score
als_rank
model_version
generated_at
run_id
```

`product_graph_metrics`:

```text
_id
product_id
neighbor_count
weighted_degree
pagerank
component_id          # optional
graph_version
min_edge_weight
computed_at
run_id
```

## 4. Cấu trúc mã đề xuất

```text
src/module4/
├── __init__.py
├── config.py          # HDFS/Mongo paths, seed, K, ALS grid, graph threshold
├── data.py            # đọc và validate input contracts
├── split.py           # internal validation split không leakage
├── als.py             # interactions, fit, recommend, save/load
├── evaluation.py      # baselines, Precision/Recall/MAP/NDCG, coverage
├── graph.py           # baskets, edges, weighted degree, PageRank
├── sink_mongo.py      # versioned snapshot và indexes
├── spark.py           # Spark session/event log/checkpoint
└── cli.py             # validate, als, graph, all

scripts/module4.py
tests/test_module4.py
tests/test_module4_spark.py
docs/evidence/module4/<run_id>/
```

Docker image phải pin GraphFrames Python package và JAR tương thích Spark
3.5/Scala 2.12. Worker không nên tải dependency từ Maven trong lúc demo.

## 5. Module 4A — ALS Recommendation

### 5.1. Validation đầu vào

Trước khi train, job phải fail nếu có một trong các lỗi sau:

- thiếu cột bắt buộc hoặc null ở `order_id`, `user_id`, `product_id`;
- prior fact join vào order không có `eval_set='prior'`;
- train fact join vào order không có `eval_set='train'`;
- khóa `(order_id, product_id)` trùng;
- order hoặc product không tồn tại trong dimension tương ứng;
- row count thay đổi bất hợp lý sau join;
- train rows xuất hiện trong tập dùng để fit hoặc tuning.

Report cần có số order, user, product, interaction và phân phối purchase count.

### 5.2. Tạo implicit interactions

Từ prior facts:

```text
(user_id, product_id) -> purchase_count
```

Thử hai dạng strength:

```text
raw: strength = purchase_count
log: strength = 1 + log(1 + purchase_count)
```

ALS cấu hình nền:

```text
implicitPrefs = true
userCol       = user_id
itemCol       = product_id
ratingCol     = strength
coldStartStrategy = drop
seed          = cố định và ghi vào report
```

Input ALS phải được tạo xác định; tránh random sampling sau repartition mà không
sort/checkpoint vì có thể làm kết quả hoặc quá trình fit không ổn định.

### 5.3. Split để tuning không leakage

Không được dùng official `train` để chọn hyperparameter. Tạo validation nội bộ
từ `prior`:

1. Chọn user có ít nhất hai prior orders.
2. Với mỗi user, lấy prior order có `order_number` lớn nhất làm validation truth.
3. Các prior orders trước đó tạo training interactions nội bộ.
4. Kiểm tra tập `order_id` hai phía disjoint.
5. Báo cáo user bị loại do chỉ có một prior order, product cold-start và coverage.

Sau khi chọn cấu hình, huấn luyện lại model trên toàn bộ prior rồi mới đánh giá
một lần trên official train.

Đây là bài toán dự đoán giỏ hàng tiếp theo. Sản phẩm user từng mua là ứng viên hợp
lệ vì reorder là tín hiệu chính của Instacart; không lọc seen products khỏi Top-K.
Nên báo cáo thêm metric tách hai nhóm:

- reordered: product đã xuất hiện trong lịch sử user;
- new-to-user: product chưa từng xuất hiện trong lịch sử user.

### 5.4. Baseline bắt buộc

ALS phải được so với ít nhất hai baseline dùng cùng cohort và cùng K:

1. **Global popularity:** top product theo số lượt mua trong prior.
2. **Personal frequency:** top product user mua nhiều nhất; thiếu candidate thì
   bổ sung global popularity.

Không có baseline thì Precision/Recall của ALS thiếu ngữ cảnh để đánh giá.

### 5.5. Tuning có giới hạn tài nguyên

Không chạy toàn bộ tích Descartes trên full data. Quy trình đề xuất:

1. Baseline ALS: `rank=32`, `regParam=0.1`, `alpha=10`, `maxIter=10`.
2. Pilot tối đa 12 cấu hình trên sample user cố định bằng hash.
3. Miền thử nghiệm:
   - `rank={16,32,64}`;
   - `regParam={0.01,0.05,0.1}`;
   - `alpha={10,40}`;
   - `maxIter={5,10,15}`;
   - strength `raw` hoặc `log`.
4. Chọn tối đa ba cấu hình tốt nhất chạy internal validation full-size.
5. Chọn model bằng Recall@10; dùng Precision@10 rồi runtime làm tie-break.
6. Retrain cấu hình thắng trên toàn bộ prior và lưu model tại
   `/instacart/models/als/<model_version>`.

Mỗi trial phải lưu:

- params, seed, strength và input counts;
- số user/product factors;
- fit time, recommendation time và evaluation time;
- Precision/Recall và coverage;
- executor/shuffle configuration;
- lỗi nếu trial thất bại.

### 5.6. Đánh giá cuối

Ground truth của user là tập product duy nhất trong train order. Tính macro-average
trên user với `K={5,10,20}`:

- Precision@K;
- Recall@K;
- MAP@K hoặc NDCG@K nên có;
- user coverage;
- catalog coverage;
- số recommendation trung bình mỗi user;
- tỷ lệ user/product cold-start.

Precision dùng mẫu số K ngay cả khi hệ thống trả ít hơn K. Chỉ user có ground
truth không rỗng mới vào mẫu số; số user bị loại và lý do phải được công bố.

Đánh giá nên có hai cohort:

- **warm cohort:** user và ground-truth product có factor phù hợp;
- **all test users:** gồm fallback cho trường hợp cold-start, nếu triển khai.

Không đặt ngưỡng Precision/Recall tùy ý. Điều kiện nghiệm thu là metric được tính
đúng, cohort minh bạch và so sánh công bằng với baseline. Nếu ALS không vượt
baseline ở metric chính, không được kết luận ALS cải thiện recommendation.

### 5.7. Sinh và publish recommendation

Model cuối sinh Top-20 cho mọi warm user. Sau khi explode kết quả ALS, xếp hạng
lại xác định bằng:

```text
als_score DESC, product_id ASC
```

MongoDB indexes:

```text
unique(model_version, user_id, product_id)
unique(model_version, user_id, als_rank)
index(model_version, user_id)
```

Publish vào collection tạm rồi rename. Chạy lại cùng input/config/version phải
không tạo document hoặc rank trùng.

## 6. Module 4B — Product Co-purchase Graph

### 6.1. Tạo vertices và edges

Graph chỉ dùng prior:

1. Join prior với orders và giữ `order_id`, `product_id`.
2. Deduplicate product trong từng order.
3. Self-join theo `order_id` với `product_a < product_b`.
4. Group theo `(product_a, product_b)` và đếm `co_purchase_count`.
5. Lọc cạnh có weight nhỏ hơn `min_edge_weight`.

Canonical undirected edge:

```text
src = min(product_a, product_b)
dst = max(product_a, product_b)
weight = co_purchase_count
```

Không được cap basket hoặc sample ngẫu nhiên mà không ghi rõ. Nếu full graph vượt
tài nguyên, fallback được phép là giới hạn top-P products theo prior popularity,
nhưng report phải gọi đó là graph subset và công bố coverage.

### 6.2. Thí nghiệm threshold

Thử tối thiểu:

```text
min_edge_weight = 2, 5, 10
```

Mỗi mức ghi:

- vertex count và edge count;
- tỷ lệ products/facts được bao phủ;
- phân phối degree/edge weight;
- runtime và shuffle;
- số isolated vertices.

Chọn threshold cân bằng khả năng chạy và giữ ý nghĩa graph; không chọn chỉ dựa
trên việc một cấu hình chạy nhanh nhất.

### 6.3. Graph metrics

**Neighbor count** là số hàng xóm khác nhau của product.

**Weighted degree** là tổng `co_purchase_count` của mọi cạnh kề product.

**PageRank:** cạnh canonical phải được mở thành hai cạnh có hướng trước khi tạo
GraphFrame để kết quả không phụ thuộc quy ước product ID. Cấu hình đề xuất:

```text
resetProbability = 0.15
maxIter = 10
```

PageRank mặc định được diễn giải là centrality theo topology. Không tuyên bố đây
là weighted PageRank sử dụng `co_purchase_count` nếu chưa tự triển khai thuật toán
có trọng số.

**Connected Components** là tùy chọn. Nếu chạy, phải cấu hình checkpoint và chỉ
đưa vào báo cáo khi component structure có insight rõ ràng.

### 6.4. Invariants của graph

Job phải tự động xác minh:

- không có `src == dst`;
- mọi canonical edge có `src < dst`;
- `(src, dst)` là duy nhất;
- mọi edge weight là số nguyên dương;
- mọi vertex/edge product tồn tại trong products dimension;
- symmetric directed graph có đúng hai directed rows trên mỗi undirected edge;
- `sum(weighted_degree) = 2 * sum(undirected edge weight)`;
- neighbor count không âm và không vượt `vertex_count - 1`;
- PageRank không null, NaN hoặc Inf;
- mỗi product xuất hiện tối đa một lần trong graph output/version.

Các product có prior purchase nhưng không còn cạnh sau threshold cần được giữ với
`neighbor_count=0`, `weighted_degree=0`; PageRank null phải được xử lý và báo cáo
theo contract đã chọn.

### 6.5. Publish graph output

MongoDB indexes:

```text
unique(graph_version, product_id)
index(graph_version, pagerank)
index(graph_version, weighted_degree)
```

Publish bằng collection tạm rồi rename. Report phải lưu Top-20 theo PageRank,
weighted degree và neighbor count, kèm product name để kiểm tra thủ công.

## 7. Chiến lược kiểm thử

### 7.1. Unit/fixture tests

- interaction strength raw/log đúng;
- internal holdout lấy đúng last prior order;
- không có order leakage;
- Precision/Recall đúng trên ví dụ tính tay;
- baseline và tie-break xác định;
- edge canonicalization, weight và neighbor count đúng;
- weighted-degree conservation đúng;
- Mongo documents/index keys đúng.

Fixture nhỏ phải có:

- một user có nhiều prior orders và một train order;
- một user chỉ có một prior order;
- reordered và new-to-user products;
- một product cold-start trong test;
- một basket tạo nhiều co-purchase pairs;
- isolated vertex và PageRank tie.

### 7.2. Spark integration

- ALS fit/recommend trên fixture;
- cùng seed/config cho cùng ranking trên fixture;
- GraphFrames PageRank chạy trong Docker Spark image;
- output schema đúng và không có NaN/duplicate rank;
- Mongo publish lặp hội tụ về cùng snapshot.

### 7.3. Full-data acceptance

Một run hoàn chỉnh phải thực hiện:

```text
validate
-> internal split
-> baselines
-> ALS tuning
-> final ALS train
-> official train evaluation
-> user_recommendations publication
-> graph threshold experiment
-> final graph metrics
-> product_graph_metrics publication
```

## 8. Ma trận đánh giá/nghiệm thu

| Nhóm | Điều kiện đạt | Evidence |
|---|---|---|
| Input | Counts/keys/FK/eval_set hợp lệ | `data_validation.json` |
| Leakage | Internal split disjoint; official train không vào fit/tuning | `split_summary.json` |
| ALS training | Model train xong; params/seed/version và factor counts có đủ | `als_model.json` |
| Recommendation | P@K/R@K tại 5/10/20 trên cùng cohort với hai baseline | `evaluation.json` |
| Coverage | Warm/cold users/items và catalog coverage được công bố | `evaluation.json` |
| Tuning | Trial có params, metric, runtime; model chọn từ internal validation | `als_trials.json` |
| ALS output | Score hữu hạn; Top-K/rank/key không trùng; count đối soát | `mongo_validation.json` |
| Graph build | Không self-loop/duplicate; weights và keys hợp lệ | `graph_validation.json` |
| Graph algorithms | Weighted degree conservation đạt; PageRank hữu hạn | `graph_metrics.json` |
| Graph scope | Threshold/subset và coverage được ghi rõ | `graph_trials.json` |
| Graph output | Một document/product/version; indexes và counts đúng | `mongo_validation.json` |
| Reproducibility | Fixture cho kết quả xác định; source/config digest được lưu | tests, `manifest.json` |
| Performance | Runtime, resources, shuffle, graph/model size được ghi | `performance.json` |

## 9. Evidence bắt buộc

```text
docs/evidence/module4/<run_id>/
├── manifest.json
├── environment.txt
├── test-results.txt
├── data_validation.json
├── split_summary.json
├── als_trials.json
├── evaluation.json
├── als_model.json
├── graph_trials.json
├── graph_validation.json
├── graph_metrics.json
├── mongo_validation.json
├── performance.json
└── README.md
```

`manifest.json` phải chứa source digest, input receipt/digest nếu có, Spark/Java/
Python/GraphFrames versions, seed, run ID, model version và graph version. Không
trộn summary, plans hoặc metrics của các run khác nhau.

## 10. Kế hoạch thực hiện

| Giai đoạn | Công việc | Thời lượng | Exit gate |
|---|---|---:|---|
| 0 | Freeze contracts, config, GraphFrames dependency và fixture | 0.5–1 ngày | Docker GraphFrames smoke đạt |
| A1 | Input validation, ALS interactions, internal split | 1 ngày | Counts khớp, không leakage |
| A2 | Baselines, ALS baseline và metric pipeline | 1 ngày | P@K/R@K fixture và internal validation đạt |
| A3 | Tuning có ngân sách, final train và official evaluation | 1–1.5 ngày | Model/version/evaluation đầy đủ |
| A4 | Publish `user_recommendations` và Mongo validation | 0.5 ngày | Index/count/idempotency đạt |
| B1 | Co-purchase edges và threshold experiment | 1 ngày | Graph invariants đạt |
| B2 | Weighted degree, PageRank và publish | 0.5–1 ngày | Graph output/Mongo đạt |
| F | Full-data evidence, báo cáo và rehearsal | 1 ngày | Ma trận bắt buộc hoàn chỉnh |

Tổng thời gian hợp lý: khoảng **5–7 ngày làm việc cho một người**. Nếu chia việc,
một người phụ trách ALS/evaluation và một người phụ trách graph; cả hai dùng chung
data validation, versioning và evidence format.

## 11. Thứ tự ưu tiên khi thiếu thời gian

1. Data contract và chống leakage.
2. ALS + Precision/Recall + hai baseline.
3. Co-purchase graph + weighted degree + PageRank.
4. Mongo outputs và idempotency.
5. Full-data evidence cùng run ID.
6. MAP/NDCG và threshold analysis mở rộng.
7. Connected Components.

Không nên bỏ internal validation để chạy nhiều hyperparameter hơn. Không nên thêm
Connected Components trước khi các graph invariants và PageRank cơ bản đã đạt.

## 12. Phần đã triển khai sau khi hoàn thành nhiệm vụ này

Ngày cập nhật: **04/10/2026**.

Đã triển khai nền tảng đầu tiên và phần lõi tiếp theo của Module 4A–4B trong
`src/module4`:

- `config.py`: cấu hình root HDFS, seed, recommendation K và graph threshold;
- `data.py`: đọc bốn input chính, kiểm tra schema, khóa, domain, foreign key,
  `eval_set`, overlap prior/train và tạo interaction `purchase_count` với hai dạng
  strength `raw`/`log`;
- `split.py`: tạo internal holdout bằng prior order cuối của từng user đủ ít nhất
  hai prior orders, trả training facts, validation facts và ground truth;
- `__init__.py`: khởi tạo package Module 4.
- `als.py`: khai báo ALS implicit-feedback parameters, tạo estimator có seed,
  fit model, sinh recommendation Top-K với tie-break xác định và lưu model;
- `evaluation.py`: tính Precision@K/Recall@K, user/catalog coverage và tạo hai
  baseline global popularity/personal frequency;
- `graph.py`: tạo canonical co-purchase edges, vertices, cạnh hai chiều cho
  GraphFrames, weighted degree và kiểm tra invariant self-loop/duplicate/weight.

Đã kiểm tra syntax bằng `py_compile` và kiểm tra cấu hình/path bằng Python. Phần
còn lại là chạy ALS/GraphFrames trên Spark, tuning thực tế, PageRank, MongoDB
publication và full-data evidence; các bước này vẫn theo thứ tự A2 → A4 → B1 → B2.

## Tài liệu kỹ thuật đối chiếu

- [Spark 3.5.5 ALS API](https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/ml/recommendation/ALS.html)
- [Spark RankingMetrics](https://spark.apache.org/docs/3.5.6/api/python/reference/api/pyspark.mllib.evaluation.RankingMetrics.html)
- [GraphFrames installation](https://graphframes.io/02-quick-start/01-installation.html)
- [GraphFrames centrality metrics](https://graphframes.io/04-user-guide/03-centralities.html)
