# Kế hoạch kỹ thuật và nghiệm thu Module 4A–4D

Mốc nghiệm thu Module 4A–4B lịch sử: **10/10/2026** (Asia/Bangkok). Bản hợp nhất Module 4A–4D cập nhật **15/10/2026**.

Ngày 10/10, historical full-data A/B acceptance passed **21/21 gates**. The recorded full-ab-20261010-fixed-first and full-ab-20261010-fixed-replay runs passed with the same model and graph versions; their evidence retains the run IDs and source revisions. This is dated evidence, not fresh post-refactor full-data verification.

ALS did not outperform personal frequency. The search tested one bounded baseline configuration, not a global optimum. This acceptance does not establish Hybrid quality, dashboard production readiness, or production scalability. See the [dated acceptance record](evidence/module4/acceptance-20261010/README.md), the [historical implementation archive](evidence/module4/ab-implementation-history.md), and [current progress and limitations](progress.md). Operational instructions are maintained in [docker.md](docker.md); the evidence index is [here](evidence/README.md).

## Mục lục

- [1. Phạm vi](#1-phạm-vi)
- [2. Mục tiêu nghiệm thu](#2-mục-tiêu-nghiệm-thu)
- [3. Hợp đồng dữ liệu](#3-hợp-đồng-dữ-liệu)
- [4. Cấu trúc mã đề xuất](#4-cấu-trúc-mã-đề-xuất)
- [5. Module 4A — ALS Recommendation](#module4a-als-recommendation)
- [6. Module 4B — Product Co-purchase Graph](#module4b-product-co-purchase-graph)
- [7. Chiến lược kiểm thử](#7-chiến-lược-kiểm-thử)
- [8. Ma trận đánh giá/nghiệm thu](#8-ma-trận-đánh-giánghiệm-thu)
- [9. Evidence bắt buộc](#9-evidence-bắt-buộc)
- [10. Kế hoạch thực hiện](#10-kế-hoạch-thực-hiện)
- [11. Thứ tự ưu tiên khi thiếu thời gian](#11-thứ-tự-ưu-tiên-khi-thiếu-thời-gian)
- [12. Kết quả kiểm tra hiện tại](#12-kết-quả-kiểm-tra-hiện-tại)
- [13. Backlog thực hiện](#13-backlog-thực-hiện-và-thứ-tự-phụ-thuộc)
- [14. Chi tiết công việc](#14-chi-tiết-công-việc-theo-thành-phần)
- [15. Ma trận test](#15-ma-trận-test-bắt-buộc-để-đóng-backlog)
- [16. Chạy nghiệm thu](#16-quy-trình-chạy-nghiệm-thu-sau-khi-sửa-code)
- [17. Checklist Module 4A–4B](#17-checklist-đóng-module-4a4b)
- [18. Quy tắc cập nhật tiến độ](#18-quy-tắc-cập-nhật-tiến-độ-và-bàn-giao)
- [19. Module 4C — Hybrid recommendation and serving](#module4c-spec)
- [20. Module 4D — Read API and dashboard specification](#module4d-spec)

## 1. Phạm vi

Phần A/B của tài liệu giữ nguyên phạm vi và tiêu chí ban đầu:

- **Module 4A — ALS Recommendation:** chuẩn bị implicit interactions, huấn luyện
  Spark MLlib ALS, tuning, đánh giá và xuất `user_recommendations`.
- **Module 4B — Product Graph:** dựng đồ thị đồng mua, tính neighbor count,
  weighted degree, PageRank và xuất `product_graph_metrics`.

Mục 19–20 mở rộng tài liệu canonical này để mô tả các hợp đồng đã triển khai
cho:

- **Module 4C — Hybrid recommendation and serving:** kết hợp snapshot A/B đã
  sẵn sàng với trend bundle Module 3 đã finalized, deterministic ranking và
  versioned per-user publication.
- **Module 4D — API and dashboard:** bounded read-only FastAPI endpoints,
  allowlisted archived evidence, và Streamlit UI đọc qua HTTP.

Đây là technical plan của Module 4A–4D, không thay đổi yêu cầu, scope đã duyệt
hay các acceptance record trong [Proposal.md](Proposal.md) và evidence.

## 2. Mục tiêu nghiệm thu

Module 4A–4B được xem là hoàn thành khi:

1. ALS chỉ học từ `order_products_prior`; `order_products_train` không tham gia
   huấn luyện hoặc tuning.
2. Precision/Recall/MAP/NDCG tại K=5/10/20 được tính đúng trên ground truth
   official `train`, với cohort minh bạch và hai baseline để đối chiếu.
3. Mọi trial ALS có cấu hình, seed, runtime và metric truy xuất được.
4. Co-purchase graph được dựng từ `prior`, không có self-loop hoặc cạnh trùng.
5. Weighted degree và PageRank bằng GraphFrames thật chạy thành công;
   Connected Components là tùy chọn.
6. `user_recommendations` và `product_graph_metrics` được ghi vào MongoDB với
   version/index rõ ràng.
7. Có test trên fixture và một lần chạy dữ liệu đầy đủ với evidence cùng `run_id`.

Checklist chi tiết bắt buộc ở mục 17; bảng này chỉ tóm tắt mục tiêu.

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
`order_id`. Đường dẫn này đã khớp với staging của Module 1 và config của Module 2.
`Proposal.md` đã ghi Module 4 là consumer ở phần contract orders; bảng handoff và
các báo cáo khác vẫn cần được đối chiếu để giữ cùng cách gọi.

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
tests/test_module4_contracts.py     # hiện có: host/unit contracts
tests/test_module4.py               # hiện có: Spark fixture
tests/test_module4_spark.py         # đề xuất: mở rộng runtime/standalone tests
tests/test_module4_mongo.py         # đề xuất: live publish/replay/failure
tests/test_module4_flow.py          # đề xuất: staged Module 1–2–4 flow
docs/evidence/module4/<run_id>/
```

Docker image phải pin GraphFrames Python package và JAR tương thích Spark
3.5/Scala 2.12. Worker không nên tải dependency từ Maven trong lúc demo.

<a id="module4a-als-recommendation"></a>

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
- MAP@K và NDCG@K;
- user coverage;
- catalog coverage;
- số recommendation trung bình mỗi user;
- tỷ lệ user/product cold-start.

Precision dùng mẫu số K ngay cả khi hệ thống trả ít hơn K. Chỉ user có ground
truth không rỗng mới vào mẫu số; số user bị loại và lý do phải được công bố.

Đánh giá phải có hai cohort, theo định nghĩa cụ thể ở mục 14.5:

- **warm cohort:** user có factor và toàn bộ ground-truth products có item factor;
- **all official-train users:** mọi user có truth không rỗng, gồm cold-start;
  fallback nếu dùng phải được ghi rõ, không loại cold truth khỏi mẫu số.

Không gọi đây là đánh giá official `eval_set='test'`: bộ Instacart không cung cấp
truth cho các test orders. Những user đó vẫn thuộc publication nếu có prior factors.

Không đặt ngưỡng Precision/Recall tùy ý. Điều kiện nghiệm thu là metric được tính
đúng, cohort minh bạch và so sánh công bằng với baseline. Nếu ALS không vượt
baseline ở metric chính, không được kết luận ALS cải thiện recommendation.

### 5.7. Sinh và publish recommendation

Model cuối sinh Top-20 cho mọi prior user có user factor, không chỉ official-train
users. Tie-break phải đúng cả membership ở biên K (mục 14.3), không chỉ xếp lại
một list đã cắt. Output order là:

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

<a id="module4b-product-co-purchase-graph"></a>

## 6. Module 4B — Product Co-purchase Graph

### 6.1. Tạo vertices và edges

Graph dùng toàn bộ products catalog làm vertices; chỉ dùng prior để dựng edges:

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

Mọi catalog product không có cạnh, kể cả chưa có prior purchase, vẫn cần được giữ
với `neighbor_count=0`, `weighted_degree=0`. PageRank phải hữu hạn theo contract
GraphFrames đã kiểm chứng; không âm thầm thay null bằng một giá trị chưa được test.

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
- một product cold-start trong official train truth;
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
-> pilot baselines + bounded ALS trials
-> full internal baselines + shortlist validation + parameter freeze
-> final full-prior ALS train + save/load verification
-> official train evaluation
-> all prior warm-user recommendations + validated publication
-> graph threshold experiment
-> final GraphFrames metrics + graph validation
-> product_graph_metrics validated publication
-> pair readiness + evidence finalization
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

`manifest.json` phải chứa source-code digest, receipt và digest dữ liệu đầu vào,
Spark/Scala/Java/Python/GraphFrames versions, seed, cấu hình runtime, run ID, model
version và graph version. Với dữ liệu HDFS do Module 1 stage, `_sources.json` là
receipt bắt buộc. Fixture tự tạo phải có manifest fixture và nhãn scope riêng.
Không trộn summary, plans hoặc metrics của các run khác nhau.

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

Đây là ước lượng triển khai ban đầu, không phải cam kết thời gian còn lại sau
audit. Cần ước lượng lại W01–W12 sau khi runtime smoke và bounded run cho biết
thời gian fit/graph/publish, dung lượng HDFS và khả năng chạy đồng thời. Nếu chia
việc, một người phụ trách ALS/evaluation và một người phụ trách graph; cả hai
dùng chung data validation, versioning và evidence format.

## 11. Thứ tự ưu tiên khi thiếu thời gian

1. Đóng lỗi P0 runner/config/schema ALS và runtime smoke.
2. Data contract, receipts và chống leakage.
3. ALS + metrics + hai baseline + full internal shortlist.
4. Co-purchase graph + weighted degree + GraphFrames PageRank + thresholds 2/5/10.
5. Mongo outputs, idempotency và pair readiness.
6. Full-data evidence cùng run ID và đồng bộ báo cáo tiến độ.
7. Tối ưu/threshold analysis mở rộng và Connected Components tùy chọn.

Không nên bỏ internal validation để chạy nhiều hyperparameter hơn. Không nên thêm
Connected Components trước khi các graph invariants và PageRank cơ bản đã đạt.

## 12. Kết quả kiểm tra hiện tại

Các tiểu mục dưới đây tóm tắt checkpoint lịch sử; chi tiết gốc, run IDs và trạng thái tại thời điểm ghi được lưu trong [archive Module 4A/4B](evidence/module4/ab-implementation-history.md).

### 12.1. Checkpoint lịch sử 08/10 — bằng chứng và giới hạn

Checkpoint ngày 08/10 xác nhận fixture/integration trên phạm vi đã ghi, nhưng lượt full-input chỉ dừng ở validate-only và chưa hoàn tất gate dữ liệu, fit ALS/graph hay publish. Xem [bản ghi 12.1](evidence/module4/ab-implementation-history.md#history-12-1) và [evidence validation-20261008](evidence/module4/validation-20261008/README.md).

### 12.2. Các vấn đề của audit ban đầu

R01–R12 ghi lại phát hiện và tác động của audit ban đầu, không phải danh sách lỗi hiện tại. Mô tả và trạng thái có ngày của audit được giữ trong [bản ghi 12.2](evidence/module4/ab-implementation-history.md#history-12-2).

### 12.3. Checkpoint 08/10 — phần đã làm và trạng thái từng vấn đề

Bảng checkpoint 08/10 ghi nhận sửa đổi và xác minh fixture/integration tại thời điểm đó; full-data acceptance khi ấy vẫn còn mở. Xem [bản ghi 12.3](evidence/module4/ab-implementation-history.md#history-12-3).

### 12.4. Luồng liên kết phải được kiểm chứng

A/B nhận các bảng curated và source receipts từ Module 1; chỉ prior facts được dùng cho interactions và graph, còn official train dành cho đánh giá cuối sau khi cố định tham số. Module 2 user_features là đầu vào cohort tùy chọn; Module 3 trend không thuộc luồng fit/tuning A/B. Xem hợp đồng dữ liệu và A/B tại §§3, 5–6, cùng ranh giới hiện hành ở [Module 4C §19](#module4c-spec). Sơ đồ checkpoint lịch sử nằm trong [bản ghi 12.4](evidence/module4/ab-implementation-history.md#history-12-4).

### 12.5. Bàn giao W01–W12 và bước tiếp theo khi tiếp tục

Hướng dẫn resume W01–W12 phản ánh môi trường và trạng thái dừng ngày 08/10; không dùng chúng như quy trình hiện hành. Bản đầy đủ, gồm lệnh machine-specific, nằm trong [bản ghi 12.5](evidence/module4/ab-implementation-history.md#history-12-5).

### 12.6. Nghiệm thu hoàn tất ngày 10/10/2026

Historical Module 4A/4B full-data acceptance passed **21/21 gates** on 10 October 2026. full-ab-20261010-fixed-first and full-ab-20261010-fixed-replay passed with the same model and graph versions; evidence preserves their source revisions. This predates later refactoring and does not establish a fresh post-refactor full-data run. The dated results are in [acceptance-20261010](evidence/module4/acceptance-20261010/README.md) and the [archived section 12.6](evidence/module4/ab-implementation-history.md#history-12-6); the prior [09 October acceptance record](evidence/module4/acceptance-20261009/README.md) remains available for its earlier attempt and CI evidence. Current implementation status remains in [progress.md](progress.md).

## 13. Backlog thực hiện và thứ tự phụ thuộc

W01–W13 was a historical implementation plan, not a current open-work register. Its original descriptions, dependencies, gates, and optional P2 work are preserved in the [archived backlog](evidence/module4/ab-implementation-history.md#history-13). Current status belongs to [progress.md](progress.md); optional Connected Components and optimization work are not A/B acceptance requirements.

## 14. Chi tiết công việc theo thành phần

The following subsections define the current Module 4A/4B engineering and evidence contracts. Historical R/W identifiers and dated implementation notes remain in the [archive](evidence/module4/ab-implementation-history.md); an archive-only note does not replace a requirement stated here.

### 14.1. W02 — Runner, config và preflight

- Resolve configuration with precedence **explicit CLI value > environment > default**. Host help must return usage without starting services or importing the heavy Spark runtime. The runner supports an explicit build for changed runtime dependencies and returns nonzero on build or job failure. Compose runner and direct invocations use --use-aliases or an equally verified driver address. Report effective configuration with credentials redacted.
- Resolve Mongo URI/database/collections, Spark root/master/seed, partitions, K, graph threshold/reset/iterations, and tuning limits from that precedence. Validate IDs, paths, resource values, tuning budgets, K values, partitions, thresholds, and iterations before work begins. Reject invalid, non-finite, out-of-range, or path-traversing values; do not overwrite an existing version with different source/config identity; model/version reuse is explicit and identity-checked.
- Preflight services and permissions required by the selected stage: image and Spark master/worker, driver DNS, curated HDFS reads, model/evidence writes, and Mongo ping/index/rename privileges for serving. Start only required services; do not use orphan removal or shared-volume cleanup to run A/B. Keep stage ownership explicit: validate does not fit or publish, ALS does not build graph, and graph does not fit ALS.
- A compute-only --skip-serving run must not require Mongo availability, including through runner/Compose dependencies, and must record serving as skipped. It is not publication or end-to-end acceptance.
- Keep publication K separate from evaluation K, or ensure recommendation generation supplies at least max(5,10,20), so a smaller published list cannot truncate evaluation candidates.
- Use shared worker-accessible ALS checkpoints and report checkpoint interval, storage level, and run-owned cleanup policy. Keep run/version IDs validated and output/model paths explicit.

### 14.2. W03 — Runtime GraphFrames được kiểm chứng

- Pin and report Spark 3.5.5, Java 17, Scala 2.12, the GraphFrames Python package/version, and the compatible JAR name/version with its pinned checksum identity. Verify the JAR checksum during Docker image build and make it available on driver and workers without resolving Maven dependencies during a job.
- Verify and report Spark/Scala/JAR compatibility and the real GraphFrames backend on both the driver and standalone worker, using an isolated-vertex PageRank fixture and standalone-worker smoke. Record package/version, JAR filename/version/checksum, and backend/runtime identity in evidence.
- Missing dependencies or GraphFrames runtime errors fail the requested stage with stack trace; they must not silently become a passed fallback. A DataFrame backend is allowed only when explicitly selected for fixture scope and labeled as such.
- Document and test production PageRank normalization and dangling/isolated-vertex semantics. Do not compare backend scores unless algorithms, normalization, and isolate policy match.

### 14.3. W01, W06, W07 — ALS input, tuning, model và recommendation

- Accept only positive int32 user/product IDs and finite positive interaction strengths; reject null, fractional, overflow, duplicate user/product, empty factors, and invalid factor/score data. Read the nested ALS recommendation rating as a double aliased to als_score. Report purchase-count/strength distributions, source scope, interaction count, and factor counts against distinct training IDs.
- Make prior input, partition/sort order, and training materialization deterministic. The internal split requires a unique order per (user_id, order_number), one latest prior holdout order per eligible user, earlier training chronology, disjoint order sets that cover eligible facts, and no official-train order in either side. Report excluded users/rows/orders/products and cold-start products.
- If the internal holdout has no eligible users, report tuning as not performed; a fixture-only baseline fallback cannot close the tuning gate. Select a seeded/hash-stable pilot with deterministic hash tie-breaking by user ID and all prior orders for selected users. Deduplicate the grid and cap it at 12 distinct configurations.
- Compute pilot baselines only from internal training history. Evaluate at most the top three pilot configurations on full internal training/holdout; choose final parameters from that full internal comparison, with Recall@10, then Precision@10, then runtime as tie-breakers. Official train is used only after parameters are frozen and never selects parameters, thresholds, or fallback policy.
- Persist shared inputs within a declared resource budget and release model factors, trial outputs, and caches in finally using run ownership. Record each trial's params, seed, input/factor counts, P/R/coverage, fit/recommend/evaluation/total time, and resource/job-group metrics. Persist failed-trial input/config/error/stack trace immediately, including when all trials fail; an incomplete shortlist is reported as incomplete/degraded, never passed.
- Fit the final model on all prior interactions, save a versioned model without unintended overwrite, and verify reload schema, factor counts, and recommendations. Publish for every prior user with user factors, including users whose terminal order is in the official test set; keep the official-train evaluation cohort distinct. Report requested publication users, factor-backed eligible users, and missing/excluded users without substituting evaluation users for the publication cohort.
- Recommendation output includes seen products unless the contract is explicitly revised. Validate finite scores, positive IDs and product FKs, unique user/product keys, ranks 1..min(K, available factors), expected per-user row counts, and the factor-backed publication cohort. Report expected versus actual recommendation rows and item-factor availability, including its effect on recommendation list length. Resolve score ties deterministically by descending score then ascending product ID, including membership at the Top-K boundary.

### 14.4. W04 — Data handoff và input validation

- Read the Module 1 curated source receipt and independently verify actual input file inventory, counts, and SHA-256 digests. Record receipt digest, source identity, paths, scope, and validation time. If multi-part inputs are supported, define a deterministic aggregate digest without changing the meaning of an individual file hash.
- Keep input stable for the whole run, using an immutable snapshot or before/after inventory and digest checks; never promote output assembled from multiple source generations. Do not scan unrelated streaming interactions merely to verify the four curated A/B inputs.
- Distinguish local prior filenames from HDFS curated directory names and validate schema/types, critical nulls including product name, integer domains, key uniqueness, order/user/cart chronology and consistency, `reordered ∈ {0,1}`, duplicate order/product rows, and foreign keys. Reject null, nonbinary, invalid, or silently coerced `reordered` values.
- Validate eval_set membership and counts before/after joins; guard against joins that multiply facts. Check every prior/train order has facts as required, count users with empty ground truth and report exclusion reasons, and verify prior/train order sets are disjoint. Derive the leakage report from executed assertions; only prior facts may feed ALS interactions and graph construction.
- Treat Module 2 user_features as optional. If enabled, validate its path, schema, unique user key, prior-only provenance, and coverage; otherwise record it as unused. Do not claim ALS used these features when it did not.
- Module 4 is read-only with respect to curated sources: do not rewrite Module 1/2 source tables or receipts.

### 14.5. W05 — Metrics, baseline và cohort chính xác

- Validate distinct truth products, one truth row per user, unique recommendation keys/ranks, positive IDs, and ranks within K. Invalid rows fail validation rather than being dropped from metrics.
- Pass K explicitly to every ranking metric. For each user u with nonempty truth G(u): Precision@K = hits/K; Recall@K = hits/|G(u)|; AP@K = sum(hit_i × precision_at_i)/min(K,|G(u)|); IDCG@K = sum(1/log2(i+1), i=1..min(K,|G(u)|)); NDCG@K = DCG@K/IDCG@K. These denominators do not depend on the number of returned predictions: Precision uses K, AP and IDCG use min(K,|G(u)|), and Recall uses |G(u)|.
- Required hand case: K=5, predictions [1], truth [1,2,3,4,5] yields P=0.2, R=0.2, AP=0.2, and NDCG≈0.33916; the current implementation must match it. See [Spark RankingMetrics 3.5.5](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala).
- User coverage counts only predicted users within the cohort with nonempty truth. Catalog coverage counts cohort predictions over the declared candidate-catalog scope. Report mean recommendation count including users with no output, min/max/count distributions, skipped users, and empty-cohort reason; empty cohorts are not applicable, not a valid zero score. All coverage values remain within [0,1].
- Warm cohort means a user with factors and all truth products with item factors. All-train keeps every nonempty truth cohort, including cold items, which are reported because factor-only recommendation cannot return them. Report cold users, cold items, mixed truth, and truth-item factor coverage.
- Define reordered/new-to-user groups from stage-appropriate history. Keep ranked predictions unchanged and filter truth for each subgroup; report subgroup-specific cohorts and coverage.
- Compare ALS with global popularity and personal frequency on identical users, truth, K, and candidate scope; final baselines use full prior history. Personal-frequency items stay first; fill short lists and cold cohorts with deterministic global fallback without duplicate products. Baseline scores are not mislabeled as model scores in production outputs. When a UDF is retained, its callable is importable on executors; prefer Spark-native computation when measured cost justifies it. Report P/R@5/10/20 and deltas without unsupported improvement claims.

### 14.6. W08 — Graph build, threshold và invariants

- Use prior baskets only, deduplicate products within each order, emit canonical product pairs, and define each undirected edge weight as the number of orders containing that pair.
- Default to the full catalog, including inactive products, with all prior baskets. If a selected-product subset is approved for a run, select the top P products by prior popularity with product ID ascending as the deterministic tie-break; preserve fact/order semantics within the selected scope and report graph scope, declared P, selected product IDs and stable digest, and catalog/prior-fact coverage. Never silently sample baskets or pairs or label a subset as the complete graph.
- Reuse one raw co-purchase aggregation for threshold runs when resources permit. Measure basket count, basket-size distribution, and pair expansion before full runs; do not infer full-scale capacity from a small resource configuration. For each threshold, report total catalog products, prior-active products, edge-active products, isolates with no prior activity, and products isolated because threshold pruning removed their edges, alongside vertices, undirected/directed edges, total weight, catalog/prior-fact coverage, degree/weight quantiles, runtime, and available shuffle/spill/memory/disk metrics. Use counts measured for that run; do not infer them from historical figures. Record threshold rationale; do not call an unexamined default an optimum.
- Validate exact vertex membership with anti-joins, unique endpoints/edges, canonical pair ordering, positive integral finite weights, finite metrics, foreign keys, exactly two reciprocal directed rows per undirected edge, degree <= vertex_count - 1, and weighted-degree conservation: sum(weighted_degree) = 2 * sum(edge weight).
- Run PageRank with real GraphFrames on symmetric topology; co-purchase weights define weighted degree. Reject null, NaN, positive Infinity, and negative Infinity values, and reject negative neighbor_count, weighted_degree, or pagerank. Preserve isolated vertices and define empty, one-vertex, isolated, and disconnected semantics. Sort Top-20 by metric descending then product ID ascending and include product name.
- Attribute graph version, run ID, source, scope, backend, threshold, PageRank parameters, and computation time in versioned output/evidence.

### 14.7. W09 — Mongo snapshot, validation và phục hồi

- Keep generated/computed timestamps outside logical identity, in consistent UTC form. Validate recommendation and graph schema, finite scores, FKs, keys/ranks, expected row counts, and expected user/product sets before publication.
- Write documents through bounded iteration and configurable bulk batches; do not collect the complete graph or recommendation output on the driver. Verify inserted counts and index keys/options before promotion. Use the supported Mongo API in production; test doubles model it rather than adding destructive production fallbacks. Any validation or index failure leaves the previous active snapshot intact.
- Promote only validated run-owned temporary collections by same-database rename verified against real Mongo. Cleanup is scoped to those collections; cleanup failures do not mask the original error and must not trigger destructive fallback against active data.
- Same-version replay must converge to the same logical document set, keys, membership, ranks, and counts under the declared numeric comparison, while timestamp changes follow the documented policy. Validate counts/version/indexes after promotion and fail if read-back differs. Document whether only the active snapshot is retained or how run-owned immutable rollback versions are retained.
- A/B use separate same-database Mongo promotions and therefore are not atomic as a pair. Mark ready only after both snapshots validate for the same run/version identity through a readiness pointer/manifest; report partial publication honestly. Enforce a single writer or validate locking/readiness behavior under concurrent writers.
- On failure, return nonzero and report completed/published stages and retry/cleanup outcome; a failed job does not imply that neither collection changed.

### 14.8. W10 — Evidence, performance và failure reporting

- Use one run ID across A/B and stage/trial/job-group IDs beneath it. Record fixture/pilot/full-data scope, model/graph/source versions, executed/skipped/failed status, command arguments, redacted resolved config, commit/dirty state, code/runtime/dependency digests, input receipts/hashes, and environment.
- Write a manifest at start and finalize it on success or failure. Record Spark/Scala/Java/Python/GraphFrames identities, backend, master, driver/executor resources, partitions, ALS blocks/checkpoints, graph scope/threshold, and runtime inventory.
- Persist stage/trial artifacts as they complete. Failure evidence includes stack trace, duration, stage/trial context, completed and published work, and cleanup outcome. Finalize event logs after Spark stops and attribute validation, fit, recommendation, evaluation, edge build, PageRank, and publication metrics.
- Store actual test/command output and exit codes, skipped checks with reasons, stage timings, shuffle/spill/task/executor metrics, model/output sizes, and artifact digests. Compact evidence includes scope, counts, parameters, cohort/baseline results, threshold, limitations, and reproduction reference. Place large evidence outside compact docs and link compact records to matching run IDs.
- Hash/check required artifacts and preserve cleanup errors without overwriting the stage failure. A summary lists all applicable acceptance gates and reports passed only when each required gate for that command/scope passes. Compute-only/skipped serving is distinct from full acceptance. Historical artifacts remain immutable and attributed to their original source/environment.

### 14.9. W11–W12 — Test, CI, full flow và báo cáo

- Exercise regressions across contract, Spark, real GraphFrames, live Mongo, runner and cross-module flow. Required runtime-backed groups do not pass by being skipped; include host help and standalone-worker paths; staged fixtures follow the real Module 1 layout.
- Exercise validate, als, graph, and all with correct stage ownership; fixture flows use isolated roots, databases, collections, run IDs, and ownership. Include Module 2 validation/feature generation only when the optional feature path is enabled. Cover each applicable regression identified by the historical audit. Cleanup cannot touch curated data, shared active snapshots, or upstream module outputs.
- CI pins runtime identities and separates fast/fixture, live-Mongo, and manual full-data scopes. Upload actual evidence/logs on success and failure; a configured workflow or dispatch is not evidence that it passed.
- Full-flow acceptance checks the same source/scope/counts/hashes, expected user/product sets, readiness, and replay. Existing Module 1–3 evidence cannot substitute for checks required in a new Module 4 run.
- Report implemented, fixture-validated, integration-validated, and full-data-accepted status separately. Update README/progress/plan/Proposal claims consistently from evidence; preserve dated Module 1–3 records as historical.
- Keep A/B completion separate from Module 4C/4D. Update the Proposal only when an approved contract field/scope changes; acceptance does not imply Hybrid quality, dashboard readiness, or production capacity.

## 15. Ma trận test bắt buộc để đóng backlog

Các case dưới đây cần triển khai/chạy; tên test là mô tả mục tiêu, không phải
khẳng định các test này đã tồn tại.

| Nhóm | Case tối thiểu | Assertion chính |
|---|---|---|
| Runner/config | Host help; env/CLI precedence; Docker unavailable; driver DNS | Help không cần Spark; URI đúng; fail rõ; worker gọi driver được |
| Receipt/data | Count/hash mismatch; missing/null/type; duplicate/FK; wrong eval_set; chronology tie | Fail trước fit; exact join/source counts; không silent casts |
| Split | 1 prior order; >=2 orders; last-order tie; official train overlap | Eligibility/disjoint/chronology đúng; không official leakage |
| ALS | Real implicit fit; nested rating; save/load; cold user; repeat seed; tie at K | Schema/finite/factors/ranks/user sets và repeatability đúng |
| Metrics | Hand example; fewer than K; empty/no prediction; extra users; duplicate ranks/products | P/R/AP/NDCG đúng, coverage [0,1], empty cohort labeled |
| Baselines/cohort | Personal short list; global tie; cold user/item; subgroup | Không duplicate; cùng users/truth/K; warm/cold definitions đúng |
| Tuning | Budget/dedup grid; pilot; full shortlist; failed/all-failed trial | Chọn từ internal; trial evidence tồn tại cả failure; factors released |
| Graph construction | Basket duplicate; repeated pair; thresholds; subset; no prior activity | Canonical weight đúng; coverage/isolation/scope được đối soát |
| GraphFrames | Real backend; missing JAR; symmetric graph; isolated/disconnected/empty graph | GraphFrames gate thật; missing dependency fail; documented PageRank |
| Graph invariants | Null/fractional/negative weight; orphan endpoint; wrong vertex set; Inf/NaN | Mọi invalid graph bị từ chối; degree conservation đúng |
| Mongo | Expected sets/count; index conflict; rerun; insert/index/rename failure | Promote sau validation; không stale/duplicate rank; previous snapshot preserved |
| Flow | Staged-layout fixture -> Module 2 -> Module 4; standalone all; skipped serving | Paths/hashes/outputs đúng; trạng thái compute/serving phân biệt |
| Failure/evidence | Spark/ALS/GraphFrames/Mongo failure ở từng stage | Nonzero exit + actual logs, manifest và completed/published stage report |
| Regression/full | Module 1–3 suite; full input run; final snapshot rerun | Không regression; scope/count/versions cùng run; reproducibility evidence |

Fixture và regression hiện bao phủ prior-active user có terminal
`eval_set='test'`, unknown evaluation user, catalog cold product, empty truth
subgroup, short lists, equal-score boundary, negative data/receipt/backend
cases, exact holdout baskets, reload execution plan và cache release. Kết quả
suite mới ngày 10/10 được lưu trong evidence, không cộng các suite lịch sử.

## 16. Quy trình chạy nghiệm thu sau khi sửa code

Use this section to define validation scope and evidence for a new run. Current prerequisites, command ownership, and operational sequences are maintained in the [Docker runbook](docker.md#module-4a4b-operation-and-full-data-acceptance). The exact commands from the historical 10 October acceptance remain in [run-acceptance.sh](evidence/module4/acceptance-20261010/run-acceptance.sh) for provenance and reproduction review; those machine-specific commands are not universal current procedures.

### 16.1. Kiểm tra tĩnh và runtime

Use the runbook's [Docker validation commands](docker.md#validation-commands) for the selected image and test ownership. Record actual command output, exit codes, scope, and skipped checks with reasons. Verify the GraphFrames backend and standalone-worker path when the selected gate requires them. Do not apply an unfiltered test command to an image that lacks required UI or Spark dependencies.

### 16.2. Xác minh handoff và bounded flow

- Validate staged curated paths and receipts against one stable source identity. Run Module 2 handoff/feature generation only when using optional user_features.
- Exercise bounded fixtures with isolated roots and Mongo collections, and label scope in the manifest. A bounded run is not full-data acceptance.
- Confirm command-stage isolation: validate does not fit/publish; ALS does not build graph; graph does not fit ALS. --skip-serving is explicitly compute-only.
- Verify real runtime/backend, Mongo replay/failure handling, source immutability, expected counts/sets, and run-scoped failure evidence before promoting active outputs.

### 16.3. Run full-data và promote

For full-data acceptance, use the full intended curated source, fixed input/configuration, new run IDs, real runtime/backend, and real Mongo publication/read-back. A same-version replay uses the accepted model and graph versions with a new run ID, and reports matching logical outputs under the declared comparison policy. Capture actual logs and stage evidence for success and failure.

The default accepted graph scope is the full catalog with all prior baskets. If a selected-product subset is approved for a particular run, disclose its selection, digest, scope, coverage, and limitations; never label it full-catalog. Report serving-skipped and compute-only runs separately from full publication acceptance.

### 16.4. Đóng báo cáo

Evaluate each applicable condition in §17 and link its run ID, source identity, manifest, test/command logs, and artifacts. Preserve partial, failed, and interrupted records; report scope, skips, failures, limitations, and measured performance honestly. A static or documentation check is not a new runtime or full-data result.

## 17. Checklist đóng Module 4A–4B

The following are the acceptance conditions for a new validation run; this list does not itself record new execution results.

- [ ] Runner/configuration precedence, clean host help, service preflight, and runtime/runner behavior satisfy §14.1.
- [ ] Pinned GraphFrames Python/JAR identities pass driver and standalone-worker smoke; the real backend is recorded.
- [ ] Handoff receipts, source identity, file hashes/counts, and curated-source immutability are verified.
- [ ] Negative validation and leakage/chronology assertions reject invalid inputs before fitting.
- [ ] Metrics, baselines, warm/all/history cohorts, and coverage within [0,1] are correct.
- [ ] The seeded pilot and full internal shortlist complete; official train does not select parameters.
- [ ] The final prior-only model saves/reloads with verified factors and model metadata.
- [ ] Publication covers every prior user with factors, including terminal test-set users.
- [ ] Recommendation scores, keys, ranks, product sets/counts, and deterministic tie handling validate.
- [ ] Graph thresholds, scope, coverage, invariants, PageRank, and isolated products validate.
- [ ] Graph output records version/run/threshold and traceable backend/parameters.
- [ ] Both Mongo outputs pass schema/index/count/set checks before and after promotion.
- [ ] Same-version staged replay converges, and failed publication preserves prior evidence/snapshots according to policy.
- [ ] Pair readiness, partial-failure reporting, and single-writer/concurrency policy are explicit and validated.
- [ ] Success/failure manifests identify input/source/runtime/version and matching evidence.
- [ ] Actual test logs, stage timings, shuffle/resource/model-size metrics are retained.
- [ ] Required fixture/live-integration and runtime CI gates pass with exact commit and workflow-run attribution.
- [ ] Module 1–2–4 fixture flow and standalone-worker path pass within their stated scope.
- [ ] Historical reference only: the recorded 10 October 2026 full-data acceptance used the full product catalog, all prior baskets, graph threshold 10, same-version full-data first/replay, real MongoDB publication, and independent read-back. For a new acceptance, use the default full-catalog/all-prior-basket scope and the graph threshold declared and approved for that run; an alternative scope requires prior approval and its corresponding correctness, coverage, evidence, and replay criteria.
  An unapproved subset cannot satisfy a full-catalog gate. In all cases, require same-version full-data first/replay, real MongoDB publication, and independent read-back; compute-only runs do not pass.
- [ ] Required Module 1–3 regression gates pass in the selected runtime.
- [ ] README/progress/plan/Proposal status and evidence links agree with the recorded results.

The historical 10 October result remains **21/21** in the concise summary and the [archived checked record](evidence/module4/ab-implementation-history.md#history-17); its run details remain in [acceptance-20261010](evidence/module4/acceptance-20261010/README.md). Connected Components, quality above the personal-frequency baseline, and Module 4C/4D are not part of the mandatory A/B acceptance criteria.

## 18. Quy tắc cập nhật tiến độ và bàn giao

- Mỗi Wxx cập nhật ngày thực hiện, files thay đổi, test commands/results,
  artifact/run ID, remaining issues và người thực hiện nếu làm nhóm.
- Tách “đã viết code”, “đã chạy fixture”, “đã chạy integration” và “đã nghiệm thu
  full-data”; checkbox chạy/publish không đóng chỉ nhờ lint hoặc syntax pass.
- Một run failed/partial/skip vẫn giữ evidence và không được sửa thành passed
  sau khi đổi source; chạy lại có run ID hoặc resume manifest theo policy rõ ràng.
- Bàn giao cho Module 4C gồm active model/graph versions cùng run ID, schema/index,
  snapshot readiness, scope/threshold/backend, publication user cohort và cách
  chọn/đọc snapshot. Consumer join bằng `user_id`/`product_id`, không trộn versions.
- Không đưa `FinalScore`, API/dashboard hoặc Kafka consumer vào định nghĩa done
  của A/B; ghi riêng backlog C/D. Nếu sau này dùng trend trong official evaluation,
  thêm cutoff/prior-train leakage gate trước khi công bố metric.

## Tài liệu kỹ thuật đối chiếu

- [Spark 3.5.5 ALS API](https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/ml/recommendation/ALS.html)
- [Spark 3.5.5 ALS recommendation implementation](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala)
- [Spark 3.5.5 RankingMetrics implementation](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala)
- [GraphFrames 0.8.3 PageRank implementation](https://github.com/graphframes/graphframes/blob/v0.8.3/src/main/scala/org/graphframes/lib/PageRank.scala)
- [Docker Compose run: --use-aliases](https://docs.docker.com/reference/cli/docker/compose/run/)
- [GraphFrames installation](https://graphframes.io/02-quick-start/01-installation.html)
- [GraphFrames centrality metrics](https://graphframes.io/04-user-guide/03-centralities.html)

<a id="module4c-spec"></a>

## 19. Module 4C — Hybrid recommendation and serving

This section specifies the implemented Module 4C boundary. It complements the
approved architecture in [Proposal §§28–32](Proposal.md) without changing its
scope. Detailed dated C8/C9 records and the original evaluation methodology are
archived in [Module 4C evidence](evidence/module4c/README.md).

### 19.1. Inputs and source consistency

Module 4C consumes the accepted Module 4A–4B snapshot and Module 3 immutable
finalized trend bundles. It does not train ALS, rebuild the graph, or treat the
legacy mutable `realtime_trending` rows as a consistent trend snapshot.
Kafka provenance, bundle identity, finalization, and retry boundaries are
specified in the [Module 3 contract guide](module3-contracts.md).

- The source context must establish that both ALS and graph/catalog data are
  available before classifying a user as warm or cold. Capture the accepted
  A/B readiness generation and its model/graph/run identities; verify readiness
  again after bounded reads under the publication lock.
- A request supplies an explicit UTC, BSON-millisecond-aligned
  `as_of_event_time` and an exact `source_namespace`. There is no implicit
  freshness threshold. C5 selects the newest eligible finalized bundle at or
  before the cutoff and verifies identity, schema, source metadata, timestamp
  precision, content hash, and expiry. Preserve the bundle's original
  `source_scope` verbatim.
- ALS products must resolve in the verified catalog. Trend-only products absent
  from that catalog are excluded from candidates. Candidate source rows and
  ranks are validated before ranking.

### 19.2. Candidate generation and ranking

The configured defaults are `als_top_n=20`, `trend_top_m=20`,
`output_top_k=20`, `lambda_weight=0.7`, and `epsilon=1e-12`. Each size is a
positive integer no greater than 20; lambda is finite in `[0,1]`.

For ALS scores, min-max normalization is
`(score - min_score) / (max_score - min_score + epsilon)`; a constant ALS score
set maps to zero. Trend scores are divided by `max_trend_score + epsilon`; an
all-zero trend set maps to zero. For each candidate, the final score is

```text
final_score = lambda_effective * normalized_als_score
            + (1 - lambda_effective) * normalized_trend_score
```

The candidate set is the union of the selected ALS and trend rows in hybrid
mode. Lambda 1 selects ALS candidates only; lambda 0 selects trend candidates
only. The final sort uses full-precision scores descending and `product_id`
ascending as a deterministic tie-break, then applies Top-K. Persisted raw
scores, normalized scores, inclusion flags, requested/effective lambda, mode,
reason, and rank make the result explainable.

If trends are unavailable while ALS rows exist, the service uses an ALS-only
fallback and records its reason. If ALS rows are absent and the trend snapshot
is unavailable, the service publishes a valid empty result with mode
`no_candidates` and a reason. When no ALS rows exist but a trend snapshot is
available, the mode remains `cold_user_trend_only`, even if the snapshot has no
eligible candidates; an empty snapshot produces an empty result with reason
`trend_snapshot_empty`. Other empty candidate sets retain the selected mode and
record the specific reason. Missing or unverified A/B graph/catalog context is
an error; it is not silently interpreted as a cold user.

### 19.3. MongoDB publication and CLI contract

`final_recommendations` contains immutable rows keyed by a content-derived
`serving_version` and user. The accepted row contract carries user/product
identity and name, raw and normalized ALS/trend scores, final score, contiguous
rank, source-inclusion flags, lambda values, mode/reason, A/B source identities,
trend namespace/scope/window/hash, explicit event-time cutoff, input digest,
generation time, and ranking configuration identity. Unique indexes protect
`(serving_version, user_id, product_id)` and
`(serving_version, user_id, rank)`.

The per-user readiness document uses `_id="hybrid:<user_id>"` and points to one
complete immutable serving version. The publisher validates prepared input,
writes rows insert-only, reads back and validates the full publication, then
promotes the pointer only if the source generation is still acceptable. A
repeated identical publication is idempotent; conflicting content, incomplete
read-back, or a stale/incomparable source generation cannot replace the active
pointer. The pointer is not a multi-document MongoDB transaction and does not
persist an intermediate `publishing` state.

The supported serving entry point is
`python3 -m src.module4.hybrid.cli serve-user`. Its five required options are
`--user-id`, `--source-namespace`, `--as-of-event-time`, `--mongodb-uri`, and
`--mongo-database`; the user ID must be positive. The cutoff must include a
timezone; timestamps and namespaces are explicit per run, not universal
defaults. Optional `--lambda-weight` defaults to `0.7`, and
`--max-event-age-seconds` is optional. There is no readiness flag:
the implementation verifies the required A/B readiness internally. This is a
publisher operation that writes immutable rows and may promote the per-user
readiness pointer, so it requires publisher credentials separate from the
dashboard's read-only account. See the [Hybrid CLI operating instructions](docker.md#hybrid-cli)
for command syntax and [docker.md](docker.md) for environment and container
operations.

### 19.4. Evaluation design and claim boundary

C8A measures responsiveness to controlled synthetic trend changes, not
relevance or recommendation accuracy. Its fixed warm-user fixture contains 20
ALS rows and stable A/B, graph, and catalog inputs. The independent score oracle
compares full-precision scores. For the recorded shift, lambda `0.0` and `0.5`
changed Top-1 from P1 to P2; lambda `0.7`, `0.9`, and `1.0` retained P1 and the
rank order. The default `0.7` result therefore showed no Top-1 or rank-order
change in this fixture. CLI process timing includes startup and Mongo serving,
not fixture setup, and is not Kafka-to-serving latency.

C8B is a bounded synthetic Kafka → Spark → finalized Module 3 bundle → Module
4C CLI integration. Its event acknowledgement, watermark, bundle publication,
and serving observations have separate recorded boundaries. It did not exercise
checkpoint restart and does not establish production throughput or
recommendation quality. See the immutable [C8 receipts](evidence/module4c/README.md)
and archived [C8B procedure and observations](evidence/module4c/c8b-streaming-demo.md).

The reproducible offline Hybrid evaluation now uses the official Instacart
`eval_set` boundary: ALS fit and tuning use only `order_products_prior`, the
trend candidate proxy is a fixed Top-20 global product-frequency ranking from
prior-only interactions, and `order_products_train` is read only as held-out
truth. It records both curated source hashes, compares the preregistered lambda
sweep `(0.0, 0.5, 0.7, 0.9, 1.0)` with ALS, Trend-only, PersonalFrequency, and
GlobalPopularity at K=5/10/20, and reports all official-train users plus the
warm-user/all-truth-items cohort. The full-data verifier checks the split
provenance and required cohort/method metrics. The fixed frequency proxy is not
equivalent to Module 3's temporal trend windows and does not support a claim
that Hybrid or ALS is superior.

A future evaluation of actual Module 3 trend windows still needs a valid
chronology and cutoff from source order sequence and timestamp reconstruction.
The ordinary Module 1 event feed combines prior and train events without
retaining `eval_set`, so it cannot establish prior-only trend provenance. If a
valid cutoff cannot be proven for that temporal stream, report that evaluation
as infeasible rather than using the mixed feed.

<a id="module4d-spec"></a>

## 20. Module 4D — Read API and dashboard specification

Module 4D is a bounded, read-only presentation layer over existing MongoDB
snapshots and fixed archived reports. The implementation is under
`src/module4/dashboard/`; the frontend uses HTTP and never connects directly to
MongoDB. The approved high-level visualization requirements remain in
[Proposal §30](Proposal.md). The field-level historical inventory and release
records are retained in the [Module 4D evidence archive](evidence/module4d/README.md).

### 20.1. Backend configuration and trust boundary

The API process requires these explicit settings; it has no project-database
fallback:

```text
MODULE4D_MONGODB_URI=<MongoDB URI for a database-scoped read-only account>
MODULE4D_MONGO_DATABASE=<explicit database name>
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated exact namespace allowlist>
MODULE4D_APP_ROOT=<application/repository root containing docs/evidence; optional default>
```

The UI process receives only:

```text
MODULE4D_API_BASE_URL=http://127.0.0.1:8000
MODULE4D_TREND_SOURCE_NAMESPACES=<same exact namespace allowlist>
```

The UI URL validator permits loopback/local API addresses or the explicit
`module4d-api` service host in the isolated demo; it rejects credentials,
arbitrary external hosts, paths, queries, and fragments.

The API reads only the fixed collections
`module4_snapshot_readiness`, `final_recommendations`,
`realtime_trending_windows`, `batch_product_metrics`, `department_metrics`,
`user_recommendations`, `product_graph_metrics`, and `streaming_metrics`. The
account needs read and index-metadata inspection privileges only. It cannot
publish, write, create indexes, rename, or drop.
The Streamlit UI accepts only `MODULE4D_API_BASE_URL` and the matching exact
trend namespace allowlist. For local use the API and UI ports bind to
`127.0.0.1`; credentials remain on the API side.

The API validates scalar IDs, UTC timestamps, namespace allowlists, enum values,
and opaque report IDs. It constructs fixed Mongo filters and projections, uses
documented indexes, explicit result limits and `maxTimeMS`, closes cursors, and
returns sanitized errors. It does not accept caller-supplied Mongo operators,
collection names, sort keys, paths, or report filenames. It does not start
Spark, Kafka, HDFS, or a Module 4C publisher.

### 20.2. Implemented HTTP routes and response contracts

All data routes return bounded JSON with source/provenance metadata. The
implemented paths below supersede the earlier D0 route proposals preserved in
the archive.

| Route | Contract and bounds | Failure behavior |
|---|---|---|
| `GET /health` | API process liveness only; does not ping MongoDB. | Process-level health response only; never implies source availability. |
| `GET /api/v1/recommendations/{user_id}` | Positive integer ID; per-user Hybrid pointer, exact serving-version rows, rank order, at most 20 rows. Returns persisted raw/normalized/final scores, mode/reason, and source identities. | `404` no published snapshot; `503` not ready or service unavailable; `409` pointer changed during read; `502` invalid stored snapshot. |
| `GET /api/v1/als/recommendations/{user_id}` | Active paired A/B model only; optional exact model version; limit 1–20; indexed stored ALS ranks/scores with matching graph names when available. | Unknown model `404`; unready `503`; changed pointer `409`; malformed rows `502`. |
| `GET /api/v1/streaming/metrics` | Exact allowlisted namespace; one latest `streaming_metrics` document through `_id_`, validated within 16 KiB. Real same-batch Spark rates and defined finalized-output latency, run/query/batch and capture time. | No document returns `available=false`; malformed `502`; query failure `503`; invalid namespace `422`. |
| `GET /api/v1/trends` | Exact allowlisted namespace plus explicit UTC cutoff/optional max event age; returns one newest eligible finalized bundle through the C5 verifier, including window, scope, publication/expiry, digest, and ranked items. | Missing, expired, and stale states remain distinct; malformed or corrupt newest bundle fails closed; missing required index is `503 trend_index_unavailable`. |
| `GET /api/v1/batch/products` | `order=purchase_count` or `reorder_rate`; default limit 20, hard limit 50; metric descending and product ID ascending. Product names are not in this Mongo source. | Invalid inputs `422`; unavailable required ranking index `503 batch_index_unavailable`; malformed source `502`. |
| `GET /api/v1/batch/departments` | Reads at most 22 records to validate the fixed 1–21 department domain; returns at most 21 rows ordered by purchase count descending then ID ascending. | More than 21 or malformed documents fail with `502`; collection-specific run/time is preserved. |
| `GET /api/v1/evidence` and `GET /api/v1/evidence/{evidence_id}` | Fixed five-ID registry and curated projections only: Module 2 analytics, Module 2 hardening benchmark, Module 4A evaluation, C8A receipt, and C8B receipt. Files are schema-validated and capped at 256 KiB. | Unknown ID `404`; registered missing file `503`; malformed/oversized/invalid content `502`. No arbitrary file paths or raw result passthrough. |
| `GET /api/v1/analytics/orders-distribution` | Archived all-order counts by source `order_dow` bins 0–6 and hour bins 0–23; counts reconcile to the recorded order total. | Invalid evidence returns sanitized `502`; this is not a prior-only product-event distribution. |
| `GET /api/v1/evaluation/als` | Archived official-train cohorts and recorded ALS/global-popularity/personal-frequency metrics at available K values; scores are not recalculated. | Invalid evidence returns sanitized `502`; no Hybrid score or quality claim is produced. |

The response schemas are strict: unknown fields and non-finite numbers are
rejected. Every error uses
`{"error":{"code":"…","message":"…","request_id":"…"}}`; internal
credentials, connection details, and filesystem paths are not returned.

| Response | Required payload fields and bounds |
|---|---|
| Recommendation | `user_id`, `serving_version`, `status="ready"`, `recommendation_count` (0–20), `mode`, nullable `reason`, `source`, `ranking_config`, nullable `generated_at`, `recommendations` (0–20), and `observed_at`. Each item has product ID/name, rank 1–20, ALS/trend inclusion flags, nullable raw scores, normalized components in [0,1], and `final_score` in [0,1]. `source` carries run/model/graph IDs, trend namespace/scope/window/hash, cutoff, input hash, and pointer update time. |
| Trend | `available`, status (`available`, `missing`, `expired`, or `stale`), nullable reason/scope/window/publication/expiry/schema/config/content identities, namespace, `row_count` (0–20), items (product ID, trend score [0,1], rank 1–20), and `observed_at`. |
| Batch products/departments | `items`, `source`, `metric_definition`, `observed_at`. Product rows contain product ID, positive purchase/user counts, reorder count/rate, run ID, and batch time; department rows contain department ID 1–21, positive purchase/user counts, reorder rate, run ID, and batch time. `source` states prior-only item purchases and `atomic_with_other_collections=false`. |
| Evidence registry/detail | Registry items contain evidence ID, title, kind, classification, safe source label, availability/status, and nullable SHA-256. Detail contains ID/title/kind, provenance (`artifact_key`, safe label, classification, archived flag, SHA-256), curated `data`, and `observed_at`. The only IDs are `module2-full-analytics`, `module2-full-hardening-benchmarks`, `module4a-full-als-evaluation`, `module4c-c8a-responsiveness`, and `module4c-c8b-streaming-demo`. |
| Orders distribution | `scope="all orders"`, `measure="order count, not item-level prior purchases"`, total orders, exactly seven day bins (0–6), exactly 24 hour bins (0–23), provenance, and observation time. Each bin carries its source dimension and nonnegative `order_count`. |
| ALS evaluation | Evidence ID/title/provenance, `algorithm_scope="ALS and recorded baselines"`, recorded method description, the three recorded K values, cohort and history-subgroup metric maps, baseline deltas, official-test-truth availability, truth summary, and observation time. Metrics include Precision/Recall/MAP/NDCG, evaluated users, and coverage where present; the API does not recompute them. |

The Hybrid read consistency protocol reads the exact
`module4_snapshot_readiness` `_id="hybrid:<user_id>"`, validates ready state,
serving version and expected row count, reads exact user/version rows with a
21-row limit, checks unique products and contiguous ranks, then rereads and
compares pointer identity. A ready empty snapshot is valid. A missing pointer
means “no published snapshot found”; it does not prove that a user is absent
from the dataset. The API never substitutes the A/B `_id="active"` pointer.

The trend route delegates candidate selection and bundle integrity to C5. It
uses an administrator-provisioned index compatible with
`(source_namespace ASC, window_end DESC)` and never falls back to a collection
scan or an older bundle when the newest eligible document is corrupt. Batch
product ranking requires both named compound indexes:

```text
realtime_trending_windows:
  idx_source_namespace_window_end_desc
  (source_namespace ASC, window_end DESC)
batch_product_metrics:
  idx_batch_product_purchase_count_desc_product_id_asc
  (purchase_count DESC, product_id ASC)
  idx_batch_product_reorder_rate_desc_product_id_asc
  (reorder_rate DESC, product_id ASC)
final_recommendations:
  uq_serving_user_rank (serving_version, user_id, rank), unique
  uq_serving_user_product (serving_version, user_id, product_id), unique
```

The default Mongo `_id_` indexes and the accepted A/B indexes must also remain
available. Module 2 creates both required D1B product-ranking indexes on the
replacement collection before promoting it. Administrators provision initial
indexes for empty or externally managed collections; they do not need to
recreate the indexes after each Module 2 publication. The API itself never
provisions or repairs indexes.

### 20.3. Dashboard views and interaction rules

The four Streamlit views are:

1. **Overview:** API liveness, independent source availability, each source's
   own run/version/window and timestamp, fixed evidence registry state, and refreshable same-batch streaming rates/latency with clock-skew disclosure.
2. **Realtime & Batch Analytics:** one selected finalized trend window,
   prior-only product/department metrics, and archived all-order day/hour
   counts. Product/department names are not inferred from IDs.
3. **Hybrid Recommendations:** the verified per-user persisted ranking,
   component and final scores, inclusion flags, mode/reason, and source
   provenance. A separate versioned ALS read is aligned by product ID for
   side-by-side comparison; unavailable scores remain blank.
4. **Evaluation & Performance:** archived ALS/baseline evaluation, Module 2
   benchmark observations, and C8A/C8B receipts, each labeled by run, scope,
   environment, and method.

Requests are user-triggered; there is no continuous polling. Successful
responses are cached only in the current Streamlit session and explicit refresh
clears that cache. Errors are shown separately from empty results; a changing
pointer offers a manual retry. The UI has no Mongo driver, database URI, report
file reader, or publication path.

### 20.4. Data availability, provenance, and metric limits

| Source | Contract and reader rule | Availability boundary |
|---|---|---|
| Module 4A `user_recommendations` | Read exact `(model_version, user_id)`, rank sort and bounded limit; bracket with A/B `_id="active"` readiness reads where ALS is exposed. | Current snapshot only; old model versions are not guaranteed to remain stored. |
| Module 4B `product_graph_metrics` | Versioned product rows include neighbor count, weighted degree, PageRank, run, threshold, and computation time. | Available for bounded diagnostics; not an MVP dashboard route. |
| Module 4A–4B readiness | `module4_snapshot_readiness`, `_id="active"`, establishes the paired model/graph generation. | Never use as the Module 4C per-user serving pointer. |
| Module 4C `final_recommendations` | Immutable versioned rows include raw/normalized components, final score/rank, mode/reason, source flags, event cutoff, source identities and hashes. | Exact user/version lookup only after the Hybrid pointer protocol. |
| Module 4C readiness | `module4_snapshot_readiness`, `_id="hybrid:<user_id>"`, identifies one complete per-user serving version and expected row count. | Pointer/rows/pointer validation is required; an absent pointer is not proof of an unknown user. |
| Module 3 trend bundles | Immutable `realtime_trending_windows` documents contain namespace/scope, finalized bounds, ranked items, content/config hashes, publish time and optional expiry. | Enabled only when configured; preserve scope and distinguish event-time age from processing-time expiry. |
| Module 2 batch collections | `batch_product_metrics` and `department_metrics` carry prior-only counts and each collection's own `run_id` and `last_batch_time`. | Their publications are not one cross-collection transaction; report mismatched run IDs. |
| Archived reports | Fixed registry only; reports carry historical run/environment/scope and source digests. | Not live Mongo freshness or request-time computation. `results/` is not scanned. |

Keep event time, Spark processing time, bundle publication time, recommendation
generation time, and API observation time distinct. `source_namespace` and
`source_scope` are preserved verbatim. The UI may show registered historical
progress and the specifically named C8B acknowledgement-to-bundle interval, but
must not call these Kafka lag or generic event-to-dashboard latency.

The inspected sources do not provide Kafka consumer lag, generic
event-to-dashboard latency, product-level hour/day distributions, or
leakage-safe Hybrid recommendation accuracy. Show these as unavailable. Module
2 `orders_by_hour` and `orders_by_dow` count all orders; product and department
purchase metrics use prior item facts. Module 4A offline metrics do not imply
Hybrid quality. Historical synthetic dates do not alone establish a stale or
expired source.

### 20.5. Acceptance boundary and implementation history

The implementation tests must exercise scalar validation, fixed query shapes,
index requirements, explicit limits, no-write behavior, error redaction,
pointer races, empty/missing/corrupt snapshots, curated report projections,
and UI loading/empty/error/retry states. Isolated Mongo tests must use
disposable databases; browser and Compose acceptance must keep host bindings
loopback, database storage disposable, and UI isolated from MongoDB.

The release evidence records D1A/D1B API, four-page UI, and D3 isolated fixture
validation. These local and CI results demonstrate the recorded integration
scope only; they do not establish production scalability. Exact dated test
results, browser observations, report hashes, and historical failure/retry
records remain in [the evidence archive](evidence/module4d/README.md).

Milestones include D1A, the D1A-FIX C5 selection-parity correction, D1B, the
D2 dashboard, and the D3 isolated synthetic fixture. Their detailed dates and
receipts remain in the evidence archive.
