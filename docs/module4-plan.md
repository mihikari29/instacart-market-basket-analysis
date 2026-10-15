# Kế hoạch kỹ thuật và nghiệm thu Module 4A–4D

Mốc nghiệm thu Module 4A–4B: **10/10/2026** (Asia/Bangkok). Bản hợp nhất Module 4A–4D cập nhật **15/10/2026**.

**Trạng thái 10/10/2026: hoàn thành và nghiệm thu 21/21 gate (100%).**
Audit đầu lượt này ghi nhận 20/21 gate (95,2%); phần còn lại là full-data A/B
publication/read-back. Hai lượt `full-ab-20261010-fixed-first` và
`full-ab-20261010-fixed-replay` đã passed toàn bộ validation, ALS, GraphFrames,
publication, serving và source-stability checks. MongoDB có 4.124.180
recommendations cho 206.209 users và 49.688 graph documents. Replay dùng lại
model đã lưu, giữ source/input/config/versions cố định và khớp cả hai snapshot
digests theo độ chính xác float sáu chữ số thập phân.

Đã sửa lỗi auto-broadcast factor sau reload gây thiếu heap; cache tạm tránh tính
lại recommendation và được giải phóng. Regression mới: **91 passed, 0 skipped**,
Ruff và standalone runtime smoke đạt. ALS baseline chưa vượt personal frequency;
đây là nghiệm thu tính đúng đắn/tái lập, không phải cam kết chất lượng tối ưu.
GitHub runtime CI của commit `11ac21c` là bằng chứng lịch sử, không phải CI của
workspace chưa commit ngày 10/10. Chi tiết tại mục 12.6 và
[acceptance-20261010](evidence/module4/acceptance-20261010/README.md).
Các mốc cũ được giữ tại [acceptance-20261009](evidence/module4/acceptance-20261009/README.md)
và [validation-20261008](evidence/module4/validation-20261008/README.md).

Trong tài liệu này, “chạy hoàn chỉnh” nghĩa là chạy đúng contract, kết quả có thể
đối soát và tái lập trong runtime đã pin, xử lý thất bại có kiểm soát và có một run
nghiệm thu trên nguồn dữ liệu đầy đủ. Đây không phải cam kết không còn lỗi trong
mọi môi trường hoặc đã đáp ứng năng lực production.

Current implementation status and limitations are maintained in
[progress.md](progress.md); operational procedures are in [docker.md](docker.md).
Detailed C8 and D3 records and the retired standalone documents are preserved
under [docs/evidence](evidence/README.md).

## Mục lục

- [1. Phạm vi](#1-phạm-vi)
- [2. Mục tiêu nghiệm thu](#2-mục-tiêu-nghiệm-thu)
- [3. Hợp đồng dữ liệu](#3-hợp-đồng-dữ-liệu)
- [4. Cấu trúc mã đề xuất](#4-cấu-trúc-mã-đề-xuất)
- [5. Module 4A — ALS Recommendation](#5-module-4a--als-recommendation)
- [6. Module 4B — Product Co-purchase Graph](#6-module-4b--product-co-purchase-graph)
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

Đã bổ sung `config.py`, `data.py`, `split.py`, `als.py`, `evaluation.py`,
`graph.py`, `sink_mongo.py`, `spark.py`, `cli.py`, host runner, test contracts,
live-Mongo tests, runtime smoke scripts và workflow CI. Các contract chính được
hiện thực theo hướng prior-only, internal holdout, hai baseline, canonical graph
và staged publication. Các đường chạy Spark/GraphFrames/Mongo đã được xác minh
trên fixture và trên nguồn đầy đủ. Nghiệm thu full-data và same-version replay
đã hoàn thành ngày 10/10/2026, xem mục 12.6. Mục 12.1–12.5 giữ các checkpoint
lịch sử ngày 08/10; các trạng thái còn mở trong đó được thay thế bởi mục 12.6.

### 12.1. Checkpoint lịch sử 08/10 — bằng chứng và giới hạn

- Audit 05/10 chỉ có contract tests và lỗi runtime/temp trên host. Các số liệu đó
  là lịch sử; kết quả hiện tại dưới đây được đo ngày 08/10/2026.
- Docker Desktop đã thử khởi động nhưng engine lỗi. Đã dùng Docker Engine 28.5.1
  riêng trong Ubuntu WSL, socket `/run/module4-docker.sock`, data root
  `/var/lib/module4-docker-20261008`; không sửa hoặc xóa shared Docker volumes.
- Build thành công `instacart-spark:3.5.5` từ base digest đã pin. Runtime smoke
  pass: Spark 3.5.5, Scala 2.12.18, GraphFrames Python 0.6/JAR 0.8.3, PageRank
  giữ isolated vertex, worker đọc/ghi 2 HDFS rows và MongoDB ping thành công.
- Regression toàn repo: **78 passed, 0 skipped**. Lượt Module 4 sau khi bổ sung
  negative data/receipt/backend tests: **33 passed, 0 skipped**. Các lượt này độc
  lập với CLI run; không cộng thành 111 test khác nhau hoặc coi là full-data run.
- `flow-923e255d911244e2b368be3809cc2bd7-first`: **passed**; 3 prior users,
  9 recommendations, 5 graph products, gồm user thuộc official test và cold/isolated
  catalog products. Model save/load, threshold 2/5/10, evaluation/baselines,
  Mongo pre/post-promote và pair readiness đều có artifacts.
- `flow-923e255d911244e2b368be3809cc2bd7-replay`: **failed** tại metadata guard.
  Source/config đã thay đổi trong lúc lượt đầu đang chạy, nên existing model
  version bị từ chối đúng policy. Giữ nguyên failed evidence; chưa đóng gate
  replay model với input/config/source cố định. Replay/fault preservation của
  Mongo snapshot riêng đã pass trong live tests.
- `cross-932c44d6fcc4439ba10d5f8e4979a272`: **passed**; Module 1 partition/receipt
  fixture → Module 2 validate/analytics và 4 user features → Module 4 all trên
  standalone worker. Input counts/hashes đối soát; publish 24 recommendations,
  6 graph documents và pair readiness=true. Fixture database/HDFS root đã cleanup.
- Sáu CSV bản mirror v1 đã tải và đối soát: 3.421.083 orders; 32.434.489 prior;
  1.384.617 train; 49.688 products; 134 aisles; 21 departments. Clean và staging
  HDFS `/instacart/curated` kèm `_sources.json` thành công. Đây là bằng chứng chuẩn
  bị nguồn, chưa phải validation/training/publication Module 4 trên full data.
- `full-input-20261008` là lệnh **validate-only**, đã dừng theo yêu cầu người dùng
  khi chờ worker; chưa hoàn thành input gate, chưa fit ALS/graph hoặc publish.
  Không còn job Module 4 đang chạy; các service và full curated data được giữ lại.
- Workflow CI đã bổ sung staged replay và cross-module smoke. Chưa có kết quả
  GitHub Actions từ bản commit này. Ruff/compile/Compose/help được kiểm tra cục bộ;
  compact JSON và actual command/test logs ở [evidence](evidence/module4/validation-20261008/README.md).

### 12.2. Các vấn đề của audit ban đầu

Bảng dưới ghi nhận lỗi được phát hiện trước khi sửa; trạng thái sửa hiện tại nằm
ở mục 12.3. Các gate runtime tương ứng vẫn mở cho tới khi có evidence thật.

| ID | Mức | Vị trí | Hiện trạng và tác động |
|---|---|---|---|
| R01 | P0 | `als.py::recommend_top_k` | Đọc `recommendation.als_score`; Spark 3.5.5 trả field `rating`. Tuning/recommendation bị chặn. |
| R02 | P0 | `cli.py::main`, `Config.environment` | CLI tạo `Config()` trực tiếp; bỏ qua Mongo URI/database/collection từ environment. Trong Compose sẽ dùng `localhost` thay vì service `mongodb`. |
| R03 | P0 | `scripts/module4.py` | `python scripts/module4.py --help` trên host lỗi import `src.module4`; runner chưa tương đương Module 2–3. |
| R04 | P1 | `cli.py::run_als` | Publish chỉ cho user trong official train truth, bỏ warm users thuộc official test. Nhãn warm cohort chưa lọc theo factors. |
| R05 | P1 | `evaluation.py` | MAP/NDCG dùng độ dài prediction thực trả thay K; coverage đếm user/product ngoài cohort, có thể vượt 100%. |
| R06 | P1 | `graph.py::pagerank` | Bắt mọi exception rồi âm thầm fallback; một run pass chưa chứng minh GraphFrames thực sự chạy. |
| R07 | P1 | Graph validation/sink | Chưa kiểm tra đủ Infinity, null/integer weight, FK và tập vertex; document graph thiếu `min_edge_weight`. |
| R08 | P1 | `data.py`, manifest | Chưa đọc/đối soát `_sources.json`, chưa có input digest liên kết với handoff Module 1–2. |
| R09 | P1 | `cli.py::tune_als` | Mới chọn model từ pilot; thiếu vòng internal validation full-size cho shortlist, giải phóng factors và evidence trial đầy đủ. |
| R10 | P1 | `sink_mongo.py`, CLI | Chưa gate đủ schema/count/rank/score trước promote; validation sau publish chưa đối chiếu đủ expected counts. |
| R11 | P1 | Evidence/tests/CI | Chưa có Spark/Mongo flow test, full-data run, actual test logs, backend/version và shuffle evidence. |
| R12 | P1 | `progress.md`, `README.md`, mục 12 cũ | Progress ghi đã chạy/publish; README vẫn ghi chưa triển khai; trạng thái và bằng chứng không đồng bộ. |

P0 là lỗi chặn đường chạy chuẩn. P1 là lỗi tính đúng đắn hoặc thiếu điều kiện
nghiệm thu phải đóng trước khi công bố hoàn thành. Tối ưu bổ sung và Connected
Components là P2, chỉ làm sau khi các gate bắt buộc đã đạt.

### 12.3. Checkpoint 08/10 — phần đã làm và trạng thái từng vấn đề

| ID | Phần đã triển khai trong workspace | Trạng thái xác minh |
|---|---|---|
| R01 | Nested `rating` → `als_score`; bỏ `predictionCol` không được ALS constructor PySpark 3.5.5 chấp nhận; tie ở biên K. | Real fit/recommend, save/load, repeat-seed và equal-score fixture pass. |
| R02 | Env/CLI precedence; Mongo preflight; resource/ALS-block options và shared HDFS checkpoint. | DNS/HDFS/Mongo standalone smoke và fixture publication pass. |
| R03 | Lazy host runner, `up --wait`, aliases, local Git ownership override. | Host help pass; wrapper đã launch full-input container nhưng job bị dừng trước completion. |
| R04 | Publication dùng mọi prior-factor users. | Fixture đầu có user official test; 9-row cohort và cross 24-row snapshot đối soát. |
| R05 | Spark-native AP/NDCG; nhiều K trong cùng lượt, cohort coverage đúng. | Hand cases, short/missing/extra/empty users, duplicates và fixture baselines pass. |
| R06 | GraphFrames thật; DataFrame backend chỉ cho explicit fixture scope. | Driver/worker PageRank, isolation/empty-edge và injected dependency failure tests pass. |
| R07 | Canonical edges, conservation, finite/FK checks và threshold metadata. | Graph thresholds 2/5/10 và Mongo graph outputs trên fixture pass; full/subset chưa chạy. |
| R08 | SHA-256 stream trong JVM; JSON write bằng bytearray; sửa split kiểm tra distinct orders thay facts. | Negative count/hash/FK/eval-set/chronology và HDFS fixture receipts pass; full input pending. |
| R09 | Pilot → full internal shortlist → final fit; deterministic materialization, incomplete-trial gate và model-byte metadata. | Hai fixture runs có pilot/full internal/final model; full Instacart tuning chưa chạy. |
| R10 | Exact index key/options validation, bounded snapshot writer, lock và readiness. | Live Mongo insert/count/index/rename fault preservation pass; cả A/B fixture publish pass. |
| R11 | Runtime, staged replay, cross-module smoke, actual logs và CI workflow. | Local suites 78/33 pass; cross flow pass; stable-source replay, GitHub CI và full run còn mở. |
| R12 | Đồng bộ plan/progress/README/Proposal; tách A/B fixture acceptance khỏi full data và C/D. | Báo cáo/link evidence cập nhật 08/10/2026; chưa ghi A/B complete. |

Ngoài các mục trên, test fixtures/contracts đã được mở rộng cho config/env,
short-list MAP/NDCG, ALS tie tại ranh giới K và Mongo snapshot/replay/failure.
Kết quả integration đã lưu riêng; không thay thế nghiệm thu full-data.

### 12.4. Luồng liên kết phải được kiểm chứng

```text
Module 1 clean + stage
  -> HDFS curated tables + _sources.json
  -> Module 2 validates the same handoff and creates optional user_features
  -> Module 4 validates input receipts/keys/counts
       -> 4A: prior holdout -> tuning -> final prior model -> official evaluation
                -> all warm-user recommendations -> Mongo snapshot
       -> 4B: prior baskets -> threshold experiment -> GraphFrames metrics
                -> Mongo snapshot

Module 1 producer -> Kafka -> Module 3 -> realtime_trending
  -> future Module 4C: combine recommendations + graph + trend
```

Module 4A–4B đọc curated data của Module 1, không cần chờ một streaming query của
Module 3 chạy. Module 2 `user_features` là đầu vào cohort tùy chọn. Việc chưa đọc
`realtime_trending` không phải lỗi phạm vi A/B; tránh bổ sung Kafka/trend vào quá
trình fit hoặc tuning. Feed của Module 3 có cả prior + train nên kết hợp trend
vào đánh giá sau này cần một thiết kế cutoff riêng ở Module 4C.

### 12.5. Bàn giao W01–W12 và bước tiếp theo khi tiếp tục

Tất cả thay đổi và evidence trong mục này được thực hiện ngày **08/10/2026**.
Việc thực thi đã dừng theo yêu cầu người dùng; bảng không phải cam kết các gate
full-data đã hoàn thành.

| Gói | Files chính | Đã xác minh | Còn lại |
|---|---|---|---|
| W01–W03 | `als.py`, `spark.py`, `config.py`, `cli.py`, `scripts/module4.py` | Real ALS, GraphFrames driver/worker, HDFS/Mongo smoke và host help | Hoàn tất một host-wrapper job sau khi worker rảnh |
| W04–W05 | `data.py`, `split.py`, `evaluation.py`, `tests/test_module4.py` | Receipt/schema/FK/leakage/chronology, hand metrics và cohorts trên fixture | Full-input validation và full-data metrics |
| W06–W07 | `als.py`, `cli.py` | Pilot/full internal/final fit và warm/test publication trên fixture | Full-data tuning/save-load/publication; stable-source model replay |
| W08 | `graph.py`, `config.py`, `cli.py` | GraphFrames, thresholds, invariants, isolation trên full-catalog fixture | Đo full pair expansion, xác minh subset nếu cần, công bố coverage |
| W09 | `sink_mongo.py`, `tests/test_module4_mongo.py` | Live Mongo replay/faults, indexes và hai validated snapshots | Replay toàn pipeline với source/config cố định; full output validation |
| W10 | `cli.py`, `spark.py`, `docs/evidence/module4/` | Success/failure manifests, actual logs, job-group shuffle và model size | Finalize evidence của full run khi chạy xong |
| W11 | `scripts/module4_cross_module_smoke.py`, `scripts/module4_flow_smoke.py`, workflow | Cross flow pass; 78-test regression và 33-test Module 4 pass | GitHub Actions bản mới và stable-source staged replay |
| W12 | Plan/progress/README/Proposal | Báo cáo hiện trạng và compact evidence đã đồng bộ | Một full-data A/B run với serving thật và nghiệm thu cuối |

Khi tiếp tục, giữ source cố định trong toàn bộ `first`/`replay`, chạy lần lượt để
tránh tranh 2 worker cores và trùng service alias `module4`. Dùng run ID mới;
`full-input-20261008` đã có manifest nên không tái sử dụng output directory đó.
Chạy lại `module4_flow_smoke.py`, xem GitHub CI, sau đó validate full input và
chỉ khởi chạy `all` khi worker đã rảnh. Full source đã được stage trong engine
WSL riêng; không cần tải hoặc stage lại để tiếp tục trên máy này.

```bash
# Chỉ dành cho engine WSL đã tạo trong lượt này
export PATH=/opt/module4-runtime-20261008/docker:$PATH
export DOCKER_HOST=unix:///run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config

docker compose run --rm --use-aliases --no-deps --entrypoint python3 module4 \
  scripts/module4_flow_smoke.py

python3 scripts/module4.py validate --scope full_data \
  --source-receipt data/raw/source-receipt.json --shuffle-partitions 96
```

`--graph-top-products` đã được thêm như fallback theo mục 6.1, nhưng **chưa được
nghiệm thu runtime subset**. Nếu dùng cho full run, phải kiểm tra trước trên
bounded data và giữ nhãn subset/selected-ID digest/catalog-fact coverage trong
report. Không ghi đã chạy graph subset hoặc full-catalog Instacart tại mốc dừng.

### 12.6. Nghiệm thu hoàn tất ngày 10/10/2026

- **Đầu lượt: 20/21; cuối lượt: 21/21 gate.** Full input khớp nguồn và receipts:
  3.421.083 orders, 32.434.489 prior facts, 1.384.617 train facts, 49.688 products.
  Internal holdout có 206.209 orders/2.139.788 facts; training 30.294.701 facts,
  không giao order sets và không chứa official train.
- Hai lượt full-data `full-ab-20261010-fixed-first`/`-replay` passed. Model
  `als-full-20261010-fixed` có 206.209 user factors/49.677 item factors, fit trên
  13.307.953 unique interactions. Save/load được đối soát; replay thực sự load
  existing model. Pilot 5.000 users, một cấu hình ALS, full internal validation;
  official train chỉ dùng đánh giá cuối, không chọn params.
- MongoDB đã publish và read-back độc lập **4.124.180 recommendations**, 20/user
  cho toàn bộ 206.209 prior-factor users, và **49.688 graph documents**. Schema,
  indexes, counts, versions và readiness đạt; replay khớp logical digests của cả
  hai collection. Scores/PageRank so ở sáu chữ số thập phân; các khóa, membership,
  rank, degree, version và threshold khớp chính xác.
- Graph dùng toàn catalog và tất cả prior baskets, không sampling. Threshold
  2/5/10 được đo; xuất threshold 10 với 3.492.837 canonical edges, 6.985.674
  directed edges, 21.164 isolates. Active catalog coverage **57,41%**, prior fact
  coverage **98,79%**; catalog vertices vẫn đầy đủ. Weighted-degree sum
  324.442.508 = 2 × retained edge weight 162.221.254. GraphFrames thật và finite
  PageRank đạt. Threshold/coverage/pair expansion/runtime có report riêng.
- Official train: 131.209 users; K=5/10/20, warm/all và reordered/new cohorts.
  Tại K=10, ALS Recall/NDCG = **0,121472/0,137102**, global popularity =
  **0,069843/0,097881**, personal frequency = **0,319232/0,386763**. ALS chưa vượt
  personal frequency; một cấu hình là baseline có giới hạn, không phải tối ưu
  toàn bộ search space. Kết quả này phù hợp tiêu chí nghiệm thu đúng contract
  và báo cáo minh bạch, không đặt thêm quality threshold sau khi đo.
- Lỗi full-data đầu tiên: auto-broadcast factor blocks sau reload làm giảm
  parallelism và thiếu heap. Đã tắt auto-broadcast, cache candidates trên disk,
  giải phóng caches, test bằng session thật và kiểm tra distributed reload plan.
  Regression toàn repo **91 passed, 0 skipped**, năm warnings, 165,09 giây;
  Ruff/runtime smoke đạt. Saved-model probe full-size cũng passed. Evidence lỗi
  được giữ nguyên; không đổi failed run thành passed.
- Bàn giao: active run `full-ab-20261010-fixed-replay`, model/graph versions
  như trên, `module4_snapshot_readiness` (`_id=active`, `ready=true`). Consumer
  xác minh pointer và versions trước khi đọc hai collection. Storage HDFS/Mongo
  nằm trong ext4 image được giữ trên D:, remount trước khi khởi động lại WSL.
- Runtime nghiệm thu single-host: 4 cores, executor 2 GiB, driver 1 GiB,
  shuffle 32, ALS blocks 8, native Linux shuffle. Không suy ra production
  capacity. Connected Components và Module 4C–4D nằm ngoài checklist A/B.

Actual commands, logs, timings, event metrics, source hashes, first/replay
summaries và read-back receipts có tại
[acceptance-20261010](evidence/module4/acceptance-20261010/README.md).
GitHub runtime CI `37798558046`/`11ac21c` là bằng chứng 09/10; workflow full-data
chưa dispatch. Toàn bộ kiểm tra của source hiện tại được chạy cục bộ trong image
đã pin, không gán kết quả CI cũ cho source chưa commit.

## 13. Backlog thực hiện và thứ tự phụ thuộc

Implementation cho phần lớn W01–W11 đã được thêm, nhưng mỗi dòng chỉ được đóng
khi đạt điều kiện ở cột cuối. Tĩnh/unit pass không thay cho runtime gate.

| Gói | Ưu tiên | Công việc | Phụ thuộc | Điều kiện đóng |
|---|---|---|---|---|
| W01 | P0 | Sửa schema recommendation ALS | Không | Spark fixture fit/recommend thành công, nested `rating` đổi thành output `als_score` |
| W02 | P0 | Sửa runner, environment, driver DNS và preflight | Không | Host runner và standalone executor truy cập driver/HDFS/Mongo đúng cấu hình |
| W03 | P1 | Pin và smoke GraphFrames Python/JAR | W02 | PageRank thật trong image, có backend/version; lỗi dependency làm job fail |
| W04 | P1 | Receipt, data validation và leakage assertions | W02 | Fixture âm tính bị từ chối; input counts/hash khớp staged receipt |
| W05 | P1 | Metric, baseline, warm/all/history cohorts | W04 | Ví dụ tính tay và Spark cùng kết quả, coverage không vượt 1 |
| W06 | P1 | Pilot + full internal shortlist + final model | W01, W04, W05 | Trial evidence đủ, chọn model từ internal validation; save/load xác minh được |
| W07 | P1 | Sinh và kiểm tra recommendation cho mọi warm user | W06 | User-set/count/rank/score khớp factors, gồm user thuộc official test |
| W08 | P1 | Graph scope, threshold, invariants, PageRank | W03, W04 | Threshold 2/5/10, conservation/FK/isolates/finite checks đạt |
| W09 | P1 | Mongo snapshot validation, promote và recovery | W07, W08 | Publish/rerun/fault-injection đạt; expected counts và indexes khớp |
| W10 | P1 | Manifest, failure evidence, event-log metrics | W02; xuyên suốt W04–W09 | Artifact từng stage có cùng run ID, source/input digests và trạng thái rõ |
| W11 | P1 | CI và fixture flow Module 1–2–4 | W01–W10 | Pinned CI chạy Spark, GraphFrames và Mongo thực; không skip gate bắt buộc |
| W12 | P1 | Full-data run, báo cáo và rehearsal | W11 | Một run đầy đủ passed, hai snapshots đối soát; báo cáo trỏ đúng evidence |
| W13 | P2 | Tối ưu tiếp, Connected Components | W12 | Có mục tiêu đo được; không thay đổi contract đã nghiệm thu |

## 14. Chi tiết công việc theo thành phần

### 14.1. W02 — Runner, config và preflight

File chính: `scripts/module4.py`, `config.py`, `cli.py`, `spark.py`, `compose.yaml`.

- Đổi host runner thành container runner như Module 2–3: xác định repo root từ
  `__file__`, đặt `cwd` đúng, forward arguments và truyền commit. Host chỉ cần
  Python + Docker; import PySpark nằm trong container.
- Hỗ trợ `--help` không cần Spark/services, `--build` cho runtime đã thay dependency
  và trả exit code khác 0 khi Docker/Spark/job thất bại. Không tạo lifecycle
  side-effect từ một lệnh chỉ hỏi help.
- Runner `compose run` dùng `--use-aliases` hoặc cấu hình driver address khác đã
  được kiểm chứng. `spark.driver.host=module4` phải resolve được từ worker.
  Hướng dẫn chạy trực tiếp Compose phải có cùng cấu hình DNS với runner.
- Không tự dùng `--remove-orphans`, `down -v` hay dọn shared volumes để chạy A/B.
  Chỉ khởi động các service cần thiết; giữ Kafka/streaming và dữ liệu module trước.
- Tạo config qua `Config.environment(...)`; precedence rõ ràng:
  **CLI được chỉ định > environment > default**. Không để argparse default
  vô tình ghi đè environment khi user không truyền cờ.
- Áp dụng URI/database/collection cho Mongo, root/master/seed, partitions, K,
  graph threshold/reset/iterations và giới hạn tuning. Validate input số nguyên,
  hữu hạn, miền giá trị; không nhận NaN/Infinity hay âm cho pilot/trial budgets.
- Tách `publish_k` khỏi `evaluation_ks` hoặc bảo đảm generation cho evaluation
  luôn đủ `max(5,10,20)`. Giảm publish K không được làm metric @20 bị hụt candidate.
- CLI graph-only/validate-only không fit ALS; help/config parsing không cần
  import runtime nặng. Công bố config đã resolve với credential được che.
- Preflight: Docker engine/image, Spark master/worker, driver DNS, quyền đọc
  curated HDFS, quyền ghi model và evidence, Mongo ping/rename/index privilege.
  `--skip-serving` phải bỏ yêu cầu Mongo, ghi serving skipped trong report.
- `--skip-serving` còn phải chạy được khi Mongo không khả dụng: kiểm tra cả
  `depends_on` của Compose/runner, không chỉ bỏ ping trong CLI. ALS checkpoint
  dùng đường dẫn shared HDFS worker truy cập được; ghi checkpoint interval,
  storage level và chính sách dọn checkpoint theo run ownership.
- Các đường dẫn ghi model/output/version phải cụ thể, không chứa path traversal;
  run/version IDs có format hợp lệ. Không overwrite một version đã tồn tại nếu
  khác config/input digest; reuse phải có quy tắc riêng và explicit.

Gate: chạy help trên host sạch; kiểm tra CLI/env precedence; chạy Spark action
qua worker và HDFS; ping đúng service Mongo trong container. Việc parse Compose
thành công chưa chứng minh DNS hoặc kết nối runtime thành công.

### 14.2. W03 — Runtime GraphFrames được kiểm chứng

File chính: `Dockerfile` (target `spark`), `requirements*.txt`, `spark.py`, `graph.py`.

- Giữ Spark 3.5.5, Java 17, Scala 2.12; chọn/pin Python wrapper và JAR GraphFrames
  thực sự tương thích. Hiện Python package là `graphframes==0.6`, JAR là
  `0.8.3-spark3.5-s_2.12`; chưa được kết luận tương thích chỉ từ tên version.
- Checksum JAR trong build, có JAR ở cả driver và workers, không tải Maven khi
  chạy job. Ghi phiên bản package, JAR filename/hash và Scala trong manifest.
- Chạy smoke graph có cạnh hai chiều và isolated vertex trên image đã build,
  rồi chạy qua standalone worker. Assert backend thực sự là `graphframes`.
- Bỏ `except Exception: pass` trong PageRank đường nghiệm thu; giữ stack trace
  và làm job fail khi GraphFrames thiếu hoặc lỗi runtime.
- Nếu giữ DataFrame backend để phát triển, chọn explicit, chỉ cho fixture và
  ghi backend/scope. Không dùng kết quả đó để đóng gate GraphFrames/full-data.
- Hai backend hiện khác cách chuẩn hóa và xử lý dangling mass. Chọn và ghi
  PageRank contract cho runtime chính; chỉ so sánh số giữa backend khi đã làm
  đồng nhất thuật toán, normalization và isolated-vertex policy.

Gate: JAR/Python smoke thật pass, backend metadata có đủ; mô phỏng thiếu JAR hoặc
lỗi JVM phải tạo report failed, không chuyển thành một run passed bằng fallback.

### 14.3. W01, W06, W07 — ALS input, tuning, model và recommendation

File chính: `als.py`, `split.py`, `cli.py`.

**Sửa lỗi chặn và kiểm tra input:**

- Đọc `recommendation.rating`, cast sang double và alias `als_score`. Trong
  [Spark ALS 3.5.5](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala),
  `recommendForUserSubset` có nested rating field cố định; `predictionCol` dùng
  cho `transform`, không đổi field này.
- ID user/product phải là integer đúng giá trị trong int32 trước cast; từ chối
  fractional, overflow, null, non-positive. Strength/purchase count hữu hạn và
  dương; không để cast âm thầm làm hai ID khác nhau thành cùng một ID.
- One row/user/product; lưu distribution purchase_count/strength, source scope
  và interaction count. Deterministic partition/sort, materialize training input
  phù hợp để ALS không phụ thuộc một lineage không xác định.
- Kiểm tra model có user/item factors không rỗng và không có factor/score lỗi.
  Công bố số factors khớp distinct IDs của training interactions.

**Internal split và tuning:**

- Assert `(user_id, order_number)` xác định duy nhất một order; last prior order
  phải duy nhất mỗi eligible user. Training order numbers nhỏ hơn holdout.
- Kiểm tra training/validation order sets disjoint và cùng bao phủ eligible facts;
  official train order IDs không xuất hiện trong cả hai. Report excluded users,
  rows/orders/products và cold-start products của internal holdout.
- Trường hợp không có eligible users: report tuning not performed; baseline
  fallback fixture có thể được công bố riêng, không đóng gate tuning nghiệm thu.
- Pilot user sample bằng hash có seed hoặc quy tắc hash đã ghi; hash tie-break
  bằng user ID; giữ tất cả prior orders của user đã chọn. Dedupe grid để tránh
  cùng baseline xuất hiện nhiều trial; giới hạn tối đa 12 cấu hình distinct.
- Tính baseline trên đúng pilot cohort từ internal training history; không dùng
  full-prior popularity khi đánh giá holdout vì có thể đưa holdout trở lại baseline.
- Lấy tối đa 3 cấu hình tốt nhất pilot, fit trên full internal training và đánh
  giá full internal holdout. Chọn final params từ vòng này, không chọn trực tiếp
  pilot rồi gọi đó là full tuning.
- Trial ghi input/factor counts, params/strength/seed, P/R/coverage, fit time,
  recommendation time, evaluation time, total time và resource/job-group metrics.
- Persist input dùng chung có budget, unpersist model factors/trial output trong
  `finally` để 12 model không chiếm bộ nhớ đồng thời. Dọn cache theo ownership,
  không ảnh hưởng cache của một job ngoài phạm vi.
- Trial failure lưu error/stack trace và input/config ngay khi xảy ra. Nếu mọi
  trial fail, vẫn phải có `als_trials.json`; CLI hiện chỉ ghi trial file sau
  `run_als` thành công nên cần sửa failure path.
- Khi full shortlist chưa đủ trials hợp lệ, khai báo incomplete/degraded;
  không đánh dấu toàn bộ run nghiệm thu passed để che thiếu stage.

**Final model và publication users:**

- Freeze selected params, refit toàn bộ prior, save model versioned bằng chế độ
  tránh overwrite. Reload model và kiểm tra factor counts/schema/recommendations.
- Official train chỉ được đánh giá sau freeze params; không dùng official score
  chọn model, threshold hay fallback strategy. Baseline final dùng full prior.
- Publication cohort là tất cả prior users có `userFactors`, gồm user có terminal
  order `eval_set='test'`. Evaluation cohort official train là một tập riêng.
- Sinh Top-K không loại seen products. Output finite, positive IDs, product FK,
  unique user/product, rank 1..min(K, available factors), mỗi user có số hàng đúng.
- Tie-break score DESC/product ID ASC áp dụng cả membership ở biên K. Sorting
  sau một Top-K đã cắt chưa bảo đảm chọn đúng nhóm equal-score ở biên; thêm test
  tie và chọn candidate/score strategy bảo đảm contract, có budget rõ ràng.
- Report publication requested users, factor-backed users, missing users,
  expected vs actual rows và số item factors giới hạn list size.

Gate: model thật fit/save/load/recommend pass; publication tập user đúng; rerun
cùng input/seed/config cho ranking fixture như nhau trong tolerance đã công bố.

### 14.4. W04 — Data handoff và input validation

File chính: `data.py`, `config.py`; đối chiếu Module 1 staging và Module 2 data.

- Đọc `/instacart/curated/_sources.json`; đối chiếu counts và SHA-256 của các file
  curated thực đang đọc với receipt, không chỉ chép hash trong receipt vào report.
  Ghi receipt digest, dataset scope, paths/file inventory và thời điểm validation.
- Các table hiện có một source Parquet/file staging; nếu hỗ trợ nhiều part files,
  định nghĩa manifest/digest tổng hợp có thứ tự và không đổi nghĩa SHA-256 file.
- Input phải được giữ ổn định trong run: dùng snapshot bất biến hoặc kiểm tra
  inventory/digest trước và sau. Nếu Module 1 stage lại cùng root giữa run,
  không promote output ghép từ hai dataset khác nhau.
- Tận dụng helper đọc HDFS receipt nếu phù hợp nhưng không ép A/B scan toàn bộ
  streaming interactions chỉ để xác minh bốn input curated cần thiết.
  `_handoff.json` của event feed chỉ là provenance bổ sung khi dùng feed đó.
- Giữ separation tên file local `order_products__prior.parquet` và HDFS directory
  `order_products_prior`; mô tả rõ để runner không nhầm root `data/clean` là layout
  HDFS curated.
- Validate schema và types, critical nulls (gồm product name), keys, integer
  domains, order/user/cart position và reordered 0/1; duplicate order/product,
  duplicate `(user_id, order_number)` và cart-position consistency.
- FK order/product, eval_set và counts trước/sau join; không thêm user metadata
  bằng một join có thể nhân rows rồi chỉ kiểm tra count ở cuối.
- Kiểm tra mọi prior/train order có fact theo contract; user ground truth trống
  được ghi số lượng/lý do loại. Đối chiếu prior users, train users và catalog với
  source hiện tại, không hard-code con số historical làm nguồn xác thực.
- `prior`/`train` fact order sets disjoint; chỉ bảng prior đã enrich đi vào
  interactions/graph. Handoff/leakage flags phải xuất phát từ assertion.
- Optional `user_features`: nếu bật cohort features, đọc đúng path từ Module 2,
  validate unique user/schema/prior-only provenance và report coverage. Nếu không
  bật thì ghi unused/optional; không quảng bá ALS đã dùng features này.
- Không ghi vào curated source tables hoặc sửa receipt của Module 1–2 trong
  quá trình Module 4 chạy.

Gate: mọi fixture lỗi schema/FK/domain/hash/count/chronology bị từ chối trước fit;
các input digest/counts Module 2 và Module 4 khớp cùng staged dataset.

### 14.5. W05 — Metrics, baseline và cohort chính xác

File chính: `evaluation.py`, `cli.py`.

- Chuẩn hóa truth thành tập distinct products; user truth unique; recommendation
  key/rank unique, positive và trong miền K. Invalid rows phải fail validation,
  không âm thầm bỏ để làm metric tốt hơn.
- Truyền K explicit vào AP/NDCG helper. Khi list trả ít hơn K vẫn dùng:

```text
Precision@K(u) = hits / K
Recall@K(u)    = hits / |G(u)|
AP@K(u)       = sum(hit_at_rank_i * precision_at_i) / min(K, |G(u)|)
IDCG@K(u)     = sum(1 / log2(i + 1), i = 1..min(K, |G(u)|))
NDCG@K(u)     = DCG@K(u) / IDCG@K(u)
```

- Ví dụ bắt buộc: K=5, predictions `[1]`, truth `[1,2,3,4,5]` -> P=0.2,
  R=0.2, AP=0.2, NDCG xấp xỉ 0.33916. Code hiện trả AP/NDCG=1 cho ví dụ này.
  Đối chiếu [Spark RankingMetrics 3.5.5](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala).
- User coverage chỉ đếm predicted users thuộc cohort/nonempty truth; catalog
  coverage chỉ đếm predictions của cohort và chia đúng candidate-catalog scope.
  User ngoài truth, truth trống và duplicates không được làm coverage vượt 1.
- Tính average recommendation count/user gồm user không được trả recommendation;
  report min/max/count distribution, skipped users và empty-cohort reason. Cohort
  rỗng phải có trạng thái not applicable, không coi số 0 là một lần đánh giá hợp lệ.
- Warm cohort định nghĩa cụ thể: user có factor và toàn bộ truth products có
  item factors. All-train cohort giữ nguyên mọi nonempty truth users/products;
  cold items vẫn ở mẫu số và được report vì global fallback không thể tạo factor.
- Report riêng user cold-start, item cold-start, user có mixed warm/cold truth,
  tỷ lệ truth items có factor. Không dùng `train_truth` làm warm filter.
- Reordered/new-to-user được phân loại từ lịch sử training thích hợp với stage;
  đánh giá từng nhóm giữ nguyên ranked predictions và chỉ đổi truth. Mỗi nhóm
  có cohort/user coverage riêng, không dùng toàn bộ user numerator cho subgroup.
- Global popularity và personal frequency dùng cùng users, truth, K và candidate
  scope với ALS ở mỗi cohort. Personal thiếu list bổ sung global, loại duplicate
  products, giữ personal trước rồi rank fallback deterministic.
- Baseline cho all/cold users trả fallback đã công bố; score baseline không được
  ghi nhầm là model prediction trong production recommendation collection.
- So sánh P/R@5/10/20; ghi delta ALS vs mỗi baseline, không tuyên bố cải thiện nếu
  số liệu không hỗ trợ. Khi giữ UDF, bảo đảm callable import được trên executor;
  dùng tính toán Spark native nếu kiểm tra cho thấy giảm chi phí có ý nghĩa.

Gate: hand-calculated cases, missing/short list, cold users/items, empty cohort,
extra users, duplicates và subgroup đều pass; mọi coverage thuộc [0,1].

### 14.6. W08 — Graph build, threshold và invariants

File chính: `graph.py`, `cli.py`.

- Freeze scope: mặc định vertices là toàn bộ `products` catalog như code hiện tại,
  gồm products không có prior activity. Report catalog/prior-active/edge-active
  counts và lý do isolated (no prior hoặc bị threshold loại hết cạnh).
- Baskets chỉ từ prior; product dedupe trong order; canonical pair `src < dst`;
  weight là số order chứa pair, không phải số dòng fact hoặc số directed edges.
- Tính raw co-purchase counts một lần rồi reuse cho thresholds 2/5/10 và final
  threshold khi tài nguyên cho phép. Hiện self-join/groupBy bị chạy lại nhiều lần.
- Đo số baskets, max/quantiles basket size và tổng pair expansion trước full run.
  Cấu hình hiện chỉ 2 cores/1 GB executor không được coi là đủ cho full graph
  nếu chưa đo spill/shuffle/memory/disk.
- Mỗi threshold có vertex/edge/directed counts, total weight, active product và
  prior-fact coverage theo công thức đã ghi, isolated count, degree/weight
  quantiles, runtime và event-log shuffle/spill metrics.
- Final threshold do cấu hình/analysis prior-only chọn; evidence ghi lý do và
  reference đến trial tương ứng. Không ghi “tuned optimum” cho một default chưa
  được phân tích.
- Nếu cần top-P subset, chọn prior popularity deterministic, giữ đầy đủ fact/order
  semantics trong scope đã chọn, ghi `graph_scope`, P, selected IDs/digest và
  catalog/prior-fact coverage. Đó là graph subset trên nguồn full-data, không phải
  full-catalog graph; không âm thầm cap baskets hoặc sample pairs.
- Validate schema/types; null/fractional/non-positive weights; canonical pairs,
  duplicate edges, endpoint FK và vertex uniqueness. Directed graph đúng hai
  reciprocal rows/undirected edge, không có row bổ sung.
- Validate output exact product set với vertices bằng anti-join hai chiều, không
  chỉ so count. Assert degree <= vertex_count-1, finite/non-null/non-negative
  metrics và `sum(weighted_degree)=2*sum(edge weight)`.
- PageRank GraphFrames thật trên symmetric topology; weight co-purchase chỉ dùng
  weighted degree. Check NaN và cả ±Infinity; không gắn `pagerank_finite=True`
  nếu các check này chưa thực thi.
- Edge-empty/one-vertex/isolated/disconnected graphs có semantics đã định nghĩa
  và được test; isolate không bị mất khi join PageRank. Top-20 sắp metric DESC,
  product ID ASC, kèm product name.
- Document final có `graph_version`, `run_id`, `min_edge_weight`, `computed_at`;
  graph backend/scope và PageRank params truy được từ version manifest.

Gate: graph manual fixture và threshold tests pass; backend real verified;
invariants/counters/coverage đúng cả full graph và subset nếu có.

### 14.7. W09 — Mongo snapshot, validation và phục hồi

File chính: `sink_mongo.py`, `cli.py`.

- Tách `generated_at`/`computed_at` khỏi logical identity; timestamp nhất quán trong
  snapshot và metadata UTC. IDs/ranks dùng integer phù hợp BSON; scores finite.
- Recommendation documents đúng contract và publication cohort; graph documents
  đúng contract ở mục 3, gồm threshold. Pre-publish validate schema, FK, row counts,
  unique key/rank và expected user/product sets trong Spark.
- Stream `toLocalIterator` với bulk batch size cấu hình; graph writer hiện collect
  toàn bộ documents vào list cần chuyển sang iterator/batch. Báo cáo đây là
  bounded driver-side writer, không tuyên bố distributed Mongo connector.
- Tạo unique/index topology ở collection tạm; kiểm tra inserted count với expected
  Spark count, exact logical sets và index key/options trước rename. Index lỗi,
  thiếu documents hoặc invalid score phải dừng và giữ active snapshot trước.
- Remove fallback `_drop`/`rename` dựa trên bắt lỗi để hỗ trợ test doubles khỏi
  production path. Test doubles phải mô phỏng API; không drop target rồi insert
  một iterator đã tiêu thụ khi `rename` thiếu.
- Cleanup chỉ collection tạm do run sở hữu; failure cleanup không che exception
  gốc. Không chuyển sang `delete_many` khi một lệnh drop thực sự lỗi permission
  hoặc connectivity.
- Same-database rename là promote một collection; chứng minh bằng Mongo thật.
  Rerun cùng version giữ nguyên logical document set và rank/count, chấp nhận
  timestamp cập nhật theo policy đã công bố.
- Chọn rõ retention: collection công khai giữ active snapshot; rename thay phiên
  trước. Nếu cần rollback, giữ immutable version collection/backup có run owner
  và retention cụ thể, không quảng bá nhiều version còn nằm trong active collection.
- Sau promote, kiểm tra lại count/version/index và raise nếu validation không đạt;
  `validation.valid=False` không được đi cùng status passed.
- ALS snapshot và graph snapshot có hai rename riêng, không atomic thành một
  cặp. Với lệnh `all`, chỉ đánh dấu run ready khi cả hai đã đối soát cùng run ID.
  Để future consumer không đọc một cặp lệch version, cần snapshot readiness
  manifest/pointer, hoặc giới hạn một writer và quy trình serving có gate rõ ràng.
- Test concurrent writers nếu cho phép; nếu chưa triển khai locking/version pointer,
  enforce một writer và ghi giới hạn đó. Không để validation của run A đọc snapshot
  vừa bị run B thay thế mà vẫn coi A pass.
- Mongo failure exit nonzero; báo stage nào đã publish, stage nào chưa, rollback/
  retry policy. Không giả định job failed nghĩa là cả hai collections chưa đổi.

Gate: real Mongo publish twice converges; count/index/schema đúng; lỗi insert,
index build, count mismatch và rename giữ snapshot trước theo policy đã kiểm chứng.

### 14.8. W10 — Evidence, performance và failure reporting

File chính: `cli.py`, `spark.py`; có thể reuse parser `module2/eventlog.py`.

- Dùng một run ID xuyên suốt A/B; thêm stage/trial IDs/job groups, scope
  fixture/pilot/full-data, model/graph versions và executed/skipped/failed status.
- Manifest được ghi lúc bắt đầu rồi finalize, cả khi job thất bại. Có command args,
  resolved config (redacted), commit/dirty flag và digest của code/runtime scripts/
  dependency configs ảnh hưởng run, input receipts/data hashes và environment.
- Ghi Spark/Scala/Java/Python/GraphFrames Python/JAR/backend; Spark master,
  driver/executor memory/cores, shuffle partitions, ALS blocks, checkpoint config,
  graph scope/threshold và runtime resource inventory.
- Ghi artifact theo stage ngay khi hoàn tất và khi failure; đừng giữ hết report
  trong RAM rồi chỉ viết sau cùng. Error giữ stack trace, duration, stage/trial
  context, completed/published stages và cleanup outcome.
- Bảo đảm event log finalize sau Spark stop; parse job-group attribution cho
  validation, fit, recommend, evaluate, edge-build, PageRank và publish. Performance
  gồm shuffle read/write, spill, task/executor failures, runtime, model/output size.
- `test-results.txt` chứa actual command/output/exit code, kèm skipped tests và lý
  do. Dòng placeholder “run pytest separately” không phải test evidence.
- `summary.json` có toàn bộ acceptance gates; status passed chỉ khi các gate cần
  cho command/scope đã pass. `all --skip-serving` chỉ là compute validation;
  serving skipped phải tách khỏi full acceptance.
- Ghi README evidence có scope, counts, params, cohort/baseline comparison,
  thresholds, limitations, reproducibility command và liên kết artifact. Chi tiết
  lớn/event logs/model Parquet nằm `results/module4/<run_id>/` hoặc HDFS; giữ
  compact evidence trong docs để tránh đưa dữ liệu lớn vào Git.
- Hash/check existence các artifact quan trọng; không trộn evidence của run khác.
  Cleanup/cache release và lỗi Spark stop không được ghi đè lỗi stage ban đầu.

Gate: successful run và fault-injected failed run đều có manifest/report đủ;
shuffle/resource/backend evidence truy được và mọi compact artifact cùng run ID.

### 14.9. W11–W12 — Test, CI, full flow và báo cáo

File chính: tests, runner smoke, `.github/workflows`, `progress.md`, `README.md`,
`Proposal.md`, `docs/evidence/module4/`.

- Thêm test thực chất cho mọi regression R01–R11, phân nhóm contract, Spark,
  GraphFrames, live Mongo và flow; import guard trên host được phép nhưng CI
  nghiệm thu phải cài runtime và không skip những nhóm cần đóng gate.
- Chạy fixture flow bằng dữ liệu đúng staging layout Module 1; chạy Module 2
  validation/feature generation nếu cohort bật; rồi Module 4 `validate`, `als`,
  `graph`, `all`. Có test qua standalone worker, không chỉ `local[2]`.
- Smoke runner dùng root/output/database/collections riêng và record ownership;
  cleanup không đụng curated dataset/shared active snapshots hoặc module trước.
- CI fast: pin image, lint/static, unit + Spark + GraphFrames fixture. CI live
  Mongo: phục vụ snapshot/idempotency/failure tests. CI full-data qua manual trigger
  với budget/timeouts cụ thể; upload evidence/logs kể cả khi fail.
- Giữ test suite Module 1–3 pass vì dependency/runtime chung đã thay; đây là
  regression gate, không đồng nghĩa benchmark module trước đã được chạy lại.
- Full flow phải chạy cả hai publications với scope/count/hash đối soát, expected
  product/user sets, readiness và rerun. Không lấy evidence cũ Module 1–3 để
  thay thế một run Module 4 hiện tại.
- Cập nhật status A/B trong README/progress/plan thống nhất. Phân biệt implemented,
  fixture validated, integration validated và full-data accepted; chỉ tick mục
  run/train/publish khi có artifact thật.
- Sửa trạng thái overclaim R12 ngay khi bắt đầu hoàn thiện backlog, trước run
  nghiệm thu; sau mỗi gate cập nhật lại bằng evidence mới. Không đợi full run
  thành công mới gỡ những tick hiện chưa có bằng chứng.
- `review123.md` là audit lịch sử Module 1–3 có ngày/commit riêng, giữ nguyên tính
  lịch sử; không dùng nó làm bằng chứng nghiệm thu Module 4 hoặc viết lại kết quả
  cũ thành kết quả mới.
- Update Proposal contract nếu scope/index/version fields cần bổ sung. Tách A/B
  và C/D trong bảng trạng thái: hoàn thành A/B không có nghĩa API/dashboard/blend
  đã hoàn thành.

Gate: fixture flow và full-data acceptance pass; report trỏ đúng run ID/source
digest; các lệnh reproduction hoạt động trên môi trường đã mô tả.

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

Full-data run và same-version replay đã passed ngày 10/10. Mục này giữ quy
trình tái lập với run IDs mới, runtime đã pin và curated dataset đã stage.
Recipe đầy đủ có tại [Module 4A/4B runbook](docker.md#module-4a4b-operation-and-full-data-acceptance);
[exact acceptance commands](evidence/module4/acceptance-20261010/run-acceptance.sh)
ghi đúng engine/storage override của máy nghiệm thu. Dùng portable override
`.github/module4-full.override.yml` cho stack Linux mới; engine WSL hiện tại
phải remount ext4 image và dùng local override để đọc đúng HDFS/Mongo data.

### 16.1. Kiểm tra tĩnh và runtime

```bash
python -m ruff check .
docker compose config --quiet
docker compose build spark-master
docker compose up -d --wait namenode datanode mongodb spark-master spark-worker

# Container fixture, Spark, GraphFrames và live-Mongo tests
docker compose run --rm --use-aliases --no-deps --entrypoint python3 module4 -m pytest -q
```

Ghi actual output/exit code và skipped list; verify GraphFrames backend trong
fixture. Chạy thêm `scripts/module4_runtime_smoke.py` để kiểm tra standalone
worker, HDFS, GraphFrames và Mongo; chỉ evidence từ lần chạy mới này đóng runtime
gate.

### 16.2. Xác minh handoff và bounded flow

1. Kiểm tra HDFS có đúng curated directories và `_sources.json`. Nếu volume chưa
   có dữ liệu, chạy Module 1 clean/stage theo runbook rồi Module 2 validate.
2. Chạy fixture/bounded flow ở root và Mongo collections riêng. Bounded scope phải
   được thể hiện trong manifest; không được ghi nhãn full-data.
3. Chạy `validate`, `als`, `graph`, `all`; verify `als` không chạy graph và `graph`
   không fit ALS. Preflight và reporting phù hợp từng command.
4. Thực hiện live Mongo rerun/failure cases trước khi dùng active collections.
5. Kiểm tra Module 1–3 không bị thay source tables, receipts hoặc serving outputs.

### 16.3. Run full-data và promote

```bash
# Handoff Module 2; services đã khởi động ở mục 16.1
docker compose run --rm --use-aliases module2 validate

# Cấu hình tài nguyên/COMPOSE_FILE theo [Module 4A/4B runbook](docker.md#module-4a4b-operation-and-full-data-acceptance) hoặc exact commands.
python scripts/module4.py validate --scope full_data \
  --source-receipt data/raw/source-receipt.json --shuffle-partitions 32
python scripts/module4.py all --scope full_data \
  --source-receipt data/raw/source-receipt.json --run-id full-first \
  --model-version als-full --graph-version graph-full \
  --shuffle-partitions 32 --executor-memory 2g --executor-cores 4 --total-cores 4 \
  --als-blocks 8 --pilot-users 5000 --max-trials 1 --graph-min-edge-weight 10

docker compose run --rm --no-deps --entrypoint python3 module4 \
  scripts/check_module4_run.py --run-dir results/module4/full-first \
  --output results/module4/first-snapshot.json
```

Không bắt buộc dùng wrapper Module 2 trong shared-stack rehearsal: wrapper hiện
có `up --remove-orphans`. Đường chạy trực tiếp ở trên tránh lệnh đó và không
khởi động lại toàn bộ Module 3; luôn đối soát các service trước/sau flow.

`all` phải thực hiện pilot, full internal shortlist, final prior fit, official
evaluation, all warm-user recommendations, threshold experiment, GraphFrames và
hai Mongo publications. Dataset/input receipts, trials/model/graph scope và
publications dùng cùng run ID. Replay phải dùng lại `--model-version` và
`--graph-version`, giữ input/config/source cố định và dùng `--run-id` mới.
Metadata guard đã được triển khai; chạy `all` hai lần với versions tự sinh
không chứng minh same-version replay.

Nếu chỉ chạy `all --skip-serving`, report ghi compute-only; không đóng A4/B2
publication hoặc full end-to-end acceptance. Graph subset nếu dùng phải có nhãn
riêng; chỉ được chấp nhận khi phù hợp scope ghi trước run, không thay tên subset
thành full-catalog để đóng checklist.

### 16.4. Đóng báo cáo

Đối chiếu tất cả gate mục 17; finalize manifest/summary, lưu compact evidence,
actual test/command logs và limitation. Update `progress.md`, README và status
trong plan bằng link run ID đã passed; rehearsal lại lệnh reproduction từ đầu
trong image/version đã ghi. Không cần retrain full model nhiều lần chỉ để làm
đẹp thời gian benchmark; mọi phép đo performance phải ghi cách đo.

## 17. Checklist đóng Module 4A–4B

Mốc 08/10: 17/21; đầu lượt này: 20/21 (95,2%). Ngày 10/10, full-data A/B
serving và independent read-back đã passed, cùng same-version full-data
replay: **21/21 gate (100%) — complete and validated**. Evidence hiện tại:
[acceptance-20261010](evidence/module4/acceptance-20261010/README.md).
CI 09/10 bên dưới là bằng chứng lịch sử; source hiện tại có local 91-test
regression, runtime smoke và full-data first/replay logs.

- [x] R01–R03 đã sửa và runtime/runner gate pass (`full-input-20261009`).
- [x] GraphFrames Python/JAR đã pin, smoke driver/worker pass; backend thật được ghi.
- [x] Handoff receipts/hash/counts đúng; source không bị Module 4 ghi đè.
- [x] Validation âm tính và leakage/chronology assertions pass.
- [x] Metrics/baselines/warm-all/history cohorts đúng, coverage [0,1].
- [x] Pilot và full internal shortlist hoàn thành; official train không chọn params.
- [x] Final prior model save/load đúng, factors và model metadata đối soát.
- [x] Publication phủ mọi prior warm user, gồm user thuộc official test.
- [x] Recommendation scores/keys/ranks/product sets/counts đúng và deterministic.
- [x] Graph thresholds/scope/coverage/invariants/PageRank/isolation đúng.
- [x] Graph output có version/run/threshold; backend/params truy được.
- [x] Mongo schema/index/count/set pre-promote và post-promote đạt cả hai outputs.
- [x] Same-version staged replay và failure preservation có live Mongo evidence; runtime CI pass.
- [x] Pair publication readiness/partial failure/single-writer policy rõ ràng.
- [x] Manifest/input/source/runtime/version/evidence đủ cho success và failure.
- [x] Actual test logs, stage timings, shuffle/resource/model-size metrics có đủ.
- [x] CI fixture/live integration pass; runtime workflow commit `11ac21c`, run `37798558046`.
- [x] Fixture flow Module 1–2–4 và standalone worker pass.
- [x] Full-data first/replay với serving thật và read-back passed; graph toàn catalog/all prior baskets, threshold 10 được công bố.
- [x] Suite regression Module 1–3 pass trong runtime mới.
- [x] README/progress/plan/Proposal contract thống nhất, link đúng evidence hiện tại.

Tất cả 21 gate bắt buộc đã đóng: **Module 4A–4B complete and validated**.
Connected Components, chất lượng model vượt baseline và Module 4C–4D không
thuộc checklist bắt buộc A/B này; các giới hạn đã đo được công bố ở mục 12.6.

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
`python3 -m src.module4.hybrid.cli serve-user`. It requires a positive user ID,
explicit `--as-of-event-time`, `--source-namespace`, MongoDB URI, and database;
ranking limits and lambda are validated against the same bounded contract.
See the [Hybrid CLI operating instructions](module4-package-migration.md#hybrid-cli)
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

A future quality evaluation must first establish leakage-safe chronology and a
cutoff from source order sequence and timestamp reconstruction. The ordinary
Module 1 event feed combines prior and train events without retaining
`eval_set`; it cannot by itself establish prior-only trend provenance. Freeze
the cutoff-specific model, graph, catalog, trend windows, cohort, and candidate
eligibility; ensure no official-train truth contributes to any of them. Compare
ALS-only, Trend-only, preregistered Hybrid lambda values, PersonalFrequency,
and GlobalPopularity at K=5/10/20 using the existing `evaluation.py` metric
definitions. Report warm and all-nonempty-truth cohorts plus cold-start
coverage. If a valid cutoff cannot be proven, report evaluation as infeasible.

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
`realtime_trending_windows`, `batch_product_metrics`, and
`department_metrics`. The account needs read and index-metadata inspection
privileges only. It cannot publish, write, create indexes, rename, or drop.
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
available. Module 2 replaces the product collection and its indexes on
publication, so administrators recreate the two D1B product indexes afterward.
The API itself never provisions or repairs indexes.

### 20.3. Dashboard views and interaction rules

The four Streamlit views are:

1. **Overview:** API liveness, independent source availability, each source's
   own run/version/window and timestamp, and fixed evidence registry state.
2. **Realtime & Batch Analytics:** one selected finalized trend window,
   prior-only product/department metrics, and archived all-order day/hour
   counts. Product/department names are not inferred from IDs.
3. **Hybrid Recommendations:** the verified per-user persisted ranking,
   component and final scores, inclusion flags, mode/reason, and source
   provenance. The implemented endpoint does not provide an ALS side-by-side
   comparison.
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
