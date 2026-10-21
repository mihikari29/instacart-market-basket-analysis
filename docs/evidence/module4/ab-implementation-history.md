# Historical Module 4A/4B Implementation Record

> **Provenance.** Source document: docs/module4-plan.md. Original baseline commit: b80373127c8fc1b4cd35e5738b42b075e97b5df1. Original plan blob: 8aa8e4d6001594773e6cf16fbfe551eaf8672eb3.
>
> This frozen extract records the 08 October 2026 checkpoints and the 10 October 2026 Module 4A/4B acceptance. Each record reflects its own dated source revision, execution environment, and validation scope. Historical unresolved issues and implementation notes do not necessarily describe current behavior. Preserve run IDs, source identities, results, and limitations as recorded; this archive is not a present-day status page.

## 12. Kết quả kiểm tra hiện tại

Đã bổ sung `config.py`, `data.py`, `split.py`, `als.py`, `evaluation.py`,
`graph.py`, `sink_mongo.py`, `spark.py`, `cli.py`, host runner, test contracts,
live-Mongo tests, runtime smoke scripts và workflow CI. Các contract chính được
hiện thực theo hướng prior-only, internal holdout, hai baseline, canonical graph
và staged publication. Các đường chạy Spark/GraphFrames/Mongo đã được xác minh
trên fixture và trên nguồn đầy đủ. Nghiệm thu full-data và same-version replay
đã hoàn thành ngày 10/10/2026, xem mục 12.6. Mục 12.1–12.5 giữ các checkpoint
lịch sử ngày 08/10; các trạng thái còn mở trong đó được thay thế bởi mục 12.6.

<a id="history-12-1"></a>

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
  compact JSON và actual command/test logs ở [evidence](validation-20261008/README.md).

<a id="history-12-2"></a>

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

<a id="history-12-3"></a>

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

<a id="history-12-4"></a>

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

<a id="history-12-5"></a>

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

<a id="history-12-6"></a>

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
[acceptance-20261010](acceptance-20261010/README.md).
GitHub runtime CI `37798558046`/`11ac21c` là bằng chứng 09/10; workflow full-data
chưa dispatch. Toàn bộ kiểm tra của source hiện tại được chạy cục bộ trong image
đã pin, không gán kết quả CI cũ cho source chưa commit.


---

<a id="history-13"></a>

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


---

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


---

## 16. Quy trình chạy nghiệm thu sau khi sửa code

Full-data run và same-version replay đã passed ngày 10/10. Mục này giữ quy
trình tái lập với run IDs mới, runtime đã pin và curated dataset đã stage.
Recipe đầy đủ có tại [Module 4A/4B runbook](../../docker.md#module-4a4b-operation-and-full-data-acceptance);
[exact acceptance commands](acceptance-20261010/run-acceptance.sh)
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

# Cấu hình tài nguyên/COMPOSE_FILE theo [Module 4A/4B runbook](../../../docker.md#module-4a4b-operation-and-full-data-acceptance) hoặc exact commands.
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


---

<a id="history-17"></a>

## 17. Checklist đóng Module 4A–4B

Mốc 08/10: 17/21; đầu lượt này: 20/21 (95,2%). Ngày 10/10, full-data A/B
serving và independent read-back đã passed, cùng same-version full-data
replay: **21/21 gate (100%) — complete and validated**. Evidence hiện tại:
[acceptance-20261010](acceptance-20261010/README.md).
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
