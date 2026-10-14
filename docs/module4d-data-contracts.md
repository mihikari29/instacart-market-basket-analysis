# Module 4D Dashboard Data Contracts

## Scope and classifications

This inventory was checked against the current Module 2, 3, 4A–4B, and 4C
source, checked-in evidence, and [Proposal §30](Proposal.md).
“Available” means the named artifact or collection contains the fields described;
it does not imply that a dashboard has a live reader or that every deployment
has published the optional output.

Availability labels used below:

- **MongoDB** — persisted collection field(s).
- **Persisted local report** — checked-in evidence or a run-scoped JSON/Parquet
  artifact, not a Mongo collection.
- **Streaming progress logs only** — fields written by a Spark query progress
  record; not present in MongoDB. A run summary may persist a derived statistic.
- **Bounded derivation** — a read-only query over an indexed collection or
  registered, bounded report can compute the requested value.
- **Unavailable** — no current persisted source contains the requested metric.

“Live” means read from a currently published serving snapshot. Local evidence is
historical and must be labeled by run, environment, scope, and date.

## Source inventory

| Source | Persisted schema and provenance | Indexes/access path | Availability, limits, and dashboard use |
|---|---|---|---|
| Module 2 product snapshot: `batch_product_metrics` | `_id=product_id`, `product_id`, `purchase_count`, `unique_users`, `reorder_count`, `reorder_rate`, `run_id`, `last_batch_time`. Features are computed from prior facts only. | Default unique `_id`; explicit ascending single-field `purchase_count` and `reorder_rate` indexes. | Live snapshot if Module 2 published it. `publish_mongo` builds a temporary collection then renames it over the target. It adds `run_id` and `last_batch_time` to each row. Do not filter by `run_id` without an index. Cross-collection publication is not transactional. |
| Module 2 department snapshot: `department_metrics` | `_id=department_id`, `department_id`, `purchase_count`, `unique_users`, `reorder_rate`, `run_id`, `last_batch_time`. Prior-only purchase facts. Currently 21 departments. | Default `_id`; explicit `purchase_count` and `reorder_rate` single-field indexes. Use a fixed `$in` list of department IDs, not an unfiltered `find({})`. | Live snapshot if published. Read at most the known IDs with `_id` index and return each row's `run_id`. No shared active pointer covers both Module 2 collections. |
| Module 2 analytics report: `results/module2/<run_id>/analytics.json`; committed example [analytics.json](evidence/full/analytics.json) | `scope`, counts, `purchase_behavior`, `orders_per_user`, `top_purchased`, `top_reordered`, `minimum_reorder_support`, `departments`, `orders_by_dow`, `orders_by_hour`, `department_hour_pivot`, `pivot_unpivot_validation`. Top product rows include ID/name, purchase/reorder counts/rate and unique users. `orders_by_*` rows contain dimension and `count`. | Local file; no Mongo index. Resolve only through fixed report registry. The checked-in compact example is the 2026-10-03 full-data evidence run. | Persisted report. `orders_by_dow/hour` counts orders from all `orders`, not prior-only item events. The department/hour pivot is prior-only purchase count. No `department_hour_unpivot` is included in the compact JSON. |
| Module 2 time Parquet: `results/module2/<run_id>/department_hour_metrics/` and `department_daily_trends/` | Hour rows: `department_id`, `order_hour_of_day`, `purchase_count`. Daily rows: `department_id`, `synthetic_date`, `purchase_count`, `rolling_7day_purchase_count`. Daily source is prior-only interactions and dates are synthetic/replay dates. | Local run directory; no Mongo index. Report registry selects a single known run, then fixed file paths. No HDFS or Spark request-time scan. | Persisted local Parquet output from `src/module2/cli.py`; not in Mongo and not packaged in the compact `docs/evidence/full` folder. May be used only when retained and explicitly registered. File/response/date bounds required. Product-by-hour/day and a non-synthetic calendar history are absent. |
| Module 2 benchmark report: [summary.json](evidence/full/summary.json) | `run_id`, `environment` (commit, source digest, Spark/Java/Python/platform/config), `join_benchmark`, `partition_pruning`, `cache_benchmark`, `validation`; `attach_metrics` adds Spark event-log input/shuffle metrics. Trials store seconds, checksums, physical plan, result. | Local JSON; no database. Fixed report registry. | Persisted report. Full-data evidence used one isolated host, Spark 3.5.5, Java 17, one 2-core executor, 1 warm-up and 3 measured runs. Not a production capacity result. The full scan and date-filtered scans have different scopes; cache materialization is separate from cached reuse. |
| Module 3 legacy mutable trend rows: `realtime_trending` | `_id` from `window_end` + `product_id`; `window_start`, `window_end`, `product_id`, `purchase_count_30m`, `purchase_count_120m`, `trend_score`, `trend_rank`, `updated_at`. | Unique `(window_end,product_id)`, unique `(window_end,trend_rank)`, TTL on `updated_at` (default 604800 seconds). | MongoDB rows exist if streaming publication is enabled. A complete-window delete then upsert is logically replay-safe but not atomic for concurrent readers; clients may see empty/partial results. Do not use for the dashboard's finalized consistent view. |
| Module 3 immutable finalized bundles: `realtime_trending_windows` | `_id="trend-window:<sha256(namespace)>:<window_end ISO UTC>"`; `schema_version`, `source_namespace`, original `source_scope`, `window_start`, `window_end`, `top_k`, `row_count`, `items[]` (`product_id`, `purchase_count_30m`, `purchase_count_120m`, `trend_score`, `trend_rank`), `content_sha256`, `config_hash`, `published_at`, optional `expires_at`. Each bundle has 1–20 items with unique product IDs and contiguous ranks. | Default `_id` index; optional absolute TTL index `expires_at` with `expireAfterSeconds=0`. There is no index on `source_namespace`/`window_end`; deterministic namespace-hash/time `_id` supports a bounded lexical `_id` range. | MongoDB when explicitly enabled. Module 3 config defaults `publish_trend_bundles=false`; the dashboard must show unavailable until deployment enables it and supplies a stable namespace. Retention defaults to seven days from `published_at`, but can be changed or disabled. Use immutable bundles, validate `source_namespace`, preserve `source_scope`, and do not infer stale from old event time. |
| Module 3 progress records: `results/module3/<run_id>/streaming_progress.jsonl`; derived `summary.json` | Spark progress fields include `batchId`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `durationMs.triggerExecution`, `durationMs.addBatch`, `eventTime.watermark`, source offsets/partition metadata, state operator metrics, and observed quality metrics. `summarize_progress` derives batch counts, total rows, median/max rates, median durations, and observed partition count. | Local run artifacts; no Mongo index. Fixed report registry only. JSONL must not be scanned without byte/line bounds; prefer a registered bounded `summary.json`. | Throughput and Spark processing duration are only in progress/log evidence, not serving collections. Run summaries are historical measurements. Kafka consumer lag and a source-event-to-dashboard latency are not recorded. |
| Module 3 compact validation evidence: [README](evidence/module3/README.md), [final-release JSON](evidence/module3/final-release-validation-2026-10-03.json), [hardening JSON](evidence/module3/hardening-validation-2026-10-03.json) | Environment, validation results, throughput smoke summary, processing rates and trigger duration; hardening report includes the 50k experiment and sink duration. | Fixed checked-in report paths, no database. | Persisted local evidence only. Label the 5k/50k experiments as bounded single-host runs. Kafka was preloaded for the 50k run, so its input rows/s value does not represent Kafka lag. |
| Module 4A ALS snapshot: `user_recommendations` | `_id=<model_version>__<user_id>__<product_id>`, `model_version`, `user_id`, `product_id`, `als_score`, `als_rank`, `run_id`, `generated_at`. Top-K is configured (accepted full run stores 20/user). | Unique `(model_version,user_id,product_id)`, unique `(model_version,user_id,als_rank)`, and `(model_version,user_id)`. Exact version/user query and rank sort, limit 21. | MongoDB current snapshot. Writer replaces the collection per run; do not assume historical model versions remain available. Pair it with readiness `_id="active"` before/after the read and require exact `model_version`; do not infer arbitrary active state. |
| Module 4B product graph snapshot: `product_graph_metrics` | `_id=<graph_version>__<product_id>`, `graph_version`, `product_id`, `neighbor_count`, `weighted_degree`, `pagerank`, `run_id`, `min_edge_weight`, `computed_at`. | Unique `(graph_version,product_id)`, `(graph_version,pagerank desc)`, `(graph_version,weighted_degree desc)`. | MongoDB current snapshot if published. Not required by Proposal §30 MVP; include only in an evidence/diagnostic extension with exact graph version and a bounded Top-K query. |
| Module 4A–4B readiness: `module4_snapshot_readiness` `_id="active"` | `status`, `ready`, `run_id`, `model_version`, `graph_version`, publication booleans, `updated_at`; an error may be included on failure. | Default `_id` unique index; exact point lookup. | Readiness for the A/B pair only. It is not the Module 4C per-user pointer and must never be used as one. |
| Module 4A evaluation: [evaluation.json](evidence/module4/full-ab-20261010-fixed-replay/evaluation.json), sibling `summary.json` | Offline cohorts and K values with `precision_at_k`, `recall_at_k`, `map_at_k`, `ndcg_at_k`, user/catalog coverage, baseline deltas for ALS vs global popularity and personal frequency, `ground_truth`, `truth_summary`, and `official_test_truth_available`. | Local evidence JSON; no Mongo index. Fixed report registry. | Verified persisted ALS evaluation; not Hybrid evaluation. The accepted run evaluates official train truth for users with non-empty train truth; the report states test truth unavailable. Preserve cohort and truth semantics next to every chart. |
| Module 4A–4B performance: [performance.json](evidence/module4/full-ab-20261010-fixed-replay/performance.json), sibling `summary.json` | `als_ms`, `graph_ms`, stage runtimes and Spark job groups with stage counts and shuffle read/write bytes. | Local JSON; fixed report registry. | Persisted full-data run evidence only. Environment, run, stage boundary, and single-host context must accompany a number. Do not compare timings across unrelated run configurations as controlled trials. |
| Module 4C immutable Hybrid rows: `final_recommendations` | `schema_version`, `_id`, `serving_version`, `algorithm_version`, `user_id`, `product_id`, `product_name`, `als_score`, `trend_score`, `normalized_als_score`, `normalized_trend_score`, `final_score`, `rank`, `from_als`, `from_trend`, `lambda_requested`, `lambda_effective`, `mode`, `reason`, `source_run_id`, `model_version`, `graph_version`, `trend_source_namespace`, `trend_source_scope`, `trend_window_start/end`, `trend_content_sha256`, `as_of_event_time`, `input_content_sha256`, `generated_at`, and ranking configuration fields/hashes. | Unique `(serving_version,user_id,product_id)`, unique `(serving_version,user_id,rank)`. Exact user/version query, rank sort, `.limit(21)`. | MongoDB, immutable versioned row set. `trend_score` is the raw component; normalized ALS/trend values are persisted separately. Inclusion flags and `mode`/`reason` support explanation. Rows are accepted only through the per-user pointer contract. |
| Module 4C per-user readiness: `module4_snapshot_readiness` `_id="hybrid:<user_id>"` | `schema_version`, `_id`, `user_id`, `status="ready"`, `ready=true`, `serving_version`, `expected_rows`, `source_run_id`, `model_version`, `graph_version`, trend namespace/window/hash, `as_of_event_time`, `algorithm_version`, `mode`, `reason`, input/config hashes, `als_top_n`, `trend_top_m`, `output_top_k`, `epsilon`, `updated_at`. | Default `_id` unique index; exact point lookup. | MongoDB pointer is authoritative only for that user's Hybrid version. It does not use `_id="active"`. Check count, unique contiguous ranks, and the same pointer identity after row reads. |
| Module 4C controlled evidence: [C8A receipt](evidence/module4c/c8a-receipt.json), [C8B receipt](evidence/module4c/c8b-receipt.json) | C8A: synthetic ranking response across lambda values, Top-20 overlap, rank displacement, three-run CLI wall timing at lambda 0.5, explicit `recommendation_accuracy_measured=false`. C8B: 48 synthetic events, window bounds/watermarks/rows, Spark trigger durations, ack-to-bundle-visible interval, repeat publication time, explicit limitations. | Fixed local JSON receipts; no database. | Persisted experiment evidence only. C8A is a responsiveness experiment, not recommendation quality. C8B's named ack-to-bundle-visible interval is a bounded synthetic boundary measurement, not Kafka lag or general end-to-end latency. Original reports are under local `results/` and are not the checked-in sanitized receipt. |

## Metric-to-source matrix

| Candidate metric/view | Classification | Exact source and query/read strategy | Contract and limitation |
|---|---|---|---|
| Finalized Top-K trending products | MongoDB | `realtime_trending_windows`, bounded `_id` range for configured namespace/time interval; max 50 bundles, each max 20 items | Return finalized event-time bounds, namespace, original scope, publication and expiry metadata. No open-window refresh semantics. Optional publisher must be enabled. |
| Trend score by finalized window | MongoDB; bounded derivation | Same bundles; plot `items[].trend_score` by `window_end`, optionally selected product ID | Verify each bundle schema/rank/hash contract; do not interpolate missing windows or label as continuously updating. |
| Trend retention/history | MongoDB | Same bounded `_id` range and optional `expires_at` metadata | TTL is based on processing-time expiry. Event-time age alone is not stale. Missing old history may mean TTL removal or never-published namespace/window. |
| Kafka input rate / Spark processed rows per second | Streaming progress logs only; persisted summaries for registered runs | JSONL progress fields or bounded `summary.json`/Module 3 evidence report | Not in MongoDB. Do not call input rate consumer lag. Do not call a historical report live throughput. |
| Spark trigger and addBatch duration | Streaming progress logs only; persisted summaries for registered runs | `durationMs.triggerExecution` / `durationMs.addBatch`; derive median/max only over a named run and its stated inclusion rule | Processing duration is not event-to-dashboard latency. Keep warm-up and empty batch handling visible. |
| Kafka consumer lag | Unavailable | No consumer-group lag metric is persisted in the inspected data sources | No lag chart or implied estimate. Spark source offset ranges alone do not establish broker end offsets or lag. |
| Event-to-dashboard or Kafka-to-serving latency | Unavailable | No common event identity is carried through to dashboard-observed time; C8B records a distinct synthetic ack-to-bundle-visible interval only | Do not fabricate generic end-to-end latency. The C8B interval can appear only as a named historical experiment with its boundaries. |
| Top purchased products | MongoDB; persisted local report | `batch_product_metrics` sorted by indexed `purchase_count`, limit 20; or registered `analytics.json` `top_purchased` | Mongo rows include live reorder fields and run metadata. Stable ties need a compound index. Report version is historical. |
| Product reorder rate | MongoDB; bounded derivation | Exact product `_id` lookup in `batch_product_metrics`; top-purchased output can include row-level `reorder_rate` | Rate denominator is prior-only `purchase_count`; only query a validated product ID. |
| Top reordered products with support threshold | Persisted local report | Registered `analytics.json.top_reordered`; threshold in `minimum_reorder_support` | Exact Spark ranking and threshold. Live query needs reviewed support-aware index and matching ordering contract. |
| Department statistics | MongoDB; bounded read | `department_metrics` using fixed `$in` of IDs 1–21 against `_id`; maximum 21 results | `purchase_count`, `unique_users`, `reorder_rate`; prior-only. Include `run_id` per row. |
| Orders by hour / day-of-week | Persisted local report | `analytics.json.orders_by_hour` and `orders_by_dow`; no Mongo query | These count all orders, not item-level prior purchase events. Label as order counts. |
| Prior purchase count by department and hour | Persisted local report; persisted Parquet | `analytics.json.department_hour_pivot` or registered `department_hour_metrics` Parquet | Prior-only synthetic historical input; report has 21 departments × 24 hours. Do not claim live hourly arrivals. |
| Purchase counts by department and synthetic calendar day / rolling 7 calendar days | Persisted local Parquet | Registered `department_daily_trends` Parquet for a single run and bounded date range | Prior-only; dates are synthetic. The compact checked-in JSON does not include this file; unavailable if the run artifact is absent. |
| Product-level purchase distribution by hour or day | Unavailable | No saved product×hour/day aggregate in current Mongo or compact report | Deriving from raw HDFS facts would be an unbounded compute path for a dashboard request; do not add this chart to MVP. |
| ALS recommendations for one user | MongoDB | Read A/B `_id="active"`; query exact `{model_version,user_id}`; sort `als_rank`; limit 21; re-read pointer | Existing version/user/rank indexes. Accept only if A/B pointer unchanged and row validation passes. Old model versions may no longer be retained. |
| Hybrid final Top-K and raw/normalized/final component scores | MongoDB | Per-user pointer protocol, then exact `{user_id,serving_version}` query, rank sort, limit 21 | Validate ready state, expected count, unique contiguous rank, and unchanged pointer. No publish call. |
| Selected serving version and model/graph/run/trend source identities | MongoDB | Hybrid readiness pointer plus repeated row metadata consistency check | Keep source IDs, namespace, original `source_scope`, selected window, `as_of_event_time`, `generated_at`, and mode/reason. |
| ALS Precision@K, Recall@K, MAP@K, NDCG@K | Persisted local report | Fixed Module 4 `evaluation.json` and selected cohort/K | ALS only. Preserve official-train truth, cohort exclusion, run ID and evaluation scope. Not Hybrid quality. |
| ALS coverage and baseline comparisons | Persisted local report | `evaluation.json` cohort metrics and `als_vs_baseline_deltas` | Baselines are global popularity and personal frequency for the named evaluation run. Do not claim ALS beats all baselines; in the accepted full run it is below personal frequency. |
| Hybrid recommendation accuracy or quality uplift | Unavailable | No leakage-safe Hybrid evaluation exists | Never present C8 responsiveness as accuracy or claim improvement. |
| Join, partition pruning, cache and shuffle metrics | Persisted local report | Module 2 `summary.json` and full evidence report; fixed report registry | Show each arm/method, data scope, Spark environment, medians/repetitions, physical plan and event metrics. Partition arms have different/equal scopes as documented; cached reuse excludes build cost unless combined. |
| Module 4A/4B runtime and shuffle | Persisted local report | Module 4 full replay `performance.json` / `summary.json` | Historical full-data run in its recorded single-host environment; not comparable to Module 2 or C8 without matching methodology. |
| Module 4C ranking response before/after synthetic trend change | Persisted local report | C8A receipt `ranking_response` | Static synthetic fixture, five lambdas, no accuracy measurement. There is no live historical per-user before/after ranking series. |
| Module 4C call timing / C8B window completion timing | Persisted local report | C8A/C8B receipts, named boundaries and repetitions | Controlled synthetic timings only. C8B ack-to-bundle-visible is not a general serving latency. |
| Graph metrics / PageRank | MongoDB | `product_graph_metrics`, exact graph version and indexed Top-K by PageRank or weighted degree | Available source, but outside initial dashboard scope. Use version filters and a strict cap. |

## MongoDB query and validation rules

1. Create collection names and field names only in server code. Never accept a
   Mongo filter document, projection, sort key, or collection name from the UI.
2. Parse IDs as positive integers, timestamp strings as UTC instants, report
   IDs as allowlisted tokens, and namespaces as exact configured values. Reject
   Mongo operator keys, unsupported enum values, and excessive time ranges.
3. Use a documented index for every query. Use exact point keys, bounded
   indexed ranges, or a top-K sort/limit supported by an index. Never query an
   unindexed `run_id`, issue an unbounded collection read, or aggregate raw
   events for a dashboard request.
4. Put explicit `.limit()` on every row query, set `maxTimeMS`, and close the
   cursor. The top-purchased query may use an index-backed `find({})` only with
   its fixed indexed sort and hard limit; it must not become a collection scan.
5. Project only fields required by the response. Limit report bytes, Parquet
   rows/date span, and nested arrays before serialization.
6. For Module 2, return each collection's `run_id` and `last_batch_time`. If
   product and department sources have different run IDs, report the mismatch;
   do not imply cross-collection atomicity.
7. For Module 4A, use `_id="active"` only to identify the A/B source version
   and verify it again after the exact ALS read. For Module 4C, use only
   `_id="hybrid:<user_id>"` for the final recommendation snapshot and verify
   its full identity around the bounded row read.
8. A read-only dashboard credential is necessary but not sufficient: API code
   has no write code path, and integration tests must verify it cannot publish,
   update, delete, rename, or start a Module 4C serving operation.

## Provenance and missing-source rules

- Carry `source_scope` unchanged from the finalized trend bundle into API
  responses and charts. Do not replace `unknown_or_mixed` or any other source
  label with a guessed value.
- Distinguish event time, Spark processing time, finalized-window publication
  time, recommendation generation time, and dashboard observation time.
- Label Module 1/3 replay and C8A/C8B synthetic sources explicitly. Historical
  synthetic event dates do not alone imply expiration or stale output.
- Show “unavailable” for Kafka lag, generic end-to-end latency, product-level
  hour/day distributions, and Hybrid accuracy. Do not substitute order counts,
  trigger duration, or responsiveness metrics under those names.
- Local reports are read-only and allowlisted. Reports are not a source of live
  Mongo freshness; include their run ID, creation time, and environment.

## Repository source links

- [Module 2 aggregation and Mongo publication](../src/module2/analytics.py)
- [Module 2 report and Parquet paths](../src/module2/cli.py)
- [Module 2 benchmark definitions](../src/module2/benchmarks.py)
- [Module 3 trend bundle schema and indexes](../src/module3/trend_bundle.py)
- [Module 3 mutable trend sink](../src/module3/sink_mongo.py)
- [Module 3 progress summary](../src/module3/benchmark.py)
- [Module 4A/4B indexes and active readiness](../src/module4/sink_mongo.py)
- [Module 4C per-user row and pointer contract](../src/module4c/sink_mongo.py)
- [Module 4C A/B and Hybrid source capture](../src/module4c/data.py)
- [Module 4C design and evidence limitations](module4c-release-review.md)
