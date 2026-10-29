# Module 4 technical contracts and acceptance

This specification owns the data, algorithm, publication, API, dashboard and
acceptance contracts for Modules 4A–4D. The filename remains stable because the
requirements baseline and generated experiment reports reference it. Sections
19 and 20 retain their established reference numbers; they are technical
contracts, not implementation milestones.

The [repository proposal](Proposal.md) defines requirements; [project status](progress.md)
records implementation and verification limits. Full historical run archives
are not included in this checkout. Historical results do not validate the
current source, and a documented acceptance condition is not an executed pass.

## Contents

- [1. Scope and acceptance boundaries](#1-scope-and-acceptance-boundaries)
- [2. Input and output contracts](#2-input-and-output-contracts)
- [3. Runtime and configuration](#3-runtime-and-configuration)
- [4. Input validation and lineage](#4-input-validation-and-lineage)
- [5. ALS recommendation](#module4a-als-recommendation)
- [6. Evaluation and cohorts](#6-evaluation-and-cohorts)
- [7. Product co-purchase graph](#module4b-product-co-purchase-graph)
- [8. MongoDB snapshot publication](#8-mongodb-snapshot-publication)
- [9. Testing, evidence and acceptance](#9-testing-evidence-and-acceptance)
- [19. Hybrid recommendation and serving](#module4c-spec)
- [20. Read API and dashboard](#module4d-spec)
- [Technical references](#technical-references)

## 1. Scope and acceptance boundaries

| Part | Implementation | Responsibility |
|---|---|---|
| 4A | `src/module4/als.py`, `src/module4/tuning.py`, `src/module4/evaluation.py` | Prior-only implicit ALS, bounded tuning, held-out evaluation and versioned recommendations. |
| 4B | `src/module4/graph.py` | Prior-basket co-purchase graph, neighbor/weighted-degree metrics and GraphFrames PageRank over the full catalog. |
| 4C | `src/module4/hybrid/` | Source-consistent ranking from ready batch generations and eligible immutable finalized trend bundles; per-user publication. |
| 4D | `src/module4/dashboard/` | Bounded read-only FastAPI views and four Streamlit pages; UI data access goes through HTTP. |

A/B acceptance requires prior-only fitting/tuning, correct Precision/Recall/MAP/
NDCG at K=5/10/20, transparent cohorts and baselines, attributable trial evidence,
valid graph invariants, real GraphFrames execution, versioned MongoDB readback,
and both fixture and full-data evidence. Connected Components is optional.
A/B completion does not establish Hybrid quality, dashboard production
readiness or production capacity. A compute-only run is not publication
acceptance. The [checklist](#acceptance-checklist) applies to a new run.

## 2. Input and output contracts

| Input | Path under the selected HDFS root | Role |
|---|---|---|
| `orders` | `curated/orders` | Supplies `user_id`, `order_number`, and `eval_set`. |
| `prior` | `curated/order_products_prior` | ALS fitting, internal tuning and graph construction. |
| `train` | `curated/order_products_train` | Held-out final evaluation truth. |
| `products` | `curated/dimensions/products` | Product keys, catalog coverage and metadata. |
| `user_features` | `features/user_features` | Optional descriptive/cohort handoff; not required ALS fit input. |

Prior and train facts do not carry `user_id`; join them to orders by `order_id`.
Historical full-input figures were 32,434,489 prior facts and 1,384,617 train
facts. These are context, not hardcoded acceptance constants: each run must
measure its own counts and retain matching source receipts.

`user_recommendations` fields:

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

`product_graph_metrics` fields:

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

## 3. Runtime and configuration

`src/module4/config.py` owns runtime paths/settings, `src/common/spark.py` owns
the shared Spark session, and `src/module4/tuning.py` owns trial construction and
selection. `scripts/module4.py` is the Compose wrapper; `src.module4.cli`
implements `validate`, `als`, `graph`, and `all`.

Pipeline YAML is only partially consumed. Local ALS fit settings seed the first
trial candidate; standalone CLI arguments still control pilot/trial budgets.
See [configuration boundaries](progress.md#configuration-and-recovery-boundaries).
The precedence and validation requirements below apply to supported sources;
they do not claim that every declared YAML value is active.

- Resolve configuration with precedence **explicit CLI value > environment > default**. Host help must return usage without starting services or importing the heavy Spark runtime. The runner supports an explicit build for changed runtime dependencies and returns nonzero on build or job failure. Compose runner and direct invocations use --use-aliases or an equally verified driver address. Report effective configuration with credentials redacted.
- Resolve Mongo URI/database/collections, Spark root/master/seed, partitions, K, graph threshold/reset/iterations, and tuning limits from that precedence. Validate IDs, paths, resource values, tuning budgets, K values, partitions, thresholds, and iterations before work begins. Reject invalid, non-finite, out-of-range, or path-traversing values; do not overwrite an existing version with different source/config identity; model/version reuse is explicit and identity-checked.
- Preflight services and permissions required by the selected stage: image and Spark master/worker, driver DNS, curated HDFS reads, model/evidence writes, and Mongo ping/index/rename privileges for serving. Start only required services; do not use orphan removal or shared-volume cleanup to run A/B. Keep stage ownership explicit: validate does not fit or publish, ALS does not build graph, and graph does not fit ALS.
- A compute-only --skip-serving run must not require Mongo availability, including through runner/Compose dependencies, and must record serving as skipped. It is not publication or end-to-end acceptance.
- Keep publication K separate from evaluation K, or ensure recommendation generation supplies at least max(5,10,20), so a smaller published list cannot truncate evaluation candidates.
- Use shared worker-accessible ALS checkpoints and report checkpoint interval, storage level, and run-owned cleanup policy. Keep run/version IDs validated and output/model paths explicit.

## 4. Input validation and lineage

- Read the Module 1 curated source receipt and independently verify actual input file inventory, counts, and SHA-256 digests. Record receipt digest, source identity, paths, scope, and validation time. If multi-part inputs are supported, define a deterministic aggregate digest without changing the meaning of an individual file hash.
- Keep input stable for the whole run, using an immutable snapshot or before/after inventory and digest checks; never promote output assembled from multiple source generations. Do not scan unrelated streaming interactions merely to verify the four curated A/B inputs.
- Distinguish local prior filenames from HDFS curated directory names and validate schema/types, critical nulls including product name, integer domains, key uniqueness, order/user/cart chronology and consistency, `reordered ∈ {0,1}`, duplicate order/product rows, and foreign keys. Reject null, nonbinary, invalid, or silently coerced `reordered` values.
- Validate eval_set membership and counts before/after joins; guard against joins that multiply facts. Check every prior/train order has facts as required, count users with empty ground truth and report exclusion reasons, and verify prior/train order sets are disjoint. Derive the leakage report from executed assertions; only prior facts may feed ALS interactions and graph construction.
- Treat Module 2 user_features as optional. If enabled, validate its path, schema, unique user key, prior-only provenance, and coverage; otherwise record it as unused. Do not claim ALS used these features when it did not.
- Module 4 is read-only with respect to curated sources: do not rewrite Module 1/2 source tables or receipts.

<a id="module4a-als-recommendation"></a>

## 5. Module 4A — ALS recommendation

### Interaction strength and internal holdout

Aggregate prior facts to `(user_id, product_id) -> purchase_count`. Supported
strengths are:

```text
raw: strength = purchase_count
log: strength = 1 + log(1 + purchase_count)
implicitPrefs = true
userCol = user_id
itemCol = product_id
ratingCol = strength
coldStartStrategy = drop
```

Record a fixed seed. Users need at least two prior orders for internal
validation: hold out the latest prior order by `order_number`, train on earlier
prior orders, require disjoint order sets, and report excluded users and cold
items. After parameter selection, retrain on all prior interactions and evaluate
on official train. Seen products remain valid next-basket candidates; report
reordered and new-to-user truth separately.

### Bounded tuning and model contract

The baseline is `rank=32`, `regParam=0.1`, `alpha=10`, `maxIter=10`. The trial
domain includes rank 16/32/64, regularization 0.01/0.05/0.1, alpha 10/40,
iterations 5/10/15, and raw/log strength. This is not a Cartesian full-data
search. Use at most 12 distinct pilot configurations, then at most three full
internal-holdout candidates. The versioned final model lives under
`/instacart/models/als/<model_version>`.

- Accept only positive int32 user/product IDs and finite positive interaction strengths; reject null, fractional, overflow, duplicate user/product, empty factors, and invalid factor/score data. Read the nested ALS recommendation rating as a double aliased to als_score. Report purchase-count/strength distributions, source scope, interaction count, and factor counts against distinct training IDs.
- Make prior input, partition/sort order, and training materialization deterministic. The internal split requires a unique order per (user_id, order_number), one latest prior holdout order per eligible user, earlier training chronology, disjoint order sets that cover eligible facts, and no official-train order in either side. Report excluded users/rows/orders/products and cold-start products.
- If the internal holdout has no eligible users, report tuning as not performed; a fixture-only baseline fallback cannot close the tuning gate. Select a seeded/hash-stable pilot with deterministic hash tie-breaking by user ID and all prior orders for selected users. Deduplicate the grid and cap it at 12 distinct configurations.
- Compute pilot baselines only from internal training history. Evaluate at most the top three pilot configurations on full internal training/holdout; choose final parameters from that full internal comparison, with Recall@10, then Precision@10, then runtime as tie-breakers. Official train is used only after parameters are frozen and never selects parameters, thresholds, or fallback policy.
- Persist shared inputs within a declared resource budget and release model factors, trial outputs, and caches in finally using run ownership. Record each trial's params, seed, input/factor counts, P/R/coverage, fit/recommend/evaluation/total time, and resource/job-group metrics. Persist failed-trial input/config/error/stack trace immediately, including when all trials fail; an incomplete shortlist is reported as incomplete/degraded, never passed.
- Fit the final model on all prior interactions, save a versioned model without unintended overwrite, and verify reload schema, factor counts, and recommendations. Publish for every prior user with user factors, including users whose terminal order is in the official test set; keep the official-train evaluation cohort distinct. Report requested publication users, factor-backed eligible users, and missing/excluded users without substituting evaluation users for the publication cohort.
- Recommendation output includes seen products unless the contract is explicitly revised. Validate finite scores, positive IDs and product FKs, unique user/product keys, ranks 1..min(K, available factors), expected per-user row counts, and the factor-backed publication cohort. Report expected versus actual recommendation rows and item-factor availability, including its effect on recommendation list length. Resolve score ties deterministically by descending score then ascending product ID, including membership at the Top-K boundary.

## 6. Evaluation and cohorts

Ground truth is a distinct product set per official train user. Report
Precision/Recall/MAP/NDCG at K=5/10/20, user and catalog coverage, mean/min/max
list lengths, and user/item cold-start rates. Only users with nonempty truth
enter metric denominators. Official test orders have no released truth;
publication still includes their users when prior user factors exist.

Mandatory baselines are global prior popularity and personal prior frequency
with deterministic global fill. Use the same cohort and K for each method.
No arbitrary quality threshold substitutes for correct metrics and fair
comparisons; if ALS trails the baseline, do not claim improvement.

- Validate distinct truth products, one truth row per user, unique recommendation keys/ranks, positive IDs, and ranks within K. Invalid rows fail validation rather than being dropped from metrics.
- Pass K explicitly to every ranking metric. For each user u with nonempty truth G(u): Precision@K = hits/K; Recall@K = hits/|G(u)|; AP@K = sum(hit_i × precision_at_i)/min(K,|G(u)|); IDCG@K = sum(1/log2(i+1), i=1..min(K,|G(u)|)); NDCG@K = DCG@K/IDCG@K. These denominators do not depend on the number of returned predictions: Precision uses K, AP and IDCG use min(K,|G(u)|), and Recall uses |G(u)|.
- Required hand case: K=5, predictions [1], truth [1,2,3,4,5] yields P=0.2, R=0.2, AP=0.2, and NDCG≈0.33916; the current implementation must match it. See [Spark RankingMetrics 3.5.5](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala).
- User coverage counts only predicted users within the cohort with nonempty truth. Catalog coverage counts cohort predictions over the declared candidate-catalog scope. Report mean recommendation count including users with no output, min/max/count distributions, skipped users, and empty-cohort reason; empty cohorts are not applicable, not a valid zero score. All coverage values remain within [0,1].
- Warm cohort means a user with factors and all truth products with item factors. All-train keeps every nonempty truth cohort, including cold items, which are reported because factor-only recommendation cannot return them. Report cold users, cold items, mixed truth, and truth-item factor coverage.
- Define reordered/new-to-user groups from stage-appropriate history. Keep ranked predictions unchanged and filter truth for each subgroup; report subgroup-specific cohorts and coverage.
- Compare ALS with global popularity and personal frequency on identical users, truth, K, and candidate scope; final baselines use full prior history. Personal-frequency items stay first; fill short lists and cold cohorts with deterministic global fallback without duplicate products. Baseline scores are not mislabeled as model scores in production outputs. When a UDF is retained, its callable is importable on executors; prefer Spark-native computation when measured cost justifies it. Report P/R@5/10/20 and deltas without unsupported improvement claims.

<a id="module4b-product-co-purchase-graph"></a>

## 7. Module 4B — Product co-purchase graph

Join prior facts with orders, deduplicate products within each basket, self-join
on `order_id` with `product_a < product_b`, and count orders containing each
pair. The canonical edge has `src=min(product_a,product_b)`,
`dst=max(product_a,product_b)`, and `weight=co_purchase_count`.
Do not silently cap baskets or sample pairs.

Compare thresholds `min_edge_weight=2,5,10`; report degree/weight distributions,
coverage, isolates, runtime and shuffle at each threshold, with a selection
rationale. PageRank uses the symmetric directed topology, with reference
parameters `resetProbability=0.15` and `maxIter=10`; co-purchase weights define
weighted degree, not weighted PageRank. Connected Components remains optional:
configure checkpointing and report it only when component structure adds insight.

### Construction and metric invariants

- Use prior baskets only, deduplicate products within each order, emit canonical product pairs, and define each undirected edge weight as the number of orders containing that pair.
- Default to the full catalog, including inactive products, with all prior baskets. If a selected-product subset is approved for a run, select the top P products by prior popularity with product ID ascending as the deterministic tie-break; preserve fact/order semantics within the selected scope and report graph scope, declared P, selected product IDs and stable digest, and catalog/prior-fact coverage. Never silently sample baskets or pairs or label a subset as the complete graph.
- Reuse one raw co-purchase aggregation for threshold runs when resources permit. Measure basket count, basket-size distribution, and pair expansion before full runs; do not infer full-scale capacity from a small resource configuration. For each threshold, report total catalog products, prior-active products, edge-active products, isolates with no prior activity, and products isolated because threshold pruning removed their edges, alongside vertices, undirected/directed edges, total weight, catalog/prior-fact coverage, degree/weight quantiles, runtime, and available shuffle/spill/memory/disk metrics. Use counts measured for that run; do not infer them from historical figures. Record threshold rationale; do not call an unexamined default an optimum.
- Validate exact vertex membership with anti-joins, unique endpoints/edges, canonical pair ordering, positive integral finite weights, finite metrics, foreign keys, exactly two reciprocal directed rows per undirected edge, degree <= vertex_count - 1, and weighted-degree conservation: sum(weighted_degree) = 2 * sum(edge weight).
- Run PageRank with real GraphFrames on symmetric topology; co-purchase weights define weighted degree. Reject null, NaN, positive Infinity, and negative Infinity values, and reject negative neighbor_count, weighted_degree, or pagerank. Preserve isolated vertices and define empty, one-vertex, isolated, and disconnected semantics. Sort Top-20 by metric descending then product ID ascending and include product name.
- Attribute graph version, run ID, source, scope, backend, threshold, PageRank parameters, and computation time in versioned output/evidence.

### Runtime compatibility

- Pin and report Spark 3.5.5, Java 17, Scala 2.12, the GraphFrames Python package/version, and the compatible JAR name/version with its pinned checksum identity. Verify the JAR checksum during Docker image build and make it available on driver and workers without resolving Maven dependencies during a job.
- Verify and report Spark/Scala/JAR compatibility and the real GraphFrames backend on both the driver and standalone worker, using an isolated-vertex PageRank fixture and standalone-worker smoke. Record package/version, JAR filename/version/checksum, and backend/runtime identity in evidence.
- Missing dependencies or GraphFrames runtime errors fail the requested stage with stack trace; they must not silently become a passed fallback. A DataFrame backend is allowed only when explicitly selected for fixture scope and labeled as such.
- Document and test production PageRank normalization and dangling/isolated-vertex semantics. Do not compare backend scores unless algorithms, normalization, and isolate policy match.

Cross-session metric fingerprints have shown unresolved variability in local
graph comparisons. Do not treat a seed or matching structural invariants as
proof of identical PageRank fingerprints across fresh Spark sessions. Retain
raw comparisons and investigate differences before closing a same-version
replay gate; do not relax its declared numeric comparison merely to obtain a
pass. See [historical graph comparison limits](progress.md#graph-comparison-limits).

## 8. MongoDB snapshot publication

Required indexes:

```text
user_recommendations:
  unique(model_version, user_id, product_id)
  unique(model_version, user_id, als_rank)
  index(model_version, user_id)
product_graph_metrics:
  unique(graph_version, product_id)
  index(graph_version, pagerank)
  index(graph_version, weighted_degree)
```

- Keep generated/computed timestamps outside logical identity, in consistent UTC form. Validate recommendation and graph schema, finite scores, FKs, keys/ranks, expected row counts, and expected user/product sets before publication.
- Write documents through bounded iteration and configurable bulk batches; do not collect the complete graph or recommendation output on the driver. Verify inserted counts and index keys/options before promotion. Use the supported Mongo API in production; test doubles model it rather than adding destructive production fallbacks. Any validation or index failure leaves the previous active snapshot intact.
- Promote only validated run-owned temporary collections by same-database rename verified against real Mongo. Cleanup is scoped to those collections; cleanup failures do not mask the original error and must not trigger destructive fallback against active data.
- Same-version replay must converge to the same logical document set, keys, membership, ranks, and counts under the declared numeric comparison, while timestamp changes follow the documented policy. Validate counts/version/indexes after promotion and fail if read-back differs. Document whether only the active snapshot is retained or how run-owned immutable rollback versions are retained.
- A/B use separate same-database Mongo promotions and therefore are not atomic as a pair. Mark ready only after both snapshots validate for the same run/version identity through a readiness pointer/manifest; report partial publication honestly. Enforce a single writer or validate locking/readiness behavior under concurrent writers.
- On failure, return nonzero and report completed/published stages and retry/cleanup outcome; a failed job does not imply that neither collection changed.

Consumers join through `user_id`/`product_id` within the captured model/graph/run
identity and must not mix generations. A downstream handoff includes schemas,
indexes, readiness, scope, threshold, backend and factor-backed publication
cohort, not just a completion label.

## 9. Testing, evidence and acceptance

### Evidence and failure reporting

- Use one run ID across A/B and stage/trial/job-group IDs beneath it. Record fixture/pilot/full-data scope, model/graph/source versions, executed/skipped/failed status, command arguments, redacted resolved config, commit/dirty state, code/runtime/dependency digests, input receipts/hashes, and environment.
- Write a manifest at start and finalize it on success or failure. Record Spark/Scala/Java/Python/GraphFrames identities, backend, master, driver/executor resources, partitions, ALS blocks/checkpoints, graph scope/threshold, and runtime inventory.
- Persist stage/trial artifacts as they complete. Failure evidence includes stack trace, duration, stage/trial context, completed and published work, and cleanup outcome. Finalize event logs after Spark stops and attribute validation, fit, recommendation, evaluation, edge build, PageRank, and publication metrics.
- Store actual test/command output and exit codes, skipped checks with reasons, stage timings, shuffle/spill/task/executor metrics, model/output sizes, and artifact digests. Compact evidence includes scope, counts, parameters, cohort/baseline results, threshold, limitations, and reproduction reference. Place large evidence outside compact docs and link compact records to matching run IDs.
- Hash/check required artifacts and preserve cleanup errors without overwriting the stage failure. A summary lists all applicable acceptance gates and reports passed only when each required gate for that command/scope passes. Compute-only/skipped serving is distinct from full acceptance. Historical artifacts remain immutable and attributed to their original source/environment.

Required run artifacts include `manifest.json`, `environment.txt`,
`test-results.txt`, `data_validation.json`, `split_summary.json`, `als_trials.json`,
`evaluation.json`, `als_model.json`, `graph_trials.json`, `graph_validation.json`,
`graph_metrics.json`, `mongo_validation.json`, `performance.json`, and a concise
run README under `results/module4/<run_id>/`. The manifest binds source-code,
input receipt and data digests, Spark/Scala/Java/Python/GraphFrames versions,
seed, effective runtime, run ID and model/graph versions. Module 1 HDFS input
requires `_sources.json`; synthetic fixtures need their own manifest and scope.
Do not mix artifacts from different runs.

### Test matrix

The following are required behaviors, not a claim that a named test or live gate
has already passed. Fixtures must include multiple/single prior-order users,
reordered and new-to-user products, an official-train cold product, multi-pair
baskets, isolated vertices and PageRank ties.

| Area | Required cases and assertions |
|---|---|
| Runner/config | Host help without services/Spark; CLI/environment precedence; missing Docker; driver DNS; correct URI and visible failure. |
| Receipt/input | Count/hash mismatch; missing/null/type/domain errors; duplicate keys/FKs; wrong `eval_set`; chronology ties; exact joins and rejection before fit. |
| Split | One versus at least two prior orders; latest-order ties; official-train overlap; eligible, disjoint chronological holdout. |
| ALS | Real implicit fit; nested rating; save/load; cold users; repeat-seed fixture ranking; score ties at K; finite schemas/factors/ranks and exact user sets. |
| Metrics | Hand examples; fewer than K; empty/no predictions; extra users; duplicate ranks/products; correct denominators, bounded coverage and explicit empty cohorts. |
| Baselines/cohorts | Short personal lists; global ties; cold users/items; history subgroups; identical truth/cohorts/K and no duplicate fill. |
| Tuning | Budget and grid deduplication; seeded pilot; full shortlist; failed/all-failed trials; internal-only selection; immediate failure evidence and released factors/caches. |
| Graph construction | Basket duplicates, repeated pairs, thresholds, declared subsets and inactive products; exact weights, isolation, scope and coverage. |
| Graph runtime | Real GraphFrames; missing JAR; symmetric topology; empty/isolated/disconnected graphs; explicit backend failure and PageRank semantics. |
| Graph validation | Null/fractional/negative weights; orphan endpoints; wrong vertex set; NaN/Inf metrics; reciprocal rows and degree conservation. |
| Mongo | Expected sets/counts; index conflicts; rerun; insert/index/rename faults; validated promotion and previous-snapshot preservation. |
| Flow | Real staged layout, optional Module 2 features, standalone worker, cross-module flow and skipped serving with distinct compute/publication status. |
| Failure/evidence | Stage-specific Spark/ALS/GraphFrames/Mongo faults, nonzero exit, actual logs and completed/published-stage manifests. |
| Regression/full data | Module 1–3 regression, full intended input and same-version replay, with matching scope/counts/versions and independent readback. |

- Exercise regressions across contract, Spark, real GraphFrames, live Mongo, runner and cross-module flow. Required runtime-backed groups do not pass by being skipped; include host help and standalone-worker paths; staged fixtures follow the real Module 1 layout.
- Exercise validate, als, graph, and all with correct stage ownership; fixture flows use isolated roots, databases, collections, run IDs, and ownership. Include Module 2 validation/feature generation only when the optional feature path is enabled. Cleanup cannot touch curated data, shared active snapshots, or upstream module outputs.
- CI pins runtime identities and separates fast/fixture, live-Mongo, and manual full-data scopes. Upload actual evidence/logs on success and failure; a configured workflow or dispatch is not evidence that it passed.
- Full-flow acceptance checks the same source/scope/counts/hashes, expected user/product sets, readiness, and replay. Existing Module 1–3 evidence cannot substitute for checks required in a new Module 4 run.
- Report implemented, fixture-validated, integration-validated, and full-data-accepted status separately. Keep overview, status, specification and approved requirement claims consistent with evidence; preserve dated Module 1–3 records as historical.
- Keep A/B completion separate from Module 4C/4D. Update the Proposal only when an approved contract field/scope changes; acceptance does not imply Hybrid quality, dashboard readiness, or production capacity.

### Acceptance execution

The [Docker A/B procedure](docker.md#module-4a4b-operation-and-full-data-acceptance)
owns commands; the [end-to-end acceptance guide](acceptance.md) owns the complete
operator sequence. The historical October acceptance shell script and full run
archive are absent; the current runbook is not a substitute for those records.

1. Run static and selected runtime checks in their appropriate images, including
   real GraphFrames and standalone-worker checks when required. Record commands,
   exit codes and skips; do not run unfiltered tests in an incompatible image.
2. Verify a stable staged source and receipts, isolated bounded flow, stage
   ownership, source immutability, expected sets/counts and publication failures.
   `validate` does not fit/publish; `als` does not build graph; `graph` does not
   fit ALS; `--skip-serving` is compute-only. Module 2 features are optional.
3. For full-data acceptance, use the full intended curated source, fixed
   input/configuration, fresh run IDs, a real backend and Mongo publication/
   readback. Replay the same model/graph versions with a new run ID under the
   declared logical/numeric comparison. Record failures as well as successes.
4. Complete the checklist with matching manifests, actual logs, stage evidence,
   source/runtime identity, measured performance and limitations. Preserve partial,
   failed and interrupted records. Lint or a documentation edit is not runtime
   or full-data acceptance.

### Acceptance checklist

- [ ] Runner/configuration precedence, clean host help, service preflight, and runtime/runner behavior satisfy the [runtime contract](#3-runtime-and-configuration).
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
- [ ] For a new acceptance, use the default full-catalog/all-prior-basket scope and the graph threshold declared and approved for that run; an alternative scope requires prior approval and its corresponding correctness, coverage, evidence, and replay criteria.
  An unapproved subset cannot satisfy a full-catalog gate. In all cases, require same-version full-data first/replay, real MongoDB publication, and independent read-back; compute-only runs do not pass.
- [ ] Required Module 1–3 regression gates pass in the selected runtime.
- [ ] Overview, status, specification and approved-requirement claims agree with the recorded results and evidence links.

Connected Components, quality above personal frequency, and Module 4C/4D are
not mandatory A/B acceptance gates. They retain their separate requirements and
claim boundaries. Historical acceptance statements and their missing original
artifacts are summarized in [project status](progress.md#historical-findings-and-evidence-limits).

<a id="module4c-spec"></a>

## 19. Module 4C — Hybrid recommendation and serving

This section specifies the implemented Module 4C boundary. It complements the
requirements baseline in [Proposal §§28–32](Proposal.md) without changing its
scope. The original C8/C9 run archive is absent; [available evidence](README.md#historical-evidence-available-here) identifies the surviving curated receipts and their limits.

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

The pure ranker also supports explicitly supplied alpha/beta/gamma scoring
when graph weight is positive and catalog PageRank values are available. The
serving CLI retains the two-source lambda contract above; pipeline YAML does
not activate three-way CLI scoring. See [configuration boundaries](progress.md#configuration-and-recovery-boundaries).

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

Controlled responsiveness experiments assess ranking changes, not accuracy.
The historical C8A method used 20 ALS rows with fixed A/B/catalog inputs and an
independent full-precision score oracle. CLI timing includes process startup
and Mongo serving but excludes fixture setup; it is not Kafka-to-serving
latency. The bounded C8B event, watermark, bundle and serving observations have
separate boundaries and did not exercise checkpoint restart. See the
[historical findings](progress.md#historical-findings-and-evidence-limits) and
surviving [C8A](../src/module4/dashboard/assets/c8a-receipt.json) and
[C8B](../src/module4/dashboard/assets/c8b-receipt.json) receipts. Their original
full procedures/logs are unavailable here; neither establishes quality or capacity.

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
MongoDB. The high-level visualization requirements remain in
[Proposal §30](Proposal.md). The historical field-inventory and release archive is absent; this section specifies the active API and dashboard contracts.

### 20.1. Backend configuration and trust boundary

The API process requires these explicit settings; it has no project-database
fallback:

```text
MODULE4D_MONGODB_URI=<MongoDB URI for a database-scoped read-only account>
MODULE4D_MONGO_DATABASE=<explicit database name>
MODULE4D_TREND_SOURCE_NAMESPACES=<comma-separated exact namespace allowlist>
MODULE4D_APP_ROOT=<application/repository root; optional default>
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
implemented paths below are the current route contract; earlier proposed routes are not operator instructions.

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

### 20.5. Acceptance boundary

The implementation tests must exercise scalar validation, fixed query shapes,
index requirements, explicit limits, no-write behavior, error redaction,
pointer races, empty/missing/corrupt snapshots, curated report projections,
and UI loading/empty/error/retry states. Isolated Mongo tests must use
disposable databases; browser and Compose acceptance must keep host bindings
loopback, database storage disposable, and UI isolated from MongoDB.


Runtime results require matching source, image, input and run evidence. The
historical release archive is unavailable, and offline API/UI tests do not
establish deployed or production acceptance. See [project status](progress.md).

## Technical references

- [Spark 3.5.5 ALS API](https://spark.apache.org/docs/3.5.5/api/java/org/apache/spark/ml/recommendation/ALS.html)
- [Spark 3.5.5 ALS recommendation implementation](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala)
- [Spark 3.5.5 RankingMetrics implementation](https://github.com/apache/spark/blob/v3.5.5/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala)
- [GraphFrames 0.8.3 PageRank implementation](https://github.com/graphframes/graphframes/blob/v0.8.3/src/main/scala/org/graphframes/lib/PageRank.scala)
- [Docker Compose run: --use-aliases](https://docs.docker.com/reference/cli/docker/compose/run/)
- [GraphFrames installation](https://graphframes.io/02-quick-start/01-installation.html)
- [GraphFrames centrality metrics](https://graphframes.io/04-user-guide/03-centralities.html)
