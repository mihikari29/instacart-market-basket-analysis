# Project Progress and Requirements Coverage

> Current repository status is assessed against the approved [Proposal](Proposal.md). Historical runs remain attributed to their recorded revisions and environments. This page records status and evidence links; detailed contracts belong in the [Module 3 guide](module3-contracts.md), [Module 4 plan](module4-plan.md), and [Docker runbook](docker.md).

---

## 1. Architecture overview and module status

```text
CSV -> Module 1
  ├-> HDFS -> Module 2 batch metrics --------------------------> Module 4D API/UI
  ├-> HDFS prior -> Module 4A ALS --┐
  ├-> HDFS prior -> Module 4B graph ┴-> Module 4C -> Hybrid snapshots -> Module 4D
  └-> Kafka -> Module 3
                  ├-> legacy mutable `realtime_trending`
                  └-> optional immutable bundles -> Module 4C and Module 4D trends API
```

| Module | Implementation status | Validation status and boundary |
|---|---|---|
| **1 — Ingestion & Storage** | Cleaning, deterministic synthetic event generation, HDFS staging, and Kafka producer are implemented. Raw-input preflight is incomplete. | Historical full-data handoff and monotonicity checks passed; see [full-data evidence](evidence/full/README.md). Malformed or partial raw input is not fail-closed at the cleaner. |
| **2 — Batch Layer** | Spark analytics, feature output, MongoDB publication, and join/pruning/cache experiments are implemented. | Historical full-data validation passed. Exact staged-input SHA validation is not performed by Module 2; full-data workflows have not been rerun after the style refactor. |
| **3 — Speed Layer** | Kafka parsing, event-time aggregation, legacy trend rows, checkpoints, and opt-in immutable bundles are implemented. | Historical bounded Kafka/Spark/Mongo, late-data, and checkpoint-recovery evidence is recorded. The recorded Module 3 CI run passed on the parent code commit; PR #11 changed documentation only. This does not establish full-data scale or defined ingestion-to-output latency. |
| **4A — ALS** | Prior-only implicit ALS, internal holdout tuning, official-train evaluation, baselines, and versioned output are implemented. | Historical full-data acceptance passed 21/21 gates on 10 October 2026. The recorded A/B runtime CI run passed; post-refactor full-data workflows remain unrun. |
| **4B — Product graph** | Prior-basket graph, full catalog, threshold experiments, weighted degree, GraphFrames PageRank, and versioned publication are implemented. | Included in the same historical full-data acceptance and the recorded A/B runtime CI run. Optional Connected Components remains absent. |
| **4C — Hybrid** | Readiness-aware source reads, deterministic ranking, and versioned publication are implemented. | Bounded local and CI integration checks exist. C8A/C8B are synthetic; official-train Hybrid quality and production capacity have not been established. |
| **4D — API and dashboard** | Read-only FastAPI and four-page Streamlit UI are implemented. | The recorded fixture and browser CI jobs passed. The dashboard does not provide an independently verified per-user ALS recommendation view; production security/scalability are outside recorded validation. |
| **Deployment** | Docker Compose development and isolated synthetic dashboard demo are implemented. | The required local Kubernetes demonstration is **not implemented** in this repository. |

The canonical source responsibilities and cross-module contracts are mapped in
the [repository layout guide](repository-layout.md). Current technical contracts
for Module 3 are in [module3-contracts.md](module3-contracts.md); Modules 4A–4D
are in the [Module 4 plan](module4-plan.md).

### 1.1. CI recorded for the audit baseline

Audit baseline: `main` at `75c4f8854d9e23df4311bf2c53f0c8133a4bb5be`,
inspected on 15 October 2026. These successful checks cover their configured
CI scope, not full-data acceptance:

- [Fast validation 38742236253](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38742236253): fixtures and the isolated Module 4D browser job passed.
- [Module 4A–4B runtime 38742235872](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38742235872): runtime workflow passed.
- [Module 3 streaming validation 38727634271](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38727634271): passed on parent commit `d00b8f047983ac4ebd28d8072d0f0d026ba3c466`; the documentation-only PR #11 did not change Module 3 source.

The earlier [Fast validation 38727634347](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38727634347)
failed its browser assertion after observing four evaluation charts instead
of five. A later PR #11 browser run and the browser run recorded for this audit
baseline passed. The cause of the historical failure is unknown; documentation
changes do not establish a fix.

### 1.2. Outstanding Proposal requirements and validation gaps

| Priority | Gap | Evidence and interpretation |
|---|---|---|
| **P1 — must-have** | Kubernetes demonstration | Proposal §§3.1, 36.2, 39, 45 require it; no manifest, deployment script, or repository evidence exists. Docker Compose does not satisfy this requirement. |
| **P1 — must-have** | Module 1 raw-input preflight | `src/module1/clean.py:55–64,117–169` skips missing CSV files, converts `reordered` before raw-domain validation, and writes outputs before its printed audit. This is a malformed/partial-input guard gap, not evidence that historical valid-data outputs were wrong. |
| **P1 — must-have** | Per-user ALS dashboard view | Proposal §30.3 requires it. `src/module4/dashboard/ui/pages/recommendations.py:174–176` explicitly says the verified ALS comparison endpoint is unavailable. |
| **P2 — measurement** | Defined streaming latency | Proposal §§30.1, 37, 42 define processing/output time minus ingestion time. Existing trigger, addBatch, sink, and bounded acknowledgement-to-bundle timings are different boundaries. |
| **P2 — experiment** | Executor-configuration comparison | Proposal §15.6 has no recorded comparison; configuration knobs alone are not experiment evidence. |
| **P2 — handoff** | Module 2 `user_features` → Module 4 | Proposal §32 lists Module 4 as consumer. Module 2 writes the features; the Module 4 path accessor has no active reader. The technical plan describes features as optional cohort input. Owner decision is needed on intended use. |
| **P2 — research** | Leakage-safe Hybrid quality evaluation | No official-train Hybrid result is established. Ordinary generated events include prior and train orders without retaining `eval_set` in the Kafka event contract. |
| **P2 — validation** | Full-data post-style comparison | The style report remains **NOT FULLY VERIFIED**. Full-data Module 2, 3, and 4 comparison workflows are explicitly **NOT RUN**. |
| **P2 — diagnosis** | Historical browser assertion | Four-chart main failure remains unexplained despite later passing browser CI. Preserve the five-chart assertion and capture evidence if it recurs. |

No missing test alone is evidence that implementation results are wrong. The
historical and current validation scopes are separated in the linked reports.

---

## 2. Module 1 — Ingestion & Storage

<a id="module-1--ingestion--storage"></a>

### 2.1. Processing flow

```text
Six CSV inputs
  -> clean typed tables
  -> deterministic synthetic event time for replay
  -> HDFS raw/curated tables and validation receipts
  -> Kafka purchase events keyed by user_id
```

The generated stream includes prior and train orders; the official test set is
not part of this feed. The producer emits a logical event ID and synthetic
event-time fields, then adds ingestion time at send time. These clocks are not
interchangeable. See [Module 3 event provenance](module3-contracts.md#kafka-event-contract-and-provenance).

<a id="22-cleaning-and-reconstruction-results"></a>

### 2.2. Historical full-data results

The 3 October 2026 evidence records 3,421,083 orders, 32,434,489 prior facts,
1,384,617 train facts, 49,688 products, 33,819,106 events, and 456 daily event
partitions. Source/local/HDFS event counts matched, with zero recorded
monotonicity violations. These are historical results for the recorded source
and runtime, not a fresh run on the audit baseline. Full environment,
configuration, and limitations are in [full-data evidence](evidence/full/README.md).

A separate 8 October source-preparation log records the cleaner's historical
audit: 206,209 first-order gaps filled, 369,323 capped gaps reconstructed within
[30, 36], zero weekday mismatches, 16 product names normalized, and 75,000 test
orders kept outside the prior/train stream. See the dated
[source-cleaning output](evidence/module4/validation-20261008/source-clean.txt);
these recorded counts do not establish a fresh run or close the cleaner's
current raw-input preflight gap.

### 2.3. Synthetic feed configurations

Entity-keyed deterministic generation is invariant to `--batch-users` for the
same source and seed. This is a deliberate improvement over the batch-size
reproduction wording in Proposal §8.2; the approved Proposal is retained as the
requirements baseline. The following figures are historical measurements copied
from the pre-consolidation documentation. They describe the earlier named feeds;
they are not fresh runs or the later hardening full-data feed.

| Feed | Scatter | Span | Peak events/day | Mean events/day | Crowding |
|---|---|---:|---:|---:|---:|
| `scatter_1w` | 1 week | 372 days | 428,965 | 90,912 | 4.72× |
| `scatter_1m` | 1 month | 393 days | 241,413 | 86,054 | 2.80× |
| `scatter_3m` (default) | 3 months | 456 days | 208,674 | 74,165 | 2.81× |

The separate 3 October 2026 hardening full-data evidence records seed 42,
13-week scatter, 2,000 users per batch, and a peak of **212,714 events/day** in
its [manifest](evidence/full/manifest.json) and [summary](evidence/full/summary.json).
That evidence value differs from the **210,195** figure in the pre-consolidation
prose; the tracked manifest and summary agree on 212,714, so this page preserves
the receipt-backed value and records the discrepancy rather than treating the
older prose figure as verified.

The cleaner still needs an all-input and raw-domain preflight before coercion,
joins, or writes. `--validate` currently prints statistics; it is not a
fail-closed validation gate. See `src/module1/clean.py` and the P1 item above.

---

## 3. Module 2 — Batch Layer

### 3.1. Handoff contracts and HDFS validation

Module 2 reads curated HDFS tables, validates receipt structure, counts, nulls,
keys, relationships, domains, and relevant fact/event multisets. It preserves
train facts for later recommendation truth and uses prior facts for historical
analytics.

The current code does **not** independently compute exact SHA-256 matches for
every staged input. The optional `--source-events` path compares a supplied
local event file's hash with its receipt; that check is narrower. Module 4 has a
separate staged-Parquet hash verification path in `src/module4/data.py`. Do not
conflate these checks.

### 3.2. Spark SQL analytics and MongoDB serving

The pipeline computes user features and writes `/instacart/features/user_features`,
creates product/department metrics, calculates weekday/hour distributions,
and checks department-hour pivot/unpivot totals. Batch outputs are published to
MongoDB for the API/dashboard. The declared Module 4 feature-consumer role is
unresolved; see §1.2.

### 3.3. Performance optimization benchmarks

Historical full-data evidence records join-strategy, partition-pruning, and
aggregate-cache comparisons. The cache experiment compares reuse of a cached
aggregated result with recomputation. It does not demonstrate reuse across
multiple independent aggregation stages. Measurements and execution plans are
in [full-data evidence](evidence/full/README.md). An executor-configuration
comparison remains unrecorded.

Historical full-data Module 2 validation passed the recorded reconciliation,
pivot/unpivot, and Mongo publication checks. The post-style full-data workflow
has **not been rerun**. CI recorded for the audit baseline covers fixture and
runtime checks only.

---

## 4. Module 3 — Speed Layer / Structured Streaming

<a id="module3-status"></a>

Module 3 implements Kafka event-time aggregation, legacy trend publication,
checkpoint recovery, and opt-in immutable bundles for Module 4C. The canonical
[Module 3 guide](module3-contracts.md) documents current parsing, windows,
ranking, publication, hashes, provenance, and retry contracts.

<!-- Compatibility anchors for the retired Module 3 subsections, now covered by
     module3-contracts.md and evidence/module3/README.md. -->
<a id="41-final-streaming-architecture"></a>
<a id="42-event-time-watermark-and-trending"></a>
<a id="43-mongodb-serving-and-idempotency"></a>
<a id="44-end-to-end-streaming-validation"></a>
<a id="45-checkpoint-recovery"></a>
<a id="46-late-data-and-watermark-validation"></a>
<a id="47-throughput-measurements"></a>
<a id="48-validation-results"></a>
<a id="49-current-technical-status-and-known-limitations"></a>

### 4.1. Historical streaming validation

The 3 October 2026 evidence records bounded live Kafka → Spark → Mongo tests,
poison/duplicate handling, watermark scenarios, finalized ranking, two-process
checkpoint continuation, throughput, and partition measurements. The 50,000
event measurement used one Kafka broker and a two-core Spark runtime; it is not
production capacity evidence. The full results, exact run context and
limitations remain in the [Module 3 evidence archive](evidence/module3/README.md).

The Module 3 workflow passed on `d00b8f0`; the later PR #11 changed documentation
only. There is no recorded full-data Module 3 workflow after the coding-style
refactor. Spark trigger/addBatch/sink timings are not the Proposal's defined
ingestion-to-output latency metric.

### 4.2. Current technical limitations

- Open windows are not recomputed into a leaderboard on each processing trigger;
  watermark-bounded deduplication is not permanent event-ID uniqueness.
- The legacy mutable sink is not transactional for readers. Bundle publication
  is opt-in; enablement and index ownership are in the [Docker guide](docker.md#module-3-streaming-and-module-3-to-module-4c-enablement).
- C8B validates a bounded integration and bundle publication, not checkpoint
  restart during that publication.
- No defined end-to-end ingestion-to-output latency is recorded.

---

## 5. Reproduction guide

This heading and its subsections remain for compatibility with existing links.
Current commands and prerequisites are maintained in the [Docker runbook](docker.md).

### 5.1. Start the service cluster with Docker Compose

See [persistent pipeline startup and existing-data boundaries](docker.md#run-the-persistent-pipeline-stack) and the separate [isolated demo](docker.md#start-and-stop-the-synthetic-dashboard). The default project has persistent pipeline volumes; review the selected project and existing data before using it.

### 5.2. Run the ingestion and staging pipeline (Module 1)

See the canonical [Module 1 ingestion, generation, staging, and replay runbook](docker.md#module-1-ingestion-generation-staging-and-kafka-replay).

### 5.3. Run Spark batch analytics and benchmarks (Module 2)

See [Module 2 batch processing and benchmark operations](docker.md#module-2-batch-processing-and-benchmarks). The `scripts/module2.py` wrapper starts Compose before forwarding its command-line arguments; do not use its `--help` option to inspect usage.

### 5.4. Run Spark Structured Streaming (Module 3)

See [Module 3 streaming operations](docker.md#module-3-streaming-operations) and [optional bundle publication for Module 4C](docker.md#module-3-streaming-and-module-3-to-module-4c-enablement). The `scripts/module3.py` wrapper starts Compose before forwarding arguments.

### 5.5. Run Module 4A–4B

See the [Module 4A/4B operating and acceptance guide](docker.md#module-4a4b-operation-and-full-data-acceptance) and the [Module 4 plan](module4-plan.md). Full-data acceptance commands are maintained in the operating guide.

### 5.6. Run the tests

See [Docker validation commands](docker.md#validation-commands), the module-specific workflows, and the dated evidence links. A skipped service-backed test is not a pass; reports identify its execution scope.

---

## 6. Deliverables and technical acceptance checklist

| Proposal area | Current status |
|---|---|
| Modules 1–3 data processing, batch/streaming contracts, Mongo outputs | Implemented with historical evidence; see §§2–4 and the linked records |
| Performance experiments | Join/pruning/cache and Kafka partition measurements exist; executor comparison is missing; streaming latency definition is not measured |
| Module 4A–4B | Historical full-data acceptance passed; recorded A/B runtime CI passed; post-style full-data workflow remains unrun |
| Module 4C–4D | Implemented and CI-tested within fixture/runtime scope; per-user ALS view and leakage-safe Hybrid quality evaluation remain outstanding |
| Docker demonstration | Implemented for local/isolated demo workflows |
| Kubernetes demonstration | Missing; explicit must-have gap |
| Full system after style refactor | **NOT FULLY VERIFIED**; full-data Module 2/3/4 workflows **NOT RUN** |

Dated full-data Module 4A–4B acceptance, including failed and interrupted
attempts, remains in [acceptance evidence](evidence/module4/acceptance-20261010/README.md).
The post-style comparative report retains its **NOT FULLY VERIFIED** assessment
and its limitations in [style regression validation](style-regression-validation.md).

---

## 7. Module 4C — Hybrid serving and streaming integration

<a id="progress-module4c"></a>

Module 4C captures a ready Module 4A/4B generation, validates bounded inputs,
reads the newest eligible immutable Module 3 bundle at an explicit event-time
cutoff, deterministically ranks candidates, and publishes versioned
per-user snapshots. Its current contracts are in [Module 4 plan §19](module4-plan.md#module4c-spec).

C8A used a fixed synthetic source; a controlled shift changed Top-1 at λ=0.0
and 0.5, but at the default λ=0.7 it changed neither Top-1 nor rank order.
Recommendation accuracy was not measured. C8B reconciled 48 planned,
acknowledged, and consumed events with zero invalid records and observed 36
finalized bundle documents. It did not demonstrate checkpoint restart. These
are bounded integration observations, not full-data Hybrid quality evidence.
The original receipts and reports remain in the [Module 4C archive](evidence/module4c/README.md).

No leakage-safe official-train Hybrid evaluation is available. Ordinary Module 1
stream artifacts combine prior and train orders without retaining `eval_set` in
the Kafka payload. A defensible chronology and evaluation source must be designed
before quality comparisons are claimed. Historical ALS already trails personal
frequency on Recall@10; that is a measured baseline outcome, not evidence of a
metric or serving defect.

---

## 8. Module 4D — Read-only API and Streamlit dashboard

<a id="progress-module4d"></a>
<a id="d3-local-integration-and-release-validation"></a>

The Module 4D API reads verified Hybrid recommendations, finalized trend
bundles, bounded batch metrics, order distributions, and allowlisted historical
evidence. MongoDB indexes are provisioned by an administrator; API credentials
are database-scoped and read-only. The four-page Streamlit UI calls the API and
has no direct MongoDB connection.

The isolated local demo uses a unique database on disposable tmpfs and
loopback-bound API/UI ports. The fixture and browser jobs recorded for the audit
baseline passed; the historical four-chart browser failure remains unexplained.
The API/UI contracts are in [Module 4 plan §20](module4-plan.md#module4d-spec),
and current commands, index requirements, and cleanup are in the
[Docker guide](docker.md). The retired design and release records remain in the
[Module 4D evidence archive](evidence/module4d/README.md).

The dashboard displays historical ALS metrics but has no independently
verified per-user ALS recommendation endpoint/view. That remains a must-have
Proposal gap; the Hybrid component scores shown in the recommendation table are
not an ALS-only recommendation list.
