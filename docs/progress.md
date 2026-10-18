# Project status

This page records the current implementation and the boundary of the checks
already completed. The approved [Proposal](Proposal.md) remains the requirements
baseline. Detailed behavior and commands are documented in the
[Module 3 contract](module3-contracts.md), [Module 4 plan](module4-plan.md),
[Docker guide](docker.md), and [Kubernetes runbook](kubernetes-demo-runbook.md).

## Current status

**Code complete; external runtime acceptance is pending.** The final independent
source-code review passed on the reviewed implementation revision. The offline
development results below are historical results for that source revision;
they do not describe this documentation-only update.

| Area | Implemented functionality | Verified development checks | Still requires external validation |
|---|---|---|---|
| [Module 1](#module-1--ingestion--storage) | Six-file input validation, cleaning, deterministic synthetic event generation, and HDFS/Kafka staging. | The broad offline suite included bounded synthetic-input cleaning and source-package checks. | Pinned runtime integration and full-data processing. |
| [Module 2](#3-module-2--batch-layer) | Spark batch analytics, prior-history user features, and executor comparison tools. | Offline unit and source checks are included in the recorded suite. | Spark/HDFS execution and actual performance and executor experiments. |
| [Module 3](#module3-status) | Event-time streaming, bounded replay, checkpoint handling, finalized trends, and completed-batch telemetry. | Focused telemetry/BSON checks and offline streaming tests passed in the recorded checks. | Pinned Kafka/Spark/MongoDB integration, runtime checkpoint recovery, and full-feed execution. |
| **Modules 4A–4B** | Prior-history ALS recommendations, product graph metrics, and versioned snapshots. | Offline ranking, publication, and contract checks are included in the recorded suite. | Pinned Spark integration and a fresh full-data run. Historical acceptance is linked below. |
| [Module 4C](#progress-module4c) | Readiness-aware Hybrid recommendations and versioned publication from batch and trend sources. | Offline reader, ranking, and publication checks are included in the recorded suite. | Runtime integration and a leakage-safe Hybrid quality result. |
| [Module 4D](#progress-module4d) | Read-only FastAPI endpoints and a Streamlit dashboard for recommendations, metrics, trends, and evidence. | API, UI client, and AppTest checks are included in the recorded suite. | Browser checks against the pinned runtime and live services. |
| **Kubernetes** | Isolated B2 serving, B3 pipeline, B4 acceptance, and guarded teardown workflows. | Offline fake-client, static validation, runner, and local rendering checks passed. | Container builds/package verification and an isolated B2/B3/B4 runtime. |

The reviewed source revision is `a5fe500`; its full identity is recorded in the
[implementation ledger](implementation-ledger.md#reviewed-source-revision).
The recorded broad offline suite reported **803 passed, 7 skipped**; repository
Ruff passed and Python compilation passed for 190 files. The skips and excluded
service-backed tests are described in the [acceptance handoff](external-acceptance-handoff.md).
No new test run or CI result is attributed to this documentation update.

## External acceptance still required

- Run Spark and MongoDB integration tests in the pinned runtime; complete the
  browser checks and verify the container build and package imports.
- Execute Kubernetes B2/B3/B4 in a dedicated environment and retain fresh
  identity-bound evidence.
- Run the full-data workflow with the six reviewed source files.
- Exercise streaming checkpoint/recovery behavior at runtime.
- Measure actual streaming latency and run both the local comparison and the
  standalone executor-layout experiment.
- Record fresh runtime acceptance results with their exact input, source,
  environment, and scope.

The [acceptance handoff](external-acceptance-handoff.md) summarizes operator
preparation and points to the existing runbooks. **Code complete does not mean
runtime accepted.**

## Historical results

Historical results remain tied to their recorded inputs and environments:

| Result | Record |
|---|---|
| Full-data input counts, Module 1 processing, and Module 2 measurements | [Full-data evidence](evidence/full/README.md) |
| Bounded Kafka, Spark, MongoDB, watermark, and checkpoint observations | [Module 3 evidence](evidence/module3/README.md) |
| Module 4A/4B full-data acceptance, 21 of 21 gates, 10 October 2026 | [Dated acceptance record](evidence/module4/acceptance-20261010/README.md) |
| Module 4C/4D design and release records | [Evidence index](evidence/README.md) |
| Earlier style-refactor comparison and its explicit limitations | [Style regression report](style-regression-validation.md) |

The historical Module 4A/4B result predates later code changes; it is not a
fresh full-data result for the reviewed revision. The named synthetic-feed
measurements below are also historical, not new measurements.

<a id="23-synthetic-feed-configurations"></a>

### Synthetic feed measurements

| Historical preset | Scatter | Span | Peak events/day | Mean events/day | Crowding |
|---|---|---:|---:|---:|---:|
| `scatter_1w` | 1 week | 372 days | 428,965 | 90,912 | 4.72× |
| `scatter_1m` | 1 month | 393 days | 241,413 | 86,054 | 2.80× |
| `scatter_3m` (default) | 3 months | 456 days | 208,674 | 74,165 | 2.81× |

These figures came from earlier named feed runs. The separate 3 October 2026
full-data manifest records a peak of 212,714 events/day for its 13-week feed;
that scope and configuration are preserved in the
[full-data evidence](evidence/full/README.md).

<a id="module-1--ingestion--storage"></a>

## Module 1 — Ingestion and storage

Module 1 validates the six source tables, builds curated data, generates
synthetic event time for replay, stages data in HDFS, and can publish events to
Kafka. The feed includes prior and train orders; event time alone does not
separate evaluation truth. See the [Module 3 provenance contract](module3-contracts.md#kafka-event-contract-and-provenance)
and the [Module 1 operating guide](docker.md#module-1-ingestion-generation-staging-and-kafka-replay).

<a id="3-module-2--batch-layer"></a>

## Module 2 — Batch layer

Module 2 computes historical product and user metrics from curated HDFS data
and writes prior-history user features. Its current benchmark procedures and
measurement boundaries are in the [executor benchmark guide](module2-executor-benchmark.md)
and [Docker guide](docker.md#module-2-batch-processing-and-benchmarks).

<a id="module3-status"></a>

<!-- Compatibility anchors for references to the former Module 3 status sections. -->
<a id="41-final-streaming-architecture"></a>
<a id="42-event-time-watermark-and-trending"></a>
<a id="43-mongodb-serving-and-idempotency"></a>
<a id="44-end-to-end-streaming-validation"></a>
<a id="45-checkpoint-recovery"></a>
<a id="46-late-data-and-watermark-validation"></a>
<a id="47-throughput-measurements"></a>
<a id="48-validation-results"></a>
<a id="49-current-technical-status-and-known-limitations"></a>

## Module 3 — Speed layer

Module 3 consumes Kafka events, aggregates by event time, and publishes trend
results. Its parsing, ranking, checkpoint, telemetry, and publication contracts
are in the [Module 3 guide](module3-contracts.md). Operating steps are in the
[Docker guide](docker.md#module-3-streaming-operations); historical measurements
are in the [Module 3 evidence archive](evidence/module3/README.md).

<a id="progress-module4c"></a>

## Module 4C — Hybrid serving

Module 4C combines eligible batch recommendation sources with finalized trend
bundles. Its evaluation boundary and serving contract are in
[Module 4 §19](module4-plan.md#module4c-spec), and its preserved records are in
the [Module 4C evidence archive](evidence/module4c/README.md).

<a id="progress-module4d"></a>

## Module 4D — API and dashboard

Module 4D exposes read-only recommendation, batch, trend, and evidence views.
The [Module 4 §20 contract](module4-plan.md#module4d-spec) defines its API and
dashboard behavior; the [Docker guide](docker.md) owns setup and validation
commands. The dashboard reads through FastAPI and does not connect directly to
MongoDB.
