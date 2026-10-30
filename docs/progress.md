# Project status and remaining work

This page summarizes the implementation and its verification limits.
[Docker operations](docker.md), [Kubernetes operations](kubernetes.md), and the
[acceptance guide](acceptance.md) own procedures. The
[repository proposal](Proposal.md) is retained as a requirements baseline;
no authoritative replacement for the revised proposal is present here.

## Status at a glance

| Status | Meaning and current scope |
|---|---|
| **IMPLEMENTED** | Module 1–4 processing, serving, dashboard, packaging, and isolated Kubernetes orchestration exist in source. This does not close every proposal requirement or acceptance gate. |
| **VERIFIED ON A SPECIFIC HISTORICAL SCOPE** | The retained batch, ALS, and synthetic Hybrid artifacts describe their own inputs, runtime and source provenance. Their results do not validate the current source end to end. |
| **NOT YET VERIFIED ON THE CURRENT SOURCE** | Fresh live integration, browser, Kubernetes B2/B3/B4, full-data, checkpoint-recovery, and deployment acceptance remain required. |
| **PLANNED / NOT IMPLEMENTED** | Bronze/Silver/Quarantine layers, distributed cleaning/ETL, and DuckDB comparisons are absent. Their requirements must be reconciled with the authoritative revised proposal. Connected Components remains optional and unimplemented. |

Local repository and Spark fixture regressions, API read contracts, dashboard
UI tests, offline Kubernetes rendering, and cached packaged imports have been
checked. Service-dependent skips were recorded rather than counted as passes.
Those checks do not demonstrate live MongoDB/Kafka behavior, a new image build,
a hosted workflow pass, Kubernetes deployment, or full-data acceptance.

## Implemented capabilities

### Module 1 — ingestion and storage

The cleaner validates all six CSV tables, raw domains, keys, foreign keys,
order relationships and contiguous cart positions before type conversion.
It publishes six typed Parquet tables through a guarded single-writer directory
replacement. Unrecognized output, symlink/path overlap, or invalid input fails
without replacing the recognized publication.

The generator creates deterministic purchase events from prior and train
orders. Entity-keyed randomness makes processing batch size independent of
generated values. HDFS staging preserves source/handoff receipts; Kafka replay
uses `user_id` keys and stable event identities. Generated event time is
simulation, and Kafka messages omit `eval_set`. See the
[Module 1 procedure](docker.md#module-1-ingestion-generation-staging-and-kafka-replay)
and [data contracts](implementation-ledger.md#2-module-1--raw-contracts-cleaning-and-publication).

### Module 2 — batch analytics

Spark validates staged handoff counts, keys, relationships, domains and exact
fact/event multisets, then computes prior-history product, department, user,
weekday/hour and pivot/unpivot analytics. MongoDB collections are validated
before individual promotion; their set is not one transaction.

The HDFS `user_features`, `user_behavior_features`, `product_features`, and
`user_product_features` handoffs are descriptive and marked `prior_only`; they
are not ALS or Hybrid ranking inputs. A standalone Module 1 temporal export
keeps official train baskets in evaluation and records source/output hashes;
it does not change the curated Modules 2/4 split or validate temporal Hybrid
quality. See the [split and feature contracts](docker.md#temporal-split-export-and-descriptive-feature-contracts). Join, pruning, cache and local-thread
benchmarks are implemented. A separate standalone-master experiment records
actual executor allocation. Requested resource settings alone do not establish
an executor-layout result. See the [batch and benchmark guide](docker.md#module-2-batch-processing-and-benchmarks).

### Module 3 — event-time streaming

The stream validates purchase messages, accounts for invalid records, and
deduplicates `event_id` within watermark-bounded state. Defaults are 30-minute
short counts within 120-minute sliding windows, a five-minute slide, ten-minute
watermark, weights 0.7/0.3, Top-20, and a ten-second trigger.

The mutable row sink and opt-in immutable finalized-window bundles have
separate publication contracts. Hybrid and the trends API use bundles, not the
mutable row sink. Finite replay completion requires reconciled producer counts
and consumed terminal offsets; it may leave an unfinalized event-time tail.
Completed-batch telemetry pairs progress and sink receipts without silently
dropping evidence. Ingestion-to-output latency includes watermark delay and
unsynchronized host clocks; it is not Kafka acknowledgement or UI latency.
See [Module 3 contracts](module3-contracts.md).

### Modules 4A–4D — recommendations and presentation

ALS fits prior interactions; the internal prior-history holdout selects
parameters, while official train baskets are reserved for final evaluation.
[`src/module4/tuning.py`](../src/module4/tuning.py) owns bounded pilot/full-holdout
experiments and trial selection. The graph deduplicates basket products,
retains isolated catalog vertices, and computes weighted degree and GraphFrames
PageRank. PageRank uses topology rather than co-purchase weights.

Hybrid captures a ready ALS/graph generation and the newest eligible immutable
trend bundle at an explicit cutoff. It validates sources, ranks bounded
candidates deterministically, and promotes an immutable per-user version
through a readiness pointer. The default serving CLI uses lambda 0.7 for ALS
and trends. The pure ranker supports explicit PageRank weighting; the CLI does
not activate YAML three-way weights. Prior-only evaluation uses fixed
prior-frequency candidates and does not establish leakage-safe temporal trend
quality.

FastAPI exposes bounded read-only Hybrid, ALS, batch, trend, telemetry and
evidence views. Streamlit's four pages call the API and have no direct MongoDB
client. Missing, empty, unavailable and malformed responses remain distinct.
API liveness is not data readiness; dashboard refresh is not stream liveness.
See [Hybrid contracts](module4-plan.md#module4c-spec) and
[API/dashboard contracts](module4-plan.md#module4d-spec).

### Kubernetes demonstration

B2 prepares a run-owned serving tier, B3 runs ordered input, ingestion, batch,
model, replay and Hybrid work, and B4 checks acceptance evidence. Dedicated
identities, private receipts, ownership checks, separate MongoDB roles,
network policies and resource bounds are implemented. Spark runs locally inside
Jobs, not as distributed Kubernetes executors. Manifest and policy validation
is owned by [`scripts/k8s/render.py`](../scripts/k8s/render.py); pipeline
orchestration imports those validators.

The committed version lock is a template until real host, cluster, storage,
image and toolchain identities are measured. A `runtime_pass` receipt records
B4 completion for its declared scope, not teardown. Offline `verify` contacts
no cluster. Resources and failure evidence remain until separately reviewed,
identity-checked teardown. Live CNI enforcement, storage and deployed behavior
remain unverified on the current source. See the [Kubernetes guide](kubernetes.md).

## Configuration and recovery boundaries

[`config/pipeline.yaml`](../config/pipeline.yaml) is not a universal runtime
control surface. Its comments identify active consumers and declared intent.
Standalone CLIs retain their own defaults and supported environment/argument
precedence. Module 4 ALS tuning reads fit parameters for its first candidate;
CLI limits still determine the standalone tuning budget. Hybrid YAML loading
requires an explicit `HybridRankingConfig.from_pipeline_config()` call.

Kubernetes consumes operational deadlines, host reserves and selected synthesis,
model, replay and Top-K settings. It does not forward every synthesis option,
streaming window/trigger, graph setting or Job resource declaration. Job
allocation remains in the manifests. Effective values must be checked at the
consumer rather than inferred from YAML alone.

Task steps may reuse existing receipt/output combinations when
`execution.resume_policy` is `enabled` or `strict`; these two policies currently
share behavior. `disabled` bypasses that reuse. This is not proof that reused
outputs match newly changed configuration. The coordinator still requires a
fresh identity/output, Jobs have no automatic retries, and checkpoint reuse
separately requires matching stream/topic/state identity. Do not describe this
as automatic end-to-end recovery.

## Remaining requirements and acceptance

- **Source and scope:** confirm the latest approved proposal and course code.
  The repository proposal says IT4931; the current coursework context says
  IT4043E. Do not change runtime identifiers or mark revised requirements done
  until this discrepancy is resolved.
- **Live integration and full data:** exercise the pinned Spark/GraphFrames,
  HDFS and MongoDB paths, publication/replay and independent readback with six
  verified source CSVs. Distinguish fixture, compute-only and full publication
  scopes using the [A/B acceptance checklist](module4-plan.md#acceptance-checklist).
- **Streaming and recovery:** verify live Kafka delivery, late/invalid/duplicate
  handling, checkpoint continuation, bundle publication and the defined latency
  boundary. Offset drainage does not prove every window finalized.
- **Kubernetes and UI:** build/load the selected images, finalize the measured
  lock, exercise B2/B3/B4, storage, permissions, allowed/denied network paths,
  API readiness and browser behavior, and retain run-bound receipts.
- **Experiments and quality:** retain actual Kafka-partition and standalone
  executor-layout experiments; local thread comparisons are a different scope.
  A leakage-safe temporal Hybrid evaluation source and quality result remain
  open. An implemented ranker is not evidence of improvement over baselines.
- **Lifecycle and capacity:** published Hybrid versions have no retention rule;
  retained HDFS checkpoints and uncompressed event logs need an explicit
  retention/export policy. Historical notes report streaming heap exhaustion
  with a two-gigabyte executor over a 456-day feed; reproduce and size that
  workload before claiming full-feed capacity.
- **Deliverables:** final experiment reporting and the required demonstration
  or presentation need their own evidence. Source implementation and local
  tests alone do not close the proposal's Definition of Done.

## Historical findings and evidence limits

The full `docs/evidence/` archive is absent. The five curated artifacts indexed
in [available evidence](README.md#historical-evidence-available-here) remain
readable with their original provenance; they are not a complete run archive.

| Record | Observation that remains useful | Limit |
|---|---|---|
| [Batch benchmark receipt](../src/module4/dashboard/assets/current-hardening-2026-10-03.json) | Historical input/generation reconciliation and join, filter and cache comparisons. | One warm host and a small standalone executor; cache construction and filtered scan scope matter. No distributed speedup is established. |
| [ALS evaluation](../src/module4/dashboard/assets/evaluation.json) | Official-train Recall@10: ALS 0.121472 versus personal frequency 0.319232. | ALS did not outperform this baseline. The plan's historical 21/21 A/B acceptance statement cannot replace its missing full run archive or current acceptance. |
| [Hybrid responsiveness](../src/module4/dashboard/assets/c8a-receipt.json) | Top-1 changed at lambda 0 and 0.5, but not at default 0.7; Top-20 overlap stayed 20/20. | Controlled synthetic inputs; no accuracy measurement. |
| [Streaming demonstration](../src/module4/dashboard/assets/c8b-receipt.json) | 48 planned/acknowledged/consumed events, zero invalid records and 36 finalized bundles. | No Spark checkpoint restart or A/B source-generation race. The original cleanup claim omitted three anonymous Kafka volumes. |
| [Style regression comparison](style-regression-validation.md) | Bounded logical outputs matched; Parquet byte differences were traced to footer serialization. | Two original browser assertions remained unexplained; possible earlier persistent-volume writes and full-data equivalence remained unresolved. Its final assessment is NOT FULLY VERIFIED. |

### Graph comparison limits

A bounded historical graph comparison found matching validation reports and
integer graph results, but metric fingerprints differed across fresh Spark
sessions for the medium workload: one of three independent baseline records
differed, while the small workload's three records agreed. The cause was not
established. Small elapsed-time gains were not reliably demonstrated, and no
production optimization was retained. Detailed measurement files are not part
of this checkout; this documentation change reruns none of those experiments.
Cross-session fingerprint stability and same-version numerical replay therefore
remain verification questions, not guarantees inferred from graph invariants.

### Historical synthetic-feed measurements

<a id="23-synthetic-feed-configurations"></a>

Earlier status prose recorded these feed summaries. Their original manifests
are not present here, so retain them as historical observations, not regenerated
results or current defaults:

| Feed | Scatter window | Span | Peak events/day | Mean events/day | Peak/mean |
|---|---:|---:|---:|---:|---:|
| `scatter_1w` | 1 week | 372 days | 428,965 | 90,912 | 4.72× |
| `scatter_1m` | 4 weeks | 393 days | 241,413 | 86,054 | 2.80× |
| `scatter_3m` | 13 weeks | 456 days | 208,674 | 74,165 | 2.81× |

The retained batch receipt separately records a 210,195-event peak for its
13-week run; older prose also cited 212,714. Without matching manifests these
figures cannot be combined or described as a reproduction. Its processing
batch size was 2,000 users, while the generator's current default is 20,000;
batch size bounds memory and does not change seeded event values. See
[synthetic generation](docker.md#synthetic-event-generation) for the algorithm.
