# IT4931 — Instacart market basket analysis

This project combines Spark batch analytics with event-time streaming over the
Instacart market-basket dataset. Its Lambda Architecture separates historical
recommendation inputs from replayable trend events and serves API/dashboard
views.

## Architecture

```text
Six Instacart CSV files -> Module 1
  ├─> HDFS curated -> Module 2 -> batch metrics ----------------> Module 4D API
  ├─> HDFS prior -> Module 4A ALS ─┐
  ├─> HDFS prior -> Module 4B graph ┴─> Module 4C
  └─> Kafka -> Module 3
                 ├─> `realtime_trending` (legacy mutable output)
                 └─> optional `realtime_trending_windows` ┬─> Module 4C
                                                          └─> Module 4D trends API
Module 4C -> versioned Hybrid recommendations -----------------> Module 4D API -> UI
```

The mutable `realtime_trending` collection preserves the legacy Module 3
output contract. Module 4C and the Module 4D trends view consume separately
published immutable `realtime_trending_windows` bundles; publication is
opt-in. Modules 4A and 4B use prior-history data. Official train baskets are
evaluation truth, not ALS training data. Module 4D reads through FastAPI; the
UI has no direct MongoDB connection.

## Project status

| Area | Status |
|---|---|
| Modules 1–2 | Implemented; historical full-data results are recorded. Module 1 raw-input preflight remains incomplete. |
| Module 3 | Implemented; bounded streaming and recovery evidence is recorded. End-to-end latency is not measured. |
| Modules 4A–4B | Historical full-data acceptance passed 21/21 gates on 10 October 2026; post-style full-data workflows have not been rerun. |
| Module 4C | Implemented and integration-tested; leakage-safe Hybrid quality and production-capacity results are not established. |
| Module 4D | Read-only API and four-page dashboard are implemented; an independently verified per-user ALS view is missing. |
| Kubernetes | Required by the Proposal; no deployment assets or evidence are present. |

See [project progress](docs/progress.md) for evidence dates, current CI, Proposal
gaps, and next work. The [Module 3 guide](docs/module3-contracts.md) owns its
technical contract; the [Module 4 plan](docs/module4-plan.md) owns Module 4
algorithms and API contracts.

## Repo layout

```text
src/module1/             cleaning, synthetic event generation, HDFS and Kafka
src/module2/             Spark batch analytics, feature extraction, benchmarks
src/module3/             Structured Streaming, trends, bundles, checkpointing
src/module4/             ALS, product graph, Hybrid serving, read API/dashboard
scripts/                 project runners and scoped validation/demo tools
deploy/module4/           isolated synthetic dashboard Compose setup
tests/                   unit, contract, Spark and service-backed tests
docs/                    Proposal, current guides, runbooks and dated evidence
.github/workflows/       CI and separately gated full-data workflows
```

See the [repository layout guide](docs/repository-layout.md) for stable entry
points. Docker targets, prerequisites, and operating procedures are in the
[Docker guide](docs/docker.md).

## Dataset (originals → cleaned)

The public source has six CSV tables and no absolute purchase timestamps.
Recorded full-data evidence covers 3,421,083 orders, 32,434,489 prior facts,
1,384,617 train facts, and 49,688 products; these are historical measurements,
not a fresh run claim. See the [full-data evidence](docs/evidence/full/README.md).

Module 1 creates **Synthetic event time for replay simulation**. Its stream
contains prior and train orders, so an event-time cutoff alone does not make the
feed safe for official-train evaluation. See [synthetic feed status](docs/progress.md#23-synthetic-feed-configurations)
and the [Module 3 provenance contract](docs/module3-contracts.md#kafka-event-contract-and-provenance).

## What to use, and for which module

Use curated HDFS/Parquet tables for batch and recommendation inputs, and the
generated Kafka JSON feed for streaming. The [repository layout guide](docs/repository-layout.md)
maps these paths to their owning modules.

## Why parquet (and not CSV)

Curated tables use Parquet for typed, columnar Spark reads; source CSV remains
the Module 1 cleaning input, while Kafka events use JSON.

## Run

For a first look, use the [isolated synthetic dashboard](docs/docker.md#start-and-stop-the-synthetic-dashboard).
It uses disposable MongoDB storage and loopback-only API/UI ports; it does not
demonstrate production behavior. The default Compose project uses persistent
pipeline services and data, so review the [Docker runbook](docs/docker.md)
before operating it.

## Cleaning (validated)

Historical full-data checks recorded the cleaning and event-generation results.
The current cleaner still needs fail-fast checks for missing inputs and raw
`reordered` values before coercion or output writes. This guard gap does not
invalidate the recorded valid-input results; details are in [progress](docs/progress.md#module-1--ingestion--storage)
and the [full-data evidence](docs/evidence/full/README.md).

## SyntheticConfig

Entity-keyed deterministic generation is invariant to `--batch-users` for the
same source and seed. See [Module 1 progress](docs/progress.md#module-1--ingestion--storage)
and the full-data evidence for the recorded scope.

## Scenario feeds (historical runs)

Older `scatter_1w`, `scatter_1m`, and `scatter_3m` measurements remain in the
[evidence archive](docs/evidence/README.md) with their original run context.

## Event schema (one row = one product event)

The Kafka payload contains event identity, order/user/product and basket fields,
order weekday/hour, synthetic event-time fields, and send-time ingestion time.
See the [Module 3 contract](docs/module3-contracts.md#kafka-event-contract-and-provenance)
for field names and validation.

## Data relationships (join keys)

```text
orders.order_id ──> order_products__prior/train.order_id      (1:N)
order_products.product_id ──> products.product_id              (N:1)
products.aisle_id ──> aisles.aisle_id                          (N:1)
products.department_id ──> departments.department_id            (N:1)
```

Generated Kafka events are denormalized; the runtime trend aggregation does not
join these dimension tables.

<a id="lambda-modules-spec-7--verify-against-assignment"></a>

## Lambda modules

| Module | Role |
|---|---|
| 1 | Clean source data, create replay events, and stage HDFS data |
| 2 | Compute prior-history batch metrics and user features |
| 3 | Aggregate Kafka events by event time and publish finalized trends |
| 4A–4B | Fit prior-only ALS and compute product-graph metrics |
| 4C | Verify source snapshots and publish versioned Hybrid recommendations |
| 4D | Serve read-only API and dashboard views |

Implementation detail and requirement coverage belong in [progress](docs/progress.md)
and the canonical technical guides.

## Team Onboarding & Environment Setup

Use the [Docker guide](docs/docker.md) for safe demo, existing-data, Module 3,
Module 3→4C, index, and cleanup procedures. The `scripts/module2.py` and
`scripts/module3.py` wrappers start Compose before forwarding arguments; do not
invoke their `--help` option to inspect usage.

## Module 2 — Spark batch layer

Module 2 publishes prior-history metrics and writes `user_features` to HDFS.
The intended Module 4 consumer contract for those features remains unresolved;
see [progress §3](docs/progress.md#3-module-2--batch-layer) and the
[full-data record](docs/evidence/full/README.md).

## Module 3 — Speed layer (Spark Structured Streaming)

Module 3 writes mutable legacy trend rows and can optionally publish immutable
finalized bundles for Module 4C. See the [canonical contract](docs/module3-contracts.md),
[operating procedures](docs/docker.md#module-3-streaming-and-module-3-to-module-4c-enablement),
and [historical streaming evidence](docs/evidence/module3/README.md).

## Validation strategy

Historical full-data results, bounded streaming evidence, and current CI have
different scopes. Current CI success does not establish full-data equivalence,
recommendation accuracy, Kubernetes deployment, or production scalability. Full-
data Module 2, 3, and 4 workflows have **not** been rerun after the coding-style
refactor; the overall comparison remains **NOT FULLY VERIFIED**.

Current main checks passed: [Fast validation 38742236253](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38742236253)
and [Module 4A–4B runtime 38742235872](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38742235872).
[Module 3 validation 38727634271](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/38727634271)
passed on the parent code commit; PR #11 changed documentation only.

## Known limitations

- Kubernetes demonstration is not implemented.
- Module 1 raw-input preflight remains incomplete.
- Module 2 does not independently hash every staged input; its optional local
  source check and Module 4's staged-Parquet hash validation are separate.
- Module 2 `user_features` consumption in Module 4 is unresolved.
- No independently verified per-user ALS dashboard view is available.
- Defined end-to-end streaming latency, executor comparison, and leakage-safe
  Hybrid quality evaluation are missing.
- A historical browser CI assertion observed four charts instead of five; later
  runs passed, but the original failure cause remains unexplained.
- Historical writes to default Compose volumes cannot be ruled out where no
  before-state snapshot exists.

See [progress](docs/progress.md) for the evidence boundaries and follow-up work.

## Module 4A–4B completed

Historical full-data acceptance passed 21/21 gates on 10 October 2026. The
measured ALS Recall@10 was below the personal-frequency baseline; see the
[acceptance record](docs/evidence/module4/acceptance-20261010/README.md) for
metrics and limitations.

## Module 4C — Hybrid serving and Kafka integration

Module 4C combines ready A/B snapshots with a verified immutable trend bundle
at an explicit event-time cutoff. C8A/C8B are bounded synthetic experiments;
no leakage-safe official-train Hybrid quality result is available. See the
[Module 4 plan](docs/module4-plan.md#module4c-spec) and [Module 4C evidence](docs/evidence/module4c/README.md).

## Module 4D — Dashboard demonstration

The four-page Streamlit UI reads through FastAPI. It displays trends, batch
metrics, Hybrid recommendations, and registered historical evidence; it has no
independently verified per-user ALS recommendation view. The isolated synthetic
demo does not establish production capacity. See the [API/UI contracts](docs/module4-plan.md#module4d-spec)
and [Docker runbook](docs/docker.md).

## Docker architecture

The root `compose.yaml` owns persistent pipeline services. The separate
`deploy/module4/compose.demo.yaml` uses disposable synthetic data and loopback
API/UI bindings. See the [Docker operations guide](docs/docker.md) for their
prerequisites and cleanup boundaries.
