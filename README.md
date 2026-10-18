# IT4931 — Instacart market basket analysis

This project analyzes the public Instacart market-basket dataset and demonstrates
trend-aware product recommendations. It uses Python, Apache Spark, Kafka,
MongoDB, FastAPI, Streamlit, Docker, and Kubernetes.

The design follows a **Lambda Architecture**: a batch layer processes historical
data, a speed layer processes event streams, and a serving layer brings their
results together. Purchase times are reconstructed for replay and are
simulations, not timestamps supplied by the dataset.

## Architecture

```text
Six source CSV files
        |
     Module 1 ──> HDFS curated Parquet ──> Module 2 batch analytics ──┐
        |                           ├──> Module 4A ALS recommendations  │
        |                           └──> Module 4B product graph        │
        └──> Kafka events ──> Module 3 event-time trends                │
                                  └──> finalized trend bundles ─────────┤
                                                    Module 4C Hybrid <──┘
                                                             |
                                                Module 4D read-only API
                                                             |
                                                     Streamlit dashboard
```

HDFS (Hadoop Distributed File System) stores curated tables for batch
processing. Kafka carries replayable purchase events. Spark runs batch jobs and
Structured Streaming. MongoDB stores published results; FastAPI provides
read-only access to the Streamlit dashboard. Docker packages the services, and
Kubernetes orchestrates an isolated local demonstration. ALS (Alternating
Least Squares) is Module 4A's collaborative-filtering method. It uses
prior-history inputs; official train baskets are held back as evaluation truth.

| Module | Responsibility |
|---|---|
| **1 — Ingestion** | Validate and clean the six CSV tables, generate deterministic replay events, and stage curated data and events. |
| **2 — Batch layer** | Compute historical purchase analytics and prior-history user features. |
| **3 — Speed layer** | Aggregate Kafka events by event time and publish trends, checkpoints, and optional finalized trend bundles. |
| **4A — Recommendations** | Train and evaluate prior-history ALS recommendations. |
| **4B — Product graph** | Build product co-purchase relationships and graph metrics. |
| **4C — Hybrid serving** | Combine ready batch recommendation sources with finalized trend bundles. |
| **4D — Read interface** | Serve versioned results and evidence through a read-only API and dashboard. |

<a id="lambda-modules-spec-7--verify-against-assignment"></a>

Principal outputs include curated Parquet tables in HDFS, batch metrics and
user features, versioned recommendation and graph snapshots, and streaming
trend results. Module 4D reads published results through the API; the dashboard
does not connect directly to MongoDB.

## Current status

**Code complete; external runtime acceptance is pending.** The final independent
source-code review passed on the implementation revision recorded in
[project status](docs/progress.md). Historical offline checks are summarized
there and in the [implementation ledger](docs/implementation-ledger.md). They
are not results from this documentation update.

Still pending are pinned Spark and MongoDB integration checks, browser and
container-package verification, Kubernetes B2/B3/B4 execution, full-data
processing, runtime checkpoint/recovery, performance and executor experiments,
and fresh runtime acceptance evidence. See the [acceptance handoff](docs/external-acceptance-handoff.md)
for scope and prerequisites.

## Start here

- Read the [approved proposal](docs/Proposal.md) for project goals and
  requirements.
- Use the [Docker guide](docs/docker.md) for setup, module commands, validation,
  and safe operation of the persistent and isolated demo environments.
- See the [Kubernetes demonstration runbook](docs/kubernetes-demo-runbook.md)
  for the separate local-cluster procedure and its safeguards.
- Check [project progress](docs/progress.md) for current implementation and
  verification boundaries.
- Browse the [documentation index](docs/README.md) or the
  [repository layout](docs/repository-layout.md) for code and guide ownership.

The dataset contains six CSV tables and no absolute purchase timestamps. The
generated event time is for replay and trend demonstrations only. See the
[Module 3 event contract](docs/module3-contracts.md) and the
[full-data evidence index](docs/evidence/full/README.md) for field definitions
and historical dataset counts.
