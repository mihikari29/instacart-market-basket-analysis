# Repository layout

This guide maps stable code and documentation entry points. Module 4 application
code remains under `src/module4/`; its deployment assets and operational tools
are grouped under `deploy/module4/` and `scripts/module4/`.

## Canonical structure

```text
Dockerfile                         shared multi-stage image definition
compose.yaml                       persistent pipeline services
requirements.txt                   shared runtime dependencies
requirements-dev.txt               shared development/test dependencies
deploy/module4/requirements/       separate API, UI, UI-dev and browser pins
deploy/module4/compose.demo.yaml   isolated synthetic dashboard environment
scripts/module1.py, module2.py…    project pipeline entry points
scripts/module4/                   Module 4C experiments and Module 4D tools
src/module1/ … src/module4/        processing and application packages
tests/                             unit, contract, Spark and service-backed tests
docs/                              requirements, canonical guides and evidence
.github/workflows/                 CI and gated full-data validation
```

### Module 4 package paths

<a id="module4-package-paths"></a>

Module 4A and 4B remain in the root package. Module 4C and 4D use dedicated
subpackages under the same canonical `src.module4` namespace:

| Former path | Canonical path |
|---|---|
| `src/module4/` | `src/module4/` (Module 4A/4B retained) |
| `src/module4c/` | `src/module4/hybrid/` |
| `src/module4d/` | `src/module4/dashboard/` |

Use `from src.module4.hybrid.service import serve_hybrid_user` for Hybrid
serving and `from src.module4.dashboard.api import create_app` for the API.
The dashboard ASGI application is `src.module4.dashboard.api:app`. There are
no compatibility packages at `src.module4c` or `src.module4d`.

## Documentation map

| Document | Responsibility |
|---|---|
| [Proposal](Proposal.md) | Approved project requirements and architecture baseline. |
| [Progress](progress.md) | Current implementation, validation scope, requirement gaps, and next work. |
| [Module 3 contracts](module3-contracts.md) | Kafka, event-time aggregation, Mongo publication, bundles, and Module 4C boundary. |
| [Module 4 plan](module4-plan.md) | Module 4A–4D algorithms, data, ranking, evaluation, API, and dashboard contracts. |
| [Docker guide](docker.md) | Image targets, runtime prerequisites, operational procedures, and scoped cleanup. |
| [Evidence archive](evidence/README.md) | Dated validation reports, receipts, historical observations, and archive indexes. |

Use current guides for current behavior and procedures. The Module 4C/4D
standalone design and release documents were relocated to sibling evidence
archives; their original dates, run identities, measurements, and limitations
remain part of the record.

## Application and deployment paths

- `src/module1/` contains cleaning, deterministic synthetic event generation,
  HDFS staging, and the Kafka producer.
- `src/module2/` contains Spark batch analytics, feature extraction, publication,
  and benchmarks.
- `src/module3/` contains Structured Streaming parsing, finalized trend
  publication, and checkpoint management; see the [Module 3 contract guide](module3-contracts.md).
- `src/module4/` contains ALS, graph, Hybrid serving, API, and dashboard code.
  The retired `src.module4c` and `src.module4d` packages are not valid imports.
- `deploy/module4/requirements/` separates API, UI, UI-development, and browser
  dependencies. The UI development manifest includes the runtime UI pins.
- `scripts/module4/` groups the Module 4C experiments and Module 4D demo,
  validation, and browser tools. Core project runners stay in `scripts/`.

The main `compose.yaml` owns persistent Kafka, Spark, HDFS, and MongoDB
services. The isolated `deploy/module4/compose.demo.yaml` uses disposable
MongoDB storage and loopback-only UI/API ports and does not mount main pipeline
data. See the [Docker guide](docker.md) for their separate boundaries.

## Stable names and evidence

`MODULE4D_` environment variables, API routes, MongoDB collection/readiness
identifiers, Hybrid serving-version format, and established test filenames are
runtime contracts. Script basenames and command arguments are also stable entry
points. Check the relevant canonical guide before changing these contracts.

Historical reports are not rewritten when code or current documentation changes.
The [evidence index](evidence/README.md) links each result to its recorded
revision and scope; the [style regression report](style-regression-validation.md)
retains its **NOT FULLY VERIFIED** assessment.
