# Repository layout

This guide describes the current project layout after consolidating the
Module 4 deployment files and operational scripts. The code remains in its
existing `src/module4/` package, while deployment-only files live under
`deploy/module4/` and related commands live under `scripts/module4/`.

## Canonical structure

```text
Dockerfile                         shared multi-stage image definition
compose.yaml                       persistent pipeline services
requirements.txt                   shared runtime dependencies
requirements-dev.txt               shared development and test dependencies
deploy/module4/
  compose.demo.yaml                isolated synthetic dashboard environment
  requirements/
    api.txt                        FastAPI and API test dependencies
    ui.txt                         Streamlit runtime dependencies
    ui-dev.txt                     UI test dependencies; includes ui.txt
    browser.txt                    separate Playwright runtime dependencies
scripts/
  module1.py, module2.py, ...      project pipeline and orchestration commands
  module4/                         Module 4C experiments and Module 4D tools
src/module1/ ... src/module4/      application and processing packages
tests/                             stable acceptance and regression tests
docs/                              requirements, canonical plans/runbooks, and evidence archive
.github/workflows/                 CI and full-data validation workflows
```

Shared dependencies remain in the two root manifests. Module 4 API, UI,
development UI, and browser dependencies have separate pinned manifests under
`deploy/module4/requirements/`. The UI development manifest includes `ui.txt`
by relative path so runtime pins stay consistent.

## Documentation map

| Document | Responsibility |
|---|---|
| [Proposal](Proposal.md) | Approved project requirements and architecture baseline. |
| [Module 4 plan](module4-plan.md) | Canonical technical specification for Modules 4A–4D, including data, ranking, evaluation, API, and dashboard contracts. |
| [Progress](progress.md) | Current milestones, validation evidence, status distinctions, remaining issues, and limitations. |
| [Docker guide](docker.md) | Docker targets, setup, API/dashboard and demo operations, validation commands, and scoped cleanup. |
| [Evidence archive](evidence/README.md) | Dated acceptance receipts, reports, historical observations, and retired standalone Module 4 design/release documents. |

Module 4C/4D standalone documents were consolidated by responsibility and
preserved as appendices under `docs/evidence/module4c/` and
`docs/evidence/module4d/`. Use the canonical documents in this table for current
contracts and procedures; the archived release records retain their original
run, revision, measurement, and limitation context.

## Pipeline and dashboard demo

The main `compose.yaml` owns the persistent Spark, Kafka, HDFS, and MongoDB
pipeline services and their established volumes. The isolated
`deploy/module4/compose.demo.yaml` defines a separate synthetic dashboard stack:
MongoDB uses disposable tmpfs, the API gets a database-scoped read-only user,
fixture administration provisions indexes, UI and database networks are
separate, and host API/UI ports bind to loopback. It does not join the main
pipeline network or mount its data.

The demo Compose build context is `../..` relative to the Compose file, which
resolves to the repository root. `scripts/module4/module4d_demo.sh` changes to
the repository root before invoking Compose and owns the temporary credentials
and scoped cleanup:

```bash
bash scripts/module4/module4d_demo.sh up
# Open http://127.0.0.1:18501 and retain the printed run ID.
bash scripts/module4/module4d_demo.sh down <12-hex-run-id>
```

## Docker targets

Build from the repository root and name the target explicitly. The targets
create separate Spark, API, Streamlit, and Playwright images; MongoDB stays an
external service.

```bash
docker build -f Dockerfile --target spark -t instacart-spark:3.5.5 .
docker build -f Dockerfile --target api -t instacart-module4d-api:local .
docker build -f Dockerfile --target ui -t instacart-module4d-ui:local .
docker build -f Dockerfile --target browser -t instacart-module4d-browser:local .
docker build -f Dockerfile --target ui --build-arg MODULE4D_UI_DEV=1 \
  -t instacart-module4d-ui-dev:local .
```

The root `.dockerignore` defaults to denying the context and then admits only
required manifests, Python source/tests, selected Module 4 tools, and five
curated evidence artifacts. It excludes local data, `results/`, credentials,
private environment files, and generated state. Each image has explicit `COPY`
instructions to keep unrelated runtime dependencies out.

## Executable scripts and stable names

The eight Module 4C/4D operational scripts are grouped under
`scripts/module4/`: the C8A responsiveness experiment, C8B streaming demo, D1
API validation, D3 browser E2E, and demo lifecycle, environment, fixture, and
HTTP validator tools. Their basenames and command arguments are preserved.
The `scripts/module4/__init__.py` marker makes this a regular package alongside
the existing `scripts/module4.py` pipeline entrypoint, so imports such as
`scripts.module4.module4c_responsiveness` resolve consistently.
Module 1–3 scripts and the core Module 4 pipeline/flow commands stay directly
under `scripts/` because they orchestrate project-wide processing rather than
the Module 4 dashboard deployment.

`src/module4/`, `src/module4/hybrid/`, and `src/module4/dashboard/` remain the
canonical Python packages. `src.module4c` and `src.module4d` intentionally do
not exist; tests keep negative assertions for those old import paths.

Names beginning `MODULE4D_`, API routes and schemas, MongoDB collection and
readiness identifiers, Hybrid serving-version format, and test filenames stay
as accepted contracts. Historical evidence retains the original experiment
identities and is not rewritten when current commands move.

## Acceptance validation

The full command sequence and Docker/Mongo isolation setup are in the
[Docker validation runbook](docker.md). In summary it covers:

1. Build all four Docker targets and the UI development variant; check pinned
   dependencies, Spark/GraphFrames imports, OpenAPI/startup, and Compose.
2. Run the repository-wide Spark suite and lint in the Spark image.
3. Run API and Hybrid unit tests plus live D1 MongoDB integration with
   `python3 scripts/module4/module4d_api_validation.py --mode all`.
4. Run the UI test files in the UI development image and the supplementary
   Module 3/4/4C live MongoDB suites.
5. Start the isolated demo, validate fixture publication, indexes, the
   read-only API, and the accepted HTTP cases; run the separate Chromium E2E
   image against all four dashboard pages and verify cleanup.

The completed layout migration passed all five target builds and dependency
checks, repository-wide lint, Markdown-link, YAML, and path audits, plus the
regression groups: Spark suite **690 passed, 69 skipped**; API/Hybrid **497
passed**; live D1 MongoDB **26 passed**; UI **64 passed**; and supplemental
live MongoDB **93 passed**. The isolated demo passed all **21 HTTP cases** and
the real Chromium flow across all four pages, then cleaned up its run-scoped
resources. The Docker context allowlist and image manifests were compared with
the accepted files and hashes; the context contained only the reviewed
allowlisted files.

The pinned dependency manifests, separate container targets, test identities,
and evidence meanings are intentionally retained to keep each accepted runtime
boundary explicit.
