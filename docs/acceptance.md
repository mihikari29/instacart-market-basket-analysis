# External acceptance tutorial

This guide takes an external operator from preparation to an evidence-backed
acceptance report. It describes work to perform; publishing or reading it does
not execute that work. All results start as **NOT RUN**. Kubernetes runtime
acceptance, full-data runtime acceptance, and production readiness remain open.

The [Proposal](Proposal.md) is the assignment baseline. The [Docker guide](docker.md)
and [Kubernetes guide](kubernetes.md) own operational commands.
The [evidence appendix](#delivered-scope-and-evidence-boundaries) distinguishes
older records from new acceptance; [project status](progress.md) owns current
capabilities and remaining work.
Use this tutorial to decide what to do next, when to stop, and what to retain.

## Contents

- [1. Introduction and acceptance model](#acceptance-model)
- [2. PHASE 0 — Orientation and prerequisites](#phase-0)
- [3. PHASE 1 — Source identity and offline validation](#phase-1)
- [4. PHASE 2 — Docker and integration validation](#phase-2)
- [5. PHASE 3 — Module-by-module verification](#phase-3)
- [6. PHASE 4 — Kubernetes fixture acceptance](#phase-4)
- [7. PHASE 5 — Full-data acceptance and experiments](#phase-5)
- [8. PHASE 6 — Evidence, reporting and handoff](#phase-6)
- [Appendix A — Acceptance report template](#report-template)
- [Appendix B — Command and side-effect register](#command-register)
- [Appendix C — Failure and recovery decision map](#failure-map)
- [Appendix D — Requirement traceability and references](#traceability)

<a id="acceptance-model"></a>
## 1. Introduction and acceptance model

An **acceptance gate** is a decision supported by a defined command or procedure
and retained evidence. A command's exit code is one observation, not the whole
decision. Keep source review, offline tests, Docker integration, Kubernetes
fixture runtime, full-data runtime, performance experiments, and production
readiness as separate report rows.

| Term | Meaning in this project |
|---|---|
| Lambda Architecture | Separate historical batch processing, a speed layer for arriving events, and a serving layer exposing their results. |
| HDFS | The distributed filesystem holding raw/curated data and run artifacts. |
| Kafka topic / offset | A partitioned event log / a record position within one partition. |
| Spark batch / streaming | Bounded dataset processing / a continuing query over arriving data and checkpointed state. |
| Synthetic event time | Project-generated replay timestamps, not observed customer purchase dates. |
| Event-time watermark | A query's event-time/state boundary for late-data handling; not a wall-clock timer that closes every finite replay tail. |
| Checkpoint | Saved streaming query offsets, state and compatibility identity; not an expendable log. |
| MongoDB publication | Writing a validated output generation and possibly promoting its readiness pointer; not one transaction across every sink/reader. |
| Source identity | Full Git SHA, cleanliness, origin, and relevant source/configuration hashes. |
| Run ID | Identity for one execution; Kubernetes uses a fresh 12-character lowercase hexadecimal ID to derive profile/context/namespace. |
| Version lock | Selected releases plus, after preparation, measured host/cluster/image/toolchain/storage identities. |
| Image digest | Image content identity; OCI manifest digest and image configuration ID are distinct and cannot be substituted. |
| PVC / PV | A persistent storage request / the persistent volume bound to it; record both with StorageClass and run ownership. |
| Receipt | A machine-readable record binding inputs, actions, identities, counts or hashes to a run. |

| Status | Meaning |
|---|---|
| PASS | The step was executed for the declared source and scope, every required observation holds, and evidence is available. |
| FAIL | An executed check contradicts its expected contract. Preserve the failure; do not remove the test or loosen the contract. |
| BLOCKED | A prerequisite, authorization, identity, dependency, capacity, or evidence requirement prevents a valid decision. |
| SKIPPED | A check was deliberately omitted, with its reason and owner recorded. It cannot satisfy a mandatory acceptance gate. |
| NOT RUN | No execution has been performed for this acceptance session. This is the default, including blank report rows. |

A phase passes only when its mandatory steps pass for its named scope.
A successful plan is not a runtime pass. A permitted offline rendering skip may
leave an offline subcheck SKIPPED, but must remain visible and be resolved when
that rendering check is required by the intended acceptance scope.

Each step card has its **Step ID** in the heading and nine fields: objective,
prerequisites, command/reference, expected observation, decision, evidence,
troubleshooting, stop/recovery, and side effects. Use [Appendix A](#report-template)
to record actual outcomes. The effects below are cumulative:

| Classification | Meaning |
|---|---|
| READ_ONLY | Inspects local files, configuration, or an explicitly selected service; does not publish application data. |
| LOCAL_WRITE | Creates logs, environments, copies, reports, or other local files. |
| SERVICE_MUTATION | Builds/loads images, creates containers/networks/clusters, or changes service state. |
| DATA_MUTATION | Writes, replaces, stages, publishes, or replays data, offsets, checkpoints, accounts, or indexes. |
| RESOURCE_INTENSIVE | May use substantial CPU, RAM, disk, network, or elapsed time. |
| DESTRUCTIVE_OR_TEARDOWN | Removes resources or data, including a helper's scoped automatic cleanup. Requires separate owner authorization covering those exact effects. |

Authorize a service-mutating procedure before starting it. Where a harness also
cleans its own temporary resources, authorize that cleanup explicitly as part of
the harness scope. Teardown of retained acceptance resources is a separate
decision after evidence review. Never apply global cleanup to shared services.

The [historical development record](#historical-development-record) preserves
older scoped results and their missing-artifact limitations. They are context,
not completed gates for a new acceptance.

<a id="phase-0"></a>
## 2. PHASE 0 — Orientation and prerequisites

**Purpose:** choose the execution scope and prepare tools, capacity, inputs, and
private evidence storage. **Entry:** permission to inspect the checkout and host.
Work from the repository root in Bash. Complete P0.5 before writing logs or
copying input data; P0.2–P0.4 can first inventory what is missing.

### P0.1 — System architecture and execution scope

| Field | Operator contract |
|---|---|
| Objective | Identify the batch, speed, and serving paths and choose the scope to evaluate. |
| Prerequisites | Read the [root overview](../README.md) and current [project status](progress.md). |
| Command/reference | Inspect the existing [architecture diagram](../README.md#architecture), [implementation ledger](implementation-ledger.md#1-architecture-and-module-boundaries), and [Proposal](Proposal.md) §§3, 33–36, 42, 45. |
| Expected observation | Module 1 prepares data; Module 2 computes batch analytics; Module 3 computes event-time trends; 4A fits ALS, 4B builds the graph, 4C publishes Hybrid snapshots, and 4D reads through FastAPI/Streamlit. |
| Decision | PASS when the operator records scope and understands each boundary; FAIL if a claim contradicts the contracts; BLOCKED if the approved requirements/source cannot be identified. |
| Evidence | Planned scope, operator/reviewer, mandatory requirement rows, and links to approved source review. |
| Troubleshooting | Use the [repository layout](../README.md#repository-layout) to locate an unfamiliar component. |
| Stop/recovery | Resolve scope disagreements before running services. Do not mark optional work mandatory without an assignment decision. |
| Side effects | READ_ONLY; LOCAL_WRITE when recording the report. |

The implemented flow is six CSVs → Module 1 → HDFS → Modules 2/4A/4B, with a
separate generated-event path through Kafka → Module 3. MongoDB holds serving
outputs; Module 4C and the direct API routes consume their documented versioned
sources. The legacy mutable trend collection is not a Hybrid input. Optional
immutable finalized bundles supply that path. Module 2 user features are
descriptive and are not ALS fitting inputs.

### P0.2 — Supported environment and required tools

| Field | Operator contract |
|---|---|
| Objective | Prepare the Linux/WSL2-oriented operator environment and identify each runtime. |
| Prerequisites | Bash, Git, an operator-owned host, and permission to install the selected tools locally. |
| Command/reference | Inventory with the commands below; use [Docker image/environment ownership](docker.md#build-each-image), [Kubernetes prerequisites](kubernetes.md#prerequisites-and-capacity), and the manifests listed in P1.2. |
| Expected observation | Required executables exist; actual versions and installation provenance are recorded. Minikube is v1.39.0; the selected cluster is Kubernetes v1.36.4 with bundled Calico v3.32.2, Docker driver, containerd, one node. |
| Decision | PASS for an equipped selected scope; FAIL for a measured version mismatch; BLOCKED for missing tools, incompatible platform, or unresolved installation permissions. |
| Evidence | OS/WSL details, tool versions, binary origin/checksum where required, and selected environment/manifests. |
| Troubleshooting | An installed Docker CLI does not prove a reachable Docker Engine; local executable checks do not prove Buildx, image pulls, or cluster operation. |
| Stop/recovery | Install/repair tools using the platform/vendor procedure before that tool's phase. Do not substitute a random cluster/CNI release. |
| Side effects | READ_ONLY for inventory; installations are LOCAL_WRITE and may be RESOURCE_INTENSIVE. Docker daemon version queries contact the daemon read-only. |

```bash
git --version
python3 --version
docker version
docker buildx version
docker compose version
kubectl version --client --output=json
minikube version --short
```

Install Docker Engine, the Buildx and Compose plugins, kubectl, and the selected
Minikube release for the Linux/WSL2 Docker-driver path. Record actual tool
versions rather than assuming a package name proves compatibility. The host
coordinator documents Python 3.10 or newer and `PyYAML==6.0.2`. Python environments
for Spark/API, UI, browser, and host checks are separate; see P1.2. Java 17 and
the reviewed Kafka/GraphFrames jars belong to the pinned Spark image.
A native Windows shell is not a substitute for the documented Bash/WSL2 path.

The committed [version template](../deploy/k8s/versions.lock.yaml) remains
incomplete. It is expected to lack actual profile, image, toolchain, storage,
and package identities. Compatibility of the selected releases is not proof
of a successful runtime deployment.

### P0.3 — Hardware capacity and WSL backing storage

| Field | Operator contract |
|---|---|
| Objective | Establish admission capacity before expensive work. |
| Prerequisites | Selected host and storage locations; P0.2 tool inventory. |
| Command/reference | Inspect `lscpu`, `free -h`, and `df -h / "$HOME"`; apply [capacity requirements](kubernetes.md#prerequisites-and-capacity) and later P4.2/P5.2 preflight. |
| Expected observation | B3/full coordinator reserve: 6 physical CPU cores, 12 GiB available RAM, 40 GiB free disk. B2 serving alone: 4 CPU, 6 GiB RAM, 20 GiB disk. WSL also needs a current measured Windows backing-disk free-capacity value. |
| Decision | PASS when measured capacity meets the intended scope; FAIL if measured values contradict a claimed reserve; BLOCKED before runtime when capacity or WSL evidence is insufficient. |
| Evidence | Timestamped available capacity, filesystem/drive identity, Docker/WSL storage location, and the Windows measurement method. |
| Troubleshooting | Logical CPU count is not proof of physical cores; total installed RAM is not available RAM; guest disk free space does not measure the Windows backing drive. |
| Stop/recovery | Stop before builds/cluster execution when the reserve fails. Free space only through a separately reviewed owner action or move to a suitable host. |
| Side effects | READ_ONLY; LOCAL_WRITE for saved measurements. |

Measure additional space for source copies, images, temporary files, models,
and retained evidence on their actual filesystems. Reserve checks are admission
conditions, not throughput or latency guarantees.


### P0.4 — Source data and provenance

| Field | Operator contract |
|---|---|
| Objective | Identify the six real source tables and protect their original evidence. |
| Prerequisites | Dataset access and a declared fixture or full-data scope. |
| Command/reference | Inspect the six-file contract below, [dataset relationships](../README.md#dataset-and-source-relationships), and [Module 1 acquisition procedure](docker.md#module-1-ingestion-generation-staging-and-kafka-replay). Use P5.1 for the separate Kubernetes input copy. |
| Expected observation | Real full input has all six CSVs, original source provenance, hashes, counts, and declared headers. Fixture mode uses the bounded image fixture and does not need external CSVs. |
| Decision | PASS when input readiness is established for the chosen scope; FAIL for malformed or contradictory input; BLOCKED for full-data work without verified source files/provenance. |
| Evidence | Original source URL/version/archive identity, file-level SHA-256 and rows, receipt, and intended input scope. |
| Troubleshooting | The fetcher writes `data/raw/source-receipt.json`; feeding that directory directly to the Kubernetes full-data coordinator introduces an extra sidecar file. |
| Stop/recovery | Preserve the original receipt and CSVs. Prepare a new directory containing only the six CSVs; never delete the receipt to make preflight pass. Missing data can leave full-data NOT RUN while fixture work proceeds explicitly. |
| Side effects | READ_ONLY for inspection. The canonical fetcher/copy procedures are LOCAL_WRITE and RESOURCE_INTENSIVE; downloading requires authorization and disk headroom. |

Required names are `orders.csv`, `order_products__prior.csv`,
`order_products__train.csv`, `products.csv`, `aisles.csv`, and `departments.csv`.
Orders connect users and evaluation sets to item facts; product IDs connect
facts to the product catalog and its aisle/department dimensions.

The [fetcher](../scripts/fetch_instacart.py) verifies its selected public mirror
version and expected row counts, hashes the archive and each CSV, and refuses
to overwrite existing source CSVs. Its receipt is separate provenance.
Filename, a directory label, or `source_scope=full` alone does not prove a full
release. Full admission rechecks headers, CSV record widths, rows, hashes and the
4 GiB raw-input cap; Module 1 later validates raw domains and relationships.

### P0.5 — Private evidence location

| Field | Operator contract |
|---|---|
| Objective | Create one private operator-owned acceptance workspace outside every repository worktree. |
| Prerequisites | Git checkout known; a suitable existing parent directory with sufficient space; review P0.3. |
| Command/reference | Review the candidate against `git worktree list --porcelain`, then create it using the two blocks below. |
| Expected observation | A fresh directory owned by the operator, mode 0700, outside all listed worktrees. Future run outputs will be new children, not pre-created coordinator output roots. |
| Decision | PASS when location/ownership/privacy are verified; FAIL for a conflicting or unsafe path; BLOCKED when a safe parent is unavailable. |
| Evidence | Absolute evidence root, owner/mode, filesystem, date/time, and report location. |
| Troubleshooting | Do not use repository `results/`, a registered worktree, a symlinked parent, or a shared directory as Kubernetes private output. |
| Stop/recovery | On collision, inspect the existing path and choose a new one; do not overwrite or clean it. |
| Side effects | READ_ONLY for inspection; LOCAL_WRITE for directory creation. |

```bash
git worktree list --porcelain
EVIDENCE_ROOT="$HOME/it4931-acceptance-$(date -u +%Y%m%dT%H%M%SZ)"
printf 'Review this candidate: %s\n' "$EVIDENCE_ROOT"
```

After confirming the candidate is outside all worktrees:

```bash
umask 077
mkdir -m 700 -- "$EVIDENCE_ROOT"
```

Keep secret values and raw credential-bearing URIs out of logs, screenshots and
shared reports. Store protected credential files separately from shareable
evidence. Sanitize logs before sharing and retain originals only in an approved
private location. Do not run with shell tracing that prints secrets.

**Phase evidence/exit gate:** record scope, tool inventory, current capacity,
input/provenance readiness, and the private evidence path. Check:

- [ ] Scope and mandatory/optional requirements are agreed.
- [ ] Required tools and environments are identified.
- [ ] Runtime capacity and WSL evidence meet the chosen runtime scope.
- [ ] Full-data inputs/receipt are available, or full-data is explicitly NOT RUN.
- [ ] Evidence storage is private and outside worktrees.

**Failure/next phase:** keep missing prerequisites BLOCKED, without destructive
repair. Proceed to Phase 1 for the selected source and host environment; revisit
runtime readiness immediately before each live phase.

<a id="phase-1"></a>
## 3. PHASE 1 — Source identity and offline validation

**Purpose:** bind checks to approved source and exercise the canonical offline
selection. **Entry:** Phase 0 evidence storage and host Python/venv support for
P1.2. This phase does not authorize Docker builds or live services.

### P1.1 — Approved source identity

| Field | Operator contract |
|---|---|
| Objective | Verify the exact source approved for this acceptance session. |
| Prerequisites | The reviewer/operator supplies the approved full SHA; expected repository and branch are known. |
| Command/reference | Run the identity block below and compare local HEAD and direct remote branch SHA with the session's approved SHA. |
| Expected observation | Origin is the named repository, branch is the reviewer-approved branch, approved local/remote revisions agree, and porcelain status is empty. |
| Decision | PASS when every identity matches; FAIL for a verified mismatch; BLOCKED for dirty/unreviewed source or unavailable remote identity. |
| Evidence | Full outputs, approved SHA/reviewer/date, tree cleanliness and remote SHA; use [Appendix A](#report-template). |
| Troubleshooting | A tracking ref can be stale; `ls-remote` reads the current remote without changing the checkout. |
| Stop/recovery | Do not reset, stash, switch, merge, or discard someone else's work. Refer source changes to the owner/reviewer. |
| Side effects | READ_ONLY; LOCAL_WRITE when saving the outputs. |

```bash
read -r -p 'Approved full commit SHA for this session: ' APPROVED_SHA
read -r -p 'Approved branch name for this session: ' APPROVED_BRANCH
git remote get-url origin
git branch --show-current
git rev-parse HEAD
git status --porcelain=v1 --untracked-files=all
git ls-remote origin "refs/heads/$APPROVED_BRANCH"
test "$(git rev-parse HEAD)" = "$APPROVED_SHA"
```

Expected origin: `https://github.com/mihikari29/instacart-market-basket-analysis.git`
(or its approved equivalent transport). Use the revision approved at acceptance
time; a historical source-review identity is context, not a permanent future gate.

### P1.2 — Host dependencies and static checks

| Field | Operator contract |
|---|---|
| Objective | Prepare a separate host test environment and identify its limits. |
| Prerequisites | P1.1 PASS, Python/venv support, P0.5, and permission for dependency downloads. |
| Command/reference | Use the host-only setup below; consult the environment table and [Docker environment definitions](docker.md). Then run `python3 -B -m ruff check --no-cache .` and `git diff --check`. |
| Expected observation | Installs and `pip check` succeed; actual interpreter/package inventory is retained; Ruff and diff checks exit 0. |
| Decision | PASS for the declared host/static scope; FAIL for resolver, lint, or syntax errors; BLOCKED if required dependencies cannot be installed/imported. |
| Evidence | Python version, resolved package list, manifests, install/static-check logs and exit codes. |
| Troubleshooting | Do not merge pinned environments conceptually. A host venv cannot prove Spark jars, container packages, Java, API/UI separation or browser behavior. |
| Stop/recovery | Do not loosen package pins or add test skips to manufacture PASS. Diagnose the environment or use the canonical pinned runtime in Phase 2. |
| Side effects | LOCAL_WRITE and RESOURCE_INTENSIVE for installation; READ_ONLY for source checks, apart from explicitly saved logs. |

| Environment | Canonical declaration and boundary |
|---|---|
| Host offline tests | Root [requirements](../requirements.txt), test-tool pins from [requirements-dev](../requirements-dev.txt), and imports needed by the selected API/UI tests. |
| Spark | Docker `spark` target: Spark 3.5.5, Java 17, Python runtime and verified connector jars. `requirements-dev.txt` also declares PySpark 3.5.5. |
| API | Docker `api` target and [API manifest](../deploy/dashboard/requirements/api.txt); Kubernetes uses the separate `k8s-runner` package. |
| UI | Docker `ui` target, [UI runtime](../deploy/dashboard/requirements/ui.txt) and [UI development manifest](../deploy/dashboard/requirements/ui-dev.txt). |
| Browser | Docker `browser` target and [browser manifest](../deploy/dashboard/requirements/browser.txt), including its matching Chromium runtime. |

The following is a host test convenience environment, deliberately omitting
PySpark from `requirements-dev.txt`. It does not reproduce any image runtime.
If PySpark is available in another host environment, record that fact and its
Java/runtime compatibility; the actual skips may differ.

```bash
python3 -m venv "$EVIDENCE_ROOT/host-venv"
. "$EVIDENCE_ROOT/host-venv/bin/activate"
python3 -m pip install -r requirements.txt \
  -r deploy/dashboard/requirements/api.txt \
  -r deploy/dashboard/requirements/ui-dev.txt \
  'pytest>=8,<9' 'ruff==0.11.13' 'PyYAML==6.0.2'
python3 -m pip check
python3 -m pip freeze > "$EVIDENCE_ROOT/host-packages.txt"
```

### P1.3 — Canonical offline test selection

| Field | Operator contract |
|---|---|
| Objective | Run the complete documented non-service selection without hiding exclusions. |
| Prerequisites | P1.1/P1.2 PASS; no service-starting wrapper is used. |
| Command/reference | Run the exact selection below; [historical counts](#historical-development-record) describe an older execution, not this run. |
| Expected observation | Collection completes, no failures occur, and every skip/exclusion has an explicit reason. Fresh totals may differ from historical 803/7. |
| Decision | PASS for executed offline checks with adequate evidence; FAIL for test/collection errors; BLOCKED for missing required imports or unexplained skips. Any unexecuted mandatory integration/Spark gate remains open. |
| Evidence | Full selection, stdout/stderr, exit code, pass/fail/skip counts, reasons and package versions. Add `-rs` in a separately recorded diagnostic rerun if skip reasons are needed. |
| Troubleshooting | Spark-dependent modules may skip when PySpark is absent. Excluded Mongo tests did not execute. Responsiveness/streaming-demo exclusions avoid unavailable PySpark imports. |
| Stop/recovery | Do not remove more tests, introduce xfails, or silently reclassify an import error. Restore the appropriate environment before its mandatory gate. |
| Side effects | LOCAL_WRITE for test temporary artifacts/logs; RESOURCE_INTENSIVE depending on installed Spark support. No service startup is authorized by this selection. |

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m pytest -p no:cacheprovider -q -m 'not integration' \
  --ignore=tests/test_module3_trend_bundle_mongo.py \
  --ignore=tests/test_module4_mongo.py \
  --ignore=tests/test_hybrid_cli_mongo.py \
  --ignore=tests/test_hybrid_data_mongo.py \
  --ignore=tests/test_hybrid_sink_mongo_mongo.py \
  --ignore=tests/test_hybrid_trend_reader_mongo.py \
  --ignore=tests/test_dashboard_api_mongo.py \
  --ignore=tests/test_dashboard_batch_api_mongo.py \
  --ignore=tests/test_hybrid_responsiveness.py \
  --ignore=tests/test_hybrid_streaming_demo.py
```

The first eight ignored files require live MongoDB. The final two suites have
PySpark-dependent imports. The historical seven skipped modules covered Hybrid
evaluation parity, Module 3, bundle integration, trend bundle writer, Module 4
Spark fixtures, Spark batch fixtures, and the Module 2 producer/Parquet/Module 4
reader handoff. Preserve these reasons even when a different host collects a
different count. Resolve the service-backed checks in Phase 2, not by calling
them passed here.

### P1.4 — Source and test evidence

| Field | Operator contract |
|---|---|
| Objective | Make every executed offline result attributable and reproducible. |
| Prerequisites | P1.1–P1.3 outputs and a private evidence root. |
| Command/reference | Complete the command/result rows in [Appendix A](#report-template); save tool/package inventories and test logs. Recheck `git status --porcelain=v1 --untracked-files=all`. |
| Expected observation | Each log has command, working directory, UTC time, source SHA, environment, exit code, selection, counts and omissions. Source remains unchanged. |
| Decision | PASS for complete evidence; FAIL for contradictory or altered logs; BLOCKED for missing identity or unexplained omissions. |
| Evidence | Source record, package inventory, Ruff/pytest logs, exit codes and skip ledger. |
| Troubleshooting | Record a command's exit status immediately with `$?`; when using `tee`, use Bash `pipefail` and capture `${PIPESTATUS[0]}` rather than tee's exit code. |
| Stop/recovery | Preserve failure logs. Do not rerun until success and discard the failed attempts. Do not publish secret-bearing transcripts. |
| Side effects | READ_ONLY for inspection; LOCAL_WRITE for reports/logs. |

**Phase evidence/exit gate:** approved clean source, reproducible environment,
static results and the full offline selection/omission ledger exist.
**Failure/next phase:** unresolved mandatory offline checks remain BLOCKED or
FAIL. With explicit authorization, Phase 2 supplies pinned runtime/service
evidence. Offline PASS alone establishes neither service nor Kubernetes PASS.

<a id="phase-2"></a>
## 4. PHASE 2 — Docker and integration validation

**Purpose:** validate actual packages, pinned Spark behavior and isolated
service integrations. **Entry:** approved source, Phase 1 evidence, adequate
resources, service ownership/ports reviewed, and explicit authorization.
Canonical procedures may clean their own disposable fixtures; review those
effects before starting them.

### P2.1 — Image build and package identity

| Field | Operator contract |
|---|---|
| Objective | Build the selected packages and record actual image/runtime provenance. |
| Prerequisites | P0 capacity, clean approved source, Docker/Buildx, network or reviewed caches, and permission to create/replace the intended local tags. |
| Command/reference | Follow [Build each image](docker.md#build-each-image). For `k8s-runner`, `ui`, and `pipeline` package imports, inspect the [manual packaging workflow](../.github/workflows/kubernetes-packaging.yml) and [packaging boundary](acceptance.md#packaging-acceptance-and-teardown-boundaries). |
| Expected observation | Selected builds/imports complete; resolved packages, base/jar identities, final image identities and supplied source SHA reconcile. Package imports use no source mount and `--network none`; builds may need network. |
| Decision | PASS for the selected successful package checks; FAIL for build/import/provenance mismatch; BLOCKED for unavailable dependencies, build tools, resources or authorization. |
| Evidence | Build commands/logs, BuildKit metadata where applicable, final image/config/manifest identities, runtime versions and package inventory. |
| Troubleshooting | Tags are mutable labels. `docker image inspect` configuration IDs and RepoDigests are different; missing RepoDigests must be reported rather than invented. |
| Stop/recovery | Preserve failed build details. Inspect existing tags before replacement. Do not fill the committed incomplete lock with guessed digests. |
| Side effects | SERVICE_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE; DESTRUCTIVE_OR_TEARDOWN for removal of temporary import containers, included in the separately authorized package-check scope. |

The Spark target owns batch/stream tests; API inherits Spark for accepted
imports/fixture validation. UI and browser are separate images; UI-development
adds test dependencies. `k8s-runner` and `pipeline` require a full source SHA
through `MODULE4_COMMIT`. Future Kubernetes preparation collects clean-source,
BuildKit, loaded-image and package-environment evidence itself. Package/import
PASS is not an application or cluster runtime PASS.

### P2.2 — Repository and service-backed integration tests

| Field | Operator contract |
|---|---|
| Objective | Execute each test selection in the environment that owns it. |
| Prerequisites | P2.1 packages, isolated test service permission, reviewed temporary-resource cleanup, and sufficient capacity. |
| Command/reference | Use [Validation commands](docker.md#validation-commands): Spark repository suite; API harness `python3 scripts/dashboard/api_validation.py --mode all`; UI-development tests; supplementary Module 3/4/4C Mongo suites. |
| Expected observation | Each selection exits 0 and reports its actual tests/skips. Supplementary Mongo tests use the documented disposable service/aliases; they are separate from authenticated dashboard MongoDB. |
| Decision | PASS only for selections actually executed successfully; FAIL for tests or cleanup verification errors; BLOCKED for missing image/service/environment or authorization. |
| Evidence | Per-selection command, image identity, logs/counts/skips, service/run labels and cleanup outcome. |
| Troubleshooting | Even API harness `--mode unit` builds an image and runs runtime smoke that starts services; the word unit does not make the harness offline. Inspect [its entry point](../scripts/dashboard/api_validation.py). |
| Stop/recovery | Obtain authorization for the whole harness, including its scoped cleanup. Do not copy the Mongo cleanup wrapper into a shared project or run cleanup by broad names. |
| Side effects | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE, DESTRUCTIVE_OR_TEARDOWN for documented disposable-resource cleanup. |

The Spark repository command excludes the two live API Mongo files and three
UI files, which have their own owning selections. The supplementary Mongo
selection owns the six live Module 3/4/4C files named in the guide; the API
harness does not cover all of them. Keep every selection and exclusion in the
report. These procedures do not run full-data acceptance.

### P2.3 — Disposable dashboard and browser

| Field | Operator contract |
|---|---|
| Objective | Verify the API/UI boundary and browser behavior on known synthetic data. |
| Prerequisites | P2.1 images, free loopback ports 18000/18501, permission for the isolated fixture and its scoped failure cleanup, plus supported host networking for the browser container. |
| Command/reference | Follow [Start and stop the synthetic dashboard](docker.md#start-and-stop-the-synthetic-dashboard): `python3 scripts/dashboard/demo.py up`. Then use the Chromium command at the end of [Validation commands](docker.md#validation-commands). |
| Expected observation | A fresh demo ID/database, loopback API/UI, tmpfs MongoDB, provisioned indexes and read account, valid synthetic responses, and the browser's documented page/selector/refresh/error checks. |
| Decision | PASS for successful fixture/browser evidence; FAIL for mismatched persisted/API/UI data or browser assertions; BLOCKED for unreachable loopback endpoints or unsupported network prerequisites. |
| Evidence | Printed demo ID, exact image IDs, sanitized startup/browser logs, response/source versions and scoped cleanup result. |
| Troubleshooting | Browser uses `--network host --ipc=host`, UI URL `http://127.0.0.1:18501`, API URL `http://127.0.0.1:18000`, namespace `module4d-demo`. Confirm that the chosen Linux/WSL2 Docker setup actually supports this path. |
| Stop/recovery | Do not expose MongoDB or substitute a shared database. Retain diagnostics before separately authorizing `scripts/dashboard/demo.py down` for the exact ID. Startup failure already attempts documented scoped cleanup. |
| Side effects | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE; DESTRUCTIVE_OR_TEARDOWN for scoped failure cleanup or separately authorized shutdown. |

FastAPI reads MongoDB with a read-only account; Streamlit receives API settings,
not MongoDB credentials. The fixture uses disposable data, not the persistent
pipeline volumes. Browser success does not verify production data, full-system
capacity, or the current run's model quality.

### P2.4 — Persistent Docker pipeline, when needed

| Field | Operator contract |
|---|---|
| Objective | Supply an intentional persistent HDFS/Kafka/Spark/Mongo environment for module work not already covered by valid evidence. |
| Prerequisites | Project/port/volume ownership reviewed, approved writes, retained data understood, and adequate resources. |
| Command/reference | Follow [Run the persistent pipeline stack](docker.md#run-the-persistent-pipeline-stack), including Compose inspection, exact service startup, and `docker compose exec -T namenode hdfs dfsadmin -safemode wait` before HDFS writes. |
| Expected observation | Required services are healthy and HDFS has left Safe Mode. Selected project/network/database/paths are known and distinct from the disposable dashboard fixture. |
| Decision | PASS for that environment's readiness; FAIL for contradictory health/data identity; BLOCKED for port collision, Safe Mode, missing worker/connector or unsafe ownership. |
| Evidence | Compose project/configuration, service/volume IDs, logs, Safe Mode wait result, Kafka/checkpoint identity and intended targets. |
| Troubleshooting | The NameNode HTTP health check is not HDFS write readiness. Root Compose named HDFS/Mongo volumes do not prove Kafka offsets survive container recreation; inspect Kafka storage and preserve topic/checkpoint identity. |
| Stop/recovery | Never use `docker compose down -v`, `docker system prune`, `docker volume prune`, or shared cleanup to repair this phase. Do not bypass Safe Mode or delete a checkpoint. |
| Side effects | READ_ONLY for inspection; SERVICE_MUTATION and LOCAL_WRITE for builds/startup; subsequent module procedures also DATA_MUTATION and RESOURCE_INTENSIVE. |

**Phase evidence/exit gate:** package identity and required Spark/Mongo/API/UI/
browser selections have fresh results and explicit scopes. Persistent stack
startup is needed only for the selected procedures; otherwise mark that
optional branch NOT RUN with a reason, not as completed integration.
**Failure/next phase:** retain failures and owned resources as appropriate.
Proceed to Phase 3 worksheets; missing module execution can be supplied later
by authorized Phase 4/5 evidence without duplicate workloads.

<a id="phase-3"></a>
## 5. PHASE 3 — Module-by-module verification

**Purpose:** reconcile each implemented module with the assignment and fresh
run artifacts. **Entry:** valid Phase 2 evidence or receipts from Phase 4/5.
These are evidence-review worksheets, not seven extra workloads. If execution
has not occurred, leave the row NOT RUN/BLOCKED and return after its owning
phase. Never inject manual module commands into a running Kubernetes coordinator.

Use the exact provenance phrase “**Synthetic event time for replay simulation**”
when presenting generated timestamps. Keep the checkpoint and each publication
boundary with their corresponding run evidence.

### P3.1 — Module 1 ingestion, simulation and delivery

| Field | Operator contract |
|---|---|
| Objective | Verify six-table validation, deterministic events, HDFS handoff and Kafka delivery. |
| Prerequisites | Matching source receipt and module/runtime outputs for one declared scope. |
| Command/reference | Review artifacts from [Module 1 operations](docker.md#module-1-ingestion-generation-staging-and-kafka-replay), [generation contract](docker.md#synthetic-event-generation), or B3 `input`/`module1`/`replay` receipts. |
| Expected observation | Raw domains, keys, relationships and contiguous cart positions pass; source/curated/event counts reconcile; seed/scenario and monotonicity are recorded; HDFS receipts and producer attempted/acknowledged/failed counts are present. |
| Decision | PASS for coherent executed evidence; FAIL for validation/count/hash mismatch; BLOCKED when source or handoff/delivery evidence is missing. |
| Evidence | Six-file receipt/hashes, clean manifest, feed manifest, `_sources.json`, `_handoff.json`, producer receipt and terminal-offset evidence. |
| Troubleshooting | Prior and train facts are generated; official test orders have no purchase truth. Synthetic dates do not recover customer chronology. |
| Stop/recovery | Keep rejected inputs and prior outputs; diagnose before any owned publication replacement. Do not replay into a shared topic as a repair. |
| Side effects | READ_ONLY and LOCAL_WRITE for this worksheet; execution belongs to the mutating owner phase. |

### P3.2 — Module 2 batch analytics and publication

| Field | Operator contract |
|---|---|
| Objective | Verify analytics and source/output reconciliation. |
| Prerequisites | P3.1 handoff plus matching Module 2 run artifacts. |
| Command/reference | Review [Module 2 procedures](docker.md#module-2-batch-processing-and-benchmarks), Proposal §§12–16, and the run's analytics/summary/physical plans. |
| Expected observation | Prior-only product/department facts, joins, pivots/unpivots, window/custom aggregations and reference totals reconcile. Prior-only user features are descriptive. Mongo readback preserves each collection's run/version identity. |
| Decision | PASS for correct reconciled outputs/plans; FAIL for altered counts, mixed publications or unequal equivalent arms; BLOCKED for missing source/plan/readback evidence. |
| Evidence | Input validation, analytics, query plans, checksums, user-feature scope and collection publication receipts. |
| Troubleshooting | Order-level hour/day distributions can include all orders while product purchase metrics use prior facts; these totals need not share the same denominator. |
| Stop/recovery | Do not overwrite the shared HDFS user-feature path or republish Mongo merely to fill a missing report row. Select an authorized owner procedure first. |
| Side effects | READ_ONLY and LOCAL_WRITE for review; actual batch publication is DATA_MUTATION and RESOURCE_INTENSIVE. |

### P3.3 — Module 3 event-time processing and recovery

| Field | Operator contract |
|---|---|
| Objective | Verify input quality, finalized trends, offset continuity and measured latency boundaries. |
| Prerequisites | Known Kafka cluster/topic/partitions, query configuration, checkpoint identity and runtime receipts. |
| Command/reference | Review [streaming operations](docker.md#module-3-streaming-operations), [Module 3 contracts](module3-contracts.md), and B4 event-time/recovery checks when available. |
| Expected observation | Invalid/deduplicated/consumed records reconcile; finalized window outputs/ranks are correct; producer success and terminal offsets agree; recovery evidence binds the same query/topic/checkpoint; latency names its clocks and finalization boundary. |
| Decision | PASS for the declared tested scope; FAIL for offset gaps, incompatible state or wrong window results; BLOCKED for missing offsets/checkpoint/recovery evidence. |
| Evidence | Producer receipt, completed-batch progress/sink JSONL, watermark/late-data results, checkpoint schema/query IDs and per-partition offsets. |
| Troubleshooting | Drained offsets do not finalize the last event-time windows. Finalized-output latency includes watermark wait and possible producer/output-host clock skew; it is not Kafka acknowledgement or dashboard latency. |
| Stop/recovery | Keep the tail checkpoint and offset records. No fabricated future event, `--allow-data-loss`, or checkpoint deletion to force PASS. Kubernetes auto-resume is disabled. |
| Side effects | READ_ONLY and LOCAL_WRITE for review; replay/recovery execution mutates topic/checkpoint/sink state under its owner procedure. |

Bounded watermark deduplication is not permanent uniqueness, and retry-convergent
Mongo writes do not establish end-to-end exactly-once transactions across all
sinks and readers. A bounded B4 checkpoint probe is not evidence of every
possible restart during immutable bundle publication.

### P3.4 — Module 4A prior-only ALS and evaluation

| Field | Operator contract |
|---|---|
| Objective | Verify leakage-safe fitting, honest metrics and versioned recommendations. |
| Prerequisites | Valid prior/train handoff and the current run's model/evaluation/publication artifacts. |
| Command/reference | Review [Module 4A/4B operations](docker.md#module-4a4b-operation-and-full-data-acceptance) and [ALS specification](module4-plan.md#module4a-als-recommendation) and [acceptance checklist](module4-plan.md#acceptance-checklist). |
| Expected observation | Fit/tuning use prior only; official train is held-out truth. Report Precision/Recall at stated K, cohorts/coverage, global-popularity and personal-frequency baselines, model config/seed/runtime and publication version. |
| Decision | PASS for valid computation, provenance and publication; FAIL for leakage, wrong cohorts or readback; BLOCKED without current model/evaluation evidence. |
| Evidence | Source hashes, split provenance, trial/model manifest, evaluation/cohort metrics, readiness and recommendation readback. |
| Troubleshooting | Official test orders have no public truth. A missing score must remain unavailable. Metrics below a baseline are valid observations, not evidence of model superiority. |
| Stop/recovery | Do not tune against official train truth or relabel archived metrics as the current model's result. |
| Side effects | READ_ONLY and LOCAL_WRITE for review; fitting/publication are RESOURCE_INTENSIVE and DATA_MUTATION in the owner phase. |

### P3.5 — Module 4B product graph

| Field | Operator contract |
|---|---|
| Objective | Verify the prior-basket graph and real GraphFrames execution. |
| Prerequisites | Matching prior source/catalog and graph run artifacts. |
| Command/reference | Review the graph sections of [Module 4 specification](module4-plan.md) and [paired A/B procedure](docker.md#module-4a4b-operation-and-full-data-acceptance). |
| Expected observation | Canonical undirected pairs, no self-loops/duplicate edges, declared threshold, correct neighbor/weighted-degree metrics, real GraphFrames backend and symmetric-topology PageRank. Full catalog coverage includes isolated products, or an explicitly declared restricted scope. |
| Decision | PASS for correct verified graph/readback; FAIL for graph invariants or claimed backend mismatch; BLOCKED for missing backend/catalog/version evidence. |
| Evidence | Graph validation, backend/jar identity, vertex/edge/catalog counts, threshold, graph version and Mongo snapshot/index receipts. |
| Troubleshooting | PageRank uses topology rather than weighted PageRank. Connected Components is optional. |
| Stop/recovery | Do not hide inactive catalog products or silently replace GraphFrames with another backend. |
| Side effects | READ_ONLY and LOCAL_WRITE for review; graph execution is RESOURCE_INTENSIVE and may DATA_MUTATION through publication. |

### P3.6 — Module 4C Hybrid sources and ranking

| Field | Operator contract |
|---|---|
| Objective | Verify ready versioned sources, deterministic ranking and truthful evaluation scope. |
| Prerequisites | Ready A/B generation, verified catalog, chosen namespace/cutoff, and an eligible immutable finalized bundle or explicitly recorded fallback. |
| Command/reference | Review [Module 4C contract](module4-plan.md#module4c-spec), [bundle boundary](module3-contracts.md#module-4c-consumption-boundary), and owner-run Hybrid receipts. The [Hybrid CLI](docker.md#hybrid-cli) is a publisher, not a read query. |
| Expected observation | Source readiness, bundle identity/hash/window, explicit cutoff, component scores, deterministic ties, serving version and per-user pointer reconcile; unavailable trends/fallback/empty results have explicit reasons. |
| Decision | PASS for the requested source/mode contract; FAIL for corrupt/newest-source fallback, mixed generation or wrong ranking; BLOCKED for absent required provenance/readiness. |
| Evidence | A/B model/graph IDs, bundle namespace/scope/hash/window, cutoff, ranking config, source digest, immutable output and pointer readback. |
| Troubleshooting | `source_scope` metadata does not establish prior-only eligibility. Ordinary mixed prior/train Kafka input cannot support leakage-safe train-quality evaluation merely by choosing a timestamp cutoff. |
| Stop/recovery | Do not publish manually inside the coordinator or use API reader credentials to write. Diagnose the newest corrupt eligible bundle rather than choosing an older one silently. |
| Side effects | READ_ONLY and LOCAL_WRITE for review; Hybrid CLI execution is DATA_MUTATION and requires publisher authorization. |

Controlled score/rank responsiveness is distinct from recommendation accuracy.
The prior-only Hybrid evaluation's fixed frequency proxy is distinct from actual
Module 3 temporal windows; report its design and limitations separately.

### P3.7 — Module 4D API, Streamlit and evidence views

| Field | Operator contract |
|---|---|
| Objective | Verify bounded read-only serving and distinguish live source identity from archived evidence. |
| Prerequisites | Matching API/UI runtime evidence, known read account/index provisioning and selected source versions. |
| Command/reference | Review [Module 4D contract](module4-plan.md#module4d-spec), [required indexes](docker.md#required-dashboard-indexes), Phase 2 fixture/browser evidence or B4 API/UI results. |
| Expected observation | API fixed/indexed bounded reads and read-only permissions hold; UI accesses only the API; ALS and Hybrid versions remain distinct; unavailable/empty/error states and provenance are explicit. |
| Decision | PASS for the executed read/interface checks; FAIL for writes, unbounded reads, wrong source or misleading metrics; BLOCKED for missing indexes, credentials, readiness or current evidence. |
| Evidence | Sanitized API/UI responses, index/permission checks, source versions, browser observations and report hashes/classification. |
| Troubleshooting | `/health` proves process liveness only. Evaluation/distribution pages may read registered archived reports; they do not recompute the current model's metrics. |
| Stop/recovery | Provision indexes through the administrator procedure, never by granting API write access. Keep historical metrics labeled with their own source/run/date. |
| Side effects | READ_ONLY and LOCAL_WRITE for worksheet review; any new fixture/browser process belongs to authorized Phase 2/4. |

**Phase evidence/exit gate:** each required module has an attributable worksheet
row and reconciled artifacts for its declared scope. A missing full-data or live
result stays NOT RUN/BLOCKED even when its offline test passed.
**Failure/next phase:** use the owning procedure to fill missing evidence after
authorization. Continue to Phase 4 for the bounded Kubernetes fixture, then
Phase 5 for fresh full data and assignment experiments.

<a id="phase-4"></a>
## 6. PHASE 4 — Kubernetes fixture acceptance

**Purpose:** run the isolated course demonstration on bounded synthetic input.
**Entry:** approved clean source, package/environment readiness, B3 capacity and
explicit authorization to create a cluster, build images, publish data and run
B2/B3/B4. A **PVC** requests persistent storage; a **PV** is the bound volume.
Keep PVC/PV/StorageClass identities with ownership receipts.

### P4.1 — Fresh identity, lock and ownership preconditions

| Field | Operator contract |
|---|---|
| Objective | Select a fresh run identity/output without adopting existing resources. |
| Prerequisites | P0/P1 ready; [current lock](../deploy/k8s/versions.lock.yaml) selects Minikube v1.39.0, Kubernetes v1.36.4, Calico v3.32.2. |
| Command/reference | Use the block below; inspect [run identity](kubernetes.md#run-identity-and-ownership) and [runbook commands](kubernetes.md#commands). |
| Expected observation | New 12-hex ID, profile/context absent, output and sibling `.preparation` absent; parent exists privately outside worktrees. |
| Decision | PASS for reviewed fresh identity/path; FAIL for a collision or unsafe path; BLOCKED if ownership/toolchain cannot be established. |
| Evidence | Chosen ID, source SHA, lock hash, planned profile/context/namespace, output and preparation paths. |
| Troubleshooting | Local preflight does not contact runtime services and cannot prove profile/namespace absence. The coordinator repeats runtime collision/ownership checks and refuses adoption. |
| Stop/recovery | Do not delete a colliding profile, context or output. Select a new ID only after understanding the prior run. Do not pass the incomplete template through `run --lock`. |
| Side effects | READ_ONLY for listings; LOCAL_WRITE only for saved records. Generating an ID does not create resources. |

```bash
FIXTURE_RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
FIXTURE_OUTPUT="$EVIDENCE_ROOT/k8s-fixture-$FIXTURE_RUN_ID"
printf '%s\n' "$FIXTURE_RUN_ID" "$FIXTURE_OUTPUT"
minikube profile list -o json
kubectl config get-contexts -o name
test ! -e "$FIXTURE_OUTPUT"
test ! -e "$FIXTURE_OUTPUT.preparation"
```

Do not reuse the runbook's example ID. Do not pre-create either output directory;
the coordinator must claim them itself. The runtime checks additionally reject
symlinks and unsafe/nested managed locations.

### P4.2 — Local preflight and plan

| Field | Operator contract |
|---|---|
| Objective | Check local admission and review every planned effect before execution. |
| Prerequisites | P4.1, current P0.3 capacity/WSL evidence, executable presence and clean source. |
| Command/reference | Run the canonical fixture preflight/plan below; source contract: [demo coordinator](../scripts/k8s/demo.py), `preflight_report`, `_plan_document`, and parser. |
| Expected observation | Preflight `runtime_execution_ready=true`, blocker null, B3 reserve passed, required executables true, clean source, valid static lock. Plan shows intended identity, capacity, input scope, six ordered steps, timeouts and disabled resume. |
| Decision | PASS for local readiness and reviewed plan only; FAIL for inconsistent identity/configuration; BLOCKED before execution if readiness is false, output is incomplete or a prerequisite is missing. |
| Evidence | Complete preflight/plan JSON, exit codes, WSL measurement and planned effects. |
| Troubleshooting | Exit 0 alone is insufficient. Read `runtime_execution_ready` and `runtime_execution_blocker` in preflight. Plan has no equivalent readiness boolean; inspect its `host_capacity`, `pipeline_steps`, `execution_model`, and policies alongside preflight. |
| Stop/recovery | Stop before `run` on any blocker. `runtime_lock_complete=false` is expected for a fresh template, not permission to invent runtime values. |
| Side effects | READ_ONLY local inspection; input hashing may be RESOURCE_INTENSIVE in full mode; LOCAL_WRITE if outputs are saved. |

```bash
python3 -m scripts.k8s.demo preflight --mode fixture
python3 -m scripts.k8s.demo plan --mode fixture \
  --run-id "$FIXTURE_RUN_ID"
```

Preflight reports `checks.required_executables_present`,
`checks.b3_capacity_reserve`, `checks.source_tree_clean`, `source_identity`,
and `host_capacity`. It does not verify Docker daemon readiness, Buildx, CNI
enforcement, registry availability, or successful deployment. Inspect these
prerequisites through their owning phases as well.

### P4.3 — Authorized coordinator execution

| Field | Operator contract |
|---|---|
| Objective | Execute one complete isolated fixture session. |
| Prerequisites | P4.1/P4.2 PASS and explicit recorded authorization for all planned effects. |
| Command/reference | Use the fixture command below, owned by [Commands](kubernetes.md#commands) and [Ordered execution and recovery](kubernetes.md#ordered-execution-and-recovery). |
| Expected observation | Preparation collects source/toolchain/cluster/storage/image identities, finalizes private locks, deploys B2 serving, runs B3 pipeline then B4; resources and evidence remain. |
| Decision | PASS only when the matching session is `runtime_pass` and B4 records `fixture_runtime_pass` / `fixture_runtime`; FAIL on executed gate failure; BLOCKED on admission/identity/prerequisite failure. |
| Evidence | stdout/stderr/exit code, preparation journal/build metadata/locks, session, B2/B3/B4 receipts and module artifacts. |
| Troubleshooting | B3 order is exactly `input → module1 → module2 → module4 → replay → hybrid`. Jobs have `backoffLimit: 0`; no automatic retries or resume. |
| Stop/recovery | Follow P4.5 at the first failed stage. Do not manually apply manifests, upgrade CNI, run a missing module, or restart a Job around the coordinator. |
| Side effects | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE. Creates/retains a fresh profile, storage, images, Secrets/accounts/indexes, services and workloads. |

```bash
python3 -m scripts.k8s.demo run --mode fixture \
  --run-id "$FIXTURE_RUN_ID" --output "$FIXTURE_OUTPUT" \
  --execute
```

B2 establishes serving and ownership; B3 performs the isolated pipeline; B4
evaluates live evidence. Fixture input is bounded to at most 50 users/5,000
generated events. A fixture runtime pass does not establish full-data behavior.

### P4.4 — Runtime receipt and scenario review

| Field | Operator contract |
|---|---|
| Objective | Verify the actual acceptance scope and all required runtime observations. |
| Prerequisites | A saved session and B2/B3/B4 artifacts from P4.3. |
| Command/reference | Inspect the session's bound artifact paths and [acceptance scenario contract](../scripts/k8s/acceptance.py); run P4.6 offline verification and complete Phase 3 worksheets. |
| Expected observation | Run/profile/context/cluster/namespace UIDs, lock and receipt hashes reconcile; all required B4 checks have `status=passed` and evidence hashes; API/UI/network/storage and source outputs agree. |
| Decision | PASS for complete matching fixture evidence; FAIL for failed/mismatched checks or hashes; BLOCKED for missing receipts, artifacts or source identity. |
| Evidence | `demo-session.json`, `version-lock.json`, B2/B3 ownership receipts, B4 receipt/result/log, scenario hashes and explicit omissions. |
| Troubleshooting | Static NetworkPolicy YAML is not packet enforcement. Verify actual allowed/denied probes; API liveness alone is not data/index readiness. |
| Stop/recovery | Do not edit a receipt or promote partial success. Preserve inconsistencies and use P4.5. |
| Side effects | READ_ONLY and LOCAL_WRITE for review. Dashboard port forwarding is a separately authorized service-access operation. |

The reviewed source defines these **14** B4 scenarios. Compare the inventory
against the approved source at acceptance time; never infer a new scenario's
result from this list:

| Scenario | Required evidence focus |
|---|---|
| `input_provenance` | Input receipt and declared scope. |
| `hdfs_readback` | Run-owned HDFS data/readback identity. |
| `kafka_delivery` | Producer/consumer and bounded delivery evidence. |
| `spark_batch_outputs` | Correct batch outputs and artifacts. |
| `spark_query_experiments` | Join, pruning and cache plans/results. |
| `spark_executor_benchmark` | Equal-source/output local-thread comparison. |
| `event_time_watermark_late_data` | Window/watermark/late-event behavior. |
| `checkpoint_recovery` | Controlled query/checkpoint continuity probe. |
| `mongo_idempotent_publication` | Publication/replay consistency. |
| `als_graph_readback` | Current ALS/graph snapshots. |
| `hybrid_trend_change` | Controlled serving score/rank responsiveness. |
| `api_ui_smoke` | Current read/interface behavior. |
| `network_policy_denied_api_path` | Live denied-path evidence, alongside required allowed paths. |
| `storage_provenance` | PVC/PV/StorageClass/run locality. |

Use only the session's printed explicit-context port-forward command after a
runtime pass; it is not started automatically. Preserve the selected user,
model/serving versions and namespace with UI observations.

### P4.5 — Failure stages and safe retention

| Field | Operator contract |
|---|---|
| Objective | Preserve partial state without assuming automated collection or teardown is always available. |
| Prerequisites | A failed attempt, its command/exit code and whichever preparation/session files exist. |
| Command/reference | Inspect the table below, the sibling `.preparation/preparation-state.json`, and [failure/recovery rules](kubernetes.md#ordered-execution-and-recovery). Use P4.6 only when its session prerequisites hold. |
| Expected observation | Last completed stage, possible retained resources and available evidence are identified without changing them. |
| Decision | PASS for a complete failure inventory, not runtime acceptance; FAIL if evidence contradicts claimed state; BLOCKED for automated collect/teardown when required session/receipts are absent. |
| Evidence | Sanitized error, stage journal, paths, known profile/namespace IDs, existing logs, missing files and owner/reviewer decision. |
| Troubleshooting | `demo-session.json` is created after preparation/lock completion and private render; some failures leave a profile or images but no session. |
| Stop/recovery | No universal automated cleanup or retry is promised. Retain state for authorized review; no manual mutation, name-only deletion or auto-resume workaround. |
| Side effects | READ_ONLY and LOCAL_WRITE for inventory; live collection requires P4.6 authorization. |

| Failure stage | Safe action and limitation |
|---|---|
| Before profile creation | Preserve local admission/source/build-preparation errors and journal if created. Establish whether any resource call occurred. |
| After profile creation, before session | Preserve preparation state/build metadata and profile identity. `verify`, `collect`, and coordinator teardown may be inapplicable without a session and complete ownership receipts. Escalate for a bounded recovery plan. |
| During B2/B3 | Preserve session/stage/available receipts. Collect live diagnostics only with a valid session and authorization; partial B3 may block automated teardown. |
| During B4 | Retain failed scenario/result/log and complete earlier receipts. Do not convert partial B4 into runtime PASS. |
| After acceptance | Preserve accepted receipts and artifacts; keep resources until evidence review and separate teardown authorization. |

### P4.6 — Verify, inventory and authorized live diagnostics

| Field | Operator contract |
|---|---|
| Objective | Verify saved evidence and distinguish local inventory from live failure collection. |
| Prerequisites | Exact run ID/output and a valid private `demo-session.json`; live diagnostic collection needs explicit authorization. |
| Command/reference | Run the two local commands below. For live diagnostics, follow [runbook commands](kubernetes.md#commands): `collect --failure --execute` with the same run ID/output after authorization. |
| Expected observation | `verify` reports matching saved receipts, `cluster_contacted=false`, `teardown_performed=false`; `collect` lists host-side paths, byte sizes and SHA-256. Live collection writes sanitized diagnostics and a pointer receipt. |
| Decision | PASS for the declared verification/collection operation only; FAIL for altered/mismatched files; BLOCKED for absent session or unsupported partial-state prerequisites. |
| Evidence | Verification JSON, local artifact inventory, failure evidence/pointer when collected, and omissions/PVC locations. |
| Troubleshooting | Neither command is a fresh live acceptance run. `collect` inventories files under the host run root; it does not export all PVC data or automatically inventory the sibling preparation directory. |
| Stop/recovery | Keep preparation evidence separately. Do not claim all artifacts are backed up while data remains on PVC; resolve required exports before teardown. |
| Side effects | READ_ONLY for local verify/inventory; LOCAL_WRITE when saving reports or collecting diagnostics. Authorized live failure collection reads cluster state/logs and writes sanitized local evidence. |

```bash
python3 -m scripts.k8s.demo verify \
  --run-id "$FIXTURE_RUN_ID" --output "$FIXTURE_OUTPUT"
python3 -m scripts.k8s.demo collect \
  --run-id "$FIXTURE_RUN_ID" --output "$FIXTURE_OUTPUT"
```

**Phase evidence/exit gate:** fixture session, complete matching B4 scope and
all mandatory scenario/ownership/module checks pass; saved evidence verifies.
The phase cannot pass because preflight/plan/collect succeeded.
**Failure/next phase:** retain failed resources/evidence and use the failure map.
Proceed to Phase 5 only with a separately prepared real source and fresh identity.

<a id="phase-5"></a>
## 7. PHASE 5 — Full-data acceptance and experiments

**Purpose:** establish correctness on verified full release input and collect
assignment performance evidence. **Entry:** fixture findings reviewed, real
six-file provenance, current B3 capacity, planned disk/time budget and explicit
authorization for full workloads. No fixture identity or runtime lock is reused.

### P5.1 — Full release input preparation

| Field | Operator contract |
|---|---|
| Objective | Prepare compliant full input without removing the original source receipt. |
| Prerequisites | Reviewed original CSVs and receipt, adequate copy/storage capacity, private evidence root and a new target path. |
| Command/reference | For absent source, use the authorized [fetch/validation procedure](docker.md#module-1-ingestion-generation-staging-and-kafka-replay), then the copy block below. Cross-check [fetcher](../scripts/fetch_instacart.py) and [six-file inspector](../scripts/k8s/data_receipts.py). |
| Expected observation | Exactly six ordinary non-symlink CSV files in `FULL_INPUT_DIR`; original receipt retained separately; fresh rows/hashes agree with approved full-source provenance and total raw bytes are at most 4 GiB. |
| Decision | PASS for verified full release input; FAIL for changed hashes/counts/headers or source contradictions; BLOCKED for absent provenance, extra sidecars, incomplete copy, or over-cap input. |
| Evidence | Original receipt/archive identity, copied-file hashes, fresh preflight source receipt with per-file rows/bytes/SHA-256 and manifest digest. |
| Troubleshooting | The fetcher mirror-v1 contract expects rows: orders 3,421,083; prior 32,434,489; train 1,384,617; products 49,688; aisles 134; departments 21. These are reference expectations, not a fresh observed result. |
| Stop/recovery | Preserve originals and partial copies. A source receipt missing after an interrupted fetch is not repaired by inventing JSON or deleting inputs; obtain reviewed provenance/recovery. Use a new target after reviewing a partial copy. |
| Side effects | LOCAL_WRITE, RESOURCE_INTENSIVE; fetch also downloads data. Source input is not modified by the copy block. |

For the canonical fetcher layout, run this only after P0.5 and source review:

```bash
SOURCE_DIR="$PWD/data/raw"
FULL_INPUT_DIR="$EVIDENCE_ROOT/full-input"
(
  set -euo pipefail
  test -f "$SOURCE_DIR/source-receipt.json"
  test ! -e "$EVIDENCE_ROOT/source-receipt.json"
  test ! -e "$FULL_INPUT_DIR"
  test ! -L "$FULL_INPUT_DIR"
  mkdir -m 700 -- "$FULL_INPUT_DIR"
  cp -- "$SOURCE_DIR/source-receipt.json" "$EVIDENCE_ROOT/source-receipt.json"
  for name in orders.csv order_products__prior.csv order_products__train.csv \
    products.csv aisles.csv departments.csv; do
    test -f "$SOURCE_DIR/$name"
    test ! -L "$SOURCE_DIR/$name"
    cp -- "$SOURCE_DIR/$name" "$FULL_INPUT_DIR/$name"
  done
  sha256sum "$FULL_INPUT_DIR/"*.csv > "$EVIDENCE_ROOT/full-input.sha256"
)
```

An approved non-fetcher source needs equivalent independently reviewed provenance.
The coordinator can inspect six syntactically valid smaller files too: successful
input preflight alone does not prove the public full release. Compare fresh
record counts/hashes with the original source contract; Module 1 must still pass
its domain/key/relationship checks.

### P5.2 — Fresh full-data preflight, plan and execution

| Field | Operator contract |
|---|---|
| Objective | Run a separately authorized full-data session on the exact prepared input. |
| Prerequisites | P5.1 PASS, fresh capacity/Windows measurement, approved clean source, all P4 ownership/version prerequisites and unused output/preparation paths. |
| Command/reference | Use the full-data block below and [canonical commands](kubernetes.md#commands). Review preflight JSON/plan before executing the separate final block. |
| Expected observation | Local readiness true, correct source receipt/hash and full-mode plan; execution produces a new profile and full-data B2/B3/B4 evidence. |
| Decision | PASS only for `full_data_runtime_pass` / `full_data_runtime` with matching receipts; FAIL for an executed gate failure; BLOCKED before run for readiness, source, capacity, identity or authorization problems. |
| Evidence | Full source/preflight/plan JSON, exact invocation/exit codes, new run ID/profile/output, journal, locks and receipts. |
| Troubleshooting | Pass input path and current WSL backing-disk evidence separately to preflight, plan and run. Sample capacity numbers are not measurements. |
| Stop/recovery | Reapply P4.5 failure stages. Never reuse fixture `--lock`, output, profile, checkpoint or namespace. No manual continuation inside the coordinator. |
| Side effects | Preflight/plan READ_ONLY with RESOURCE_INTENSIVE input hashing/counting. Run: SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE. |

```bash
FULL_RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
FULL_OUTPUT="$EVIDENCE_ROOT/k8s-full-$FULL_RUN_ID"
python3 -m scripts.k8s.demo preflight --mode full \
  --input-dir "$FULL_INPUT_DIR"
python3 -m scripts.k8s.demo plan --mode full --run-id "$FULL_RUN_ID" \
  --input-dir "$FULL_INPUT_DIR"
```

Inspect readiness, blockers, source receipt, plan and fresh paths as in P4.1/P4.2.
Only after those gates and explicit authorization:

```bash
python3 -m scripts.k8s.demo run --mode full --run-id "$FULL_RUN_ID" \
  --input-dir "$FULL_INPUT_DIR" --output "$FULL_OUTPUT" \
  --execute
```

The current full replay retains the entire generated feed at 1,000 events/s
with an 18-hour ceiling; it is not a short smoke test. These are configured
bounds, not a measured throughput guarantee. Budget the host and PVC space
described in the runbook, including all retained failed attempts.

### P5.3 — Correctness and complete lineage

| Field | Operator contract |
|---|---|
| Objective | Reconcile real-source input through final serving outputs. |
| Prerequisites | P5.2 run artifacts plus original full-source provenance. |
| Command/reference | Apply P3.1–P3.7 to full scope, inspect [B4 contract](../scripts/k8s/acceptance.py), and follow [A/B full-data checks](docker.md#module-4a4b-operation-and-full-data-acceptance) only if separately needed. |
| Expected observation | Source hashes/rows → curated relationships/HDFS → generated events → attempted/acknowledged/consumed counts and terminal partition offsets → Spark/model/graph/Hybrid outputs → API/UI source versions reconcile. |
| Decision | PASS for complete fresh lineage; FAIL for unequal identities/counts or leakage; BLOCKED for missing source/output/readback evidence. |
| Evidence | Source/curated receipts, event and producer totals, offsets, Spark summaries, ALS split/model/evaluation, graph coverage/versions, Hybrid publication and API/UI provenance. |
| Troubleshooting | Prior-only training eligibility must be established from actual split/source evidence, not descriptive `source_scope`. Missing current metrics cannot be supplied by dashboard archives. |
| Stop/recovery | Do not relabel a fixture as full data or rerun a publisher into the accepted generation to repair missing evidence. Preserve and review. |
| Side effects | READ_ONLY and LOCAL_WRITE for reconciliation; additional owner procedures require their own mutation authorization. |

### P5.4 — Performance and experiment evidence

| Field | Operator contract |
|---|---|
| Objective | Measure performance separately from correctness and state the experimental limits. |
| Prerequisites | Equal-source/output correctness evidence, sufficient resources, reviewed workload settings and authorization for any missing experiments. |
| Command/reference | Reuse valid B3 query/local-thread experiment artifacts. For missing work, use [Module 2 benchmarks](docker.md#module-2-batch-processing-and-benchmarks), [Module 3 benchmarks](docker.md#module-3-streaming-operations), and Proposal §15. |
| Expected observation | Join plans/runtime/shuffle; pruning filters/files/bytes/runtime; cache materialization versus reuse; streaming input/processed rate/trigger duration; partition arms and finalized-output latency all have method, input size, warmups, repeated trials and actual measurements. |
| Decision | PASS for each completed measurement with correct equivalent outputs; FAIL for unequal arms or unsupported speedup claims; BLOCKED for missing required experiment/instrumentation. |
| Evidence | Config/partition count, rows/bytes/source hash, physical plans, task/shuffle metrics, repetitions/warmups, raw trials and summaries, latency boundary/clocks/skew. |
| Troubleshooting | Host scheduling and warm OS/HDFS caches affect timings. All-date and filtered scans read different scopes; include cache-fill cost. Kafka partition experiments reset trial topics and must not touch shared topics. |
| Stop/recovery | No invented speedup, SLA or latency. Run missing experiments in their separately approved owner environment, not by inserting manual commands into B3. |
| Side effects | READ_ONLY/LOCAL_WRITE for evidence review; benchmark execution is SERVICE_MUTATION, DATA_MUTATION and RESOURCE_INTENSIVE; trial-topic reset is DESTRUCTIVE_OR_TEARDOWN requiring explicit scope approval. |

Record Proposal §15.5's requested partition layouts and any justified unavailable
arm; no skipped mandatory experiment becomes PASS. Proposal §3.2 calls the Kafka
partition benchmark should-have: keep its priority explicit in the agreed
acceptance matrix rather than quietly promoting or dropping it.

### P5.5 — Actual standalone executor-layout experiment

| Field | Operator contract |
|---|---|
| Objective | Supply Proposal §15.6 evidence from actual executor allocation. |
| Prerequisites | A separate approved Spark standalone master/workers, identical accessible code/Python and HDFS input, reachable driver/UI REST API, capacity and authorization. |
| Command/reference | Use [Existing standalone master: Proposal §15.6](docker.md#spark-executor-layout-experiment). Review its `plan --execution-mode standalone --master ... --executor-configs ...`, then its authorized `run` procedure. |
| Expected observation | Two through eight distinct configurations, at least one warmup/three measured repetitions per configuration, equal source/rows/checksum, and observed executor IDs/hosts/cores/effective configuration. |
| Decision | PASS for valid actual allocation/equal-output evidence; FAIL for driver-only/missing/mismatched allocation; BLOCKED without a suitable reachable standalone environment. |
| Evidence | Plan, actual master, requested/effective Spark config, observed allocation/REST records, input identity/size, physical plans, event-log metrics and trials. |
| Troubleshooting | B3 `local[1]`/`local[2]` compares threads in one local driver. It does not allocate multiple Spark executors. Observed `maxMemory` is storage memory, not requested heap. |
| Stop/recovery | Do not claim multi-executor scaling from local mode, change cluster allocation around an unreviewed run, or treat a printed plan as a measurement. |
| Side effects | Plan READ_ONLY; run SERVICE_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE through Spark applications. HDFS source is read; no cluster is created by the harness. |

### P5.6 — Full-data acceptance decision

| Field | Operator contract |
|---|---|
| Objective | Close the correct full-data scope without promoting unrelated results. |
| Prerequisites | P5.1–P5.5 required evidence and complete full session/receipts. |
| Command/reference | Run full-session verify/collect below, complete Phase 3 worksheets and the [acceptance matrix](#report-template). |
| Expected observation | Full B4 receipt verifies against current full input/source/lock/UIDs; every mandatory module/result/performance row has its own evidence/status. |
| Decision | PASS only for the named completed scope; FAIL for failed/mismatched runtime evidence; BLOCKED for missing mandatory lineage/experiment/artifact. |
| Evidence | Full verify/inventory JSON, source and receipt hashes, experiment reports, remaining gaps and reviewer decision. |
| Troubleshooting | `fixture_runtime` PASS, `full_data_runtime` PASS, performance evidence complete and production readiness are four different statements. |
| Stop/recovery | Preserve a successful runtime scope even if a separate experiment is blocked, but do not mark overall assignment acceptance complete while mandatory evidence is missing. |
| Side effects | READ_ONLY local verification/inventory; LOCAL_WRITE for the final report. |

```bash
python3 -m scripts.k8s.demo verify \
  --run-id "$FULL_RUN_ID" --output "$FULL_OUTPUT"
python3 -m scripts.k8s.demo collect \
  --run-id "$FULL_RUN_ID" --output "$FULL_OUTPUT"
```

**Phase evidence/exit gate:** verified full release lineage, full-data runtime
receipt and separately complete required performance/executor evidence.
**Failure/next phase:** unresolved rows retain FAIL/BLOCKED/NOT RUN. Proceed to
Phase 6 to report actual outcomes, including partial completion and omissions.

<a id="phase-6"></a>
## 8. PHASE 6 — Evidence, reporting and handoff

**Purpose:** preserve attributable evidence, explain what passed and what
remains, and hand resources over safely. **Entry:** all executed attempts and
available artifacts, including failures; a runtime pass is not required to
write an honest report.

### P6.1 — Host and storage artifact inventory

| Field | Operator contract |
|---|---|
| Objective | Separate collected files from artifacts still retained on Kubernetes storage. |
| Prerequisites | Run records/paths/UIDs, P4.6/P5.6 inventories, and preparation siblings. |
| Command/reference | Reconcile host inventory with pipeline/B4 artifact references and [evidence safeguards](kubernetes.md#evidence-and-teardown-safeguards). Inspect the run-owned PVC/HDFS locations through an approved read/export plan. |
| Expected observation | Each required artifact is located, hashed and classified as exported, still on PVC/HDFS, unavailable or omitted. Source/input/image/runtime identity and all attempts are traceable. |
| Decision | PASS for a complete truthful inventory and retained required evidence; FAIL for corrupt/substituted artifacts; BLOCKED for required inaccessible/unexported artifacts before destructive teardown. |
| Evidence | Host receipt/log list, preparation/build/lock files, PVC/PV/HDFS IDs and paths, hashes, export status, known omissions and retained resources. |
| Troubleshooting | Host `collect` is not a universal PVC backup. Models, Parquet, Spark event logs, checkpoints or benchmark details referenced by a step may remain on storage. |
| Stop/recovery | Keep the cluster/storage while required exports are unresolved. If no canonical export procedure covers an artifact, request an exact run-scoped read/export plan; do not invent broad copy or cleanup commands. |
| Side effects | READ_ONLY inventory; LOCAL_WRITE and possibly RESOURCE_INTENSIVE for separately authorized export, with no publication change. |

### P6.2 — Requirement and acceptance matrix

| Field | Operator contract |
|---|---|
| Objective | Map each assignment requirement to actual evidence and a reviewer. |
| Prerequisites | P6.1 inventory, agreed priorities and module/phase outcomes. |
| Command/reference | Fill [Appendix A](#report-template), guided by [Appendix D](#traceability) and Proposal §§3, 38–39, 42, 45. |
| Expected observation | Every row names scope, actual command/test, source/run, evidence path/hash, outcome, reason and reviewer. |
| Decision | PASS for complete justified decisions; FAIL for unsupported PASS or hidden failures; BLOCKED for missing mandatory evidence/reviewer. |
| Evidence | Reviewed matrix, exceptions/optional-scope decisions and open actions with owners. |
| Troubleshooting | Optional Connected Components, bucketing, cloud deployment, advanced dashboard and graph score in FinalScore are not mandatory gates. |
| Stop/recovery | Unknown results remain NOT RUN; unavailable prerequisites are BLOCKED; skips retain their reasons and cannot close mandatory rows. |
| Side effects | READ_ONLY and LOCAL_WRITE. |

### P6.3 — Final reproducible report

| Field | Operator contract |
|---|---|
| Objective | Produce a self-contained report another reviewer can assess. |
| Prerequisites | P6.1/P6.2 and sanitized evidence. |
| Command/reference | Complete every [report-template field](#report-template), retaining source, versions, capacity, input and run identities, artifact hashes, commands, modules, scenarios, performance and limitations. |
| Expected observation | Current results and historical references are separately attributed; omissions, failures, uncollected artifacts, resources and teardown status are explicit. |
| Decision | PASS for complete traceable reporting; FAIL for fabricated/misattributed metrics; BLOCKED when mandatory provenance cannot be reconstructed. |
| Evidence | Final Markdown/PDF report, private evidence manifest, reviewer/date and handoff acknowledgement. |
| Troubleshooting | Screenshots without run/version identity do not prove freshness. A historical ALS chart is not current evaluation. |
| Stop/recovery | Sanitize secrets/URIs before sharing; preserve the private original under owner policy. Do not rewrite historical evidence. |
| Side effects | LOCAL_WRITE; READ_ONLY evidence review. |

### P6.4 — 8–12 minute demonstration

| Field | Operator contract |
|---|---|
| Objective | Present the actual run against Proposal §39. |
| Prerequisites | Reviewed run evidence, selected scope, dashboard access authorization and identified gaps. |
| Command/reference | Follow [Dashboard and demo script](kubernetes.md#dashboard-and-demo-script) and [Proposal](Proposal.md) §39, using the session's exact explicit-context access command. |
| Expected observation | Architecture/HDFS/batch/plans/Kafka/streaming/late-data/ALS/Hybrid/benchmark/resources are shown with their current run identities and truthful scope. |
| Decision | PASS for the declared demonstrated requirements; FAIL for unsupported live claims or substituted historical results; BLOCKED when required evidence/access is absent. |
| Evidence | Slides/video/time plan, source/run/scope, displayed receipt paths/versions, command log and reviewer observations. |
| Troubleshooting | Long full-data processing need not be rerun in 12 minutes; present its retained evidence. Refresh is not stream liveness. |
| Stop/recovery | Do not improvise new replay, fault injection or publication on a shared/accepted run just for a visual change. Use approved controlled evidence. |
| Side effects | READ_ONLY presentation; SERVICE_MUTATION for authorized port forwarding/access processes; LOCAL_WRITE for recordings. |

### P6.5 — Separately authorized teardown

| Field | Operator contract |
|---|---|
| Objective | Review an exact deletion plan only after preserving evidence. |
| Prerequisites | Correct session, complete run-specific lock, valid B2/B3 ownership receipts, required acceptance evidence, exported required artifacts and reviewed live diagnostic evidence. |
| Command/reference | Follow [canonical teardown commands](kubernetes.md#commands). Select one exact existing run and print the no-`--execute` plan below; execution requires separate authorization. |
| Expected observation | `status=plan_only`, `command_executed=false`, exact namespace/profile and evidence hashes. Authorized execution later rechecks live UIDs, ownership and storage before deletion and writes its receipt. |
| Decision | PASS for an approved plan or separately evidenced teardown operation; FAIL for identity/ownership mismatch; BLOCKED for missing session/receipts/exports/diagnostics or authorization. |
| Evidence | Plan, exact target IDs/UIDs, evidence/export review, approver/time, execution result/receipt if performed, and retained-resource inventory. |
| Troubleshooting | The coordinator requires `collect --failure --execute` diagnostics and their pointer receipt before teardown, including after a successful run. Partial preparation/B2/B3 may lack required receipts and block this path. |
| Stop/recovery | Stop on any mismatch. No global cleanup, deletion by name alone, PVC deletion shortcut or cleanup of persistent Compose services. Missing receipts require a separate bounded recovery design. |
| Side effects | Plan READ_ONLY local checks. Authorized diagnostic collection reads live state and writes evidence. Teardown SERVICE_MUTATION, DATA_MUTATION, DESTRUCTIVE_OR_TEARDOWN; separate authorization is mandatory. |

```bash
read -r -p 'Exact reviewed run ID: ' RUN_ID
read -r -p 'Exact reviewed absolute output path: ' RUN_OUTPUT
python3 -m scripts.k8s.demo teardown \
  --run-id "$RUN_ID" --output "$RUN_OUTPUT"
```

After review and separate authorization, the canonical execution adds
`--execute` to that exact invocation. Do not execute it merely because this
tutorial reaches its last phase. Runtime PASS does not mean teardown occurred.

**Phase evidence/exit gate:** final report/matrix, artifact inventory, limitations
and reviewer handoff are complete. Teardown may remain NOT RUN with resources
explicitly retained; it is not required to claim a valid B4 runtime scope.
**Failure/next phase:** hand over unresolved rows and retained resources to their
owners. No automatic cleanup or promotion to production readiness follows.

<a id="report-template"></a>
## Appendix A — Acceptance report template

Copy this section into a new operator-owned report outside the repository.
Replace placeholders only with observed evidence; leave unexecuted results
NOT RUN.

| Identity/environment field | Value |
|---|---|
| Date/time UTC; operator; reviewer | NOT RUN |
| Repository URL; branch; approved source SHA | NOT RUN |
| Local/remote SHA; source cleanliness/tree/config hashes | NOT RUN |
| OS/WSL2; interpreter/tool/runtime versions; package/jar inventory | NOT RUN |
| Physical CPU; available RAM/disk; storage paths | NOT RUN |
| Windows backing drive/free GiB/time/method when applicable | NOT RUN |
| Evidence root; owner; permissions | NOT RUN |
| Input origin/version/archive hash; six-file rows/bytes/hashes | NOT RUN |
| Fixture/full run IDs and distinct profile/context/namespace UIDs | NOT RUN |
| Docker project/container/network/image identities | NOT RUN |
| Kubernetes/Minikube/Calico; lock/build/config/manifest digests | NOT RUN |
| PVC/PV/StorageClass/HDFS/checkpoint/topic identities and paths | NOT RUN |
| Exact test selections; exclusions; skips and reasons | NOT RUN |
| B2/B3/B4 receipt paths and SHA-256 | NOT RUN |
| B4 scenarios, statuses and evidence hashes | NOT RUN |
| Benchmark inputs/configs/warmups/trials/plans/allocation | NOT RUN |
| Uncollected PVC artifacts; known omissions and limitations | NOT RUN |
| Failed attempts; retained resources; recovery owner | NOT RUN |
| Teardown plan/authorization/status/receipt | NOT RUN |
| Final handoff acknowledgement | NOT RUN |

Duplicate one row per command/attempt and requirement. Preserve failed attempts.

| Step / requirement | Scope | Source / run ID | Exact command, cwd, environment, UTC | Exit / counts | Evidence path / SHA-256 | Outcome | Failure / skip / blocker | Reviewer |
|---|---|---|---|---|---|---|---|---|
| NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN |

| Scope decision | Outcome | Required evidence / remaining work |
|---|---|---|
| Offline/static selection | NOT RUN | NOT RUN |
| Pinned Docker/package/integration/browser | NOT RUN | NOT RUN |
| Module 1 / 2 / 3 / 4A / 4B / 4C / 4D (separate rows) | NOT RUN | NOT RUN |
| Kubernetes fixture_runtime | NOT RUN | NOT RUN |
| Kubernetes full_data_runtime | NOT RUN | NOT RUN |
| Required performance and standalone allocation evidence | NOT RUN | NOT RUN |
| Human 8–12 minute demonstration / final report | NOT RUN | NOT RUN |
| Production readiness | NOT RUN | Not established by the course demonstration. |
| Teardown | NOT RUN | Retained resources and separate authorization must be listed. |

For each benchmark, record metric boundary/unit, source/rows/bytes, configuration,
warmups, every measured trial, equivalent-output check, physical plan, observed
allocation, cache/scheduling conditions and unavailable metrics. Do not prefill a
numeric speedup or latency. For each B4 check, retain its actual name/status/hash
and scope. Keep fixture and full-data report rows separate.

<a id="command-register"></a>
## Appendix B — Command and side-effect register

A command inherits all effects of its wrapper and children. This register does
not authorize execution; the owning step's prerequisites and decision apply.

| Step | Major command/procedure | Effects | Canonical owner / safety boundary |
|---|---|---|---|
| P0.2/P0.3 | Version, CPU/RAM/disk inventory | READ_ONLY; LOCAL_WRITE if logged | [Kubernetes prerequisites](kubernetes.md#prerequisites-and-capacity); Docker daemon reads are not startup. |
| P0.5/P1.2 | Private directories, venv/pip and package inventory | LOCAL_WRITE, RESOURCE_INTENSIVE for installs | Existing dependency manifests/P1.2; no system-wide install implied. |
| P1.1 | Git identity/status/ls-remote | READ_ONLY | Approved session source; no branch mutation. |
| P1.2/P1.3 | Ruff/diff check and canonical host pytest | READ_ONLY source checks; LOCAL_WRITE test/log artifacts; possible RESOURCE_INTENSIVE | [Canonical offline selection](#p13--canonical-offline-test-selection); preserve exclusions. |
| P2.1 | Docker targets/package imports | SERVICE_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE, DESTRUCTIVE_OR_TEARDOWN for temporary-container removal | [Build guide](docker.md#build-each-image), [packaging workflow](../.github/workflows/kubernetes-packaging.yml); authorize disposable-resource cleanup, imports isolated, builds may use network. |
| P2.2 | Spark/UI selections, API harness, isolated Mongo suite | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE, DESTRUCTIVE_OR_TEARDOWN for scoped cleanup | [Validation commands](docker.md#validation-commands); review cleanup before authorization. |
| P2.3 | `scripts/dashboard/demo.py up`, Chromium, exact-ID `down` | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE; DESTRUCTIVE_OR_TEARDOWN for cleanup/down | [Disposable demo](docker.md#start-and-stop-the-synthetic-dashboard); startup failure can clean its own run. |
| P2.4 | Compose config/ps versus build/up | READ_ONLY inspection; SERVICE_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE startup | [Persistent stack](docker.md#run-the-persistent-pipeline-stack); preserve shared data/volumes. |
| P2.4 | HDFS Safe Mode wait | READ_ONLY service readiness | Same guide; blocks before writes, do not bypass. |
| P3.1 | Module 1 clean/generate/stage/producer owner procedures | LOCAL_WRITE, DATA_MUTATION, RESOURCE_INTENSIVE | [Module 1 guide](docker.md#module-1-ingestion-generation-staging-and-kafka-replay); may replace recognized outputs/HDFS or append Kafka. |
| P3.2/P3.4/P3.5 | Module 2 stats/all and Module 4 wrapper/all | SERVICE_MUTATION through wrappers, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE | [Module 2](docker.md#module-2-batch-processing-and-benchmarks), [A/B](docker.md#module-4a4b-operation-and-full-data-acceptance); validate-only wrappers may still start services. |
| P3.3/P5.4 | Module 3 stream/smoke/recovery/benchmarks | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE; DESTRUCTIVE_OR_TEARDOWN for reset helpers | [Streaming operations](docker.md#module-3-streaming-operations); topics/checkpoints/sinks must be owned. |
| P3.6 | Hybrid `serve-user` | DATA_MUTATION, LOCAL_WRITE | [Hybrid CLI](docker.md#hybrid-cli); publisher credentials, not API reader. |
| P4.1/P4.2/P5.2 | Profile/context listing; local preflight/plan | READ_ONLY; RESOURCE_INTENSIVE full-input inspection; LOCAL_WRITE saved output | [Coordinator commands](kubernetes.md#commands); plan/exit 0 is not acceptance. |
| P4.3/P5.2 | `demo run --execute` | SERVICE_MUTATION, DATA_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE | Same runbook; fresh profile/storage/images/locks, B2/B3/B4; retains resources. |
| P4.6/P5.6 | `demo verify`, plain `collect` | READ_ONLY local files; LOCAL_WRITE saved reports | Same runbook; no fresh live acceptance or complete PVC export. |
| P4.6/P6.5 | `collect --failure --execute` | READ_ONLY live diagnostics, LOCAL_WRITE | Same runbook; valid session and explicit authorization required. |
| P5.1 | Fetch/copy/hash six CSVs | LOCAL_WRITE, RESOURCE_INTENSIVE | [Fetcher/Module 1](docker.md#module-1-ingestion-generation-staging-and-kafka-replay); preserve original receipt outside six-file input copy. |
| P5.5 | Executor benchmark `plan` versus `run` | READ_ONLY plan; SERVICE_MUTATION, LOCAL_WRITE, RESOURCE_INTENSIVE execution | [Standalone benchmark guide](docker.md#spark-executor-layout-experiment); existing cluster, actual allocation evidence. |
| P6.1/P6.4 | Artifact exports; explicit-context port forward | READ_ONLY source reads, LOCAL_WRITE/RESOURCE_INTENSIVE exports; SERVICE_MUTATION access process | [Evidence safeguards](kubernetes.md#evidence-and-teardown-safeguards), [demo script](kubernetes.md#dashboard-and-demo-script). |
| P6.5 | `demo teardown` versus `teardown --execute` | READ_ONLY plan; SERVICE_MUTATION, DATA_MUTATION, DESTRUCTIVE_OR_TEARDOWN execution | [Teardown procedure](kubernetes.md#commands); separate authorization/live UID guards. |

<a id="failure-map"></a>
## Appendix C — Failure and recovery decision map

| Condition | Decision and safe behavior | Evidence / route |
|---|---|---|
| Missing dependency/import | BLOCKED; record exact environment/import failure, install only the declared dependency in its intended environment; no new skips. | P1.2/P2.1 logs/manifests. |
| Insufficient capacity or absent WSL measurement | BLOCKED before runtime; measure again or select an adequate host; no arbitrary lower reserve. | P0.3/P4.2 capacity and blocker fields. |
| Dirty/unapproved checkout or remote mismatch | STOP/BLOCKED; preserve work and ask the source owner/reviewer to resolve identity. | P1.1 full outputs. |
| Invalid/missing/extra source input or over 4 GiB | STOP/FAIL or BLOCKED according to the observed problem; keep inputs/receipt, prepare a new compliant copy after review. | P0.4/P5.1/P5.2 source receipt. |
| HDFS unavailable or still in Safe Mode | BLOCKED before writes; inspect the exact NameNode/DataNode and wait; do not force unsafe writes. | P2.4 service/Safe Mode logs. |
| Incomplete committed runtime lock | Expected static template; PASS only as static input. Do not use it with `run --lock`; authorized fresh preparation collects identities. | P4.1/P4.2/P4.3. |
| Incomplete claimed runtime lock or version mismatch | STOP/FAIL; retain installed/observed identities and lock; no validator relaxation or manual CNI upgrade. | Preparation journal, lock and component inventory. |
| Missing `demo-session.json` | Automated verify/collect/teardown may be BLOCKED; preserve preparation sibling and possible resources for an exact recovery review. | P4.5 failure-stage table. |
| Checkpoint/query/offset inconsistency | STOP/FAIL; retain checkpoint, schema/config/query IDs, topic/cluster and terminal offsets; no deletion or data-loss bypass. | P3.3; [checkpoint contract](module3-contracts.md#checkpoints-retries-and-failure-boundaries). |
| Missing required receipt/hash/evidence | BLOCKED; distinguish unavailable from not executed, locate originals, never manufacture a receipt. | P4.4/P6.1. |
| Required artifacts still on PVC/HDFS | BLOCKED for destructive teardown until approved export/retention review; host inventory is not backup. | P6.1/P6.5 inventory and paths. |
| Teardown UID/ownership/storage mismatch | STOP/FAIL; retain resources and diagnostic evidence; no name-only or global deletion. | P6.5; [teardown implementation](../scripts/k8s/teardown.py). |
| Runtime PASS but separate experiment missing | Keep runtime scope's result; leave experiment/overall requirement BLOCKED or NOT RUN and assign an owner. | P5.6/P6.2 matrix. |

<a id="traceability"></a>
## Appendix D — Requirement traceability and references

Priorities come from Proposal §§3 and 42–45. The table covers implemented
graph/API work as requested; optional features remain optional. Each row still
needs the actual command/test, scope, evidence path/hash, outcome, reason and
reviewer in Appendix A.

| Assignment requirement | Canonical documentation | Verification phase | Required evidence | Acceptance scope |
|---|---|---|---|---|
| Module 1; §§7–11, 42 | [Generation](docker.md#synthetic-event-generation), [Module 1 operations](docker.md#module-1-ingestion-generation-staging-and-kafka-replay) | P0.4, P3.1, P4/P5 | Six-file validation/provenance, deterministic feed, HDFS receipts, Kafka delivery/offsets | Fixture and full data recorded separately. |
| Module 2; §§12–16, 42 | [Batch operations](docker.md#module-2-batch-processing-and-benchmarks), [ledger](implementation-ledger.md) | P3.2, P5.3/P5.4 | Reconciled analytics/features/publication, plans, joins/windows/pivots and measurements | Correctness plus explicit benchmark input scope. |
| Module 3; §§17–22, 37–38, 42 | [Streaming contract](module3-contracts.md), [operations](docker.md#module-3-streaming-operations) | P3.3, P4/P5 | Parse/dedupe, windows/watermark/late data, sink idempotency, offsets/checkpoint recovery, rates/latency | Actual runtime evidence; no universal exactly-once claim. |
| Module 4A; §§23–25, 38, 42 | [Module 4 specification](module4-plan.md), [A/B procedure](docker.md#module-4a4b-operation-and-full-data-acceptance) | P3.4, P5.3 | Prior-only fit/tuning, held-out train, cohorts/baselines/metrics, model and publication identity | Current run, including full-data evaluation. |
| Module 4B; §§26–27, 42 | Same Module 4 references | P3.5, P5.3 | Prior graph invariants, real GraphFrames, catalog coverage, weighted degree/PageRank, versioned readback | Implemented graph; Connected Components optional. |
| Module 4C; §§28–29, 38, 42 | [Hybrid specification](module4-plan.md#module4c-spec), [bundle boundary](module3-contracts.md#module-4c-consumption-boundary) | P3.6, P4/P5 | Ready A/B plus eligible bundle/fallback, deterministic score/rank, source hashes/pointer; separately justified quality design | Responsiveness, runtime integration and quality are separate. |
| Module 4D; §§30, 42, 45 | [Read API/UI](module4-plan.md#module4d-spec), [dashboard fixture](docker.md#start-and-stop-the-synthetic-dashboard) | P2.3, P3.7, P4/P5 | Bounded indexed read-only API, current source identities, UI/browser states, archived-report labels | Disposable fixture/current runtime; archives keep historical scope. |
| Kafka/HDFS/Mongo/Spark integration; §§6, 9–10, 31–35, 45 | [Docker guide](docker.md), [Kubernetes guide](kubernetes.md) | P2, P4, P5 | Source-to-storage-to-serving reconciliation, actual service/image/receipt identities | Pinned integration and isolated runtime. |
| Docker environment; §§35–36, 45 | [Build targets](docker.md#build-each-image), [validation](docker.md#validation-commands) | P2.1/P2.2 | Build/package/import versions/digests and owned test selections | Package/integration evidence, not cluster acceptance. |
| Kubernetes demo; §§3.1, 36.2, 39, 45 | [Foundation](kubernetes.md), [canonical runbook](kubernetes.md) | P4, P5.2/P5.6 | Profile/UID/lock/image/PVC receipts, B2/B3/B4, live NetworkPolicy probes | Fixture/full-data runtime distinct; single-node course scope. |
| Batch/stream benchmarks; §§15, 37–38 | [Batch/stream operations](docker.md), [executor guide](docker.md#spark-executor-layout-experiment) | P5.4/P5.5 | Equal-source/output trials, warmups, plans/shuffle/input size, partition/rate/latency records and actual standalone allocation | Workload/environment-specific; priority of should-have partition work explicit. |
| Full-data validation; §§4, 38, 42, 45 | [Handoff](acceptance.md#delivered-scope-and-evidence-boundaries), P5 procedures | P5.1–P5.6 | Verified complete real source, row/hash/lineage checks, full-data B4, current model/graph/evaluation and reports | Full release proof, not filenames or metadata alone. |
| Final report; §§38, 42, 45 | [Available historical artifacts](README.md#historical-evidence-available-here), [report template](#report-template) | P6.1–P6.3 | Command/identity/hash/status matrix, limitations, omissions/resources/reviewer | Current acceptance plus separately attributed historical references. |
| 8–12 minute demo; §39, §45 | [Canonical demonstration](kubernetes.md#dashboard-and-demo-script) | P6.4 | Current run receipts/UI/plans/metrics, presentation/video and reviewer notes | Human demonstration; not implied by a runtime receipt. |

Additional orientation: [documentation index](README.md), [repository layout](../README.md#repository-layout),
[current progress](progress.md), and [implementation ledger](implementation-ledger.md).
Historical artifacts retain their original provenance and cannot replace fresh
evidence for the chosen source and scope. The full historical archive is not
present in this checkout; the available-artifact index does not recreate it.

## Delivered scope and evidence boundaries

[Project status](progress.md) owns implemented scope, unfinished requirements,
and current verification limits. The [design ledger](implementation-ledger.md),
[streaming contracts](module3-contracts.md), and [Module 4 specification](module4-plan.md)
own technical behavior. This appendix records historical provenance and
availability; it is not approval of the current worktree or an executed
acceptance report.

### Historical development record

The earlier source-review summary recorded these identities. The underlying
review report, detailed test logs and full run archive are not included here,
so their reported conclusions cannot be independently reconstructed from this
checkout alone.

| Historical record | Recorded source identity | Scope |
|---|---|---|
| Independent source review | `a5fe5003134efb26f2a8826ce51e98bc291a2ff9` | Reported source review and development checks; not current runtime acceptance. |
| Subsequent documentation cleanup | `e4bd750b2e744c80025cc9deead4b3c2365c18f4` | Reported Markdown-only follow-up; no assertion that later source still matches it. |

The reported host checks used Python 3.14 with lightweight dependencies: 803
passes and seven PySpark-dependent **module** skips in the broad selection,
59 focused telemetry/BSON passes, 134 focused telemetry/API/UI passes, Ruff,
and compilation of 190 files. A later historical C0 Kubernetes selection was
reported as 115 passed, zero failed and one skipped; its raw logs are also
unavailable here. These selections overlap. Eight live MongoDB
modules and two PySpark-importing suites were excluded; this was not the pinned
Spark/Java or live-service gate. The selection is recorded once in
[P1.3](#p13--canonical-offline-test-selection); its counts are historical and
must not be substituted for fresh results.

### Available and missing historical evidence

The full historical archive for Module 1/2, Module 3, Module 4 A/B acceptance,
Hybrid experiments, and API/UI release records is absent. This includes the
original full-data acceptance shell script. Missing logs, manifests and source
receipts have not been recreated or replaced with current instructions.

[Available evidence](README.md#historical-evidence-available-here) lists the five
curated application artifacts. They retain historical analytics, benchmark,
ALS/baseline and synthetic Hybrid observations, including negative findings.
They are bounded summaries rather than the full experiment archive. The
[historical style comparison](style-regression-validation.md) separately
retains a NOT FULLY VERIFIED result and unavailable-artifact limitations.

For a new acceptance, apply Phases 1–6 above and record each selected fixture,
live-service, full-data and experiment gate separately. Current source approval,
builds, browser checks, B2/B3/B4, recovery, full-data publication and executor
experiments require new qualifying evidence. Historical counts and configured
workflows cannot close those gates.

## Packaging, acceptance and teardown boundaries

The manual Kubernetes packaging workflow builds images and runs isolated
imports; it starts no persistent data services. Builds may need registry or
dependency-network access. Import containers use `--network none`, a read-only
filesystem, temporary `/tmp`, and no source mount. A configured workflow is not
a passing run; retain actual results for the selected revision.

A `runtime_pass` receipt records B4 completion for its fixture/full-data scope,
not deletion of resources. Offline `verify` contacts no cluster and reports
teardown as not performed. Resources remain after success or failure; collect
and review sanitized evidence before separately authorized, identity-checked
teardown. Never use global prune, shared Compose cleanup or name-only deletion.
The [Kubernetes guide](kubernetes.md) owns the exact procedure.

The operator owns source receipts, finalized locks, B2/B3/B4 receipts, logs,
command output, image identities and failure evidence. Record executed,
skipped, failed and blocked outcomes with reasons and source/run identity.
Keep secrets out of evidence, export retained PVC/HDFS artifacts before
teardown, and distinguish historical observations from the current run.
