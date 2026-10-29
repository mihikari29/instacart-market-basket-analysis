# Kubernetes demonstration foundation

## Status and relationship to the Proposal

**Implementation available; Kubernetes runtime acceptance remains pending.**
The implementation targets a bounded Lambda demonstration in one dedicated
local Minikube profile. It owns its HDFS, Kafka, MongoDB, Spark, API, dashboard
resources, and storage. Spark runs in local mode inside Kubernetes-managed
Jobs. This is a single-node course demonstration, not a high-availability or
production-capacity design. See [project status](progress.md) for the current
verification summary.

The repository [Proposal](Proposal.md) is retained as the requirements baseline. Its
§§3.1, 36.2, 39, 42, and 45 define the demonstration, acceptance, and
deliverables. This implementation follows the fully isolated topology. A real
runtime demonstration is still required to close the Proposal requirements.

The implementation includes private rendering, B2 serving and ownership
proof, B3 isolated HDFS/Kafka/Spark Jobs, B4 runtime acceptance and guarded
teardown, and the `scripts.k8s.demo` coordinator. It also includes the per-user
ALS API/dashboard view and the prior-only Hybrid evaluation harness.

**RUNTIME NOT VERIFIED:** no image build, cluster deployment, pipeline run,
live service probe, or teardown has been accepted for this implementation.
Offline receipts and tests are not Kubernetes runtime acceptance.

## Implemented components

- **B1 — static foundation:** serving manifests, identity contract, image
  packaging target, version lock, runbook, and pure/static checks.
- **B2 — serving implementation:** staged lock completion, host/toolchain/profile/storage checks,
  run-scoped image builds and loading, private credentials, Mongo bootstrap/indexes,
  explicit workload apply/readiness, and complete live receipts are implemented.
- **B3 — bounded Lambda integration:** isolated HDFS, Kafka, batch/replay/streaming,
  Module 1/2/4, and Hybrid Jobs use run-specific PVCs, names, manifests, and receipts.
- **B4 — acceptance and teardown:** the runtime scenario collector, sanitized
  failure evidence, and separately gated exact-profile/namespace teardown are implemented.
- **Coordinator:** `python3 -m scripts.k8s.demo run --mode fixture
  --execute` can discover a fresh run identity, create the dedicated profile,
  build and inspect images, finalize the lock, deploy B2/B3, run B4, and retain
  evidence. `preflight`, `plan`, `verify`, `collect`, and `teardown` are separate
  commands; only execution-bearing commands contact runtime services.

The per-user ALS recommendation endpoint and dashboard view are implemented in
the application work; Kubernetes B2 remains a serving-only deployment layer.

## Run identity and ownership

Generate a fresh 12-character lowercase hexadecimal run ID before rendering.
The deterministic profile and namespace are both
`it4931-k8s-<run-id>`. Object names stay stable inside that unique namespace so
the existing UI can continue to call `http://module4d-api:8000`. The owner,
run-ID, and manager labels are attached by the private render overlay. The
renderer requires a new output root beneath an existing, non-symlinked parent
outside the repository and all registered Git worktrees. It atomically claims
that root with mode `0700`, refuses nested managed outputs, and preserves partial
output if rendering fails. Rendering itself never creates credentials or
contacts Kubernetes. The demo runner checks local profile/context collisions
before creating the fresh profile; every later `kubectl` command supplies that
run's explicit context. Runtime identity discovery validates the profile,
Kubernetes version, single-node architecture, runtime, and CNI before lock
finalization.
Raw base resources carry an invalid `__K8S_RENDER_REQUIRED__` namespace and
have no committed Kustomize root. The renderer creates that root in the private
output and replaces the sentinel with the unique namespace, so an unrendered
base cannot silently target the current context namespace.

Example of the **offline renderer only** (the output must be a new private
directory):

```bash
python3 -m scripts.k8s.render --run-id 0123456789ab --output /tmp/it4931-k8s-0123456789ab-render
kubectl kustomize /tmp/it4931-k8s-0123456789ab-render/run
```

The first command writes a per-run Namespace and ConfigMap patches for the
database name `module4d_demo_<run-id>` and trend namespace
`module4d-demo-<run-id>`. The second command renders locally; `kubectl kustomize`
does not contact a cluster. The example ID is illustrative and must not be
reused for an actual run.

After a real namespace is created, the B2 collector records the exact Minikube
profile and Kubernetes context, cluster marker, namespace UID, direct resource
inventory and UIDs, controller owner references and child UIDs, PVC/PV claim
binding, StorageClass UID/provisioner/local path/node, and runtime image IDs.
The pure validator also compares a recorded receipt with a fresh live receipt.
B4 provides a separate `teardown` command/API that rechecks live receipts and
requires captured failure evidence. No cleanup runs automatically.

## Serving boundary and credentials

Only ClusterIP Services are defined. MongoDB uses `module4d-mongo:27017`,
FastAPI uses `module4d-api:8000`, and Streamlit calls the exact API hostname
`http://module4d-api:8000` required by the current UI client. The UI receives no
MongoDB credential or database URI. No Ingress, NodePort, LoadBalancer,
hostNetwork, hostPath, Docker socket, or existing Compose volume is used.

The MongoDB StatefulSet requires the run-scoped Secret
`module4d-mongo-admin` with nonempty `username` and `password` keys. The B2
helper generates four opaque Secrets in memory and sends them only through
explicit-context `kubectl` stdin: admin credentials, separate database account
credentials, API read URI, and publisher URI. Secret payloads are not written to
the render tree or receipt and are omitted from logs and exceptions. A non-root
init-container check rejects empty values without printing either value, and
`mongod` receives `--auth` explicitly. Its root credential
is used only to initialize and administer this isolated MongoDB instance. The
API requires `module4d-api-readonly` with a single `mongodb-uri` key for a
database-scoped account granted MongoDB's `read` role. A separate
`module4d-mongo-publisher` URI is generated for data publishers and is scoped to
the run-specific application database. The bootstrap grants MongoDB's
database-scoped `readWrite` role; B3 should narrow it if the actual write
contracts can use a smaller custom role. Git contains no Kubernetes Secret
object or credential value. Secret creation and Mongo account bootstrap are
implemented but remain unexecuted.
The API URI must authenticate against the generated `module4d_demo_<run-id>`
database (`authSource` set to that same database); URL-encode credentials when
they are not generated from URI-safe characters. Its service host is
`module4d-mongo:27017`.
Root credentials must not be reused by the publisher or API.

The API expects a read account with collection reads and `listIndexes` access.
At readiness it requires a URI with explicit credentials, a matching database
and `authSource`, and a MongoDB `connectionStatus` response naming exactly that
database-scoped read account. It then performs a read-only ping and checks the
required trend, batch-ranking, and unique serving indexes. It does not create
or repair indexes. Product and serving indexes require their declared names,
ordered keys, compatible visibility/options, simple collation, and serving
uniqueness. Trend selection accepts the canonical index or another index with
the compatible prefix accepted by the application reader; sparse trend indexes
remain allowed because the reader supports them. The canonical trend name is
`idx_source_namespace_window_end_desc`,
`idx_batch_product_purchase_count_desc_product_id_asc`,
`idx_batch_product_reorder_rate_desc_product_id_asc`,
`uq_serving_user_rank`, and `uq_serving_user_product`.
Readiness will remain false until the B2 bootstrap has created the database,
read account, and indexes. The current `/health` endpoint checks only the API
process; it is used only for startup/liveness. The separate API readiness probe
checks Mongo connectivity and index availability without logging credentials
or driver error text.

Each Pod uses a dedicated ServiceAccount with automatic API-token mounting
disabled and no Kubernetes API permissions. NetworkPolicies start with
namespace-wide ingress and egress denial, allow DNS to the `kube-system` CoreDNS
Pods, permit only API-to-Mongo on TCP/27017, and permit only UI-to-API on
TCP/8000. These policies express intended boundaries; enforcement depends on a
NetworkPolicy-capable CNI and has not been tested. The B2 static/live inventory
validator checks the exact policy set and labels but does not claim packet-flow
enforcement. The selected profile uses Calico; B4 must verify allowed and denied
connections, including loopback-only port-forward behavior.

## Storage and resource bounds

MongoDB has one StatefulSet replica and one isolated 2 GiB ReadWriteOnce PVC
for `/data/db`; config and temporary files use bounded ephemeral volumes. The
manifest relies on the profile's default dynamic StorageClass. B2's runtime
checks require Immediate binding and a StorageClass that provisions a profile-local volume, and verify
that the actual PVC/PV binding and capacity match the run receipt. B3 adds isolated
1 GiB NameNode, 16 GiB DataNode, 2 GiB Kafka, 16 GiB pipeline, and 2 GiB
checkpoint claims. Together with Mongo, the run requests 39 GiB of profile-local
storage, within the 40 GiB namespace quota. No Compose volume or checkpoint path
is reused.

The serving Pods request 400 millicores and 896 MiB in total; their combined
limits are 1.75 CPU and 1.75 GiB. Namespace quota caps the phase at 2 CPU,
2 GiB requested memory, 4 CPU, 4 GiB memory, 3 GiB PVC requests, and 2 GiB
ephemeral-storage. These are initial planning values, not measurements.

The runner fails closed below 6 physical CPU cores, 12 GiB available RAM, and
40 GiB free disk for B3 and the image builds. The B2 serving reserve alone is 4
CPU, 6 GiB RAM, and 20 GiB disk. Full-data mode also requires exactly six
reviewed CSV inputs and applies the 40 GiB host reserve. On WSL, the operator
must provide a measured Windows backing-disk value with
`--windows-backing-disk-gib` to `preflight`, `plan`, or `run`; guest free space is
not accepted as evidence of Windows capacity. Full-data mode caps the six raw
files at 4 GiB, streams them directly from the run PVC input directory without a
second raw copy, reserves 2 GiB for filesystem and metadata overhead, and
budgets up to three times the raw byte count for raw, cleaned, and generated feed
working sets (14 GiB maximum against the 16 GiB pipeline claim). The measured
values below show this machine does not meet the B3 reserve.

## Images and version lock

The API runner target derives from the existing pinned `spark` stage, packages
the complete Module 4 source tree plus the Module 3 trend-bundle contract that
the API imports, and includes the exact source/provenance files read by
`src.module4.cli.source_digest()`. It does not include `.git`, data, results,
checkpoints, `.env` files, credentials, or host mounts. Its default command only
prints a message; Kubernetes overrides it with the explicit Uvicorn command.
The existing API, UI, browser, and Spark stage defaults remain unchanged. The
existing UI image remains the Streamlit target and receives only API settings.

Builds must pass exactly 40 lowercase hexadecimal characters through
`MODULE4_COMMIT`; the runner target rejects a missing, malformed, or wrong-length
SHA before creating its user. The API's existing source
digest meaning is preserved by placing every file it hashes at its original
repository-relative path. The runner inherits current `requirements.txt` and
`requirements-dev.txt` resolution through the Spark stage. The API and UI
manifests pin their declared packages. A deterministic package inventory is
embedded in each application image so later runtime discovery can record the
exact Python version and transitive package closure. Those values are not
present until authorized image builds occur.
MongoDB's pinned official 7.0.16 image uses UID 999, which is the StatefulSet's
non-root account ([image metadata](https://hub.docker.com/layers/library/mongo/7.0.16-jammy/)).
Later acceptance must compare each built OCI image identity with the image loaded
into the dedicated profile, the digest rendered in the workload, and the actual
running container image ID. The B2 renderer pins complete-lock digests, while
the build plan uses run-scoped local tags. A valid digest string alone is not
evidence of provenance.
Because `.git` is excluded from the image context, builds must come from a
clean checkout and the supplied SHA must be recorded externally; inside the
image the revision is available, while the dirty-state value remains unknown.

`scripts.k8s.image_operations.image_build_plan` creates the API/UI BuildKit
commands from a clean full Git SHA, run ID, and one target platform. The plan
collects BuildKit metadata, checks each local image's revision label and config
digest, reads the embedded Python inventory from a stopped container, then
loads and verifies images against the selected Minikube profile. Build and load
functions require explicit `execute=True`; none was invoked during this coding
campaign. `scripts.k8s.deployment.deploy_serving` requires a complete lock and
verified host/toolchain/cluster/storage/image evidence before applying the
namespace, Secrets, or serving overlay.

`deploy/k8s/versions.lock.yaml` selects Minikube v1.39.0, Kubernetes v1.36.4,
and Calico v3.32.2. For this `--cni=calico` bootstrap path, Minikube v1.39.0
bundles Calico v3.32.2; its supported Kubernetes version list includes v1.36.4
and its kubeadm image map defines the matching control-plane dependencies.
Calico 3.32 documents testing on Kubernetes 1.34–1.36. This upstream version
compatibility does not verify a deployed cluster, the installed CNI, or policy
enforcement. B2 must inspect and record the actual Kubernetes and Calico image
digests used by the profile. The repository's Spark and Mongo base image digests
are retained from the locked Dockerfile and Compose file. The committed template
remains **incomplete**. Strict validation rejects it until measured host
capacity, supported host architecture,
cluster/profile UID, Minikube checksum, kubectl and Docker Engine versions,
control-plane and Calico image roles, app image identities/build provenance,
resolved Python environment digests/runtime versions, and default
StorageClass/provisioner/profile-local behavior are recorded. The one-session
runner discovers these values, builds the images, and writes finalized B2/B3
locks only after their evidence validates. No runtime preflight or cluster
operation was executed in this campaign.

Sources: [Minikube v1.39.0 release](https://github.com/kubernetes/minikube/releases/tag/v1.39.0),
[its bundled Calico version](https://github.com/kubernetes/minikube/blob/v1.39.0/pkg/minikube/bootstrapper/images/images.go),
[its supported Kubernetes versions](https://github.com/kubernetes/minikube/blob/v1.39.0/pkg/minikube/constants/constants_kubernetes_versions.go),
[its Kubernetes 1.36.4 kubeadm image map](https://github.com/kubernetes/minikube/blob/v1.39.0/pkg/minikube/constants/constants_kubeadm_images.go),
[Kubernetes v1.36.4 release](https://github.com/kubernetes/kubernetes/releases/tag/v1.36.4), and
[Calico 3.32 system requirements](https://docs.tigera.io/calico/3.32/getting-started/kubernetes/requirements).
The Streamlit probe path follows the [official Docker deployment guide](https://docs.streamlit.io/deploy/tutorials/docker#health-check).
Profile-specific image loading follows the [Minikube image-loading guide](https://minikube.sigs.k8s.io/docs/handbook/pushing/).

## Verification and remaining runtime checks

Offline fake-client, static, runner and local-rendering checks exercise source
contracts. [Project status](progress.md) owns the current verification summary;
the [acceptance appendix](acceptance.md#historical-development-record) records
the older host-check scope and missing artifacts. Neither those checks nor
cached package imports establish deployed Kubernetes behavior.

Fresh Kubernetes acceptance still requires qualifying image builds and loading;
Minikube scheduling and DNS; PVC/PV behavior; MongoDB initialization with
authentication enabled; creation and denial of admin, publisher, and API
permissions; startup/liveness/readiness behavior; API query and index
readiness; Streamlit-to-API requests; network-policy enforcement; actual host
resource headroom; and any B2/B3/B4 pipeline or acceptance behavior.

## Evidence and teardown safeguards

A future run must retain a manifest of run ID, Git SHA and dirty state, image
digests, lock-file digest, profile and cluster identity, Kubernetes version,
namespace name and UID, direct and controller-created resource UIDs, PVC/PV
identity, phase commands and exit codes, Pod events/current and previous logs,
Mongo readback/index and permission checks, API/UI receipts, and a teardown
receipt. Secrets and raw Mongo URIs must never enter these records.

Run evidence is private under the selected output root; build metadata and the
finalized lock are kept in the sibling preparation directory. Failure evidence
must be collected with `demo collect --failure --execute` and reviewed before
teardown. `demo teardown` first prints the exact namespace/profile deletion
plan; adding `--execute` is a separate authorization and the implementation
rechecks B2/B3/B4 receipts and live UIDs. No global prune or name-only deletion
is supported.

The complete command reference, acceptance matrix, demo script, and limitations
are in the [Kubernetes demo runbook](kubernetes.md).

## Operator procedure

The commands below are owned by the coordinator in [`scripts/k8s/demo.py`](../scripts/k8s/demo.py). Runtime acceptance remains pending; consult [project status](progress.md) before planning a run.

## Prerequisites and capacity

- Python 3.10 or newer with the repository's pinned `PyYAML==6.0.2` available.
- Docker Engine and Docker Buildx. The runner records the server version and
  builds only run-tagged API, UI, and pipeline images from a clean committed
  source revision.
- Minikube v1.39.0, `kubectl`, Docker driver, one containerd node, Calico, and a
  default profile-local dynamic StorageClass with ReadWriteOnce and Immediate
  binding. Delayed binding is rejected because the runner waits for PVCs before Jobs.
- At least 6 physical CPU cores, 12 GiB available RAM, and 40 GiB free disk for
  B3 and image work. B2 serving alone reserves 4 CPU, 6 GiB RAM, and 20 GiB disk.
- In WSL, a separately measured Windows backing-disk value of at least 40 GiB;
  provide it with `--windows-backing-disk-gib`. The guest's free space does not
  prove Windows has the corresponding capacity.
- Fixture mode uses the bounded deterministic image fixture (at most 50 users
  and 5,000 generated events). Full-data mode accepts exactly the six required
  CSV files with declared headers, hashes them before any cluster creation,
  requires the B3 reserve, and caps raw inputs at 4 GiB. The 16 GiB pipeline
  claim budgets up to 3× raw bytes plus 2 GiB; the DataNode has its own 16 GiB claim. Module 1 performs its full raw-domain validation inside the run.

Run preflight on the operator's selected host before each run. In WSL, measure
the Windows backing disk independently and provide its current value; guest
free space does not establish backing-disk capacity. Preflight does not contact
Docker Engine or Kubernetes.

## Commands

Run all commands from the repository root. `preflight` and `plan` are local
checks; they do not call Docker, Minikube, `kubectl`, or application services.
They do inspect local CPU/memory/disk, the input files, Git source identity, and
executable presence.

```bash
python3 -m scripts.k8s.demo preflight --mode fixture
python3 -m scripts.k8s.demo plan --mode fixture --run-id 0123456789ab
```

Choose a fresh 12-character lowercase hexadecimal run ID. Review the run plan
before execution. `plan` lists the profile/context/namespace, dependency order,
timeouts, retry policy, Spark execution model, and effects authorized by
`run --execute`.

Fixture run:

```bash
python3 -m scripts.k8s.demo run \
  --mode fixture \
  --run-id 0123456789ab \
  --execute
```

Full-data run:

```bash
python3 -m scripts.k8s.demo preflight \
  --mode full \
  --input-dir /data/instacart \
  --windows-backing-disk-gib 120

python3 -m scripts.k8s.demo plan \
  --mode full \
  --run-id 0123456789ab \
  --input-dir /data/instacart \
  --windows-backing-disk-gib 120

python3 -m scripts.k8s.demo run \
  --mode full \
  --run-id 0123456789ab \
  --input-dir /data/instacart \
  --windows-backing-disk-gib 120 \
  --execute
```

On WSL, replace `120` with a current measurement of Windows backing-disk free
capacity. Supply that measured value independently to each applicable
`preflight`, `plan`, and `run` invocation; an earlier command does not persist
it for the next one. The runner records this as operator-supplied evidence and
does not inspect Windows free capacity from inside WSL.

The runner refuses an existing profile/context, namespace, output path, or
preparation path. With no `--lock`, it creates a new profile and derives a
complete lock from live and image-build evidence. An optional `--lock` is for a
fully finalized run-specific B3 lock and existing profile; it skips the
preparation stages and still validates the exact profile identity.

By default, run evidence is written to `/tmp/it4931-k8s-<run-id>` and
preparation evidence to its sibling
`/tmp/it4931-k8s-<run-id>.preparation`. `--output` can select another new path
under an existing parent outside the repository and its worktrees. Both paths
are private, and partial evidence is preserved after failure.

Inspect results and sanitized evidence:

```bash
python3 -m scripts.k8s.demo verify \
  --run-id 0123456789ab \
  --output /tmp/it4931-k8s-0123456789ab

python3 -m scripts.k8s.demo collect \
  --run-id 0123456789ab \
  --output /tmp/it4931-k8s-0123456789ab
```

`verify` checks saved B2/B3 ownership receipts, lock identity, acceptance
scenario inventory, and artifact hashes without contacting the cluster. It
reports runtime pass only when those saved receipts reconcile; it is not a
fresh live identity comparison. `collect` prints a local artifact inventory
containing paths, sizes, and hashes, not credentials or raw dataset contents.

If a stage fails, resources remain for inspection. Capture sanitized live
failure evidence before teardown:

```bash
python3 -m scripts.k8s.demo collect \
  --run-id 0123456789ab \
  --output /tmp/it4931-k8s-0123456789ab \
  --failure \
  --execute
```

Teardown is separate and requires another explicit authorization. The first
command prints the exact action plan only; inspect it before adding
`--execute`:

```bash
python3 -m scripts.k8s.demo teardown \
  --run-id 0123456789ab \
  --output /tmp/it4931-k8s-0123456789ab

# Separately authorized only after reviewing evidence and the exact plan:
python3 -m scripts.k8s.demo teardown \
  --run-id 0123456789ab \
  --output /tmp/it4931-k8s-0123456789ab \
  --execute
```

The teardown gate rechecks the complete B2/B3/B4 receipts, exact run profile,
namespace and live object UIDs, storage ownership, and captured failure
evidence. It deletes only the run namespace and its dedicated Minikube profile.
Do not use global prune, Compose cleanup, or name-only deletion.

## Ordered execution and recovery

The run stages are: source/toolchain identity; collision check and fresh
Minikube profile; cluster/storage identity; API/UI build and environment
inventory; Kafka/HDFS/pipeline image inspection/build; B2/B3 lock finalization;
private render and namespace absence check; serving deployment; pipeline
execution; B4 acceptance. Each API operation retains its reviewed timeouts.
Pipeline Jobs have `backoffLimit: 0`; failed steps do not retry automatically.
After HDFS readiness, a checked superuser bootstrap creates only the fresh
`/instacart/<run-id>` root, assigns it to `k8spipeline` with mode 750, and refuses
an existing run root. Staging, Spark writes and readback use that pipeline identity.
No recursive ownership changes affect other paths.

Replay waits for successful producer delivery and committed terminal Kafka
offsets, then reconciles generated/attempted/acknowledged/consumed counts.
Fixture replay has a 1,200-second hard deadline. Full replay retains the entire
feed at 1,000 events/second with an 18-hour ceiling; its Job allows 65,400 seconds.
Final event-time tail state remains in the checkpoint; no future events are
fabricated to close it. Inspect both completion and output evidence.

The pipeline order is `input → module1 → module2 → module4 → replay → hybrid`.
The Module 2 step also runs three measured trials each for Spark `local[1]` and
`local[2]`, after one warmup per arm, against the same HDFS source. The B4
receipt checks the shared source hash, output checksum, actual master, runtime,
input size, and task/shuffle metrics. This compares local thread pools; it does
not claim distributed executor scaling.

The coordinator does not automatically recover a failed run. Container-step
receipt reuse is a separate `execution.resume_policy` behavior; see
[configuration and recovery boundaries](progress.md#configuration-and-recovery-boundaries).
After a failed run, inspect the session and
preparation state, `collect` sanitized evidence, and confirm which Jobs,
checkpoints, and PVCs exist. Use a verified continuation only if an explicit
recovery procedure can bind the live ownership receipts and Kafka offsets to
the checkpoint. Otherwise leave the failed resources in place for review and
start a new run only after the old run is separately handled.

Secrets are generated in memory and sent to the explicit context through
`kubectl` stdin. Receipts omit Secret contents, URI values, and command output.
Mongo admin, API read, and publisher accounts are distinct. The UI receives
only API configuration. The publisher's built-in `readWrite` role is limited to
the run-specific database; it has no admin or cross-database privileges.
Current Module 2/3/4 writers create run-owned staging collections, indexes, and
replacement snapshots, so the role is broad within that isolated database. A
collection-specific role would require changing those publication contracts
and validating the result against the real runtime. NetworkPolicies deny by
default and allow the declared DNS, API-to-Mongo, UI-to-API,
pipeline-to-HDFS/Kafka/Mongo, and narrow HDFS/Kafka peer traffic. Static policy
checks are not proof of packet-flow enforcement; B4 must pass the real
allowed/denied probes.

HDFS, Kafka, Mongo, and Spark use run-owned PVCs and paths. The run never uses
existing Compose volumes or checkpoints. Every command passes
`--context it4931-k8s-<run-id>` explicitly. Spark runs as local mode inside
Kubernetes Jobs, not as native Spark-on-Kubernetes executors.

## Dashboard and demo script

After a runtime pass, start the displayed explicit-context port-forward command
from the session receipt in a separate terminal, then open `http://localhost:8501`.
The runner does not start port forwarding automatically. Use the run-specific
user and product evidence produced by the fixture; synthetic timestamps must be
described as replay simulation, never actual purchase times.

The following 8–12 minute walk-through follows Proposal §39. The runbook does
not invent example counts, latency values, or ranking changes; use only the
current run's receipts and dashboard.

| Time | Demonstration action | Evidence to show |
|---|---|---|
| 0:00–1:00 | Explain the isolated Lambda architecture and synthetic replay timestamps. | Run plan, B3 plan, selected lock, and explicit `local[2]` execution model. |
| 1:00–2:00 | Show HDFS raw and curated paths for the run ID. | Module 1 source receipt and B4 WebHDFS readback. |
| 2:00–3:00 | Show Spark batch aggregation and Module 4 ALS/graph outputs. | Module 2/4 hashed artifacts and B4 batch/ALS checks. |
| 3:00–4:00 | Compare sort-merge and broadcast plans where emitted by batch experiments. | Recorded physical plans; state their fixture/full-data scope. |
| 4:00–5:00 | Show Kafka replay delivery and the run-specific topic. | Producer acknowledgement and B4 round-trip evidence. |
| 5:00–6:00 | Explain Structured Streaming windows, event time, and watermarks. | B4 window/watermark receipt and current trend result. |
| 6:00–7:00 | Open the dashboard and show the trend-backed response. | Current run's API/UI acceptance and displayed source versions. |
| 7:00–8:00 | Explain inclusion/exclusion for on-time and late events. | B4 late-event scenario receipt; do not fabricate a timestamp. |
| 8:00–9:00 | Select a user and show the actual ALS list. | User ID, model version, ranks, and only available scores. |
| 9:00–10:00 | Show how a controlled trend changes Hybrid score/rank. | B4 trend-sensitivity result and run-specific Hybrid snapshot. |
| 10:00–11:00 | Compare local[1]/local[2] benchmark arms. | Source hash, input rows/bytes, runtime, physical plans, and shuffle metrics. |
| 11:00–12:00 | Show retained Kubernetes Pods, Services, PVCs, and acceptance receipt. | Explicit-context `kubectl get pods,services,pvc` and `demo verify` output. |

## Acceptance matrix and validation scope

| Proposal requirement | Implementation/evidence gate | What a passing runtime result means |
|---|---|---|
| §42 Module 1 | Six-file input receipt, raw preflight, deterministic event generation, HDFS stage, Kafka replay. | The selected fixture or six full-data files completed the run's Module 1 contract. |
| §42 Module 2 | Batch aggregations, joins/plans, outputs, and source-backed `local[1]`/`local[2]` experiment. | Same measured input/output identities and actual local execution metrics were recorded. |
| §42 Module 3 | Kafka delivery, event-time windows, watermark/late data, output publication, checkpoint recovery. | B4 scenarios ran against isolated run-owned Kafka/checkpoint/Mongo resources. |
| §42 Module 4 | ALS, graph, API/UI, Hybrid publication and trend sensitivity. | The run's model/snapshot and read paths passed; no superiority claim follows without quality metrics. |
| §45 infrastructure | HDFS, Kafka, MongoDB, Spark Jobs, FastAPI, Streamlit, Kubernetes deployment, experimental reports. | Resources and receipts bind to a dedicated profile, namespace, image lock, and PVC/PV identities. |
| §39 demonstration | 12-minute sequence above. | A human presents fresh run evidence and dashboard behavior. Runtime pass alone is not a video/presentation. |

Static validation, offline fake-executor tests, CI, fixture-runtime acceptance,
and full-data-runtime acceptance are distinct. A fixture runtime pass proves
only the bounded synthetic fixture. A full-data pass requires the exact six
input receipts and full-data B4 scope. Mock tests and historical CI are not
runtime acceptance. The test matrix and evidence scope are recorded in
[`implementation-ledger.md`](implementation-ledger.md).

## Known limitations

- Runtime acceptance for the selected fixture or full-data scope requires the
  pinned images, isolated profile, workload execution, B4 checks, and retained
  identity-bound evidence. Teardown is a separate lifecycle action and is not
  part of the B4 runtime-pass receipt.
- WSL backing disk is operator-supplied evidence and is not independently
  inspected by the CLI.
- Automatic coordinator recovery is unavailable. It requires a new run
  identity and fresh output path; task-level reuse does not establish safe
  Kafka offset/checkpoint recovery.
- B4 verifies NetworkPolicy behavior only at runtime. Static manifest validity
  does not establish that the chosen CNI enforces the intended flows.
- The automated B3 comparison measures local thread pools. Proposal §15.6
  executor-layout evidence needs the separate existing-standalone-master
  experiment in [the benchmark guide](docker.md#spark-executor-layout-experiment). Neither
  mode implies a speedup; scheduling and caches affect timing.
- The run keeps resources after success or failure. Teardown needs a separate
  review and explicit `--execute`.

The coordinator's `runtime_pass` state means that B4 returned a complete
passing receipt for the declared scope; it does not mean the profile or
namespace was deleted. The offline `verify` command checks saved receipts
without contacting the cluster and reports `teardown_performed: false`.
