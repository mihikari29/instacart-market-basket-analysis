# Kubernetes demonstration foundation

## Status and relationship to the Proposal

**DESIGNED:** the final target is a bounded Lambda demonstration in one
dedicated local Minikube profile. It will own its HDFS, Kafka, MongoDB, Spark,
API, and dashboard resources and storage. Spark will run in local mode inside
Kubernetes-managed workloads. This is a single-node course demonstration, not
a high-availability or production-capacity design.

The approved [Proposal](Proposal.md) remains the requirements baseline. Its
§§3.1, 36.2, 39, and 45 require a real Kubernetes demonstration and describe
the expected pipeline walk-through. Proposal §36.2 permits HDFS to remain in
Compose if Kubernetes is too resource-intensive, provided the actual topology
is documented. This runbook describes a staged plan; it does not close that
requirement.

**CODE IMPLEMENTED, RUNTIME PENDING:** K8S-B1 provides the private per-run
render path and serving foundation. The offline B2 implementation adds staged
version-lock validation, explicit-profile discovery, image provenance and
loading helpers, in-memory MongoDB Secret generation, bootstrap users/indexes,
deployment/readiness orchestration, NetworkPolicy inventory validation, and
live ownership/storage/image receipt collection. It still contains no HDFS,
Kafka, Spark, streaming, Hybrid, or pipeline Job resources. Module 1 separately
adds the per-user ALS API and dashboard view.

**RUNTIME NOT VERIFIED:** no image was built or loaded, and no Minikube profile,
namespace, Pod, Service, PVC, Secret, Mongo user, NetworkPolicy, or application
request was created or tested for this change. A rendered manifest is not an
executed Kubernetes demonstration.

## Planned implementation phases

- **B1 — static foundation:** serving manifests, identity contract, image
  packaging target, version lock, runbook, and pure/static checks.
- **B2 — serving implementation:** code is present for lock completion from
  discovered evidence, host/toolchain/profile/storage checks, run-scoped image
  builds and loading, private credentials, Mongo bootstrap/indexes, explicit
  workload apply, readiness, and full live receipts. The committed lock remains
  incomplete until a separately authorized runtime session supplies real
  evidence; no B2 service behavior has been executed in this campaign.
- **B3 — bounded Lambda integration:** add isolated HDFS, Kafka, controlled
  Spark batch and stream workloads, ALS/graph and Hybrid publication, and
  bounded fixture receipts. This requires separate review and application
  dependency decisions.
- **B4 — acceptance:** prove replay, late-data behavior, checkpoint recovery,
  API/UI acceptance, network-policy enforcement, evidence capture, and scoped
  teardown in fresh runs.

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
output if rendering fails. It never creates credentials or contacts a
Kubernetes context. The offline B2 preflight code checks local profile/context
collisions before creation. Runtime identity discovery is explicit opt-in and
validates the profile, Kubernetes version, single-node architecture, runtime,
and CNI before lock finalization.
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
No teardown helper is implemented or executed; B4 must build the separate
teardown gate on this receipt contract. No cleanup runs automatically.

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
enforcement. Minikube is planned with Calico; B4 must verify allowed and denied
connections, including loopback-only port-forward behavior.

## Storage and resource bounds

MongoDB has one StatefulSet replica and one isolated 2 GiB ReadWriteOnce PVC
for `/data/db`; config and temporary files use bounded ephemeral volumes. The
manifest relies on the profile's default dynamic StorageClass. B2 must confirm
that a StorageClass exists, that it provisions a profile-local volume, and that
the actual PVC/PV binding and capacity match the run receipt. B1 defines no
shared volume, no reuse of Compose data, and no pipeline checkpoints.

The serving Pods request 400 millicores and 896 MiB in total; their combined
limits are 1.75 CPU and 1.75 GiB. Namespace quota caps the phase at 2 CPU,
2 GiB requested memory, 4 CPU, 4 GiB memory, 3 GiB PVC requests, and 2 GiB
ephemeral-storage. These are initial planning values, not measurements.

Reserve at least 4 host CPU cores, 6 GiB RAM, and 20 GiB free disk for a B2
serving rehearsal and image build. The later B3 pipeline target is approximately
6 CPU cores, 12 GiB RAM, and 30–40 GiB free disk for images and bounded
artifacts. Check the machine's actual capacity and leave headroom for existing
Compose services before running Minikube. These planning figures have not been
validated on the user's host.

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

`deploy/k8s/versions.lock.yaml` records Minikube v1.39.0, Kubernetes v1.37.1,
and Calico v3.33.0 as selected release versions. Calico 3.33 documents testing
on Kubernetes 1.35–1.37; this release-level compatibility reference does not
verify the installed CNI or policy enforcement. B2 must inspect and record the
actual Kubernetes and Calico image digests used by the profile. The repository's
Spark and Mongo base image digests are retained from the locked Dockerfile and
Compose file. The lock remains **incomplete**, and strict validation rejects it
until measured host capacity, supported host architecture, cluster/profile UID,
Minikube checksum, kubectl and Docker Engine versions, control-plane and Calico
image roles, app image identities/build provenance, resolved Python environment
digests/runtime versions, and default StorageClass/provisioner/profile-local
behavior are recorded. Staged read-only identity discovery and explicit
deployment code are implemented, but no runtime preflight or cluster operation
was executed in this campaign.

Sources: [Minikube v1.39.0 release](https://github.com/kubernetes/minikube/releases/tag/v1.39.0),
[Kubernetes 1.37 release status](https://kubernetes.io/releases/1.37/), and
[Calico 3.33 system requirements](https://docs.tigera.io/calico/latest/getting-started/kubernetes/requirements).
The Streamlit probe path follows the [official Docker deployment guide](https://docs.streamlit.io/deploy/tutorials/docker#health-check).
Profile-specific image loading follows the [Minikube image-loading guide](https://minikube.sigs.k8s.io/docs/handbook/pushing/).

## Verification and remaining runtime checks

**OFFLINE VALIDATION:** 47 focused Kubernetes tests passed in the isolated
worktree, including local Kustomize rendering, fake executor and receipt tests,
and Mongo Secret/index contract checks. Python AST parsing passed for 21
Kubernetes implementation/test files, YAML parsing passed for 8 lock/manifest
files, and `git diff --check` passed. These checks do not exercise Docker,
Minikube, Kubernetes, MongoDB, or application runtime behavior.

The following remain **RUNTIME NOT VERIFIED**: image builds and image loading;
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

Failure evidence must be collected before any teardown. Teardown must target
only the exact dedicated profile and namespace after validating the complete
ownership receipt and storage ownership; never use a global prune or delete
resources selected only by a name. B1 provides no cleanup recipe because no
live resources are created here.
