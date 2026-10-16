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

**STATICALLY VALIDATED:** K8S-B1 adds a private per-run render path and the
serving-only foundation in `deploy/k8s/base/`. It defines isolated MongoDB,
read-only FastAPI, Streamlit, bounded storage/resources, and network boundaries.
It contains no working HDFS, Kafka, Spark, streaming, Hybrid, or pipeline Job
resources. There is no per-user ALS endpoint in this change.

**RUNTIME NOT VERIFIED:** no image was built or loaded, and no Minikube profile,
namespace, Pod, Service, PVC, Secret, Mongo user, NetworkPolicy, or application
request was created or tested for this change. A rendered manifest is not an
executed Kubernetes demonstration.

## Planned implementation phases

- **B1 — static foundation:** serving manifests, identity contract, image
  packaging target, version lock, runbook, and pure/static checks.
- **B2 — serving validation:** complete the version lock, confirm host
  capacity, create a dedicated profile and namespace, build and load immutable
  images, provision private credentials, bootstrap MongoDB and indexes, then
  verify API/UI behavior and isolation. Mongo credential bootstrap and all
  service behavior remain unverified.
- **B3 — bounded Lambda integration:** add isolated HDFS, Kafka, controlled
  Spark batch and stream workloads, ALS/graph and Hybrid publication, and
  bounded fixture receipts. This requires separate review and application
  dependency decisions.
- **B4 — acceptance:** prove replay, late-data behavior, checkpoint recovery,
  API/UI acceptance, network-policy enforcement, evidence capture, and scoped
  teardown in fresh runs.

The per-user ALS recommendation endpoint and dashboard view remain a separate
application review and are not part of B1 or B2.

## Run identity and ownership

Generate a fresh 12-character lowercase hexadecimal run ID before rendering.
The deterministic profile and namespace are both
`it4931-k8s-<run-id>`. Object names stay stable inside that unique namespace so
the existing UI can continue to call `http://module4d-api:8000`. The owner,
run-ID, and manager labels are attached by the private render overlay. The
renderer refuses to overwrite an existing output directory and never creates
credentials or contacts a Kubernetes context. B1 can validate the ID's shape,
but only a live B2 preflight can establish that the profile and namespace are
unused; it must reject collisions before creating anything.
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

After a real namespace is created, a future execution receipt must record the
exact Minikube profile and Kubernetes context, namespace UID, direct resource
inventory and UIDs, owner labels, PVC identity and UID, bound PV identity, and
the resources created by controllers. Names alone do not prove ownership.
Future destructive operations must reject missing, incomplete, or mismatched
receipts. B1 implements pure validation only; it has no `kubectl apply`,
namespace deletion, profile deletion, or cleanup command.

## Serving boundary and credentials

Only ClusterIP Services are defined. MongoDB uses `module4d-mongo:27017`,
FastAPI uses `module4d-api:8000`, and Streamlit calls the exact API hostname
`http://module4d-api:8000` required by the current UI client. The UI receives no
MongoDB credential or database URI. No Ingress, NodePort, LoadBalancer,
hostNetwork, hostPath, Docker socket, or existing Compose volume is used.

The MongoDB StatefulSet requires the externally provisioned Secret
`module4d-mongo-admin` with `username` and `password` keys. Its root credential
is used only to initialize and administer this isolated MongoDB instance. The
API requires `module4d-api-readonly` with a single `mongodb-uri` key for a
database-scoped account granted MongoDB's `read` role. A future B3 publisher
must use a distinct `module4d-mongo-publisher` credential with only the write
rights its collections require. B1 commits no Kubernetes Secret object and no
credential value. Secret creation and Mongo account bootstrap are B2 work.
The API URI must authenticate against the generated `module4d_demo_<run-id>`
database (`authSource` set to that same database); URL-encode credentials when
they are not generated from URI-safe characters. Its service host is
`module4d-mongo:27017`.
The minimum publisher role remains unresolved: B3 must check the actual
publication operations and grant only the database-scoped privileges they
require. Root credentials must not be reused by the publisher or API.

The API expects a read account with collection reads and `listIndexes` access.
At readiness it performs a read-only authenticated ping and checks the required
trend, batch-ranking, and unique serving indexes. It does not create indexes.
The exact names are `idx_source_namespace_window_end_desc`,
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
NetworkPolicy-capable CNI and has not been tested. Minikube is planned with
Calico; a future live check must verify allowed and denied connections,
including loopback-only port-forward behavior.

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

Builds must pass the full lowercase Git SHA through `MODULE4_COMMIT`; the
runner target rejects a missing or malformed SHA. The API's existing source
digest meaning is preserved by placing every file it hashes at its original
repository-relative path. The runner inherits current `requirements.txt` and
`requirements-dev.txt` resolution through the Spark stage. The API and UI
manifests pin their declared packages, but the complete resolved
Spark-derived Python runtime and UI transitive closure are not yet locked.
MongoDB's pinned official 7.0.16 image uses UID 999, which is the StatefulSet's
non-root account ([image metadata](https://hub.docker.com/layers/library/mongo/7.0.16-jammy/)).
Later acceptance must capture the final OCI image digests and use those digests
in the rendered workload configuration; the `:b1` tags are local staging
names, not immutable acceptance identities.
Because `.git` is excluded from the image context, builds must come from a
clean checkout and the supplied SHA must be recorded externally; inside the
image the revision is available, while the dirty-state value remains unknown.

After the lock is complete, B2 will build the exact manifest image names with
the full source SHA and load those images into that run's dedicated profile.
These are planned commands and were not run for B1:

```bash
docker build -f Dockerfile --target k8s-runner \
  --build-arg MODULE4_COMMIT=<full-lowercase-git-sha> \
  --tag instacart-module4d-k8s-api:b1 .
docker build -f Dockerfile --target ui \
  --tag instacart-module4d-k8s-ui:b1 .
minikube image load -p <it4931-k8s-run-id> instacart-module4d-k8s-api:b1
minikube image load -p <it4931-k8s-run-id> instacart-module4d-k8s-ui:b1
```

The B1 version lock is incomplete, so these commands do not authorize a
cluster run. An accepted deployment must render the actual image digests and
verify the loaded image identities before scheduling Pods.

`deploy/k8s/versions.lock.yaml` records Minikube v1.39.0, Kubernetes v1.37.1,
and Calico v3.33.0 as verified compatible release versions. Calico 3.33
documents testing on Kubernetes 1.35–1.37. The repository's Spark and Mongo
base image digests are retained from the locked Dockerfile and Compose file.
The lock remains **incomplete**, and the pure preflight contract rejects it
until the host architecture/capacity, Minikube checksum and Docker Engine,
Calico and Kubernetes image digests, app-image digests, dependency closure, and
default StorageClass are recorded. No executable deployment preflight or
cluster operation is included in B1.

Sources: [Minikube v1.39.0 release](https://github.com/kubernetes/minikube/releases/tag/v1.39.0),
[Kubernetes 1.37 release status](https://kubernetes.io/releases/1.37/), and
[Calico 3.33 system requirements](https://docs.tigera.io/calico/v3.33/getting-started/kubernetes/requirements).
The Streamlit probe path follows the [official Docker deployment guide](https://docs.streamlit.io/deploy/tutorials/docker#health-check).
Profile-specific image loading follows the [Minikube image-loading guide](https://minikube.sigs.k8s.io/docs/handbook/pushing/).

## Verification and remaining runtime checks

**STATICALLY VALIDATED** checks for this change are limited to Python unit
tests, Python/YAML syntax and Kustomize offline rendering, manifest and
Dockerfile inspection, secret-pattern scanning, Markdown link checks, and
`git diff --check`. Each executed result is recorded in the implementation PR.

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
