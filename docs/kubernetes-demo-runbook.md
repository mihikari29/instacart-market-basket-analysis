# Kubernetes demonstration runbook

This is the operating guide for the offline-first coordinator in
[`scripts/k8s/demo.py`](../scripts/k8s/demo.py). It records the intended future
runtime flow; the implementation campaign did not run it.

## Scope and status

The coordinator implements a complete isolated Minikube run: local checks,
dedicated-profile creation, runtime identity collection, three application
image builds, Kafka/HDFS image inspection, B2/B3 lock finalization, serving
deployment, the six-step batch/stream pipeline, B4 acceptance, and private
evidence retention. `run --execute` is the only command that performs that
workflow. It never falls back to the current Kubernetes context or Compose
services, and it never deletes resources.

The exact selected releases are Minikube **v1.39.0**, Kubernetes **v1.37.1**,
Calico **v3.33.0**, Apache Spark **3.5.5**, Kafka **3.7.0**, MongoDB **7.0.16**,
and the locked Hadoop **3.2.1** images. The Python runtime and transitive image
environments are measured during preparation and written to the private lock;
the code does not invent those identities.

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

An implementation-time local preflight on this host measured 6 physical CPU
cores, 5.271 GiB available RAM, 7.46 GiB total RAM, and 947.6 GiB free in the
WSL guest. The Windows backing disk was unknown and Minikube was absent. The
source tree was dirty during that check; rerun preflight against the final
committed source before any later execution. The measured RAM fails the B3
reserve in any case. Docker and kubectl executables were present, but this
campaign did not query the Docker Engine or Kubernetes.

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
  --input-dir /data/instacart

python3 -m scripts.k8s.demo run \
  --mode full \
  --run-id 0123456789ab \
  --input-dir /data/instacart \
  --windows-backing-disk-gib 120 \
  --execute
```

Replace the backing-disk example with a current measurement. The numeric flag
is recorded as operator-supplied evidence; the runner cannot inspect the
Windows filesystem from inside WSL.

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

Automatic resume is disabled. After a failed run, inspect the session and
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

- No image build, profile creation, workload, runtime acceptance, browser demo,
  or teardown was executed during offline implementation.
- The closing local preflight is blocked: 5.285 GiB available RAM is below the
  12 GiB B3 reserve; Minikube is not installed; WSL backing-disk capacity is
  unknown.
- WSL backing disk is operator-supplied evidence and is not independently
  inspected by the CLI.
- Automatic resume is intentionally disabled. The runner requires a new run
  identity and fresh output path; it does not resume Kafka offsets/checkpoints.
- B4 verifies NetworkPolicy behavior only at runtime. Static manifest validity
  does not establish that the chosen CNI enforces the intended flows.
- The automated B3 comparison measures local thread pools. Proposal §15.6
  executor-layout evidence needs the separate existing-standalone-master
  experiment in [the benchmark guide](module2-executor-benchmark.md). Neither
  mode implies a speedup; scheduling and caches affect timing.
- The run keeps resources after success or failure. Teardown needs a separate
  review and explicit `--execute`.

## CODE COMPLETE — READY FOR EXTERNAL ACCEPTANCE

The implementation and feasible offline checks are recorded in the
[external acceptance handoff](external-acceptance-handoff.md). Real image identity discovery, capacity validation on an eligible
host, deployment behavior, B2/B3/B4 runtime receipts, interactive demonstration,
and teardown remain pending. This status makes no claim of runtime success,
full-data acceptance, or production readiness.
