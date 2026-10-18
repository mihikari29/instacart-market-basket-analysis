# Module 2 execution comparisons

`src.module2.executor_benchmark` compares the same source-backed product-count
aggregation under Spark `local[1]` and `local[2]`. These are local task-thread
configurations on one host. The harness does not claim Kubernetes pod or native
Spark executor scaling; the Kubernetes pipeline remains Spark local mode.

Print the command plan without starting Spark or reading HDFS:

```bash
python3 -m src.module2.executor_benchmark plan \
  --root hdfs://namenode:8020/instacart \
  --output results/module2-executor \
  --run-id review-run
```

Execute the comparison only in the later authorized runtime session:

```bash
python3 -m src.module2.executor_benchmark run \
  --root hdfs://namenode:8020/instacart \
  --output results/module2-executor \
  --run-id review-run \
  --repetitions 3 --warmups 1
```

Each arm runs in its own Spark process and writes a `summary.json`. The parent
report requires identical source receipt hashes, interaction row counts, and
aggregate checksums. Per-arm evidence includes the actual Spark master,
`spark_conf`, source and code identities, measured input bytes and rows,
repeated wall times, physical plans, and Spark event-log task metrics including
shuffle bytes. No benchmark results are recorded in this repository during
offline implementation.

The comparison does not control host scheduling or operating-system/HDFS
caches, so timing is descriptive for the recorded environment. It is not a
distributed executor scaling result.

The Kubernetes B3 Module 2 Job runs this same comparison after its batch
publication. The run-specific step receipt includes the source identity,
input rows/bytes, actual `local[1]` and `local[2]` masters, repeated runtime,
physical plans, and Spark task/shuffle metrics. B4 rejects acceptance if either
arm is missing or the arms do not reconcile to the same source and output.
Those values are generated only by a future authorized runtime run; no result
is included here.

## Existing standalone master: Proposal §15.6

For an actual executor-layout experiment, use an existing Spark standalone
master with workers that can allocate the requested memory and cores. Every
worker must have the same Python runtime/code and reach the curated HDFS root;
the driver must be reachable from workers. The harness creates no cluster.
Keep the automatic B3 local-mode evidence distinct from this experiment.

```bash
python3 -m src.module2.executor_benchmark plan \
  --root hdfs://namenode:8020/instacart \
  --run-id standalone-review --execution-mode standalone \
  --master spark://spark-master:7077 \
  --executor-configs '[{"executor_memory":"1g","executor_cores":1,"total_cores":2},{"executor_memory":"1g","executor_cores":2,"total_cores":2}]'
```

Review the plan, then change `plan` to `run` during the authorized runtime
session. This example compares two one-core executors with one two-core
executor, keeping the total cores fixed. Memory and total cores can also be
varied explicitly. Use two through eight distinct configurations; each needs
at least one warmup and three measured repetitions.

The harness verifies the requested standalone master and effective Spark
configuration and records actual active executor IDs, host/ports, core counts
and `maxMemory` from the driver REST API after the workload. Driver-only,
missing or mismatched core allocation fails the arm. `maxMemory` is Spark's
reported storage memory, not the requested executor heap. The driver UI REST
endpoint must be available. Source receipt, row count and output checksum must
match across arms. Results describe these layouts on the recorded cluster;
no speedup or Kubernetes executor-scaling claim is inferred. No standalone
experiment has been executed during offline implementation.
