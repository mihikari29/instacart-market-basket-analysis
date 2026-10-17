# Module 2 local execution comparison

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
