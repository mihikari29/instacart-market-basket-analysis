# Module 4A–4B acceptance — 09/10/2026 (Asia/Bangkok)

Initial plan checklist: 17/21 gates checked (fixture/integration scope). Four
remaining gates were host-wrapper completion, stable-source replay, observed CI,
and a full-data A/B serving run.

Verified so far:

- [full-input-20261009](../full-input-20261009/summary.json): host wrapper completed full-input validation. Counts:
  3,421,083 orders, 32,434,489 prior facts, 1,384,617 train facts, 49,688 products,
  206,209 prior users and 131,209 official-train users. Staged file hashes match
  Module 1 receipts. Validation took 190.186 seconds.
- [GitHub runtime workflow](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/37798558046)
  passed for commit `11ac21cc134f1de54bb16bbae6f593a0ebf57038`. Runtime, repository
  tests, staged same-version replay, cross-module flow and static checks succeeded.
  [Run receipt](github-run.json) and [job/step receipt](github-jobs.json) were fetched
  read-only from the GitHub API on 09/10. The newly added manual full-data workflow
  has not been dispatched on GitHub.
- [Host contracts](host-contract-tests.txt): 12 passed, including acceptance checks
  that reject incomplete, compute-only, changed-version and changed-snapshot runs.
- [Fresh repository regression](regression-tests.txt): **91 passed, zero skipped**,
  five PySpark warnings, exit code 0. This suite includes the host contracts;
  counts must not be added together. [Fresh standalone runtime](runtime-smoke.txt)
  also passed with exit code 0 after the disk-location change.
- [Runtime override](runtime.override.yml) redirects worker/driver shuffle, spill
  and worker logs to the D: workspace. C: had approximately 1.6 GB free; the first
  attempt was interrupted before ALS fit/publication to avoid filling it. Its
  [interruption receipt](interrupted-first-attempt.json) preserves the original
  manifest. Curated data and existing MongoDB snapshots were retained.
- Windows later expanded its pagefile and C: fell below 300 MB free while the
  WSL root filesystem remained at 7.7 GB used. MongoDB storage was copied after a
  clean service stop into an 8 GiB ext4 image on D:, mounted at
  `/mnt/module4-storage-20261009`, with a 0.25 GiB WiredTiger cache. The original
  MongoDB volume remains intact. Spark/HDFS were not restarted during this move.
  See the [storage receipt](storage-receipt.json) and
  [preparation command](prepare-mongo-storage.sh).

Full-data pipeline and replay are being run sequentially by the local script
`results/module4/acceptance-20261009/run-final-acceptance.sh`. Each step records its exact
command, output and exit code. Large event logs and temporary data remain under
`results/`; compact acceptance artifacts will be copied here after completion.

The ALS search budget is one baseline configuration (`rank=32`, `regParam=0.1`,
`alpha=10`, `maxIter=10`, raw strength) with 5,000 deterministic pilot users,
followed by full internal validation and final fit on all prior interactions.
This is bounded validation, not an exhaustive hyperparameter search. Official
train is reserved for final evaluation. Graph starts with the full catalog,
thresholds 2/5/10, real GraphFrames PageRank and no basket/pair sampling.

The unoptimized full-data attempt was stopped during holdout preparation before
any ALS fit or publication. [Its receipt](interrupted-unoptimized-attempt.json)
preserves the original manifest; [command/output](unoptimized-pipeline.txt)
records exit 137 from the deliberate container stop. The holdout now selects last
orders from deduplicated orders, caches owned intermediate DataFrames, reuses
counts and releases those caches on success/failure. The runner persists enriched
prior facts on disk across ALS/graph, and allows automatic broadcast for small
dimensions up to 10 MiB. Dataset, holdout and evaluation semantics are unchanged.
New fixture assertions check exact baskets/counts and cache cleanup, including
official-train leakage failure. Fresh regression/full-data runs use the new
source digest and `*-optimized` run IDs/model/graph versions.

HDFS storage was subsequently cloned, after clean service stops, into the same
D: ext4 filesystem to leave room for model checkpoints and replay. Original
NameNode/DataNode volumes remain intact. The job that was reading HDFS during
the move [failed during input validation](../full-ab-20261009-optimized/summary-compact.json);
its failed artifacts are retained. The restored services are healthy, safe mode
is off, and curated directories are present. The final run IDs are
`full-ab-20261009-final` and `full-ab-20261009-final-replay`, with the same frozen
optimized source/model/graph versions. The 91-test regression remains applicable;
a fresh standalone smoke checks the restored HDFS/Mongo runtime before the run.
See [HDFS storage preparation](prepare-hdfs-storage.sh).

After a WSL restart, mount the preserved MongoDB image before starting the local
runtime override, so the services read the accepted snapshot:

```bash
sudo bash docs/evidence/module4/acceptance-20261009/mount-runtime-storage.sh
export PATH=/opt/module4-runtime-20261008/docker:$PATH
export DOCKER_HOST=unix:///var/run/module4-docker.sock
export DOCKER_CONFIG=/opt/module4-runtime-20261008/config
export COMPOSE_FILE=compose.yaml:docs/evidence/module4/acceptance-20261009/runtime.override.yml
# The isolated Docker daemon must also be running with its original data root.
python3 scripts/module4.py --help
```

The ext4 image and large Spark artifacts stay under ignored `results/` paths.
Keep that image to retain the published full-data MongoDB snapshots.
