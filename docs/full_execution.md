# Reproduce full-data Module 2 execution

The repository now includes a GitHub Actions workflow that downloads the public
Instacart mirror, validates all six source counts, regenerates the corrected
Module 1 feed, builds the pinned Spark image, runs the tests, stages HDFS and runs
all Module 2 commands. It uses real Docker containers and standalone Spark on a
Linux runner; the Windows development machine does not need Docker installed.

Workflow: `.github/workflows/module2-full.yml`. Source changes on
`module2-spark-batch-layer` trigger a run. Existing runs can be rerun from the
Actions page. Runtime data is temporary; compact receipts, analytics, benchmark
plans and configuration are retained as the `module2-full-evidence` artifact for
14 days. Raw data and giant event logs are not uploaded or committed.

## Source provenance

Public mirror: [psparks/instacart-market-basket-analysis](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis),
version 1. Kaggle's dataset listing labels the mirror CC0/Public Domain. Download
uses the public API without credentials; HTTP failure aborts rather than invoking
an authentication workaround. Original dataset attribution: *The Instacart Online
Grocery Shopping Dataset 2017*.

Observed archive SHA-256:
`c347cd7fa301c068ed391c7c77620f0e26e18f65d04cb907b6389c670db03038`.
The acquisition script records individual file hashes and checks the following
counts before the source is accepted:

| File | Rows |
|---|---:|
| orders.csv | 3,421,083 |
| order_products__prior.csv | 32,434,489 |
| order_products__train.csv | 1,384,617 |
| products.csv | 49,688 |
| aisles.csv | 134 |
| departments.csv | 21 |

The timestamp generator uses seed 42, 13-week scatter and 2,000 users per batch
in this workflow. Batch size changes the seeded synthetic calendar, so it is
recorded in the feed manifest. It does **not** limit the dataset: all 206,209
users and 33,819,106 product events must be present.

## Equivalent local commands

Run in a fresh checkout with Docker Linux containers and enough free disk/RAM:

```bash
python -m pip install -r requirements.txt
python scripts/fetch_instacart.py
python src/clean.py --validate
python src/generate.py --scenario default --scatter-weeks 13 --batch-users 2000 --out-dir data/synthesized/scatter_3m
docker compose up -d --wait namenode datanode
docker compose exec -T namenode hdfs dfsadmin -safemode wait
python src/stage_hdfs.py
python scripts/module2.py all --shuffle-partitions 32 --source-events data/synthesized/scatter_3m/events.parquet
docker compose run --rm --no-deps --entrypoint python3 module2 scripts/check_full_run.py
```

The download command refuses to overwrite existing source files. The final check
requires full row counts, a standalone Spark master, HDFS input paths, equal
benchmark results and MongoDB counts matching the generated aggregate Parquet.

## Defects found by real-data execution

The first full-source run reproduced a NumPy 2 integer overflow in the existing
generator: an int16 hour column was multiplied by 3,600,000 before widening.
Timestamp arithmetic now widens to int64 first; hash-mode order IDs also widen
before multiplication. A regression checks late-hour timestamps and a large
order ID using the actual narrow input types. The source cleaning run reported
369,323 recovered capped gaps and zero day-of-week mismatches.

Full execution is accepted only when the workflow and `check_full_run.py` pass.
Final observed results are recorded separately with their workflow URL and SHA.


Accepted run: [36162730659](https://github.com/mihikari29/instacart-market-basket-analysis/actions/runs/36162730659), all steps successful.
See [full measured results](evidence/full/README.md). No Docker installation
on the Windows development machine was required for this hosted run.
