# Module 2: historical Spark processing

Status: implemented; local fixture evidence is in `docs/evidence/`. Full Instacart
execution and Docker/HDFS smoke execution have not been performed on this host.
Fixture timings do not establish 32-million-row performance.

## Architecture

Module 1 stages normalized Parquet and synthetic interactions in HDFS. Module 2
validates the handoff, computes historical analytics and features, publishes batch
aggregates to MongoDB, and compares physical execution strategies. Module 3 owns
streaming; Module 4 owns ALS, graph processing and recommendations. This follows
Proposal §§12–16, 32 and 42, including caching and user-feature outputs.

Purchase metrics, user features and temporal trends use **prior only**. Train
facts are validated and counted but remain evaluation ground truth. All-orders
counts and hour/day distributions include prior, train and test orders. Synthetic
calendar trends describe replay simulations, not real calendar seasonality.

## Runtime and canonical execution

Requirements: Docker Engine/Desktop with Linux containers and Compose v2. The
image pins Apache Spark 3.5.5, Java 17 and its registry digest. Existing Hadoop
3.2.1 NameNode/DataNode and Kafka services are retained. MongoDB 7.0.16 supplies
the proposal's batch collections. No host Java/Spark installation is needed.

```bash
python -m pip install -r requirements.txt
# Original Instacart CSVs must already be present in data/raw.
python src/clean.py --validate
python src/generate.py --scenario default --scatter-weeks 13 --out-dir data/synthesized/scatter_3m
docker compose up -d namenode datanode
python src/stage_hdfs.py
python scripts/module2.py all
```

`python scripts/module2.py all` builds the Spark image, starts dependencies and
launches a driver after healthchecks. The worker offers two cores and 2 GiB;
the application requests two cores and a 1 GiB executor. Allow extra RAM for the
driver and other services. The master UI binds to localhost:8080; Spark RPC stays
inside the network. The driver's module2 alias is supplied by --use-aliases.
All Spark nodes mount the repository at /workspace, including local result paths.
Run one Module 2 job at a time. Individual subcommands use the same validation:

```bash
python scripts/module2.py validate
python scripts/module2.py stats
python scripts/module2.py benchmark-joins
python scripts/module2.py benchmark-partitions
python scripts/module2.py benchmark-cache
```

For an installed Spark 3.5.5 / Java 17 local environment:

```bash
python -m pip install -r requirements-dev.txt
python -m src.module2.fixtures --root data/fixture
python -m src.module2.cli all --root file:///ABSOLUTE/PATH/TO/data/fixture --skip-serving --min-support 1
python -m pytest -q
```

Fixture creation refuses to overwrite. --skip-serving is explicit and recorded.
After building the image, container fixture tests can run with:

```bash
docker compose run --rm --no-deps --entrypoint python3 module2 -m pytest -q
```

Tests use local Spark and Parquet, not HDFS. Docker is the supported team runtime;
standalone Windows Spark also requires native Hadoop helpers. Delivery-only
portable tools are not repository dependencies.

## Input contracts

Default root: hdfs://namenode:8020/instacart. Override with --root or MODULE2_ROOT,
including file:/// fixtures. Paths below are relative to curated/.

| Table | Path | Required fields |
|---|---|---|
| orders | orders | order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day |
| prior | order_products_prior | order_id, product_id, add_to_cart_order, reordered |
| train | order_products_train | same as prior |
| products | dimensions/products | product_id, product_name, aisle_id, department_id |
| aisles | dimensions/aisles | aisle_id, aisle |
| departments | dimensions/departments | department_id, department |
| interactions | interactions | event_id, order_id, user_id, product_id, add_to_cart_order, reordered, aisle_id, department_id, event_time_epoch_ms, synthetic_date, synthetic_year/month/day |

Validation rejects missing columns, critical nulls, empty tables, duplicate
dimension/order/event keys, duplicate fact order-product keys, invalid order time
domains, orphan relationships, wrong eval-set membership and inconsistent event
dimensions. Full fact/event multisets are compared on order_id, product_id,
add_to_cart_order and reordered. Declared limit_users feeds are checked against
the generator's sorted-user subset. Facts remain distributed throughout.

curated/_sources.json records cleaned Parquet counts and SHA-256s at staging.
interactions/_handoff.json records source SHA-256, source/local counts, file and
partition counts, and feed manifest. Spark requires source = local = staged
counts and checks partition fields against synthetic_date and UTC event epoch.
--source-events PATH also compares the current source hash against its receipt.
Receipts establish provenance, not attestation of an untrusted upstream.

Reference counts from README: 3,421,083 orders; 32,434,489 prior; 1,384,617 train;
49,688 products; 134 aisles; 21 departments; 206,209 users; 33,819,106 full events.
Runtime expectations come from source receipts, not duplicated constants.

## Module 1 repairs and migration

The old writer called write_dataset once per row group, reusing default
part-0.parquet names. An executable regression writes six events in three
overlapping-date row groups and retains only two. The replacement retains six.

partition_events.py supplies 65,536-row batches to **one** dataset writer. Limits:
512 open files, minimum 1,024-row groups, maximum 65,536-row groups and 1,048,576-row
files. Standard 456-day feeds fit the writer budget. Larger partition domains can
evict writers and produce extra files without name reuse. No whole-feed pandas
conversion occurs. ISO dates and source/local counts are checked before upload.

Staging uploads a unique sibling subtree, then replaces only curated/interactions.
Obsolete partitions disappear. Upload failure preserves the existing target.
Replacement is not transactional: a crash between deletion and rename leaves
the pending copy for recovery. Do not run concurrent stagers/readers during
publication. --interactions-only does not change facts or dimensions. Failed
attempts can leave pending sibling directories; inspect the identified attempt
before removing it.

Other repairs directly relevant to the handoff:

- Compute cap-30 masks after sorting so updates follow the correct orders.
- Restore documented order_number in events and record batch_users in manifests.
- Reject a bodyless initial WebHDFS 201 instead of falsely reporting an upload.
- Propagate HDFS tree errors instead of silently suppressing them.

Regenerate cleaned data and feeds from raw CSVs after the cap-30 repair, then
restage. Old staging without receipts fails validation. An old file-upload report
does not establish complete HDFS counts.

## Analytics and outputs

Each invocation creates results/module2/UTC_timestamp_id/. summary.json records
completion/failure, source identities, code digest, Git revision/dirty status,
runtime versions and Spark configuration. The digest identifies dirty source
runs independently of HEAD. Partial runs retain failed status.

analytics.json includes counts, prior reorder rate and average basket size,
orders-per-user min/mean/max and approximate quantiles, top purchased products,
supported top reorder rates, departments, order-hour/day distributions and a
department-hour pivot. Default minimum support is 100; --min-support is recorded.
SparkSQL computes purchase and basket summaries. A calendar-day range window
computes rolling seven-day department volume on observed dates; missing dates
contribute zero but are not emitted. Only small final summaries are collected.
Trends and aggregate tables are also written as Parquet.

features/user_features under the configured root is replaced by a prior-only
snapshot: user_id, order_count, purchase_count, unique_products, avg_basket_size,
reorder_rate. This is the Module 4 handoff.

MongoDB receives batch_product_metrics and department_metrics with natural IDs,
run_id and last_batch_time. Bounded 1,000-document batches write temporary
collections; rename replaces each snapshot and removes stale keys. The two
renames are not jointly transactional; consumers should check run_id consistency.
Failure can leave a temporary collection. MONGODB_URI is never copied into reports.
Unrelated collections are untouched.

## Controlled experiments

### Joins

Both arms use the same prior → products → departments departmental aggregate.
AQE and automatic broadcast are disabled for both. MERGE hints force two
SortMergeJoin operators; explicit dimension broadcasts force two BroadcastHashJoin
operators. Both pre-action and executed plans must contain the requested joins
without competing strategies. One warm-up per arm precedes at least three
measured trials. Order alternates, Spark caches are cleared, and resources/shuffle
partitions match. The timer covers collect, excluding plan inspection. OS/HDFS
page caches are not flushed, so this is a warm-system experiment. Sorted result
SHA-256s must match across strategies/trials. Report individual values and median.

### Partition pruning

The earliest staged date starts a seven-day window. Three arms rotate order:
full scan; synthetic_date BETWEEN; direct year/month/day Hive predicates.
Each uses warm-ups/repetitions. Counting and summing reordered/product_id forces
payload reads beyond metadata-only counts. Plans must show empty partition
filters for the first two and year/month/day filters for the third. The two date
filters must have equal checksums. Full and restricted scopes differ; their
timings alone are not an equivalent-query speedup. Parquet pushdown can reduce
bytes without directory pruning, so observed metrics determine scan reduction.
A short feed can have no reduction when its entire span fits seven days.

### Caching

Compare recomputing the department aggregate with reusing its MEMORY_AND_DISK
snapshot. Build cost is separate from reuse timing; plans must show
InMemoryTableScan and outputs must match. Each pair starts with cleared caches
and unpersists synchronously. This models a compact reusable aggregate, not
caching all facts. Include build cost when assessing whether caching pays off.

## Task metrics

Uncompressed, nonrolling Spark event logs are parsed after Spark stops. Job-group
IDs attribute stages/tasks to trials. Successful tasks in the final successful
stage attempt contribute input bytes/records, local+remote shuffle reads, shuffle
writes and executor runtime. Speculative duplicates are deduplicated by task
index. JobStart also lists cached dependencies that never execute; these count
as skipped stages. Only StageSubmitted attributes actual work to a trial. Failed
and discarded task attempts are reported separately. These are task I/O metrics,
not directory sizes. No file-count claim
is inferred from DataFrame.inputFiles(). Plans/metrics are embedded in summary.json.
Huge logs, datasets and Parquet outputs are ignored by Git.

## Verification, limitations and troubleshooting

```bash
python -m pytest -q
python -m ruff check .
python -m compileall -q src scripts tests
docker compose config --quiet
git diff --check
```

See docs/evidence for executed checks and measurements. Tests cover row-group
collisions, stale partitions, upload failures, cap-30 sorting, contracts,
analytics, joins, pruning and event-log attribution.

This host has no Docker service or WSL distribution. A portable Compose binary
validated configuration, but image build, worker registration, HDFS connectivity
and the canonical Docker command remain unexecuted. Local Spark does not replace
that gate. Full data and Kaggle credentials were absent. Full-scale correctness
and performance remain pending; fixture success does not establish full completion.

- Missing receipts: rerun corrected staging; do not invent counts.
- HDFS: check service logs/free disk and wait for safemode to end; Spark uses
  container hostnames rather than localhost.
- Driver connections: use the canonical runner and its network alias.
- Plan failures: inspect AQE/hints and retain the failed run; keep assertions.
- MongoDB failure fails the run; skip only with explicit --skip-serving.
- A passed summary means its requested command completed on the recorded input,
  not that full-data or HDFS validation occurred.

## References

- [Arrow dataset writer](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html)
- [Spark SQL tuning](https://spark.apache.org/docs/3.5.5/sql-performance-tuning.html)
- [Spark monitoring](https://spark.apache.org/docs/3.5.5/monitoring.html)
- [Apache Spark Docker definitions](https://github.com/apache/spark-docker)
