# Data directory policy

The full e-commerce dataset must remain outside this Git repository.

For the current WSL development environment, its expected location is:

```text
/mnt/c/Users/Minh/Desktop/hust/big_data/project/dataset
```

Configure that location with `DATASET_PATH` in a local `.env` file created from
`.env.example`. Future containers that require the source must mount the path
read-only.

Do not place CSV files, ZIP archives, Parquet output, Spark checkpoints, HDFS
blocks, Kafka logs, Cassandra files, or other generated runtime data here.
`.gitignore` enforces this policy while retaining this document.

Small, non-sensitive synthetic fixtures may be added under `tests/fixtures` in
a later phase when tests require them. They must not be extracted copies of the
full dataset.
