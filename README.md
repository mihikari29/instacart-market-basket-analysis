# Real-Time E-commerce Big Data Analytics

A university Big Data project for replaying historical e-commerce behavior as
events and processing them with a streaming and batch analytics platform.

## Project status

The repository is currently at **Phase 0: repository foundation**. It contains
the project structure and documentation only. Infrastructure services and
application logic have not been implemented.

## Planned architecture

```text
Historical e-commerce dataset
              |
              v
     Event Replay Generator
              |
              v
         Apache Kafka
              |
              v
Spark Structured Streaming
       |              |
       v              v
HDFS / Parquet   Apache Cassandra
       |              |
       +-------+------+
               v
      Streamlit Dashboard

Spark Batch reads historical Parquet data from HDFS.
```

The planned technology stack is Python 3.11, Apache Kafka, Apache Spark
Structured Streaming and Batch, HDFS, Parquet, Apache Cassandra, Streamlit,
and Docker Compose.

## Development environment

The intended local environment is Windows 11 with WSL2 Ubuntu, Docker Desktop
with WSL integration, and VS Code connected to WSL.

Python 3.11 is the target application runtime. Java and the versions of Kafka,
Spark, Hadoop, and Cassandra will be pinned and managed in containers rather
than installed directly in WSL.

`compose.yaml` is intentionally an empty, documented skeleton in Phase 0. It
must not be used to start services until the service definitions are added in a
later phase.

## Dataset

The source dataset is external to the repository. The expected development
path is configured in `.env.example`:

```text
/mnt/c/Users/Minh/Desktop/hust/big_data/project/dataset
```

Do not copy the full dataset into this repository. Dataset archives, CSV files,
Parquet files, generated outputs, and runtime state are excluded by
`.gitignore`. Future containers must mount the source directory read-only.

See [data/README.md](data/README.md) for the data-handling policy and
[docs/architecture.md](docs/architecture.md) for architecture boundaries and
planned data flow.

## Repository layout

```text
src/ecommerce_analytics/common     Shared schemas, configuration, and utilities
src/ecommerce_analytics/producer   Historical event replay producer
src/ecommerce_analytics/streaming  Spark Structured Streaming jobs
src/ecommerce_analytics/batch      Spark batch analytics jobs
src/ecommerce_analytics/dashboard  Streamlit dashboard
tests/unit                         Isolated unit tests
tests/integration                  Cross-service integration tests
docs                               Architecture and design documentation
data                               Data policy only; no full datasets
```

Application logic, infrastructure definitions, and dependencies for production
services will be introduced incrementally in later phases.
