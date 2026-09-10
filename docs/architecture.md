# Architecture

## Purpose

The system will turn a historical e-commerce behavior dataset into a controlled
event stream, process that stream in near real time, retain an analytical event
history, serve low-latency aggregates, and support historical batch analysis.

Phase 0 defines boundaries only. It does not define infrastructure services or
implement application behavior.

## Target data flow

```text
External historical CSV dataset
              |
              v
Python Event Replay Generator
              |
              v
         Apache Kafka
              |
              v
Apache Spark Structured Streaming
       |                       |
       v                       v
HDFS / partitioned Parquet   Cassandra
       |                       |
       v                       v
Spark Batch analytics       Streamlit
```

## Component responsibilities

### Event replay generator

The Python producer will read the external source incrementally and publish
schema-controlled events at a configurable rate. It will not require the source
dataset to be copied into the repository.

### Apache Kafka

Kafka will provide the ingestion boundary and replayable event transport. Local
development will use container-managed brokers and storage.

### Apache Spark Structured Streaming

The streaming job will parse and validate events, apply event-time processing,
and write to both durable analytical storage and query-oriented serving tables.
Checkpoint data will be runtime state and will never be committed.

### HDFS and Parquet

HDFS will hold the durable analytical event history in Parquet format. Data will
be partitioned according to validated query and retention needs. To conserve
local disk space, the future local-development HDFS configuration will use a
replication factor of **1**. Production-style replication is outside the local
development scope.

### Apache Cassandra

Cassandra will store query-oriented aggregates designed around dashboard access
patterns. It is not intended to replace the immutable analytical history in
HDFS.

### Spark Batch

Batch jobs will read historical Parquet data from HDFS for recomputation and
analytics that do not belong in the real-time path.

### Streamlit

The dashboard will present real-time serving metrics from Cassandra and, where
appropriate, outputs produced by batch analytics. It will not scan raw CSV data
interactively.

## Deployment boundary

Kafka, Spark, HDFS, and Cassandra will run as Docker Compose services. Java and
all service versions will be pinned inside those container definitions.
Python application code will remain under `src/` and can be mounted or packaged
into purpose-specific containers. The target Python runtime is 3.11 regardless
of the host WSL Python version.

The external dataset path will be supplied through `DATASET_PATH` and mounted
read-only into only the component that needs it. Service state will use named
Docker volumes so that Kafka logs, HDFS blocks, Cassandra data, checkpoints,
and generated outputs remain outside version control.

## Scope boundaries

The initial architecture does not include Kubernetes, CI/CD pipelines, or Spark
ML. These are intentionally excluded until the core streaming and batch data
paths are implemented and verified.
