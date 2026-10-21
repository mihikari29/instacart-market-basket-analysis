"""Small Kafka administration helpers for deterministic Module 3 scenarios."""

from __future__ import annotations

import json
import re
import time
from typing import Any


def safe_topic_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]", "-", value).strip(".-")
    return cleaned[:249]


def create_topic(bootstrap_servers: str, topic: str, partitions: int, reset: bool = False) -> dict:
    """Create a topic and return observed broker metadata.

    ``reset`` is intended only for scenario-owned temporary topics.
    """
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=10000)
    try:
        existing = set(admin.list_topics())
        if reset and topic in existing:
            try:
                admin.delete_topics([topic], timeout_ms=10000)
            except UnknownTopicOrPartitionError:
                pass
            deadline = time.monotonic() + 20
            while topic in set(admin.list_topics()) and time.monotonic() < deadline:
                time.sleep(0.25)
        try:
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)],
                timeout_ms=10000,
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()

    deadline = time.monotonic() + 20
    metadata = None
    last_error = None
    while time.monotonic() < deadline:
        try:
            metadata = topic_metadata(bootstrap_servers, topic)
            if metadata["partitions"] == partitions:
                break
        except RuntimeError as exc:
            last_error = exc
        time.sleep(0.25)
    if metadata is None:
        raise RuntimeError(f"Topic {topic} metadata did not become available") from last_error
    if metadata["partitions"] != partitions:
        raise RuntimeError(
            f"Topic {topic} has {metadata['partitions']} partitions; expected {partitions}"
        )
    return metadata


def topic_metadata(bootstrap_servers: str, topic: str) -> dict:
    """Return actual partition metadata and current end offsets."""
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, request_timeout_ms=10000)
    try:
        described = admin.describe_topics([topic])
    finally:
        admin.close()
    found = next(
        (item for item in described if item.get("name", item.get("topic")) == topic),
        None,
    )
    if found is None or found.get("error_code", 0) != 0:
        raise RuntimeError(f"Kafka topic not available: {topic}: {found}")

    indexes = sorted(
        part.get("partition_index", part.get("partition")) for part in found["partitions"]
    )
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, request_timeout_ms=10000)
    try:
        topic_partitions = [TopicPartition(topic, index) for index in indexes]
        raw_offsets = consumer.end_offsets(topic_partitions)
    finally:
        consumer.close()
    end_offsets = {str(tp.partition): int(offset) for tp, offset in raw_offsets.items()}
    return {
        "topic": topic,
        "partitions": len(indexes),
        "partition_ids": indexes,
        "end_offsets": end_offsets,
        "total_records": sum(end_offsets.values()),
    }


def produce_json_events(bootstrap_servers: str, topic: str, events: list[dict]) -> int:
    """Synchronously publish a small deterministic event set."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
    )
    try:
        for event in events:
            producer.send(topic, key=event["user_id"], value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close(timeout=10)
    return len(events)


def validate_kafka(config: Any) -> dict:
    """Connect, list partitions, return topic metadata. Does NOT consume data."""
    from kafka import KafkaAdminClient
    from ..common.runtime import redact_sensitive_text

    info = {
        "bootstrap_servers": config.bootstrap_servers,
        "topic": config.topic,
        "ok": False,
    }
    try:
        admin = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers, request_timeout_ms=5000)
        try:
            partitions = admin.describe_topics([config.topic])
            topic_info = next(
                (
                    topic
                    for topic in partitions
                    if topic.get("name", topic.get("topic")) == config.topic
                ),
                None,
            )
            if topic_info is None:
                info["error"] = "topic not found"
                return info
            partition_count = len(topic_info.get("partitions", []))
            info["partitions"] = partition_count
            info["ok"] = True
            info["replication_factor"] = (
                len(
                    topic_info["partitions"][0].get(
                        "replica_nodes",
                        topic_info["partitions"][0].get("replicas", []),
                    )
                )
                if partition_count
                else 0
            )
        finally:
            admin.close()
    except Exception as exc:
        info["error"] = redact_sensitive_text(f"{type(exc).__name__}: {exc}")
    return info


def validate_mongo(config: Any) -> dict:
    """Ping Mongo, list existing collections to confirm reachable but untouched."""
    from ..common.runtime import redact_sensitive_text, redact_uri

    info = {"mongodb_uri": redact_uri(config.mongodb_uri), "database": config.mongo_database, "ok": False}
    try:
        from pymongo import MongoClient

        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=5000) as client:
            client.admin.command("ping")
            info["ok"] = True
            db = client[config.mongo_database]
            info["existing_collections"] = sorted(db.list_collection_names())
    except Exception as exc:
        info["error"] = redact_sensitive_text(f"{type(exc).__name__}: {exc}")
    return info
