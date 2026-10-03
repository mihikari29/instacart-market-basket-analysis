"""Kafka connectivity validation (the `validate` subcommand, no streaming)."""

from .config import Config


def validate_kafka(config: Config) -> dict:
    """Connect, list partitions, return topic metadata. Does NOT consume data."""
    from kafka import KafkaAdminClient

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
        info["error"] = f"{type(exc).__name__}: {exc}"
    return info


def validate_mongo(config: Config) -> dict:
    """Ping Mongo, list existing collections to confirm reachable but untouched."""
    info = {"mongodb_uri": config.mongodb_uri, "database": config.mongo_database, "ok": False}
    try:
        from pymongo import MongoClient

        with MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=5000) as client:
            client.admin.command("ping")
            info["ok"] = True
            db = client[config.mongo_database]
            info["existing_collections"] = sorted(db.list_collection_names())
    except Exception as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
    return info
