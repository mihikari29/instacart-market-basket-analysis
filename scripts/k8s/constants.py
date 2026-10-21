"""Names, ports and storage sizes that the Kubernetes deployment agrees on.

The coordinator derives the profile, namespace, object names, database name,
Job names and storage budget from one run identity. Those derivations were
written out in each module, so a change to a name or a claim size had to be
repeated in five places and a mismatch failed only at runtime, deep inside a
stage.

Everything here is a pure function of the run identity, so the values can be
compared directly against the rendered manifests.
"""

from __future__ import annotations

#: Kubernetes service ports used by the pipeline and the serving layer.
HDFS_NAMENODE_PORT = 8020
HDFS_NAMENODE_HTTP_PORT = 9870
HDFS_DATANODE_PORT = 9864
HDFS_DATANODE_TRANSFER_PORT = 9866
KAFKA_BOOTSTRAP_PORT = 29092
KAFKA_CONTROLLER_PORT = 9093
MONGO_PORT = 27017
API_PORT = 8000
STREAMLIT_PORT = 8501

#: Persistent volume claims the B3 pipeline creates, with their requested size.
PIPELINE_CLAIMS: dict[str, str] = {
    "it4931-hdfs-namenode": "1Gi",
    "it4931-hdfs-datanode": "16Gi",
    "it4931-kafka-data": "2Gi",
    "it4931-pipeline-data": "16Gi",
    "it4931-pipeline-checkpoints": "2Gi",
}

#: Total storage the pipeline run is allowed to request.
PIPELINE_STORAGE_BUDGET = "39Gi"

#: Objects every namespace receives from the cluster's own controllers.
SYSTEM_NAMESPACE_OBJECTS: frozenset[tuple[str, str]] = frozenset(
    {
        ("ServiceAccount", "default"),
        ("ConfigMap", "kube-root-ca.crt"),
    }
)

#: The MongoDB database the demo run publishes into.
DEMO_DATABASE_PREFIX = "module4d_demo_"

#: The trend source namespace the pipeline publishes finalized windows under.
TREND_SOURCE_NAMESPACE = "instacart-k8s-demo-v1"


def demo_database(run_id: str) -> str:
    """Return the per-run MongoDB database name."""
    return f"{DEMO_DATABASE_PREFIX}{run_id}"


def pipeline_job_name(step: str, run_id: str) -> str:
    """Return the name of one B3 pipeline Job."""
    return f"it4931-{step}-{run_id}"


def acceptance_job_name(run_id: str) -> str:
    """Return the name of the B4 acceptance Job."""
    return f"it4931-acceptance-{run_id}"


def als_model_version(run_id: str) -> str:
    """Return the ALS model version the pipeline publishes under."""
    return f"als-k8s-{run_id}"


def graph_version(run_id: str) -> str:
    """Return the product-graph version the pipeline publishes under."""
    return f"graph-k8s-{run_id}"


def scoped_run_id(run_id: str, prefix: str) -> str:
    """Return a run-scoped identifier for one module inside the run."""
    return f"{prefix}-{run_id}"


def claim_names() -> set[str]:
    """Return the set of PVC names the pipeline is allowed to create."""
    return set(PIPELINE_CLAIMS)


def expected_claim_requests() -> dict[str, str]:
    """Return the PVC name to requested-size mapping."""
    return dict(PIPELINE_CLAIMS)
