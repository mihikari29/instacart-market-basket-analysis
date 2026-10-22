"""Bootstrap the one run-owned HDFS boundary for non-root Spark writers."""

from .ownership import RunIdentity
from .runtime import kubectl


PIPELINE_HDFS_USER = "k8spipeline"


def bootstrap_hdfs(executor, identity: RunIdentity, *, execute: bool = False) -> dict:
    """Create a fresh run directory, without adopting or changing an existing tree."""
    if execute is not True or not isinstance(identity, RunIdentity):
        raise ValueError("HDFS bootstrap requires an explicit validated run and execute=True")
    root = f"/instacart/{identity.run_id}"
    prefix = kubectl(identity, "--namespace", identity.namespace, "exec", "statefulset/hdfs-namenode", "--", "env", "HADOOP_OPTS=-Dfs.defaultFS=hdfs://localhost:8020")

    def checked(arguments, timeout=60):
        result = executor.run(prefix + arguments, timeout=timeout)
        if result.returncode != 0:
            raise RuntimeError("run-scoped HDFS bootstrap failed; resources retained")

    checked(["hdfs", "dfsadmin", "-safemode", "wait"], timeout=300)
    # The parent may be shared within this dedicated profile. Only the fresh
    # child is assigned to the writer; mkdir without -p refuses an existing run.
    checked(["hdfs", "dfs", "-mkdir", "-p", "/instacart"])
    checked(["hdfs", "dfs", "-mkdir", root])
    checked(["hdfs", "dfs", "-chown", PIPELINE_HDFS_USER, root])
    checked(["hdfs", "dfs", "-chmod", "750", root])
    return {"root": root, "owner": PIPELINE_HDFS_USER, "permission": "750"}
