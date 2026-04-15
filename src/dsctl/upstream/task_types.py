from __future__ import annotations

from typing import Final

_UPSTREAM_DEFAULT_TASK_TYPES_BY_CATEGORY: Final[dict[str, tuple[str, ...]]] = {
    "Universal": (
        "SHELL",
        "JAVA",
        "PYTHON",
        "PROCEDURE",
        "SQL",
        "SPARK",
        "FLINK",
        "GRPC",
        "HTTP",
        "MR",
        "DINKY",
        "FLINK_STREAM",
        "HIVECLI",
        "REMOTESHELL",
    ),
    "Cloud": (
        "EMR",
        "K8S",
        "DMS",
        "DATA_FACTORY",
        "ALIYUN_SERVERLESS_SPARK",
    ),
    "Logic": (
        "SUB_WORKFLOW",
        "DEPENDENT",
        "CONDITIONS",
        "SWITCH",
    ),
    "DataIntegration": (
        "SEATUNNEL",
        "DATAX",
        "SQOOP",
    ),
    "MachineLearning": (
        "JUPYTER",
        "MLFLOW",
        "OPENMLDB",
        "DVC",
        "SAGEMAKER",
        "KUBEFLOW",
    ),
    "Other": (
        "ZEPPELIN",
        "CHUNJUN",
        "DATASYNC",
        "LINKIS",
    ),
}


def upstream_default_task_types_by_category() -> dict[str, tuple[str, ...]]:
    """Return the DS 3.4.1 default task types grouped by upstream category."""
    return dict(_UPSTREAM_DEFAULT_TASK_TYPES_BY_CATEGORY)


def upstream_default_task_types() -> tuple[str, ...]:
    """Return the flattened DS 3.4.1 default task type list."""
    return tuple(
        task_type
        for task_types in _UPSTREAM_DEFAULT_TASK_TYPES_BY_CATEGORY.values()
        for task_type in task_types
    )


__all__ = [
    "upstream_default_task_types",
    "upstream_default_task_types_by_category",
]
