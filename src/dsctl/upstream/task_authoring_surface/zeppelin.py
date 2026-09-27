from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ZeppelinConnectionMode = Literal[
    "WORKER_CONFIG",
    "REST_ENDPOINT",
    "DATASOURCE",
]
ZeppelinCredentialSource = Literal[
    "worker-config",
    "task-rest-endpoint",
    "task-inline",
    "datasource",
]
ZeppelinCredentialLogging = Literal[
    "none",
    "inline-task-params",
    "resolved-username",
    "resolved-credentials",
]
ZeppelinResultOutput = Literal["absent", "task-params-only", "var-pool"]


@dataclass(frozen=True, slots=True)
class ZeppelinAuthoringSurface:
    """Exact paragraph connection, parameter, and runtime-result semantics."""

    available: bool
    connection_mode: ZeppelinConnectionMode | None
    literal_parameters: bool
    credential_source: ZeppelinCredentialSource | None
    credential_logging: ZeppelinCredentialLogging
    result_output: ZeppelinResultOutput
    failover_supported: bool


_ZEPPELIN_ABSENT = ZeppelinAuthoringSurface(
    available=False,
    connection_mode=None,
    literal_parameters=False,
    credential_source=None,
    credential_logging="none",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_WORKER_CONFIG = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="WORKER_CONFIG",
    literal_parameters=False,
    credential_source="worker-config",
    credential_logging="none",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_REST_ENDPOINT = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="REST_ENDPOINT",
    literal_parameters=True,
    credential_source="task-rest-endpoint",
    credential_logging="none",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_INLINE_CREDENTIALS = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="REST_ENDPOINT",
    literal_parameters=True,
    credential_source="task-inline",
    credential_logging="inline-task-params",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_DATASOURCE = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="DATASOURCE",
    literal_parameters=True,
    credential_source="datasource",
    credential_logging="resolved-username",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_DATASOURCE_LOGGED = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="DATASOURCE",
    literal_parameters=True,
    credential_source="datasource",
    credential_logging="resolved-credentials",
    result_output="absent",
    failover_supported=False,
)
_ZEPPELIN_DATASOURCE_TASK_PARAMS_OUTPUT = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="DATASOURCE",
    literal_parameters=True,
    credential_source="datasource",
    credential_logging="resolved-credentials",
    result_output="task-params-only",
    failover_supported=False,
)
_ZEPPELIN_DATASOURCE_VAR_POOL_OUTPUT = ZeppelinAuthoringSurface(
    available=True,
    connection_mode="DATASOURCE",
    literal_parameters=True,
    credential_source="datasource",
    credential_logging="resolved-credentials",
    result_output="var-pool",
    failover_supported=False,
)


def _zeppelin_surface(version: str) -> ZeppelinAuthoringSurface:
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }:
        return _ZEPPELIN_ABSENT
    if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _ZEPPELIN_WORKER_CONFIG
    if version in {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }:
        return _ZEPPELIN_REST_ENDPOINT
    if version == "3.2.0":
        return _ZEPPELIN_INLINE_CREDENTIALS
    if version == "3.2.1":
        return _ZEPPELIN_DATASOURCE
    if version in {"3.2.2", "3.3.1", "3.3.2", "3.4.0"}:
        return _ZEPPELIN_DATASOURCE_LOGGED
    if version == "3.4.1":
        return _ZEPPELIN_DATASOURCE_TASK_PARAMS_OUTPUT
    if version in {"3.4.2", "3.4.3"}:
        return _ZEPPELIN_DATASOURCE_VAR_POOL_OUTPUT
    message = f"No exact ZEPPELIN authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
