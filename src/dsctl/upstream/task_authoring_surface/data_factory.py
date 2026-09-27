from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DataFactoryWireEpoch = Literal["literal-three-field-pipeline-trigger"]
DataFactoryCredentialSource = Literal["worker-properties"]
DataFactoryApplicationIdPersistence = Literal["appIds-callback"]


@dataclass(frozen=True, slots=True)
class DataFactoryAuthoringSurface:
    """Azure pipeline identity, credentials, and recovery semantics."""

    available: bool
    wire_epoch: DataFactoryWireEpoch | None
    credential_source: DataFactoryCredentialSource | None
    credential_keys: tuple[str, ...]
    query_interval_key: str | None
    parameter_substitution: bool
    resource_files_supported: bool
    pipeline_parameters_supported: bool
    task_params_logged: bool
    credentials_logged: bool
    result_output_supported: bool
    application_id_field: str | None
    application_id_persistence: DataFactoryApplicationIdPersistence | None
    failover_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool


_DATA_FACTORY_ABSENT = DataFactoryAuthoringSurface(
    available=False,
    wire_epoch=None,
    credential_source=None,
    credential_keys=(),
    query_interval_key=None,
    parameter_substitution=False,
    resource_files_supported=False,
    pipeline_parameters_supported=False,
    task_params_logged=False,
    credentials_logged=False,
    result_output_supported=False,
    application_id_field=None,
    application_id_persistence=None,
    failover_supported=False,
    retry_may_resubmit=False,
    callback_persistence_gap=False,
)
_DATA_FACTORY_PIPELINE_TRIGGER = DataFactoryAuthoringSurface(
    available=True,
    wire_epoch="literal-three-field-pipeline-trigger",
    credential_source="worker-properties",
    credential_keys=(
        "resource.azure.client.id",
        "resource.azure.client.secret",
        "resource.azure.subId",
        "resource.azure.tenant.id",
    ),
    query_interval_key="resource.query.interval",
    parameter_substitution=False,
    resource_files_supported=False,
    pipeline_parameters_supported=False,
    task_params_logged=True,
    credentials_logged=False,
    result_output_supported=False,
    application_id_field="runId",
    application_id_persistence="appIds-callback",
    failover_supported=True,
    retry_may_resubmit=True,
    callback_persistence_gap=True,
)


def _data_factory_surface(version: str) -> DataFactoryAuthoringSurface:
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
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
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
        return _DATA_FACTORY_ABSENT
    if version in {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }:
        return _DATA_FACTORY_PIPELINE_TRIGGER
    message = f"No exact DATA_FACTORY authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
