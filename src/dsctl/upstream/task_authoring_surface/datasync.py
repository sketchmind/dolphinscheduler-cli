from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

DatasyncWireEpoch = Literal["create-and-execute-normal-and-raw-json"]
DatasyncCredentialSource = Literal[
    "worker-properties-static-basic",
    "aws-authentication-static-basic",
]
DatasyncApplicationIdPersistence = Literal["appIds-callback"]


@dataclass(frozen=True, slots=True)
class DatasyncAuthoringSurface:
    """AWS DataSync public modes, credentials, defects, and lifecycle facts."""

    available: bool
    wire_epoch: DatasyncWireEpoch | None
    credential_source: DatasyncCredentialSource | None
    credential_keys: tuple[str, ...]
    parameter_substitution: bool
    resource_files_supported: bool
    local_params_consumed: bool
    task_params_logged: bool
    converted_task_params_logged: bool
    credentials_logged: bool
    secret_storage_supported: bool
    cli_secret_detection: bool
    cli_secret_redaction: bool
    raw_json_model: str | None
    raw_json_unknown_fields_ignored: bool
    raw_json_inherited_parameter_unknown_enum_becomes_null: bool
    options_effective: bool
    includes_behavior: str | None
    schedule_persists_recurring_task: bool
    fresh_run_calls: tuple[str, ...]
    remote_task_deleted: bool
    client_closed: bool
    result_output_supported: bool
    application_id_field: str | None
    application_id_persistence: DatasyncApplicationIdPersistence | None
    failover_supported: bool
    cancel_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool
    poll_deadline: bool


_DATASYNC_ABSENT = DatasyncAuthoringSurface(
    available=False,
    wire_epoch=None,
    credential_source=None,
    credential_keys=(),
    parameter_substitution=False,
    resource_files_supported=False,
    local_params_consumed=False,
    task_params_logged=False,
    converted_task_params_logged=False,
    credentials_logged=False,
    secret_storage_supported=False,
    cli_secret_detection=False,
    cli_secret_redaction=False,
    raw_json_model=None,
    raw_json_unknown_fields_ignored=False,
    raw_json_inherited_parameter_unknown_enum_becomes_null=False,
    options_effective=False,
    includes_behavior=None,
    schedule_persists_recurring_task=False,
    fresh_run_calls=(),
    remote_task_deleted=False,
    client_closed=False,
    result_output_supported=False,
    application_id_field=None,
    application_id_persistence=None,
    failover_supported=False,
    cancel_supported=False,
    retry_may_resubmit=False,
    callback_persistence_gap=False,
    poll_deadline=False,
)
_DATASYNC_PUBLIC = DatasyncAuthoringSurface(
    available=True,
    wire_epoch="create-and-execute-normal-and-raw-json",
    credential_source=None,
    credential_keys=(),
    parameter_substitution=False,
    resource_files_supported=False,
    local_params_consumed=False,
    task_params_logged=True,
    converted_task_params_logged=True,
    credentials_logged=False,
    secret_storage_supported=False,
    cli_secret_detection=False,
    cli_secret_redaction=False,
    raw_json_model="DatasyncParameters",
    raw_json_unknown_fields_ignored=True,
    raw_json_inherited_parameter_unknown_enum_becomes_null=True,
    options_effective=False,
    includes_behavior="copied-to-excludes-and-overwrites",
    schedule_persists_recurring_task=True,
    fresh_run_calls=("CreateTask", "StartTaskExecution"),
    remote_task_deleted=False,
    client_closed=False,
    result_output_supported=False,
    application_id_field="taskExecutionArn",
    application_id_persistence="appIds-callback",
    failover_supported=True,
    cancel_supported=True,
    retry_may_resubmit=True,
    callback_persistence_gap=True,
    poll_deadline=False,
)
_DATASYNC_WORKER_PROPERTIES = replace(
    _DATASYNC_PUBLIC,
    credential_source="worker-properties-static-basic",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
)
_DATASYNC_AWS_AUTHENTICATION = replace(
    _DATASYNC_PUBLIC,
    credential_source="aws-authentication-static-basic",
    credential_keys=(
        "aws.datasync.access.key.id",
        "aws.datasync.access.key.secret",
        "aws.datasync.region",
    ),
)


def _datasync_surface(version: str) -> DatasyncAuthoringSurface:
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
        return _DATASYNC_ABSENT
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _DATASYNC_WORKER_PROPERTIES
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _DATASYNC_AWS_AUTHENTICATION
    message = f"No exact DATASYNC authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
