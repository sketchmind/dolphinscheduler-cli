from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DmsWireEpoch = Literal["explicit-resume-existing-full-load"]
DmsCredentialSource = Literal["worker-properties", "aws-authentication"]
DmsApplicationIdPersistence = Literal["appIds-callback"]


@dataclass(frozen=True, slots=True)
class DmsAuthoringSurface:
    """AWS DMS credentials, tracking, and recovery semantics for one release."""

    available: bool
    wire_epoch: DmsWireEpoch | None
    credential_source: DmsCredentialSource | None
    credential_keys: tuple[str, ...]
    credential_provider_types: tuple[str, ...]
    poll_interval_ms: int | None
    parameter_substitution: bool
    resource_files_supported: bool
    task_params_logged: bool
    credentials_logged: bool
    resource_identifiers_logged: bool
    result_output_supported: bool
    application_id_field: str | None
    application_id_persistence: DmsApplicationIdPersistence | None
    failover_supported: bool
    cancel_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool
    declared_migration_type: str | None
    remote_migration_type_verified: bool
    remote_task_state_verified: bool
    cdc_without_stop_position_reports_success_after_start: bool
    resume_full_load_may_reload_incomplete_tables: bool
    reload_target_exposed: bool
    explicit_internal_timeout: bool


_DMS_ABSENT = DmsAuthoringSurface(
    available=False,
    wire_epoch=None,
    credential_source=None,
    credential_keys=(),
    credential_provider_types=(),
    poll_interval_ms=None,
    parameter_substitution=False,
    resource_files_supported=False,
    task_params_logged=False,
    credentials_logged=False,
    resource_identifiers_logged=False,
    result_output_supported=False,
    application_id_field=None,
    application_id_persistence=None,
    failover_supported=False,
    cancel_supported=False,
    retry_may_resubmit=False,
    callback_persistence_gap=False,
    declared_migration_type=None,
    remote_migration_type_verified=False,
    remote_task_state_verified=False,
    cdc_without_stop_position_reports_success_after_start=False,
    resume_full_load_may_reload_incomplete_tables=False,
    reload_target_exposed=False,
    explicit_internal_timeout=False,
)
_DMS_RESUME_LEGACY_CREDENTIALS = DmsAuthoringSurface(
    available=True,
    wire_epoch="explicit-resume-existing-full-load",
    credential_source="worker-properties",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    credential_provider_types=("AWSStaticCredentialsProvider",),
    poll_interval_ms=1000,
    parameter_substitution=False,
    resource_files_supported=False,
    task_params_logged=True,
    credentials_logged=False,
    resource_identifiers_logged=True,
    result_output_supported=False,
    application_id_field="replicationTaskArn",
    application_id_persistence="appIds-callback",
    failover_supported=True,
    cancel_supported=True,
    retry_may_resubmit=True,
    callback_persistence_gap=True,
    declared_migration_type="full-load",
    remote_migration_type_verified=False,
    remote_task_state_verified=False,
    cdc_without_stop_position_reports_success_after_start=True,
    resume_full_load_may_reload_incomplete_tables=True,
    reload_target_exposed=False,
    explicit_internal_timeout=False,
)
_DMS_RESUME_AWS_AUTHENTICATION = DmsAuthoringSurface(
    available=True,
    wire_epoch="explicit-resume-existing-full-load",
    credential_source="aws-authentication",
    credential_keys=(
        "aws.dms.credentials.provider.type",
        "aws.dms.access.key.id",
        "aws.dms.access.key.secret",
        "aws.dms.region",
        "aws.dms.endpoint",
    ),
    credential_provider_types=(
        "AWSStaticCredentialsProvider",
        "InstanceProfileCredentialsProvider",
    ),
    poll_interval_ms=1000,
    parameter_substitution=False,
    resource_files_supported=False,
    task_params_logged=True,
    credentials_logged=False,
    resource_identifiers_logged=True,
    result_output_supported=False,
    application_id_field="replicationTaskArn",
    application_id_persistence="appIds-callback",
    failover_supported=True,
    cancel_supported=True,
    retry_may_resubmit=True,
    callback_persistence_gap=True,
    declared_migration_type="full-load",
    remote_migration_type_verified=False,
    remote_task_state_verified=False,
    cdc_without_stop_position_reports_success_after_start=True,
    resume_full_load_may_reload_incomplete_tables=True,
    reload_target_exposed=False,
    explicit_internal_timeout=False,
)


def _dms_surface(version: str) -> DmsAuthoringSurface:
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
        return _DMS_ABSENT
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        return _DMS_RESUME_LEGACY_CREDENTIALS
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _DMS_RESUME_AWS_AUTHENTICATION
    message = f"No exact DMS authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
