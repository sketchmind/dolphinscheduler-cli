from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

SagemakerExecutionEpoch = Literal[
    "legacy-synchronous-worker",
    "abstract-remote-task",
]
SagemakerCredentialSource = Literal[
    "worker-properties-static-basic",
    "datasource-static-basic",
    "aws-authentication",
]
SagemakerLocalParamForwarding = Literal[
    "input-only",
    "all-local-params",
]
SagemakerApplicationIdPersistence = Literal[
    "legacy-pre-submit-null",
    "appIds-callback",
]


@dataclass(frozen=True, slots=True)
class SagemakerAuthoringSurface:
    """SageMaker request, credential, polling, and recovery semantics."""

    available: bool
    execution_epoch: SagemakerExecutionEpoch | None
    credential_source: SagemakerCredentialSource | None
    credential_keys: tuple[str, ...]
    credential_provider_types: tuple[str, ...]
    datasource_required: bool
    datasource_type_required: bool
    datasource_initialized: bool
    datasource_credentials_used: bool
    raw_request_model: str | None
    raw_request_property_naming: str | None
    raw_request_unknown_fields_ignored: bool
    raw_request_mapper_unknown_enum_values_become_null: bool
    whole_text_parameter_substitution: bool
    local_param_forwarding: SagemakerLocalParamForwarding | None
    parameter_values_json_escaped: bool
    resource_files_supported: bool
    task_params_logged: bool
    resolved_request_logged: bool
    pipeline_identifiers_logged: bool
    pipeline_steps_logged: bool
    credentials_logged: bool
    secret_storage_supported: bool
    poll_interval_ms: int | None
    polling_statuses: tuple[str, ...]
    success_statuses: tuple[str, ...]
    result_output_supported: bool
    application_id_fields: tuple[str, ...]
    application_id_persistence: SagemakerApplicationIdPersistence | None
    failover_supported: bool
    cancel_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool
    client_closed: bool
    explicit_internal_timeout: bool
    exclusion_reason: str | None = None


_SAGEMAKER_ABSENT = SagemakerAuthoringSurface(
    available=False,
    execution_epoch=None,
    credential_source=None,
    credential_keys=(),
    credential_provider_types=(),
    datasource_required=False,
    datasource_type_required=False,
    datasource_initialized=False,
    datasource_credentials_used=False,
    raw_request_model=None,
    raw_request_property_naming=None,
    raw_request_unknown_fields_ignored=False,
    raw_request_mapper_unknown_enum_values_become_null=False,
    whole_text_parameter_substitution=False,
    local_param_forwarding=None,
    parameter_values_json_escaped=False,
    resource_files_supported=False,
    task_params_logged=False,
    resolved_request_logged=False,
    pipeline_identifiers_logged=False,
    pipeline_steps_logged=False,
    credentials_logged=False,
    secret_storage_supported=False,
    poll_interval_ms=None,
    polling_statuses=(),
    success_statuses=(),
    result_output_supported=False,
    application_id_fields=(),
    application_id_persistence=None,
    failover_supported=False,
    cancel_supported=False,
    retry_may_resubmit=False,
    callback_persistence_gap=False,
    client_closed=False,
    explicit_internal_timeout=False,
)
_SAGEMAKER_PUBLIC = SagemakerAuthoringSurface(
    available=True,
    execution_epoch=None,
    credential_source=None,
    credential_keys=(),
    credential_provider_types=(),
    datasource_required=False,
    datasource_type_required=False,
    datasource_initialized=False,
    datasource_credentials_used=False,
    raw_request_model="StartPipelineExecutionRequest",
    raw_request_property_naming="UpperCamelCase",
    raw_request_unknown_fields_ignored=True,
    raw_request_mapper_unknown_enum_values_become_null=True,
    whole_text_parameter_substitution=True,
    local_param_forwarding="input-only",
    parameter_values_json_escaped=False,
    resource_files_supported=False,
    task_params_logged=True,
    resolved_request_logged=True,
    pipeline_identifiers_logged=True,
    pipeline_steps_logged=True,
    credentials_logged=False,
    secret_storage_supported=False,
    poll_interval_ms=5000,
    polling_statuses=("Executing",),
    success_statuses=("Succeeded",),
    result_output_supported=False,
    application_id_fields=("pipelineExecutionArn", "clientRequestToken"),
    application_id_persistence=None,
    failover_supported=False,
    cancel_supported=True,
    retry_may_resubmit=True,
    callback_persistence_gap=False,
    client_closed=False,
    explicit_internal_timeout=False,
)
_SAGEMAKER_LEGACY_SYNCHRONOUS = replace(
    _SAGEMAKER_PUBLIC,
    execution_epoch="legacy-synchronous-worker",
    credential_source="worker-properties-static-basic",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    credential_provider_types=("AWSStaticCredentialsProvider",),
    application_id_fields=(),
    application_id_persistence="legacy-pre-submit-null",
)
_SAGEMAKER_REMOTE_WORKER_PROPERTIES = replace(
    _SAGEMAKER_PUBLIC,
    execution_epoch="abstract-remote-task",
    credential_source="worker-properties-static-basic",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    credential_provider_types=("AWSStaticCredentialsProvider",),
    application_id_persistence="appIds-callback",
    failover_supported=True,
    callback_persistence_gap=True,
)
_SAGEMAKER_DATASOURCE = replace(
    _SAGEMAKER_REMOTE_WORKER_PROPERTIES,
    credential_source="datasource-static-basic",
    credential_keys=(
        "datasource.userName",
        "datasource.password",
        "datasource.awsRegion",
    ),
    datasource_required=True,
    datasource_type_required=True,
    datasource_initialized=True,
    datasource_credentials_used=True,
    credentials_logged=True,
)
_SAGEMAKER_IGNORED_DATASOURCE = replace(
    _SAGEMAKER_DATASOURCE,
    credential_source="aws-authentication",
    credential_keys=(
        "aws.sagemaker.credentials.provider.type",
        "aws.sagemaker.access.key.id",
        "aws.sagemaker.access.key.secret",
        "aws.sagemaker.region",
        "aws.sagemaker.endpoint",
    ),
    credential_provider_types=(
        "AWSStaticCredentialsProvider",
        "InstanceProfileCredentialsProvider",
    ),
    datasource_credentials_used=False,
)
_SAGEMAKER_ALL_LOCAL_PARAMS = replace(
    _SAGEMAKER_IGNORED_DATASOURCE,
    local_param_forwarding="all-local-params",
)


def _sagemaker_surface(version: str) -> SagemakerAuthoringSurface:
    if version in {"3.1.1", "3.1.2"}:
        return replace(
            _SAGEMAKER_REMOTE_WORKER_PROPERTIES,
            available=False,
            exclusion_reason="pipeline-polling-status-never-refreshed",
        )
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
    }:
        return _SAGEMAKER_ABSENT
    if version == "3.1.0":
        return _SAGEMAKER_LEGACY_SYNCHRONOUS
    if version in {
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
    }:
        return _SAGEMAKER_REMOTE_WORKER_PROPERTIES
    if version in {"3.2.1", "3.2.2"}:
        return _SAGEMAKER_DATASOURCE
    if version in {"3.3.1", "3.3.2", "3.4.0"}:
        return _SAGEMAKER_IGNORED_DATASOURCE
    if version in {"3.4.1", "3.4.2", "3.4.3"}:
        return _SAGEMAKER_ALL_LOCAL_PARAMS
    message = f"No exact SAGEMAKER authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
