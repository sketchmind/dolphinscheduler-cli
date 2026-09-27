from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

EmrProgramType = Literal["RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"]
EmrCredentialSource = Literal[
    "worker-properties",
    "worker-resource-properties",
    "aws-authentication",
]


@dataclass(frozen=True, slots=True)
class EmrAuthoringSurface:
    """EMR execution modes and placeholder behavior for one exact release."""

    available: bool
    program_types: tuple[EmrProgramType, ...]
    native_program_type: bool
    parameter_substitution: bool
    credential_source: EmrCredentialSource | None
    credential_keys: tuple[str, ...]
    failover_supported: bool


_EMR_ABSENT = EmrAuthoringSurface(
    available=False,
    program_types=(),
    native_program_type=False,
    parameter_substitution=False,
    credential_source=None,
    credential_keys=(),
    failover_supported=False,
)
_EMR_RUN_JOB_FLOW_AWS_PROPERTIES = EmrAuthoringSurface(
    available=True,
    program_types=("RUN_JOB_FLOW",),
    native_program_type=False,
    parameter_substitution=False,
    credential_source="worker-properties",
    credential_keys=(
        "aws.access.key.id",
        "aws.secret.access.key",
        "aws.region",
    ),
    failover_supported=False,
)
_EMR_RUN_JOB_FLOW_RESOURCE_PROPERTIES = EmrAuthoringSurface(
    available=True,
    program_types=("RUN_JOB_FLOW",),
    native_program_type=False,
    parameter_substitution=False,
    credential_source="worker-resource-properties",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    failover_supported=False,
)
_EMR_DUAL_LITERAL_JSON = EmrAuthoringSurface(
    available=True,
    program_types=("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
    native_program_type=True,
    parameter_substitution=False,
    credential_source="worker-resource-properties",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    failover_supported=False,
)
_EMR_DUAL_PARAMETERIZED_RESOURCE_PROPERTIES = EmrAuthoringSurface(
    available=True,
    program_types=("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
    native_program_type=True,
    parameter_substitution=True,
    credential_source="worker-resource-properties",
    credential_keys=(
        "resource.aws.access.key.id",
        "resource.aws.secret.access.key",
        "resource.aws.region",
    ),
    failover_supported=False,
)
_EMR_DUAL_AWS_AUTHENTICATION = EmrAuthoringSurface(
    available=True,
    program_types=("RUN_JOB_FLOW", "ADD_JOB_FLOW_STEPS"),
    native_program_type=True,
    parameter_substitution=True,
    credential_source="aws-authentication",
    credential_keys=(
        "aws.emr.credentials.provider.type",
        "aws.emr.region",
        "aws.emr.access.key.id",
        "aws.emr.access.key.secret",
    ),
    failover_supported=False,
)


def _emr_surface(version: str) -> EmrAuthoringSurface:
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
        return _EMR_ABSENT
    if version == "3.0.0":
        return _EMR_RUN_JOB_FLOW_AWS_PROPERTIES
    if version in {"3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"}:
        return _EMR_RUN_JOB_FLOW_RESOURCE_PROPERTIES
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
        "3.2.0",
        "3.2.1",
    }:
        return _EMR_DUAL_LITERAL_JSON
    if version == "3.2.2":
        return _EMR_DUAL_PARAMETERIZED_RESOURCE_PROPERTIES
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}:
        return _EMR_DUAL_AWS_AUTHENTICATION
    message = f"No exact EMR authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
