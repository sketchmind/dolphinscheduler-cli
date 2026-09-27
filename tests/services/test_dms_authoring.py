from __future__ import annotations

import re
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._closed_facet_contract import (
    ABSENT_VERSIONS,
    REPRESENTATIVE_VERSIONS,
    TYPED_VERSIONS,
    ClosedFacetContractCase,
    ClosedFacetContractSuite,
)

from dsctl.errors import UserInputError
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue


_FACET = "DMS/resume_existing_full_load"
_FINGERPRINTS = {
    "3.2.0": "sha256:47196753e57b2e22a9d926c4e8081180b3e6a7f8a02605c975e834b8a65781b3",
    "3.2.1": "sha256:47196753e57b2e22a9d926c4e8081180b3e6a7f8a02605c975e834b8a65781b3",
    "3.2.2": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
    "3.3.1": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
    "3.3.2": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
    "3.4.0": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
    "3.4.1": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
    "3.4.2": "sha256:a4a3ee1abc8638ca4898ec174f5c5bf20e7a6d146c2ad3cec1bf364298cd89d7",
}
_FIELDS = {
    "isRestartTask",
    "isJsonFormat",
    "migrationType",
    "startReplicationTaskType",
    "replicationTaskArn",
}
_DEFAULT_ARN = "arn:aws:dms:us-east-1:123456789012:task:K55IUCGBASJS5VHZJIINA45FII"
_LEGACY_CREDENTIAL_KEYS = (
    "resource.aws.access.key.id",
    "resource.aws.secret.access.key",
    "resource.aws.region",
)
_AWS_AUTH_CREDENTIAL_KEYS = (
    "aws.dms.credentials.provider.type",
    "aws.dms.access.key.id",
    "aws.dms.access.key.secret",
    "aws.dms.region",
    "aws.dms.endpoint",
)


def _canonical(*, arn: str = _DEFAULT_ARN) -> YamlObject:
    return {
        "isRestartTask": True,
        "isJsonFormat": False,
        "migrationType": "full-load",
        "startReplicationTaskType": "resume-processing",
        "replicationTaskArn": arn,
    }


def _opaque_native() -> YamlObject:
    return {
        "isRestartTask": True,
        "isJsonFormat": False,
        "migrationType": "full-load-and-cdc",
        "startReplicationTaskType": "reload-target",
        "replicationTaskArn": (
            "arn:aws:dms:us-west-2:123456789012:task:OPAQUE-RELOAD-TARGET"
        ),
        "localParams": [
            {
                "prop": "not_owned",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${not_typed}",
            }
        ],
        "resourceList": [],
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


_CASE = ClosedFacetContractCase(
    task_type="DMS",
    facet=_FACET,
    task_name="resume-existing-full-load",
    review="dms-resume-existing-full-load-exact-subset",
    family="dms-existing-full-load-resume-v1",
    params_model_name="DmsResumeExistingFullLoadTaskParamsSpec",
    fingerprints=_FINGERPRINTS,
    fields=frozenset(_FIELDS),
    canonical=_canonical,
    opaque_native=_opaque_native,
    task_spec_extras={"retry": {"times": 0, "interval": 0}, "timeout": 3600},
    timeout=3600,
)


class TestDmsClosedFacetContract(ClosedFacetContractSuite):
    case = _CASE


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_dms_json_schema_keeps_explicit_constants_and_arn_shape(
    version: str,
) -> None:
    task_params = _CASE.task_params_schema(version)
    expected_constants: dict[str, object] = {
        "isRestartTask": True,
        "isJsonFormat": False,
        "migrationType": "full-load",
        "startReplicationTaskType": "resume-processing",
    }
    for field_name, expected in expected_constants.items():
        field_schema = _CASE.resolved_field_schema(task_params, field_name)
        assert field_schema["const"] == expected
    arn_schema = _CASE.resolved_field_schema(task_params, "replicationTaskArn")
    assert arn_schema["type"] == "string"
    assert arn_schema["minLength"] == 1
    assert isinstance(arn_schema["pattern"], str)


@pytest.mark.parametrize("version", TYPED_VERSIONS)
def test_dms_minimal_template_is_bounded_and_warns_about_remote_prerequisites(
    version: str,
) -> None:
    yaml_text = _CASE.template_yaml(version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    retry = task["retry"]
    timeout = task["timeout"]
    assert isinstance(params, dict)
    assert isinstance(retry, dict)
    assert isinstance(timeout, int)

    assert task["type"] == "DMS"
    assert set(params) == _FIELDS
    assert params == _canonical(arn=cast("str", params["replicationTaskArn"]))
    assert retry["times"] == 0
    assert timeout == 0
    assert "minutes" in yaml_text.lower()
    assert "disabled" in yaml_text
    normalized = get_task_authoring_catalog(version).normalize_task_params(
        "DMS",
        cast("YamlObject", params),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == params
    assert _CASE.compiled(version, normalized) == params

    guidance = yaml_text.lower().replace("_", " ")
    for term in (
        "warning",
        "caller",
        "previously",
        "stopped",
        "full-load",
        "cannot verify",
        "resume-processing",
        "reload-target",
        "truncate",
        "drop",
        "partially",
        "retry",
        "appids",
        "callback",
        "cdc",
        "continues",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dms_model_and_projector_preserve_exact_explicit_resume_wire(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical(
        arn=("arn:aws-us-gov:dms:us-gov-west-1:210987654321:task:resume-full-load-01")
    )
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params("DMS", params, intent=intent)

    assert normalized == params
    assert normalized is not params
    assert _CASE.encode(version, normalized) == params
    decoded, source = _CASE.decode(
        version,
        params,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert decoded == params
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _CASE.compiled(version, params) == params


@pytest.mark.parametrize("version", TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_dms_public_patch_and_file_edit_compile_the_exact_resume_wire(
    version: str,
    input_mode: str,
) -> None:
    params = _canonical(
        arn="arn:aws-cn:dms:cn-north-1:123456789012:task:RESUME-SECOND-TASK"
    )

    plan = _CASE.edit_plan(version, params, input_mode=input_mode)

    assert _CASE.compiled_from_plan(plan) == params


def test_dms_arn_json_schema_pattern_matches_runtime_validation() -> None:
    field_schema = _CASE.resolved_field_schema(
        _CASE.task_params_schema("3.4.2"),
        "replicationTaskArn",
    )
    pattern = field_schema["pattern"]
    assert isinstance(pattern, str)
    valid = (
        _DEFAULT_ARN,
        "arn:aws-cn:dms:cn-north-1:123456789012:task:lowercase-task-1",
        ("arn:aws-us-gov:dms:us-gov-west-1:210987654321:task:GOVCLOUDTASK01"),
    )
    invalid = (
        "",
        " ",
        "arn:aws:s3:us-east-1:123456789012:task:NOT-DMS",
        "arn:aws:dms:us-east-1:123456789012:endpoint:NOT-A-TASK",
        "arn::dms:us-east-1:123456789012:task:MISSING-PARTITION",
        "arn:aws:dms::123456789012:task:MISSING-REGION",
        "arn:aws:dms:us-east-1:12345:task:SHORT-ACCOUNT",
        "arn:aws:dms:us-east-1:123456789012:task:",
        "arn:aws:dms:us-east-1:123456789012:task:${task}",
        "arn:aws:dms:us-east-1:123456789012:task:$[task]",
        " arn:aws:dms:us-east-1:123456789012:task:LEADING",
        "arn:aws:dms:us-east-1:123456789012:task:TRAILING ",
        "arn:aws:dms:us-east-1:123456789012:task:LINE\nBREAK",
        "arn:aws:dms:us-east-1:123456789012:task:NUL\x00BYTE",
        "arn:aws:dms:us-east-1:123456789012:task:DELETE\x7fBYTE",
        "arn:aws:dms:us-east-1:123456789012:task:SURROGATE\ud800",
    )

    for value in valid:
        assert re.fullmatch(pattern, value), value
        assert _CASE.runtime_accepts("replicationTaskArn", value) is True
    for value in invalid:
        assert re.fullmatch(pattern, value) is None, value
        assert _CASE.runtime_accepts("replicationTaskArn", value) is False


@pytest.mark.parametrize(
    ("field_name", "valid_value", "invalid_values"),
    [
        ("isRestartTask", True, (False, None, 1, "true")),
        ("isJsonFormat", False, (True, None, 0, "false")),
        ("migrationType", "full-load", ("cdc", "full-load-and-cdc", "", None)),
        (
            "startReplicationTaskType",
            "resume-processing",
            ("start-replication", "reload-target", "", None),
        ),
    ],
)
def test_dms_json_schema_constants_match_runtime_validation(
    field_name: str,
    valid_value: YamlValue,
    invalid_values: tuple[YamlValue, ...],
) -> None:
    field_schema = _CASE.resolved_field_schema(
        _CASE.task_params_schema("3.4.2"), field_name
    )

    assert field_schema["const"] == valid_value
    assert _CASE.runtime_accepts(field_name, valid_value) is True
    for value in invalid_values:
        assert _CASE.runtime_accepts(field_name, value) is False


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("jsonData", '{"ReplicationTaskArn":"secret-ish"}'),
        ("replicationTaskIdentifier", "new-task"),
        ("sourceEndpointArn", "arn:aws:dms:us-east-1:123456789012:endpoint:SRC"),
        ("targetEndpointArn", "arn:aws:dms:us-east-1:123456789012:endpoint:DST"),
        ("replicationInstanceArn", "arn:aws:dms:us-east-1:123456789012:rep:ONE"),
        ("tableMappings", "{}"),
        ("replicationTaskSettings", "{}"),
        ("cdcStartPosition", "checkpoint:V1#27"),
        ("cdcStopPosition", "server_time:2026-08-20T12:00:00"),
        ("localParams", []),
        ("resourceList", []),
        ("futureField", {"native": True}),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dms_typed_authoring_rejects_unowned_native_state(
    field_name: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DMS",
            params,
            intent=intent,
        )


def test_dms_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = {**_canonical(), "cdcStopPosition": "server_time:later"}
    native = {**_canonical(), "localParams": []}

    with pytest.raises(TaskParameterProjectionError, match="cdcStopPosition"):
        _CASE.encode("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="localParams"):
        _CASE.decode(
            "3.4.2",
            native,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("isRestartTask", False),
        ("isJsonFormat", True),
        ("migrationType", "full-load-and-cdc"),
        ("startReplicationTaskType", "reload-target"),
        (
            "replicationTaskArn",
            "arn:aws:dms:us-east-1:123456789012:endpoint:WRONG-KIND",
        ),
        ("jsonData", "{}"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_dms_invalid_public_create_and_edit_never_downgrade_to_opaque(
    input_mode: str,
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        _CASE.compiled("3.4.2", params)
    with pytest.raises(UserInputError, match=field_name):
        _CASE.edit_plan("3.4.2", params, input_mode=input_mode)

    catalog = get_task_authoring_catalog("3.4.2")
    for requested in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        assert (
            catalog.effective_authoring_intent(
                "DMS",
                requested=requested,
                task_params=params,
            )
            is requested
        )


@pytest.mark.parametrize("version", ABSENT_VERSIONS)
def test_dms_runtime_surface_is_absent_before_3_2(version: str) -> None:
    surface = get_task_authoring_surface(version).dms

    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.credential_source is None
    assert surface.credential_keys == ()
    assert surface.credential_provider_types == ()
    assert surface.poll_interval_ms is None
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.task_params_logged is False
    assert surface.credentials_logged is False
    assert surface.resource_identifiers_logged is False
    assert surface.result_output_supported is False
    assert surface.application_id_field is None
    assert surface.application_id_persistence is None
    assert surface.failover_supported is False
    assert surface.cancel_supported is False
    assert surface.retry_may_resubmit is False
    assert surface.callback_persistence_gap is False
    assert surface.declared_migration_type is None
    assert surface.remote_migration_type_verified is False
    assert surface.remote_task_state_verified is False
    assert surface.cdc_without_stop_position_reports_success_after_start is False
    assert surface.resume_full_load_may_reload_incomplete_tables is False
    assert surface.reload_target_exposed is False
    assert surface.explicit_internal_timeout is False


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2"])
def test_dms_runtime_surface_locks_the_legacy_static_credential_epoch(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).dms

    assert surface.available is True
    assert surface.wire_epoch == "explicit-resume-existing-full-load"
    assert surface.credential_source == "worker-properties"
    assert surface.credential_keys == _LEGACY_CREDENTIAL_KEYS
    assert surface.credential_provider_types == ("AWSStaticCredentialsProvider",)


@pytest.mark.parametrize(
    "version",
    ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"],
)
def test_dms_runtime_surface_locks_the_aws_authentication_credential_epoch(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).dms

    assert surface.available is True
    assert surface.wire_epoch == "explicit-resume-existing-full-load"
    assert surface.credential_source == "aws-authentication"
    assert surface.credential_keys == _AWS_AUTH_CREDENTIAL_KEYS
    assert surface.credential_provider_types == (
        "AWSStaticCredentialsProvider",
        "InstanceProfileCredentialsProvider",
    )


@pytest.mark.parametrize("version", TYPED_VERSIONS)
def test_dms_runtime_surface_discloses_tracking_recovery_and_replay_limits(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).dms

    assert surface.poll_interval_ms == 1000
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.task_params_logged is True
    assert surface.credentials_logged is False
    assert surface.resource_identifiers_logged is True
    assert surface.result_output_supported is False
    assert surface.application_id_field == "replicationTaskArn"
    assert surface.application_id_persistence == "appIds-callback"
    assert surface.failover_supported is True
    assert surface.cancel_supported is True
    assert surface.retry_may_resubmit is True
    assert surface.callback_persistence_gap is True
    assert surface.explicit_internal_timeout is False


@pytest.mark.parametrize("version", TYPED_VERSIONS)
def test_dms_runtime_surface_distinguishes_declaration_from_aws_validation(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).dms

    assert surface.declared_migration_type == "full-load"
    assert surface.remote_migration_type_verified is False
    assert surface.remote_task_state_verified is False
    assert surface.cdc_without_stop_position_reports_success_after_start is True
    assert surface.resume_full_load_may_reload_incomplete_tables is True
    assert surface.reload_target_exposed is False


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_dms_guidance_discloses_credentials_logging_and_no_output(
    version: str,
) -> None:
    guidance = _CASE.guidance(version)

    for term in (
        "credential",
        "worker",
        "replicationtaskarn",
        "info",
        "logged",
        "result",
        "output",
        "1000",
        "timeout",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_dms_guidance_discloses_unverified_state_cdc_success_and_replay_window(
    version: str,
) -> None:
    guidance = _CASE.guidance(version)

    for term in (
        "caller",
        "previously",
        "stopped",
        "full-load",
        "cannot verify",
        "resume-processing",
        "partially",
        "cdc",
        "success",
        "continues",
        "appids",
        "callback",
        "failover",
        "cancel",
        "retry",
        "resubmit",
        "reload-target",
        "truncate",
        "drop",
    ):
        assert term in guidance
