from __future__ import annotations

import re
from functools import partial
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


_FACET = "DATA_FACTORY/pipeline_trigger"
_FINGERPRINTS = {
    "3.2.0": "sha256:3aa9582e0500a82aab1f1e4ea350997e8a22ca8657348961257537d598ea258c",
    "3.2.1": "sha256:3aa9582e0500a82aab1f1e4ea350997e8a22ca8657348961257537d598ea258c",
    "3.2.2": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
    "3.3.1": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
    "3.3.2": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
    "3.4.0": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
    "3.4.1": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
    "3.4.2": "sha256:43bed583a77f0210e384308372c4993569c6d747101de5982f1277d1a843b4bd",
}
_FIELDS = {"factoryName", "resourceGroupName", "pipelineName"}


def _canonical(
    *,
    factory_name: str = "analytics-factory",
    resource_group_name: str = "analytics-rg",
    pipeline_name: str = "daily-copy",
) -> YamlObject:
    return {
        "factoryName": factory_name,
        "resourceGroupName": resource_group_name,
        "pipelineName": pipeline_name,
    }


def _opaque_native() -> YamlObject:
    return {
        **_canonical(pipeline_name="opaque-pipeline"),
        "runId": "3c7182f4-d107-43c7-af2a-8c7b3ed1d614",
        "localParams": [
            {
                "prop": "ignored_parameter",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${not_consumed}",
            }
        ],
        "resourceList": [],
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


_CASE = ClosedFacetContractCase(
    task_type="DATA_FACTORY",
    facet=_FACET,
    task_name="trigger-data-factory-pipeline",
    review="data-factory-literal-pipeline-trigger",
    family="data-factory-pipeline-trigger-v1",
    params_model_name="DataFactoryPipelineTriggerTaskParamsSpec",
    fingerprints=_FINGERPRINTS,
    fields=frozenset(_FIELDS),
    canonical=_canonical,
    opaque_native=_opaque_native,
    task_spec_extras={},
)


class TestDataFactoryClosedFacetContract(ClosedFacetContractSuite):
    case = _CASE


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_data_factory_json_schema_fields_are_nonempty_literal_strings(
    version: str,
) -> None:
    task_params = _CASE.task_params_schema(version)
    for field_name in _FIELDS:
        field_schema = _CASE.resolved_field_schema(task_params, field_name)
        assert field_schema["type"] == "string"
        assert field_schema["minLength"] == 1
        assert isinstance(field_schema["pattern"], str)


@pytest.mark.parametrize("version", TYPED_VERSIONS)
def test_data_factory_minimal_template_compiles_to_exact_native_identity(
    version: str,
) -> None:
    yaml_text = _CASE.template_yaml(version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "DATA_FACTORY"
    assert set(params) == _FIELDS
    normalized = get_task_authoring_catalog(version).normalize_task_params(
        "DATA_FACTORY",
        cast("YamlObject", params),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == params
    assert _CASE.compiled(version, normalized) == params

    guidance = yaml_text.lower().replace("_", ".")
    for term in (
        "azure",
        "worker",
        "client.secret",
        "runid",
        "appids",
        "failover",
        "retry",
        "pipeline parameters",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_data_factory_model_and_projector_preserve_exact_literal_identity(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical(
        factory_name="factory-v2",
        resource_group_name="production analytics",
        pipeline_name="每日复制-v2",
    )
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params("DATA_FACTORY", params, intent=intent)

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


@pytest.mark.parametrize("field_name", sorted(_FIELDS))
def test_data_factory_json_schema_pattern_matches_runtime_validation(
    field_name: str,
) -> None:
    field_schema = _CASE.resolved_field_schema(
        _CASE.task_params_schema("3.4.2"), field_name
    )
    pattern = field_schema["pattern"]
    assert isinstance(pattern, str)
    valid = (
        "analytics-factory",
        "production analytics",
        "每日复制-v2",
        "literal.name_(v2)",
    )
    invalid = (
        "",
        " ",
        "\u3000",
        " leading",
        "trailing ",
        "${runtime_name}",
        "$[yyyyMMdd]",
        "line\nbreak",
        "nul\x00byte",
        "delete\x7fbyte",
        "control\x85byte",
        "surrogate\ud800",
        "surrogate\udfff",
    )

    for value in valid:
        assert re.fullmatch(pattern, value), (field_name, value)
        assert _CASE.runtime_accepts(field_name, value) is True
    for value in invalid:
        assert re.fullmatch(pattern, value) is None, (field_name, value)
        assert _CASE.runtime_accepts(field_name, value) is False


@pytest.mark.parametrize("field_name", sorted(_FIELDS))
@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_data_factory_identity_fields_reject_nonstrings(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DATA_FACTORY",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("runId", "3c7182f4-d107-43c7-af2a-8c7b3ed1d614"),
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("futureField", {"enabled": True}),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_data_factory_typed_authoring_rejects_runtime_inherited_and_future_state(
    field_name: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DATA_FACTORY",
            params,
            intent=intent,
        )


def test_data_factory_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = {**_canonical(), "runId": "runtime-only"}
    native = {**_canonical(), "localParams": []}

    with pytest.raises(TaskParameterProjectionError, match="runId"):
        _CASE.encode("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="localParams"):
        _CASE.decode(
            "3.4.2",
            native,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("direction", ["encode", "decode"])
@pytest.mark.parametrize(
    ("params", "field_name", "reason"),
    [
        ({}, "factoryName", "missing-required-field"),
        ({"factoryName": None}, "factoryName", "invalid-type"),
        (
            {**_canonical(), "resourceGroupName": ""},
            "resourceGroupName",
            "unsafe-value",
        ),
        (
            {**_canonical(), "pipelineName": "surrogate\ud800"},
            "pipelineName",
            "unsafe-value",
        ),
        (
            {"factoryName": None, "zFuture": [], "aFuture": None},
            "aFuture",
            "outside-reviewed-pipeline-trigger-subset",
        ),
        (
            {**_canonical(), "factory_name": "authoring-alias"},
            "factory_name",
            "outside-reviewed-pipeline-trigger-subset",
        ),
        (
            {"pipelineName": None, "resourceGroupName": None, "factoryName": "${bad}"},
            "factoryName",
            "unsafe-value",
        ),
    ],
)
def test_data_factory_projection_preserves_error_details_and_precedence(
    direction: str,
    params: YamlObject,
    field_name: str,
    reason: str,
) -> None:
    project = (
        _CASE.encode
        if direction == "encode"
        else partial(_CASE.decode, source=ProjectionSource.TYPED_AUTHORING)
    )
    with pytest.raises(TaskParameterProjectionError) as caught:
        project("3.4.2", params)

    assert caught.value.details == {
        "version": "3.4.2",
        "direction": direction,
        "task_type": "DATA_FACTORY",
        "field": f"task_params.{field_name}",
        "reason": reason,
    }


def test_data_factory_authoring_aliases_remain_outside_native_decode() -> None:
    aliases: YamlObject = {
        "factory_name": "factory",
        "resource_group_name": "group",
        "pipeline_name": "pipeline",
    }
    catalog = get_task_authoring_catalog("3.4.2")
    normalized = catalog.normalize_task_params(
        "DATA_FACTORY", aliases, intent=TaskAuthoringIntent.TYPED_CREATE
    )
    assert normalized == _canonical(
        factory_name="factory", resource_group_name="group", pipeline_name="pipeline"
    )

    decoded, source = _CASE.decode("3.4.2", aliases)
    assert decoded == aliases
    assert source is ProjectionSource.OPAQUE_PRESERVE


def test_data_factory_projection_preserves_native_field_order() -> None:
    params: YamlObject = dict(reversed(tuple(_canonical().items())))

    encoded = _CASE.encode("3.4.2", params)
    decoded, source = _CASE.decode("3.4.2", params)

    assert tuple(encoded.items()) == tuple(params.items())
    assert tuple(decoded.items()) == tuple(params.items())
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("factoryName", "${factory}"),
        ("resourceGroupName", " group"),
        ("pipelineName", "pipeline\nname"),
        ("runId", "runtime-only"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_data_factory_invalid_public_create_and_edit_never_downgrade_to_opaque(
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
                "DATA_FACTORY",
                requested=requested,
                task_params=params,
            )
            is requested
        )


@pytest.mark.parametrize("version", ABSENT_VERSIONS)
def test_data_factory_runtime_surface_is_absent_before_3_2(version: str) -> None:
    surface = get_task_authoring_surface(version).data_factory

    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.credential_source is None
    assert surface.credential_keys == ()
    assert surface.query_interval_key is None
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.pipeline_parameters_supported is False
    assert surface.task_params_logged is False
    assert surface.credentials_logged is False
    assert surface.result_output_supported is False
    assert surface.application_id_field is None
    assert surface.application_id_persistence is None
    assert surface.failover_supported is False
    assert surface.retry_may_resubmit is False
    assert surface.callback_persistence_gap is False


@pytest.mark.parametrize("version", TYPED_VERSIONS)
def test_data_factory_runtime_surface_locks_one_exact_remote_execution_epoch(
    version: str,
) -> None:
    surface = get_task_authoring_surface(version).data_factory

    assert surface.available is True
    assert surface.wire_epoch == "literal-three-field-pipeline-trigger"
    assert surface.credential_source == "worker-properties"
    assert surface.credential_keys == (
        "resource.azure.client.id",
        "resource.azure.client.secret",
        "resource.azure.subId",
        "resource.azure.tenant.id",
    )
    assert surface.query_interval_key == "resource.query.interval"
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.pipeline_parameters_supported is False
    assert surface.task_params_logged is True
    assert surface.credentials_logged is False
    assert surface.result_output_supported is False
    assert surface.application_id_field == "runId"
    assert surface.application_id_persistence == "appIds-callback"
    assert surface.failover_supported is True
    assert surface.retry_may_resubmit is True
    assert surface.callback_persistence_gap is True


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_data_factory_guidance_discloses_credentials_literal_fields_and_outputs(
    version: str,
) -> None:
    guidance = _CASE.guidance(version)

    for term in (
        "azure",
        "worker",
        "resource.azure.client.id",
        "resource.azure.client.secret",
        "resource.azure.subid",
        "resource.azure.tenant.id",
        "credential",
        "literal",
        "placeholder",
        "localparams",
        "pipeline parameters",
        "task params",
        "info",
        "result",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
def test_data_factory_guidance_discloses_run_id_recovery_and_replay_window(
    version: str,
) -> None:
    guidance = _CASE.guidance(version)

    for term in (
        "runid",
        "appids",
        "callback",
        "failover",
        "resume",
        "cancel",
        "retry",
        "resubmit",
        "duplicate",
        "resource.query.interval",
        "10000",
    ):
        assert term in guidance
