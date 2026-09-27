from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.render import workflow_live_baseline
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result, task_template_types_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject


_TASK_TYPE = "DATASYNC"
_FACET = "DATASYNC/create_and_execute"
_TYPED_VERSIONS = (
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
)
_FINGERPRINTS = {
    "3.2.0": "sha256:dbf050c3d683573b44addb7b9e8a24d38f7ebb716d7a1c1b8fdf2088ed707d6a",
    "3.2.1": "sha256:dbf050c3d683573b44addb7b9e8a24d38f7ebb716d7a1c1b8fdf2088ed707d6a",
    "3.2.2": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
    "3.3.1": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
    "3.3.2": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
    "3.4.0": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
    "3.4.1": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
    "3.4.2": "sha256:75322f0bf370951f6cfdb49e8b5786b73c26e1665632d1ff4fef3370afbd83b4",
}
_FIELDS = {
    "jsonFormat",
    "name",
    "sourceLocationArn",
    "destinationLocationArn",
    "cloudWatchLogGroupArn",
    "json",
}
_NORMAL_REQUIRED_FIELDS = {
    "name",
    "sourceLocationArn",
    "destinationLocationArn",
}
_LEGACY_CREDENTIAL_KEYS = (
    "resource.aws.access.key.id",
    "resource.aws.secret.access.key",
    "resource.aws.region",
)
_MODERN_CREDENTIAL_KEYS = (
    "aws.datasync.access.key.id",
    "aws.datasync.access.key.secret",
    "aws.datasync.region",
)
_REFS = TaskRefIndex.from_code_by_name({})
_RAW_JSON = (
    '{\n  "Name": "raw-transfer",\n'
    '  "SourceLocationArn": "arn:aws:datasync:cn-north-1:123456789012:location/src",\n'
    '  "DestinationLocationArn": '
    '"arn:aws:datasync:cn-north-1:123456789012:location/dst"\n}'
)


class _DatasyncSurface(Protocol):
    available: bool
    wire_epoch: str | None
    credential_source: str | None
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
    application_id_persistence: str | None
    failover_supported: bool
    cancel_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool
    poll_deadline: bool


class _Surface(Protocol):
    datasync: _DatasyncSurface


def _normal_authored() -> YamlObject:
    return {
        "name": "nightly-transfer",
        "sourceLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/source"
        ),
        "destinationLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/destination"
        ),
        "cloudWatchLogGroupArn": (
            "arn:aws:logs:cn-north-1:123456789012:log-group:datasync"
        ),
    }


def _normal_native() -> YamlObject:
    return {"jsonFormat": False, **_normal_authored()}


def _raw_authored() -> YamlObject:
    return {"jsonFormat": True, "json": _RAW_JSON}


def _spec(version: str, params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    task_name = (
        cast("str", params["name"])
        if params.get("jsonFormat") is not True and isinstance(params.get("name"), str)
        else "raw-datasync"
    )
    return validate_workflow_document(
        {
            "workflow": {"name": f"datasync-{version}"},
            "tasks": [
                {
                    "name": task_name,
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([41_000])["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert definition["taskType"] == _TASK_TYPE
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _decode(
    version: str,
    params: YamlObject,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _fake_dag(
    version: str,
    params: YamlObject,
    *,
    workflow_name: str = "datasync-edit",
    task_name: str = "nightly-transfer",
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name=task_name,
        project_code_value=7,
        project_name_value="analytics",
        task_type_value=_TASK_TYPE,
        task_params_value=json.dumps(params),
        worker_group_value="default",
    )
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )


def _edit_plan(
    version: str,
    params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dag(version, _normal_native())
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "datasync-edit", "project": "analytics"},
            "tasks": [
                {"name": "nightly-transfer", "type": _TASK_TYPE, "task_params": params}
            ],
        },
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    params = json.loads(definition["taskParams"])
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _template_yaml(version: str, variant: str) -> str:
    return authoring_prep.template_yaml(_TASK_TYPE, version, variant=variant)


def _task_params_schema(version: str) -> dict[object, object]:
    return authoring_prep.task_params_schema(_TASK_TYPE, version)


def _resolve_schema(
    root: dict[object, object],
    schema: dict[object, object],
) -> dict[object, object]:
    reference = schema.get("$ref")
    if not isinstance(reference, str):
        return schema
    current = root
    for part in reference.removeprefix("#/").split("/"):
        child = current[part]
        assert isinstance(child, dict)
        current = child
    return current


def _guidance(version: str) -> str:
    return authoring_prep.schema_guidance(_TASK_TYPE, version)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_catalog_exposes_exact_normal_and_raw_public_modes(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)
    review = catalog.task_type_facts[_TASK_TYPE].typed_authoring_review

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is True
    assert profile.category == "Other"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert review is not None
    assert (
        catalog.task_type_facts[_TASK_TYPE].semantic_fingerprint
        == _FINGERPRINTS[version]
    )
    assert review.semantic_fingerprint == _FINGERPRINTS[version]
    assert review.review == "datasync-normal-and-raw-json-exact-public-modes"
    assert membership.contract.review == review.review
    assert membership.contract.family == "datasync-create-and-execute-v1"
    assert membership.contract.params_model is not None
    assert membership.contract.params_model.__name__ == "DatasyncTaskParamsSpec"
    assert membership.contract.opaque_authoring_selector is not None
    assert membership.contract.restrict_opaque_authoring_to_selector is True
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_datasync_is_upstream_absent_before_3_2(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert _TASK_TYPE not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is False
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert _TASK_TYPE not in catalog.authoring_task_types


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_normal_public_create_defaults_false_and_compiles_exact_wire(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    authored = _normal_authored()
    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == _normal_native()
    assert normalized is not authored
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(normalized)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert projected.task_params == _normal_native()
    assert _compiled(version, authored) == _normal_native()
    decoded, source = _decode(version, _normal_native())
    assert decoded == _normal_native()
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_raw_public_create_is_selector_bound_opaque_identity(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    raw = _raw_authored()

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=raw,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_EDIT,
            task_params=raw,
        )
        is TaskAuthoringIntent.OPAQUE_EDIT
    )
    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        raw,
        intent=TaskAuthoringIntent.OPAQUE_CREATE,
    )
    assert normalized == raw
    assert normalized is not raw
    assert _compiled(version, raw) == raw
    decoded, source = _decode(version, raw)
    assert decoded == raw
    assert source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_raw_unknown_filter_type_string_stays_opaque_identity(
    version: str,
) -> None:
    raw_json = (
        '{\n  "Name": "raw-transfer",\n'
        '  "SourceLocationArn": '
        '"arn:aws:datasync:cn-north-1:123456789012:location/src",\n'
        '  "DestinationLocationArn": '
        '"arn:aws:datasync:cn-north-1:123456789012:location/dst",\n'
        '  "Includes": [{"FilterType": "FUTURE_FILTER", "Value": "/future"}]\n}'
    )
    raw: YamlObject = {"jsonFormat": True, "json": raw_json}

    assert _compiled(version, raw) == raw
    decoded, source = _decode(version, raw)
    assert decoded == raw
    assert decoded["json"] == raw_json
    assert source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datasync_raw_public_edit_preserves_authored_json_bytes(
    version: str,
    input_mode: str,
) -> None:
    raw = _raw_authored()

    plan = _edit_plan(version, raw, input_mode=input_mode)

    assert _compiled_from_plan(plan) == raw


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_datasync_explicit_opaque_authoring_is_restricted_to_raw_json_selector(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog(version)
    raw = _raw_authored()

    assert catalog.normalize_task_params(_TASK_TYPE, raw, intent=intent) == raw
    with pytest.raises(UnsupportedFeatureError, match="create_and_execute"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            _normal_native(),
            intent=intent,
        )


@pytest.mark.parametrize(
    "params",
    [
        {"jsonFormat": True},
        {"json": _RAW_JSON},
        {"jsonFormat": True, "json": ""},
        {"jsonFormat": True, "json": None},
        {"jsonFormat": True, "json": "   \n"},
        {"jsonFormat": True, "json": "not-json"},
        {"jsonFormat": True, "json": "[]"},
        {"jsonFormat": True, "json": '"string"'},
        {"jsonFormat": True, "json": "null"},
        {"jsonFormat": True, "json": _RAW_JSON, "name": "sibling"},
        {"jsonFormat": True, "json": _RAW_JSON, "localParams": []},
        {"jsonFormat": True, "json": _RAW_JSON, "futureField": True},
        {"jsonFormat": False, "json": _RAW_JSON, **_normal_authored()},
    ],
)
@pytest.mark.parametrize(
    "requested",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_datasync_invalid_raw_shape_fails_closed_without_opaque_fallback(
    params: YamlObject,
    requested: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=requested,
            task_params=params,
        )
        is requested
    )
    with pytest.raises(ValueError):
        catalog.normalize_task_params(_TASK_TYPE, params, intent=requested)


@pytest.mark.parametrize("value", [1, 0, "true", "false", None, [], {}])
def test_datasync_json_format_is_a_strict_boolean(value: YamlValue) -> None:
    params = _normal_authored()
    params["jsonFormat"] = value

    with pytest.raises(ValueError, match="jsonFormat"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("field_name", sorted(_NORMAL_REQUIRED_FIELDS))
def test_datasync_normal_mode_requires_all_three_identity_fields(
    field_name: str,
) -> None:
    params = _normal_authored()
    params.pop(field_name)

    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        with pytest.raises(ValueError, match=field_name):
            get_task_authoring_catalog("3.4.2").normalize_task_params(
                _TASK_TYPE,
                params,
                intent=intent,
            )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("name", ""),
        ("name", None),
        ("sourceLocationArn", " "),
        ("sourceLocationArn", None),
        ("destinationLocationArn", "${destination}"),
        ("destinationLocationArn", None),
        ("cloudWatchLogGroupArn", "$[biz.date]"),
        ("cloudWatchLogGroupArn", ""),
        ("cloudWatchLogGroupArn", None),
        ("name", "line\nbreak"),
        ("sourceLocationArn", "nul\x00byte"),
        ("destinationLocationArn", "delete\x7fbyte"),
        ("cloudWatchLogGroupArn", "control\x85byte"),
        ("name", "surrogate\ud800"),
    ],
)
def test_datasync_normal_literal_fields_reject_blank_placeholder_and_controls(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _normal_authored()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("resources", []),
        ("Options", {}),
        ("Schedule", {}),
        ("Includes", []),
        ("Excludes", []),
        ("Tags", []),
        ("futureField", {"native": True}),
    ],
)
def test_datasync_typed_normal_authoring_rejects_unowned_state(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _normal_authored()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datasync_normal_public_edit_compiles_only_the_owned_wire(
    version: str,
    input_mode: str,
) -> None:
    params: YamlObject = {
        "name": "nightly-transfer-v2",
        "sourceLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/source-v2"
        ),
        "destinationLocationArn": (
            "arn:aws:datasync:cn-north-1:123456789012:location/destination-v2"
        ),
    }

    plan = _edit_plan(version, params, input_mode=input_mode)

    assert _compiled_from_plan(plan) == {"jsonFormat": False, **params}


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_datasync_projector_rejects_versions_without_the_plugin(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError, match=_TASK_TYPE):
        encode_task_parameters(
            version=version,
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", _normal_native()),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "params",
    [
        {"jsonFormat": True, "json": "[]"},
        {"jsonFormat": True, "json": _RAW_JSON, "futureField": True},
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_datasync_explicit_opaque_mode_rejects_near_raw_payloads(
    params: YamlObject,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError, match="create_and_execute"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=intent,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_fields_and_state_rules_make_both_public_modes_discoverable(
    version: str,
) -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"].removeprefix("task_params."): field
        for field in result.data["fields"]
        if isinstance(field, dict)
        and isinstance(field.get("path"), str)
        and field["path"].startswith("task_params.")
    }
    rules = {
        rule["when"]: rule
        for rule in result.data["state_rules"]
        if isinstance(rule, dict) and isinstance(rule.get("when"), str)
    }

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Other"
    assert result.data["kind"] == "typed"
    assert set(fields) == _FIELDS
    assert fields["jsonFormat"]["required"] is False
    assert fields["jsonFormat"]["default"] is False
    assert fields["jsonFormat"]["type"] == "boolean"
    for field_name in _NORMAL_REQUIRED_FIELDS | {"json"}:
        assert fields[field_name]["required"] is True
        assert fields[field_name]["active_when"]
    assert fields["cloudWatchLogGroupArn"]["required"] is False
    for field_name in _FIELDS:
        assert fields[field_name]["compile_path"] == (
            f"taskDefinitionJson[].taskParams.{field_name}"
        )

    assert set(rules) == {
        "task_params.jsonFormat == false",
        "task_params.jsonFormat == true",
    }
    normal = rules["task_params.jsonFormat == false"]
    raw = rules["task_params.jsonFormat == true"]
    assert normal["condition_paths"] == ["task_params.jsonFormat"]
    assert set(normal["active_paths"]) == {
        "task_params.name",
        "task_params.sourceLocationArn",
        "task_params.destinationLocationArn",
        "task_params.cloudWatchLogGroupArn",
    }
    assert normal["inactive_paths"] == ["task_params.json"]
    assert raw["condition_paths"] == ["task_params.jsonFormat"]
    assert raw["active_paths"] == ["task_params.json"]
    assert set(raw["inactive_paths"]) == {
        "task_params.name",
        "task_params.sourceLocationArn",
        "task_params.destinationLocationArn",
        "task_params.cloudWatchLogGroupArn",
    }


@pytest.mark.parametrize("version", ["3.2.0", "3.2.2", "3.4.2"])
def test_datasync_json_schema_is_closed_and_discriminates_normal_from_raw(
    version: str,
) -> None:
    task_params = _task_params_schema(version)
    properties = task_params["properties"]
    variants = task_params["oneOf"]
    assert isinstance(properties, dict)
    assert isinstance(variants, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _FIELDS
    json_format = _resolve_schema(
        task_params,
        cast("dict[object, object]", properties["jsonFormat"]),
    )
    assert json_format["type"] == "boolean"
    assert json_format["default"] is False
    by_mode = {
        variant["properties"]["jsonFormat"]["const"]: variant for variant in variants
    }
    assert set(by_mode) == {False, True}
    assert set(by_mode[False]["required"]) == _NORMAL_REQUIRED_FIELDS
    assert set(by_mode[True]["required"]) == {"jsonFormat", "json"}
    for field_name in (
        "name",
        "sourceLocationArn",
        "destinationLocationArn",
        "cloudWatchLogGroupArn",
        "json",
    ):
        field_schema = _resolve_schema(
            task_params,
            cast("dict[object, object]", properties[field_name]),
        )
        assert field_schema["type"] == "string"
        assert field_schema["minLength"] == 1
        assert "anyOf" not in field_schema


@pytest.mark.parametrize("version", ["3.2.0", "3.2.2", "3.4.2"])
def test_datasync_summary_and_compile_mappings_publish_both_modes(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    result = task_type_schema_result(
        _TASK_TYPE,
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["category"] == "Other"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == ["raw-json"]
    assert set(mappings) == {f"task_params.{field}" for field in _FIELDS}
    for field_name in _FIELDS:
        assert mappings[f"task_params.{field_name}"] == (
            f"taskDefinitionJson[].taskParams.{field_name}"
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("variant", ["minimal", "raw-json"])
def test_datasync_templates_compile_without_rewriting_selected_mode(
    version: str,
    variant: str,
) -> None:
    yaml_text = _template_yaml(version, variant)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == _TASK_TYPE
    if variant == "minimal":
        assert params["jsonFormat"] is False
        assert set(params).issubset(_FIELDS - {"json"})
        assert set(params).issuperset(_NORMAL_REQUIRED_FIELDS | {"jsonFormat"})
        assert task["name"] == params["name"]
        assert _compiled(version, cast("YamlObject", params)) == params
    else:
        assert set(params) == {"jsonFormat", "json"}
        assert params["jsonFormat"] is True
        raw_json = params["json"]
        assert isinstance(raw_json, str)
        assert isinstance(json.loads(raw_json), dict)
        assert _compiled(version, cast("YamlObject", params)) == params


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datasync_template_index_and_resolved_category_are_other(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    index = task_template_types_result(catalog=catalog)
    template = task_template_result(
        _TASK_TYPE,
        catalog=catalog,
    )
    assert isinstance(index.data, dict)
    assert isinstance(template.data, dict)
    rows = [
        row
        for row in index.data["rows"]
        if isinstance(row, dict) and row.get("task_type") == _TASK_TYPE
    ]

    assert index.resolved == {"mode": "index"}
    assert index.data["task_types_by_category"]["Other"].count(_TASK_TYPE) == 1
    assert _TASK_TYPE not in index.data["task_types_by_category"]["Cloud"]
    assert rows == [
        {
            "task_type": _TASK_TYPE,
            "kind": "typed",
            "category": "Other",
            "variants": ["raw-json"],
            "next_command": "dsctl task-type get DATASYNC",
        }
    ]
    assert template.data["template"]["category"] == "Other"
    assert template.resolved["task_category"] == "Other"


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "native",
    [
        _normal_native(),
        {
            **_normal_authored(),
            "jsonFormat": False,
            "localParams": [],
            "resourceList": [],
        },
        {**_normal_authored(), "localParams": [], "resourceList": []},
        {
            **_normal_authored(),
            "jsonFormat": False,
            "cloudWatchLogGroupArn": "",
            "localParams": [],
            "resourceList": [],
        },
    ],
)
def test_datasync_decode_canonicalizes_safe_ui_normal_wire_as_typed(
    version: str,
    native: YamlObject,
) -> None:
    decoded, source = _decode(version, native)
    expected = _normal_native()
    if native.get("cloudWatchLogGroupArn") == "":
        expected.pop("cloudWatchLogGroupArn")

    assert decoded == expected
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    "native",
    [
        {**_normal_native(), "localParams": [{"prop": "ignored"}]},
        {**_normal_native(), "resourceList": [{"id": 9}]},
        {**_normal_native(), "varPool": []},
        {**_normal_native(), "Options": {"VerifyMode": "ONLY_FILES_TRANSFERRED"}},
        {**_normal_native(), "futureField": {"native": True}},
    ],
)
def test_datasync_richer_normal_native_wire_is_opaque_preserve_only(
    native: YamlObject,
) -> None:
    decoded, source = _decode("3.4.2", native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters_with_provenance(
            version="3.4.2",
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", deepcopy(native)),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


def test_datasync_raw_json_is_never_reclassified_as_typed_on_decode() -> None:
    raw = _raw_authored()
    decoded, source = _decode("3.4.2", raw)

    assert decoded == raw
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters_with_provenance(
            version="3.4.2",
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", deepcopy(raw)),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    "params",
    [
        {"jsonFormat": True, "json": "[]"},
        {"jsonFormat": True, "json": _RAW_JSON, "futureField": True},
        {"jsonFormat": False, "json": _RAW_JSON, **_normal_authored()},
        {**_normal_authored(), "localParams": []},
    ],
)
def test_datasync_invalid_public_create_and_edit_never_fall_open(
    input_mode: str,
    params: YamlObject,
) -> None:
    with pytest.raises(ValueError):
        _compiled("3.4.2", params)
    with pytest.raises(UserInputError):
        _edit_plan("3.4.2", params, input_mode=input_mode)


def test_datasync_opaque_preserve_deep_copies_unknown_native_state() -> None:
    native: YamlObject = {
        **_normal_native(),
        "Schedule": {"ScheduleExpression": "rate(1 day)"},
        "futureField": {"nested": ["native", {"preserve": True}]},
    }
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == expected
    assert preserved is not native
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    assert preserved == expected


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_normal_native(), _normal_native(), ProjectionSource.TYPED_AUTHORING),
        (_raw_authored(), _raw_authored(), ProjectionSource.OPAQUE_PRESERVE),
        (
            {**_normal_native(), "futureField": {"native": True}},
            {**_normal_native(), "futureField": {"native": True}},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_datasync_export_distinguishes_typed_normal_and_opaque_native_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version("3.4.2")
    baseline = workflow_live_baseline(
        _fake_dag("3.4.2", native, workflow_name="datasync-export"),
        project=project,
        catalog=catalog,
    )

    assert baseline.projection_sources["nightly-transfer"] is source
    assert baseline.spec.tasks[0].task_params == exported


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_datasync_runtime_surface_is_absent_before_3_2(version: str) -> None:
    surface = cast("_Surface", get_task_authoring_surface(version)).datasync

    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.credential_source is None
    assert surface.credential_keys == ()
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.local_params_consumed is False
    assert surface.task_params_logged is False
    assert surface.converted_task_params_logged is False
    assert surface.credentials_logged is False
    assert surface.secret_storage_supported is False
    assert surface.cli_secret_detection is False
    assert surface.cli_secret_redaction is False
    assert surface.raw_json_model is None
    assert surface.raw_json_unknown_fields_ignored is False
    assert surface.raw_json_inherited_parameter_unknown_enum_becomes_null is False
    assert surface.options_effective is False
    assert surface.includes_behavior is None
    assert surface.schedule_persists_recurring_task is False
    assert surface.fresh_run_calls == ()
    assert surface.remote_task_deleted is False
    assert surface.client_closed is False
    assert surface.result_output_supported is False
    assert surface.application_id_field is None
    assert surface.application_id_persistence is None
    assert surface.failover_supported is False
    assert surface.cancel_supported is False
    assert surface.retry_may_resubmit is False
    assert surface.callback_persistence_gap is False
    assert surface.poll_deadline is False


@pytest.mark.parametrize(
    ("version", "credential_source", "credential_keys"),
    [
        *[
            (version, "worker-properties-static-basic", _LEGACY_CREDENTIAL_KEYS)
            for version in ("3.2.0", "3.2.1", "3.2.2")
        ],
        *[
            (version, "aws-authentication-static-basic", _MODERN_CREDENTIAL_KEYS)
            for version in ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2")
        ],
    ],
)
def test_datasync_runtime_surface_locks_execution_and_credential_epochs(
    version: str,
    credential_source: str,
    credential_keys: tuple[str, ...],
) -> None:
    surface = cast("_Surface", get_task_authoring_surface(version)).datasync

    assert surface.available is True
    assert surface.wire_epoch == "create-and-execute-normal-and-raw-json"
    assert surface.credential_source == credential_source
    assert surface.credential_keys == credential_keys
    assert surface.parameter_substitution is False
    assert surface.resource_files_supported is False
    assert surface.local_params_consumed is False
    assert surface.task_params_logged is True
    assert surface.converted_task_params_logged is True
    assert surface.credentials_logged is False
    assert surface.secret_storage_supported is False
    assert surface.cli_secret_detection is False
    assert surface.cli_secret_redaction is False
    assert surface.raw_json_model == "DatasyncParameters"
    assert surface.raw_json_unknown_fields_ignored is True
    assert surface.raw_json_inherited_parameter_unknown_enum_becomes_null is True
    assert surface.options_effective is False
    assert surface.includes_behavior == "copied-to-excludes-and-overwrites"
    assert surface.schedule_persists_recurring_task is True
    assert surface.fresh_run_calls == ("CreateTask", "StartTaskExecution")
    assert surface.remote_task_deleted is False
    assert surface.client_closed is False
    assert surface.result_output_supported is False
    assert surface.application_id_field == "taskExecutionArn"
    assert surface.application_id_persistence == "appIds-callback"
    assert surface.failover_supported is True
    assert surface.cancel_supported is True
    assert surface.retry_may_resubmit is True
    assert surface.callback_persistence_gap is True
    assert surface.poll_deadline is False


@pytest.mark.parametrize("version", ["3.2.0", "3.2.2", "3.4.2"])
def test_datasync_guidance_discloses_public_modes_security_and_lifecycle(
    version: str,
) -> None:
    guidance = _guidance(version)
    templates = " ".join(
        _template_yaml(version, variant).lower() for variant in ("minimal", "raw-json")
    )

    for term in (
        "normal",
        "raw json",
        "jsonformat",
        "datasyncparameters",
        "uppercamelcase",
        "unknown fields",
        "ignored",
        "unknown enum values",
        "inherited",
        "localparams/varpool",
        "property",
        "direct/type",
        "become null",
        "enum-like strings",
        "not enum-validated",
        "null-converted",
        "filtertype",
        "aws validation",
        "options",
        "ineffective",
        "includes",
        "excludes",
        "schedule",
        "recurring",
        "localparams",
        "runtime-dead",
        "task params",
        "converted",
        "info",
        "not secret storage",
        "does not detect or redact",
        "createtask",
        "starttaskexecution",
        "does not delete",
        "client",
        "does not close",
        "taskexecutionarn",
        "appids",
        "callback",
        "failover",
        "resume",
        "cancel",
        "retry",
        "duplicate",
        "leak",
        "deadline",
        "no structured output",
    ):
        assert term in guidance
        assert term in templates


@pytest.mark.parametrize(
    ("version", "credential_keys"),
    [
        ("3.2.0", _LEGACY_CREDENTIAL_KEYS),
        ("3.2.2", _LEGACY_CREDENTIAL_KEYS),
        ("3.3.1", _MODERN_CREDENTIAL_KEYS),
        ("3.4.2", _MODERN_CREDENTIAL_KEYS),
    ],
)
def test_datasync_guidance_names_exact_worker_credential_keys(
    version: str,
    credential_keys: tuple[str, ...],
) -> None:
    guidance = _guidance(version)
    template = _template_yaml(version, "minimal").lower()

    for key in credential_keys:
        assert key.lower() in guidance
        assert key.lower() in template


def test_datasync_guidance_explains_ui_name_coupling() -> None:
    guidance = _guidance("3.4.2")
    template = _template_yaml("3.4.2", "minimal").lower()

    for text in (guidance, template):
        assert "outer task name" in text
        assert "same ui model" in text
        assert "create" in text
        assert "edit" in text
        assert "rest" in text
        assert "independent ui field" not in text
