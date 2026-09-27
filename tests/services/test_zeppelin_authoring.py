from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import (
    WorkflowMutationPlan,
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_ZEPPELIN_FACET = "ZEPPELIN/paragraph"
_ZEPPELIN_VERSIONS = (
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_ZEPPELIN_ABSENT_VERSIONS = ("1.3.9", "2.0.0", "2.0.9")
_WORKER_CONFIG_VERSIONS = ("3.0.0", "3.0.6")
_REST_ENDPOINT_VERSIONS = ("3.1.0", "3.1.9", "3.2.0")
_DATASOURCE_VERSIONS = (
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_PARAMETER_VERSIONS = _REST_ENDPOINT_VERSIONS + _DATASOURCE_VERSIONS
_ZEPPELIN_FINGERPRINTS = {
    "3.0.0": "sha256:40cec726a11579e7dddb146ab9cb9a8657fc3369a55a59472babfcbe7a160221",
    "3.0.6": "sha256:40cec726a11579e7dddb146ab9cb9a8657fc3369a55a59472babfcbe7a160221",
    "3.1.0": "sha256:321e5f2975b37312fcb127ceccbc061c868c3ed4bc904655b65be748321c2829",
    "3.1.9": "sha256:321e5f2975b37312fcb127ceccbc061c868c3ed4bc904655b65be748321c2829",
    "3.2.0": "sha256:cc7d1e675961288bf9d815b1329d82d0eb06ab0fb46be3d763571e6763df4b26",
    "3.2.1": "sha256:9f9e99e03f0eb72e895a1b92c4be53eb557150022ec444cb509a47b35f4de029",
    "3.2.2": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
    "3.3.1": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
    "3.3.2": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
    "3.4.0": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
    "3.4.1": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
    "3.4.2": "sha256:447fd6b189d5ce9a87dfc94d560d21046b688136c28d6d46b274df2d6314532d",
}
_COMMON_TYPED_FIELDS = {"noteId", "paragraphId", "connectionMode", "parameters"}
_EMPTY_REFS = TaskRefIndex.from_code_by_name({})


def _connection_mode(ds_version: str) -> str:
    if ds_version in _WORKER_CONFIG_VERSIONS:
        return "WORKER_CONFIG"
    if ds_version in _REST_ENDPOINT_VERSIONS:
        return "REST_ENDPOINT"
    return "DATASOURCE"


def _canonical_params(
    ds_version: str,
    *,
    parameters: YamlObject | None = None,
) -> YamlObject:
    task_params: YamlObject = {
        "noteId": "2FZ4VC2MX",
        "paragraphId": "paragraph-1",
        "connectionMode": _connection_mode(ds_version),
        "parameters": {} if parameters is None else parameters,
    }
    if ds_version in _REST_ENDPOINT_VERSIONS:
        task_params["restEndpoint"] = "https://zeppelin.example.com"
    elif ds_version in _DATASOURCE_VERSIONS:
        task_params["datasource"] = 17
    return task_params


def _expected_native_params(
    ds_version: str,
    canonical: YamlObject,
) -> YamlObject:
    native: YamlObject = {
        "noteId": canonical["noteId"],
        "paragraphId": canonical["paragraphId"],
    }
    if ds_version in _REST_ENDPOINT_VERSIONS:
        native["restEndpoint"] = canonical["restEndpoint"]
    elif ds_version in _DATASOURCE_VERSIONS:
        native["datasource"] = canonical["datasource"]
        native["type"] = "ZEPPELIN"
    parameters = canonical.get("parameters")
    if isinstance(parameters, dict) and parameters:
        native["parameters"] = json.dumps(
            parameters,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    return native


def _zeppelin_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    return validate_workflow_document(
        {
            "workflow": {"name": f"zeppelin-{ds_version}"},
            "tasks": [
                {
                    "name": "run-notebook-paragraph",
                    "type": "ZEPPELIN",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog(ds_version),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    prepared = prepare_workflow_create_compilation(
        _zeppelin_spec(task_params, ds_version=ds_version),
        catalog=get_task_authoring_catalog(ds_version),
    )
    payload = prepared.materialize([32_100])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "ZEPPELIN"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=ds_version,
        task_type="ZEPPELIN",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = decode_task_parameters(
        version=ds_version,
        task_type="ZEPPELIN",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _template_yaml(ds_version: str, variant: str) -> str:
    if variant == "params":
        return authoring_prep.parameter_example_yaml("ZEPPELIN", ds_version)
    return authoring_prep.template_yaml("ZEPPELIN", ds_version, variant=variant)


def _template_task(ds_version: str, variant: str) -> YamlObject:
    document = yaml.safe_load(_template_yaml(ds_version, variant))
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _opaque_native_params(ds_version: str) -> YamlObject:
    native: YamlObject = {
        "noteId": "native-note",
        "paragraphId": "native-paragraph",
        "localParams": [
            {
                "prop": "runtime",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/native/zeppelin.json",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "appIds": "native-note-native-paragraph",
        "futureField": {"nested": ["native"]},
    }
    if ds_version in _WORKER_CONFIG_VERSIONS:
        return native

    native.pop("paragraphId")
    native["productionNoteDirectory"] = "/production/clone/"
    native["parameters"] = '{"run_date":"${native_day}"}'
    if ds_version in _REST_ENDPOINT_VERSIONS:
        native["restEndpoint"] = "https://native.example.com?opaque=true"
        if ds_version == "3.2.0":
            native["username"] = "native-user"
            native["password"] = "native-password"
    else:
        native["datasource"] = 91
        native["type"] = "ZEPPELIN"
    return native


def _zeppelin_edit_plan(
    ds_version: str,
    task_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    baseline_params = _opaque_native_params(ds_version)
    task = FakeTaskDefinition(
        code=101,
        name="native-zeppelin",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="ZEPPELIN",
        task_params_value=json.dumps(baseline_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="zeppelin-edit",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "native-zeppelin"},
                                "set": {"task_params": task_params},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
    desired = validate_workflow_document(
        {
            "workflow": {"name": "zeppelin-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "native-zeppelin",
                    "type": "ZEPPELIN",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )
    return prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )


def _resolved_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    return authoring_prep.resolved_field_schema(task_params_schema, field_name)


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_catalog_exposes_one_exact_reviewed_paragraph_facet(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("ZEPPELIN")
    membership = catalog.require_facet("ZEPPELIN", _ZEPPELIN_FACET)
    fact = catalog.task_type_facts["ZEPPELIN"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("ZEPPELIN") is True
    assert catalog.supports_opaque_authoring("ZEPPELIN") is True
    assert profile.category == "Other"
    assert profile.default_facet == _ZEPPELIN_FACET
    assert set(profile.facets) == {_ZEPPELIN_FACET}
    assert fact.semantic_fingerprint == _ZEPPELIN_FINGERPRINTS[ds_version]
    assert source_review is not None
    assert source_review.review == (
        "zeppelin-paragraph-literal-params-exact-projection"
    )
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_schema_is_closed_and_selects_one_exact_connection_epoch(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "ZEPPELIN",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_fields = {
        path.removeprefix("task_params."): field
        for path, field in fields.items()
        if path.startswith("task_params.")
    }
    expected_fields = set(_COMMON_TYPED_FIELDS)
    if ds_version in _REST_ENDPOINT_VERSIONS:
        expected_fields.add("restEndpoint")
    elif ds_version in _DATASOURCE_VERSIONS:
        expected_fields.add("datasource")

    assert result.data["task_type"] == "ZEPPELIN"
    assert result.data["category"] == "Other"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == expected_fields
    assert task_param_fields["noteId"]["required"] is True
    assert task_param_fields["paragraphId"]["required"] is True
    assert task_param_fields["connectionMode"]["required"] is True
    assert task_param_fields["connectionMode"]["choices"] == [
        _connection_mode(ds_version)
    ]
    assert "compile_path" not in task_param_fields["connectionMode"]
    assert task_param_fields["parameters"]["required"] is False
    assert task_param_fields["parameters"]["default"] == {}
    if ds_version in _WORKER_CONFIG_VERSIONS:
        assert "compile_path" not in task_param_fields["parameters"]
    else:
        assert task_param_fields["parameters"]["compile_path"].endswith(
            "taskParams.parameters"
        )
    if ds_version in _REST_ENDPOINT_VERSIONS:
        assert task_param_fields["restEndpoint"]["required"] is True
        assert task_param_fields["restEndpoint"]["compile_path"].endswith(
            "taskParams.restEndpoint"
        )
    elif ds_version in _DATASOURCE_VERSIONS:
        assert task_param_fields["datasource"]["required"] is True
        assert task_param_fields["datasource"]["compile_path"].endswith(
            "taskParams.datasource"
        )

    assert result.data["state_rules"] == []

    descriptions = (
        " ".join(
            str(field.get("description", "")) for field in task_param_fields.values()
        )
        .lower()
        .replace("-", " ")
    )
    assert "paragraph" in descriptions
    assert "whole note" in descriptions
    assert "clone" in descriptions
    assert "placeholder" in descriptions
    assert "failover" in descriptions
    if ds_version in _WORKER_CONFIG_VERSIONS:
        assert "zeppelin.rest.url" in descriptions
    elif ds_version in _REST_ENDPOINT_VERSIONS:
        assert "rest endpoint" in descriptions or "restendpoint" in descriptions
    else:
        assert "datasource" in descriptions
        assert "credential" in descriptions


@pytest.mark.parametrize(
    "ds_version",
    ["3.0.0", "3.1.0", "3.2.0", "3.2.1", "3.4.2"],
)
def test_zeppelin_json_schema_matches_the_exact_closed_projection(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "ZEPPELIN",
        json_schema=True,
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)

    expected_properties = set(_COMMON_TYPED_FIELDS)
    expected_required = {"noteId", "paragraphId", "connectionMode"}
    if ds_version in _REST_ENDPOINT_VERSIONS:
        expected_properties.add("restEndpoint")
        expected_required.add("restEndpoint")
    elif ds_version in _DATASOURCE_VERSIONS:
        expected_properties.add("datasource")
        expected_required.add("datasource")

    assert task_params["additionalProperties"] is False
    assert set(task_params["required"]) == expected_required
    assert set(properties) == expected_properties
    mode_schema = _resolved_property_schema(task_params, "connectionMode")
    assert mode_schema["enum"] == [_connection_mode(ds_version)]
    for field_name in ("noteId", "paragraphId"):
        identity_schema = _resolved_property_schema(task_params, field_name)
        assert identity_schema["type"] == "string"
        assert identity_schema["minLength"] == 1
        identity_pattern = identity_schema["pattern"]
        assert identity_pattern == r"^(?!\.{1,2}$)[A-Za-z0-9._~-]+$"
        assert isinstance(identity_pattern, str)
        assert re.search(identity_pattern, ".") is None
        assert re.search(identity_pattern, "..") is None
    parameters_schema = _resolved_property_schema(task_params, "parameters")
    assert parameters_schema["type"] == "object"
    assert parameters_schema["default"] == {}
    assert parameters_schema["propertyNames"] == {
        "pattern": r"^[A-Za-z0-9_.-]+$",
        "type": "string",
    }
    if ds_version in _WORKER_CONFIG_VERSIONS:
        assert parameters_schema["maxProperties"] == 0
    else:
        value_schema = parameters_schema["additionalProperties"]
        assert isinstance(value_schema, dict)
        assert value_schema["type"] == "string"
        value_pattern = value_schema["pattern"]
        assert value_pattern == r"^(?!.*(?:\$\{|\$\[))[^\x00-\x1f\x7f]*$"
        assert isinstance(value_pattern, str)
        for invalid_value in ("${secret}", "$[yyyyMMdd]", "line\nnext"):
            assert re.search(value_pattern, invalid_value) is None
    if ds_version in _REST_ENDPOINT_VERSIONS:
        endpoint_schema = _resolved_property_schema(task_params, "restEndpoint")
        assert endpoint_schema["type"] == "string"
        assert endpoint_schema["minLength"] == 1
        endpoint_pattern = endpoint_schema["pattern"]
        assert isinstance(endpoint_pattern, str)
        assert endpoint_pattern.startswith("^https?://")
        for invalid_endpoint in (
            "HTTPS://zeppelin.example.com",
            "https://alice:secret@zeppelin.example.com",
            "https://zeppelin.example.com?token=secret",
            "https://zeppelin.example.com#fragment",
            "https://zeppelin.example.com/${tenant}",
            "https://:",
            "https://zeppelin.example.com:bad",
            "https://zeppelin.example.com:99999",
            "https://[::1",
        ):
            assert re.search(endpoint_pattern, invalid_endpoint) is None
        assert "anyOf" not in endpoint_schema
        assert "default" not in endpoint_schema
    elif ds_version in _DATASOURCE_VERSIONS:
        datasource_schema = _resolved_property_schema(task_params, "datasource")
        assert datasource_schema["anyOf"] == [
            {"minimum": 1, "type": "integer"},
            {"pattern": r"\S", "type": "string"},
        ]
        assert "default" not in datasource_schema
    for excluded_field in (
        "username",
        "password",
        "productionNoteDirectory",
        "type",
        "localParams",
        "varPool",
        "resourceList",
        "appIds",
    ):
        assert excluded_field not in properties


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_summary_and_compile_mappings_are_exactly_version_selected(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("ZEPPELIN", catalog=catalog)
    result = task_type_schema_result(
        "ZEPPELIN",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_param_mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }
    expected_required_paths = {
        "name",
        "type",
        "task_params",
        "task_params.noteId",
        "task_params.paragraphId",
        "task_params.connectionMode",
    }
    expected_mapping_fields = {"noteId", "paragraphId"}
    expected_variants: list[str] = []
    if ds_version in _REST_ENDPOINT_VERSIONS:
        expected_required_paths.add("task_params.restEndpoint")
        expected_mapping_fields.update({"restEndpoint", "parameters"})
    elif ds_version in _DATASOURCE_VERSIONS:
        expected_required_paths.add("task_params.datasource")
        expected_mapping_fields.update({"datasource", "parameters"})

    assert summary["task_type"] == "ZEPPELIN"
    assert summary["category"] == "Other"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == expected_variants
    assert set(summary["required_paths"]) == expected_required_paths
    assert set(task_param_mappings) == {
        f"task_params.{field_name}" for field_name in expected_mapping_fields
    }
    for authoring_path, payload_path in task_param_mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_minimal_templates_are_canonical_and_compile_exactly(
    ds_version: str,
) -> None:
    yaml_text = _template_yaml(ds_version, "minimal")
    task = _template_task(ds_version, "minimal")
    params = task["task_params"]
    assert isinstance(params, dict)
    expected = _canonical_params(ds_version)

    assert task["type"] == "ZEPPELIN"
    assert params == expected
    assert _compiled_task_params(ds_version, params) == _expected_native_params(
        ds_version,
        expected,
    )

    guidance = yaml_text.lower().replace("-", " ")
    assert "paragraph" in guidance
    assert "whole note" in guidance
    assert "clone" in guidance
    assert "placeholder" in guidance
    assert "failover" in guidance
    if ds_version in _WORKER_CONFIG_VERSIONS:
        assert "zeppelin.rest.url" in guidance
    elif ds_version in _REST_ENDPOINT_VERSIONS:
        assert "restendpoint" in guidance or "rest endpoint" in guidance
    else:
        assert "datasource" in guidance
        assert "credential" in guidance


@pytest.mark.parametrize("ds_version", _PARAMETER_VERSIONS)
def test_zeppelin_params_templates_use_literal_string_maps_and_compile_exactly(
    ds_version: str,
) -> None:
    yaml_text = _template_yaml(ds_version, "params")
    task = _template_task(ds_version, "params")
    params = task["task_params"]
    assert isinstance(params, dict)
    parameters = params["parameters"]
    assert isinstance(parameters, dict)

    assert task["type"] == "ZEPPELIN"
    assert parameters
    assert all(
        isinstance(key, str)
        and isinstance(value, str)
        and "${" not in key
        and "$[" not in key
        and "${" not in value
        and "$[" not in value
        for key, value in parameters.items()
    )
    expected_native = _expected_native_params(ds_version, params)
    compiled = _compiled_task_params(ds_version, params)
    assert compiled == expected_native
    raw_parameters = compiled["parameters"]
    assert isinstance(raw_parameters, str)
    assert json.loads(raw_parameters) == parameters
    assert "literal" in yaml_text.lower()


@pytest.mark.parametrize("ds_version", _WORKER_CONFIG_VERSIONS)
def test_zeppelin_params_template_is_absent_before_native_parameter_support(
    ds_version: str,
) -> None:
    with pytest.raises(UserInputError) as captured:
        task_template_result(
            "ZEPPELIN",
            variant="params",
            catalog=get_task_authoring_catalog(ds_version),
        )

    available_variants = captured.value.details["available_variants"]
    assert isinstance(available_variants, list)
    assert "params" not in available_variants


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_typed_normalization_preserves_the_canonical_intent(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(ds_version)

    normalized = get_task_authoring_catalog(ds_version).normalize_task_params(
        "ZEPPELIN",
        params,
        intent=intent,
    )

    assert normalized == params
    assert normalized is not params


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_projector_and_prepared_compile_select_the_exact_native_wire(
    ds_version: str,
) -> None:
    parameters: YamlObject = (
        {}
        if ds_version in _WORKER_CONFIG_VERSIONS
        else {"zeta": "last", "alpha": "\u4e00"}
    )
    canonical = _canonical_params(ds_version, parameters=parameters)
    expected_native = _expected_native_params(ds_version, canonical)

    encoded = _encoded_task_params(ds_version, canonical)
    assert encoded == expected_native
    if "parameters" in encoded:
        raw_parameters = encoded["parameters"]
        assert isinstance(raw_parameters, str)
        assert json.loads(raw_parameters) == parameters
    assert _decoded_task_params(ds_version, expected_native) == canonical
    assert _compiled_task_params(ds_version, canonical) == expected_native


def test_zeppelin_typed_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = _canonical_params("3.4.2")
    canonical["futureField"] = {"enabled": True}
    native = _expected_native_params("3.4.2", _canonical_params("3.4.2"))
    native["futureField"] = {"enabled": True}

    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encoded_task_params("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _decoded_task_params("3.4.2", native)


def test_zeppelin_30_typed_decode_rejects_absent_native_parameters_field() -> None:
    native: YamlObject = {
        "noteId": "2FZ4VC2MX",
        "paragraphId": "paragraph-1",
        "parameters": "",
    }

    with pytest.raises(TaskParameterProjectionError, match="parameters"):
        _decoded_task_params("3.0.0", native)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("noteId", ""),
        ("noteId", "   "),
        ("noteId", None),
        ("paragraphId", ""),
        ("paragraphId", "   "),
        ("paragraphId", None),
        ("connectionMode", ""),
        ("connectionMode", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_typed_create_and_edit_reject_blank_or_null_required_values(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.4.2")
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("noteId", "note/paragraph"),
        ("noteId", ".."),
        ("noteId", "note%2Fparagraph"),
        ("noteId", "note?token=secret"),
        ("noteId", "${note}"),
        ("paragraphId", "paragraph/child"),
        ("paragraphId", "."),
        ("paragraphId", "paragraph#fragment"),
        ("paragraphId", "$[yyyyMMdd]"),
        ("paragraphId", "paragraph\nnext"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_ids_are_conservative_single_url_path_segments(
    field: str,
    value: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.4.2")
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "rest_endpoint",
    [
        "http://127.0.0.1:8080",
        "https://zeppelin.example.com",
        "https://zeppelin.example.com:8443/api/v1",
        "https://zeppelin.example.com/tenant/acme/",
    ],
)
def test_zeppelin_rest_endpoint_accepts_absolute_http_endpoints(
    rest_endpoint: str,
) -> None:
    params = _canonical_params("3.2.0")
    params["restEndpoint"] = rest_endpoint

    assert (
        get_task_authoring_catalog("3.2.0").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == params
    )


@pytest.mark.parametrize(
    "rest_endpoint",
    [
        "zeppelin.example.com:8080",
        "ftp://zeppelin.example.com",
        "HTTPS://zeppelin.example.com",
        "https:///missing-host",
        "https://:",
        "https://zeppelin.example.com:bad",
        "https://zeppelin.example.com:99999",
        "https://[::1",
        "https://alice:secret@zeppelin.example.com",
        "https://zeppelin.example.com?token=secret",
        "https://zeppelin.example.com/#fragment",
        "https://zeppelin.example.com/${tenant}",
        "https://zeppelin.example.com/$[yyyyMMdd]",
        "https://zeppelin .example.com",
        "https://zeppelin.example.com/path with space",
        "https://zeppelin.example.com/path\nnext",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_rest_endpoint_rejects_relative_credentialed_or_dynamic_urls(
    rest_endpoint: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.2.0")
    params["restEndpoint"] = rest_endpoint

    with pytest.raises(ValueError, match="restEndpoint"):
        get_task_authoring_catalog("3.2.0").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("datasource", [0, -1, True, "", "  ", 17.0, None])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_datasource_requires_a_positive_id_or_nonblank_name(
    datasource: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.4.2")
    params["datasource"] = datasource

    with pytest.raises(ValueError, match="datasource"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("ds_version", "mode"),
    [
        ("3.0.0", "REST_ENDPOINT"),
        ("3.1.0", "WORKER_CONFIG"),
        ("3.2.0", "DATASOURCE"),
        ("3.2.1", "REST_ENDPOINT"),
        ("3.4.2", "datasource"),
    ],
)
def test_zeppelin_connection_mode_is_the_exact_profile_literal(
    ds_version: str,
    mode: str,
) -> None:
    params = _canonical_params(ds_version)
    params["connectionMode"] = mode
    params.pop("restEndpoint", None)
    params.pop("datasource", None)
    normalized_mode = mode.upper()
    if normalized_mode == "REST_ENDPOINT":
        params["restEndpoint"] = "https://zeppelin.example.com"
    elif normalized_mode == "DATASOURCE":
        params["datasource"] = 17

    with pytest.raises((ValueError, UnsupportedFeatureError), match="connectionMode"):
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "ZEPPELIN",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("ds_version", "inactive_field"),
    [
        ("3.0.0", "restEndpoint"),
        ("3.0.0", "datasource"),
        ("3.1.0", "datasource"),
        ("3.2.0", "datasource"),
        ("3.2.1", "restEndpoint"),
        ("3.4.2", "restEndpoint"),
    ],
)
@pytest.mark.parametrize("inactive_value", [None, "inactive"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_inactive_connection_fields_must_be_absent_even_when_null(
    ds_version: str,
    inactive_field: str,
    inactive_value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(ds_version)
    params[inactive_field] = inactive_value

    with pytest.raises(ValueError, match=inactive_field):
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("parameters", "field"),
    [
        (["not", "an", "object"], "parameters"),
        ({"limit": 7}, "parameters"),
        ({7: "value"}, "parameters"),
        ({"${name}": "value"}, "parameters"),
        ({"$[yyyyMMdd]": "value"}, "parameters"),
        ({"name": "${value}"}, "parameters"),
        ({"name": "$[yyyyMMdd]"}, "parameters"),
        ({"name\nnext": "value"}, "parameters"),
        ({"name": "value\tnext"}, "parameters"),
        ({"name": "value\x00next"}, "parameters"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_literal_parameters_reject_non_strings_placeholders_and_controls(
    parameters: YamlValue,
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.4.2")
    params["parameters"] = parameters

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


def test_zeppelin_literal_parameters_accept_unicode_spaces_and_punctuation() -> None:
    parameters: YamlObject = {
        "run_date": "2026-08-20",
        "display_name": "\u65e5\u7ec8 notebook: approved & ready",
    }
    params = _canonical_params("3.4.2", parameters=parameters)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "ZEPPELIN",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    compiled = _compiled_task_params("3.4.2", params)
    raw_parameters = compiled["parameters"]
    assert isinstance(raw_parameters, str)
    assert json.loads(raw_parameters) == parameters


@pytest.mark.parametrize(
    ("ds_version", "credential_logging"),
    [
        ("3.0.0", "none"),
        ("3.1.0", "none"),
        ("3.2.0", "inline-task-params"),
        ("3.2.1", "resolved-username"),
        ("3.2.2", "resolved-credentials"),
        ("3.4.2", "resolved-credentials"),
    ],
)
def test_zeppelin_surface_tracks_exact_credential_logging_epoch(
    ds_version: str,
    credential_logging: str,
) -> None:
    surface = get_task_authoring_surface(ds_version).zeppelin

    assert surface.credential_logging == credential_logging


@pytest.mark.parametrize(
    ("ds_version", "guidance"),
    [
        ("3.2.0", "raw task parameters"),
        ("3.2.1", "resolved datasource username"),
        ("3.2.2", "resolved datasource credentials"),
    ],
)
def test_zeppelin_schema_exposes_exact_credential_logging_risk(
    ds_version: str,
    guidance: str,
) -> None:
    result = task_type_schema_result(
        "ZEPPELIN",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    descriptions = " ".join(
        str(field.get("description", ""))
        for field in result.data["fields"]
        if isinstance(field, dict)
    ).lower()

    assert guidance in descriptions
    assert "does not detect or redact" in descriptions


@pytest.mark.parametrize("ds_version", _WORKER_CONFIG_VERSIONS)
def test_zeppelin_30_rejects_nonempty_parameters_before_projection(
    ds_version: str,
) -> None:
    params = _canonical_params(ds_version)
    params["parameters"] = {"run_date": "2026-08-20"}

    with pytest.raises(UnsupportedFeatureError, match="parameters"):
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "ZEPPELIN",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    with pytest.raises(TaskParameterProjectionError, match="parameters"):
        _encoded_task_params(ds_version, params)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("username", "alice"),
        ("password", "secret"),
        ("productionNoteDirectory", "/production/clone/"),
        ("type", "ZEPPELIN"),
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("appIds", "note-paragraph"),
        ("result", "runtime-result"),
        ("futureField", {"enabled": True}),
        ("username", None),
        ("productionNoteDirectory", None),
        ("futureField", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_zeppelin_typed_authoring_rejects_clone_auth_runtime_and_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params("3.4.2")
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "ZEPPELIN",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
        TaskAuthoringIntent.OPAQUE_PRESERVE,
    ],
)
def test_zeppelin_opaque_whole_note_clone_auth_and_runtime_shapes_are_deep_copied(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    native = _opaque_native_params(ds_version)
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "ZEPPELIN",
        native,
        intent=intent,
    )

    assert preserved == expected
    assert preserved is not native
    resources = native["resourceList"]
    assert isinstance(resources, list)
    resource = resources[0]
    assert isinstance(resource, dict)
    metadata = resource["futureMetadata"]
    assert isinstance(metadata, dict)
    metadata["checksum"] = "mutated"
    var_pool = native["varPool"]
    assert isinstance(var_pool, list)
    runtime = var_pool[0]
    assert isinstance(runtime, dict)
    runtime["value"] = {"state": "mutated"}
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_public_create_and_edit_select_native_opaque_shapes(
    ds_version: str,
) -> None:
    native = _opaque_native_params(ds_version)
    catalog = get_task_authoring_catalog(ds_version)

    assert _compiled_task_params(ds_version, native) == native

    document: YamlObject = {
        "workflow": {"name": f"opaque-zeppelin-{ds_version}"},
        "tasks": [
            {
                "name": "native-zeppelin",
                "type": "ZEPPELIN",
                "task_params": native,
            }
        ],
    }
    validated = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert validated.tasks[0].task_params == native


def test_zeppelin_mixed_typed_and_opaque_tasks_keep_independent_provenance() -> None:
    ds_version = "3.2.1"
    canonical = _canonical_params(ds_version, parameters={"run_mode": "typed"})
    opaque = _opaque_native_params(ds_version)
    catalog = get_task_authoring_catalog(ds_version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "mixed-zeppelin"},
            "tasks": [
                {
                    "name": "typed-paragraph",
                    "type": "ZEPPELIN",
                    "task_params": canonical,
                },
                {
                    "name": "opaque-note",
                    "type": "ZEPPELIN",
                    "task_params": opaque,
                },
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([32_101, 32_102])
    definitions = {
        definition["name"]: json.loads(definition["taskParams"])
        for definition in json.loads(payload["taskDefinitionJson"])
    }

    assert definitions["typed-paragraph"] == _expected_native_params(
        ds_version,
        canonical,
    )
    assert definitions["opaque-note"] == opaque


def test_zeppelin_invalid_canonical_create_does_not_downgrade_to_opaque() -> None:
    params = _canonical_params("3.4.2")
    params["futureField"] = {"native": True}

    with pytest.raises(ValueError, match="futureField"):
        _compiled_task_params("3.4.2", params)


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.1.0", "3.2.1"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_zeppelin_typed_edits_keep_per_task_projection_provenance(
    ds_version: str,
    input_mode: str,
) -> None:
    canonical = _canonical_params(
        ds_version,
        parameters=(
            {} if ds_version in _WORKER_CONFIG_VERSIONS else {"run_mode": "patched"}
        ),
    )
    plan = _zeppelin_edit_plan(ds_version, canonical, input_mode=input_mode)

    payload = plan.compilation.materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == _expected_native_params(ds_version, canonical)
    assert "connectionMode" not in compiled


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_zeppelin_invalid_canonical_edits_do_not_downgrade_to_opaque(
    input_mode: str,
) -> None:
    params = _canonical_params("3.2.1")
    params["futureField"] = {"native": True}

    with pytest.raises(UserInputError, match="futureField"):
        _zeppelin_edit_plan("3.2.1", params, input_mode=input_mode)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_zeppelin_reviewed_opaque_edits_preserve_native_shape(
    input_mode: str,
) -> None:
    native = _opaque_native_params("3.2.1")
    native["futureField"] = {"edited": ["still-native"]}

    plan = _zeppelin_edit_plan("3.2.1", native, input_mode=input_mode)
    payload = plan.compilation.materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native


@pytest.mark.parametrize("ds_version", _ZEPPELIN_VERSIONS)
def test_zeppelin_opaque_export_and_unchanged_patch_round_trip_losslessly(
    ds_version: str,
) -> None:
    native_params = _opaque_native_params(ds_version)
    task = FakeTaskDefinition(
        code=101,
        name="native-zeppelin",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="ZEPPELIN",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="zeppelin-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert document["tasks"][0]["task_params"] == native_params

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native_params


@pytest.mark.parametrize("ds_version", _ZEPPELIN_ABSENT_VERSIONS)
def test_zeppelin_is_unavailable_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("ZEPPELIN") is False
    assert catalog.supports_opaque_authoring("ZEPPELIN") is False
    assert "ZEPPELIN" not in catalog.authoring_task_types
