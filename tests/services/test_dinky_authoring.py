from __future__ import annotations

import json
import re
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
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
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
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject


_DINKY_FACET = "DINKY/job_trigger"
_DINKY_VERSIONS = (
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
_DINKY_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_DINKY_FINGERPRINTS = {
    "3.1.0": "sha256:f6047316ab965a5c3d4b8b01f741ce3334fd59a694b9b32f473b18d0900c1001",
    "3.1.9": "sha256:f6047316ab965a5c3d4b8b01f741ce3334fd59a694b9b32f473b18d0900c1001",
    "3.2.0": "sha256:253c03d0b57ebfcb5d97b3bd6fda8e4f86e7e7ba18c9f228ec315192d9023b75",
    "3.2.1": "sha256:253c03d0b57ebfcb5d97b3bd6fda8e4f86e7e7ba18c9f228ec315192d9023b75",
    "3.2.2": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
    "3.3.1": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
    "3.3.2": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
    "3.4.0": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
    "3.4.1": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
    "3.4.2": "sha256:bf574a5a5de579d3c33d11c7291480d66d4ea03d93ceb58713be3967542ead47",
}
_REQUIRED_TYPED_FIELDS = {"address", "taskId"}
_ALL_TYPED_FIELDS = _REQUIRED_TYPED_FIELDS | {"online"}
_EMPTY_REFS = TaskRefIndex.from_code_by_name({})


class _DinkyRuntimeSurface(Protocol):
    available: bool
    variable_forwarding: str | None
    version_negotiation: bool
    task_params_logged: bool
    variables_logged: bool
    authenticated_request: bool
    explicit_http_timeout: bool
    failover_supported: bool
    retry_may_resubmit: bool


class _TaskAuthoringSurfaceWithDinky(Protocol):
    dinky: _DinkyRuntimeSurface


def _canonical_params(
    *,
    address: str = "https://dinky.example.com",
    task_id: str = "1842",
    online: bool = False,
) -> YamlObject:
    return {"address": address, "taskId": task_id, "online": online}


def _dinky_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(ds_version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"dinky-{ds_version}"},
            "tasks": [
                {
                    "name": "trigger-dinky-job",
                    "type": "DINKY",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(ds_version)
    prepared = prepare_workflow_create_compilation(
        _dinky_spec(task_params, ds_version=ds_version),
        catalog=catalog,
    )
    payload = prepared.materialize([33_100])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "DINKY"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=ds_version,
        task_type="DINKY",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = decode_task_parameters(
        version=ds_version,
        task_type="DINKY",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _template_yaml(ds_version: str) -> str:
    return authoring_prep.template_yaml("DINKY", ds_version, variant="minimal")


def _template_task(ds_version: str) -> YamlObject:
    document = yaml.safe_load(_template_yaml(ds_version))
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _opaque_native_params() -> YamlObject:
    return {
        "address": "http://native-dinky.internal:8888",
        "taskId": "native-job-7",
        "online": True,
        "localParams": [
            {
                "prop": "runtime_secret",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${project_secret}",
            }
        ],
        "varPool": [
            {
                "prop": "runtime_state",
                "value": {"status": "ready", "attempt": 1},
            }
        ],
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _fake_dinky_dag(
    ds_version: str,
    task_params: YamlObject,
    *,
    workflow_name: str,
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name="trigger-dinky-job",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="DINKY",
        task_params_value=json.dumps(task_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
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


def _dinky_edit_plan(
    ds_version: str,
    task_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dinky_dag(
        ds_version,
        _canonical_params(),
        workflow_name="dinky-edit",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "dinky-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "trigger-dinky-job",
                    "type": "DINKY",
                    "task_params": task_params,
                }
            ],
        },
        input_mode=input_mode,
    )


def _resolved_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    return authoring_prep.resolved_field_schema(task_params_schema, field_name)


def _schema_guidance(ds_version: str) -> str:
    return authoring_prep.schema_guidance("DINKY", ds_version)


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_catalog_exposes_one_exact_job_trigger_facet(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("DINKY")
    membership = catalog.require_facet("DINKY", _DINKY_FACET)
    fact = catalog.task_type_facts["DINKY"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("DINKY") is True
    assert catalog.supports_opaque_authoring("DINKY") is False
    assert profile.category == "Universal"
    assert profile.default_facet == _DINKY_FACET
    assert set(profile.facets) == {_DINKY_FACET}
    assert fact.semantic_fingerprint == _DINKY_FINGERPRINTS[ds_version]
    assert source_review is not None
    assert source_review.review == "dinky-job-trigger-literal-endpoint-subset"
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.contract.opaque_authoring_selector is None
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _DINKY_ABSENT_VERSIONS)
def test_dinky_is_absent_before_its_exact_upstream_release(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("DINKY") is False
    assert catalog.supports_opaque_authoring("DINKY") is False
    assert "DINKY" not in catalog.authoring_task_types


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_schema_exposes_only_the_literal_job_trigger_subset(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "DINKY",
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

    assert result.data["task_type"] == "DINKY"
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == _ALL_TYPED_FIELDS
    for field_name in _REQUIRED_TYPED_FIELDS:
        assert task_param_fields[field_name]["required"] is True
    assert task_param_fields["online"]["required"] is False
    assert task_param_fields["online"]["default"] is False
    for field_name in _ALL_TYPED_FIELDS:
        assert task_param_fields[field_name]["compile_path"].endswith(
            f"taskParams.{field_name}"
        )
    assert result.data["state_rules"] == []
    for excluded in ("localParams", "varPool", "futureField"):
        assert excluded not in task_param_fields

    descriptions = " ".join(
        str(field.get("description", "")) for field in task_param_fields.values()
    ).lower()
    assert "dinky" in descriptions
    assert "worker" in descriptions
    assert "http" in descriptions
    assert "global" in descriptions
    assert "logged" in descriptions
    assert "failover" in descriptions


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
def test_dinky_json_schema_matches_the_closed_literal_projection(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "DINKY",
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

    assert task_params["additionalProperties"] is False
    assert set(task_params["required"]) == _REQUIRED_TYPED_FIELDS
    assert set(properties) == _ALL_TYPED_FIELDS

    address_schema = _resolved_property_schema(task_params, "address")
    assert address_schema["type"] == "string"
    assert address_schema["minLength"] == 1
    address_pattern = address_schema["pattern"]
    assert isinstance(address_pattern, str)
    for valid in (
        "https://dinky.example.com",
        "http://dinky.internal:8888/base",
    ):
        assert re.fullmatch(address_pattern, valid)
    for invalid in (
        "ftp://dinky.example.com",
        "https://alice:secret@dinky.example.com",
        "https://dinky.example.com?token=value",
        "https://dinky.example.com#fragment",
        "http://999.999.999.999",
        "http://[:::]",
        "http://[1.2.3.4]",
        "http://" + ".".join(["a" * 63] * 4),
        "${DINKY_ADDRESS}",
        "https://dinky.example.com\nnext",
    ):
        assert re.fullmatch(address_pattern, invalid) is None

    task_id_schema = _resolved_property_schema(task_params, "taskId")
    assert task_id_schema["type"] == "string"
    assert task_id_schema["minLength"] == 1
    task_id_pattern = task_id_schema["pattern"]
    assert isinstance(task_id_pattern, str)
    assert re.fullmatch(task_id_pattern, "1842")
    assert re.fullmatch(task_id_pattern, "daily-job_7")
    for invalid in ("daily job", "${job_id}", "$[yyyyMMdd]", "job\nnext"):
        assert re.fullmatch(task_id_pattern, invalid) is None

    online_schema = _resolved_property_schema(task_params, "online")
    assert online_schema["type"] == "boolean"
    assert online_schema["default"] is False


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.2", "3.4.2"])
def test_dinky_summary_and_compile_mappings_cover_the_exact_typed_surface(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("DINKY", catalog=catalog)
    result = task_type_schema_result(
        "DINKY",
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

    assert summary["task_type"] == "DINKY"
    assert summary["category"] == "Universal"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.address",
        "task_params.taskId",
    }
    assert set(mappings) == {
        "task_params.address",
        "task_params.taskId",
        "task_params.online",
    }
    for authoring_path, payload_path in mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_minimal_template_compiles_to_exact_native_identity(
    ds_version: str,
) -> None:
    yaml_text = _template_yaml(ds_version)
    task = _template_task(ds_version)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "DINKY"
    assert params == _canonical_params()
    assert _compiled_task_params(ds_version, params) == params

    guidance = yaml_text.lower().replace("-", " ")
    assert "worker" in guidance
    assert "dinky" in guidance
    assert "global" in guidance
    surface = get_task_authoring_catalog(ds_version).authoring_surface.dinky
    assert ("version negotiation" in guidance) is surface.version_negotiation
    if ds_version != "3.4.2":
        assert "3.4.2" not in guidance
    assert "logged" in guidance
    assert "retry" in guidance
    assert "failover" in guidance


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_strict_model_and_projector_preserve_exact_native_identity(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    catalog = get_task_authoring_catalog(ds_version)

    normalized = catalog.normalize_task_params("DINKY", params, intent=intent)

    assert normalized == params
    assert normalized is not params
    assert _encoded_task_params(ds_version, normalized) == params
    assert _decoded_task_params(ds_version, params) == params
    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize(
    "address",
    [
        "https://dinky.example.com",
        "http://dinky.internal:8888",
        "https://dinky.example.com/platform",
        "https://127.0.0.1:9443/dinky",
        "http://[2001:db8::7]:8888",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_address_accepts_literal_http_base_urls(
    address: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(address=address)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DINKY",
        params,
        intent=intent,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "address",
    [
        "",
        "   ",
        " dinky.example.com",
        "dinky.example.com",
        "ftp://dinky.example.com",
        "https://",
        "https://alice:secret@dinky.example.com",
        "https://dinky.example.com?token=value",
        "https://dinky.example.com#fragment",
        "https://dinky.example.com/ ",
        "${DINKY_ADDRESS}",
        "$[dinky_address]",
        "https://dinky.example.com\nnext",
        7,
        None,
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_address_rejects_nonliteral_ambiguous_or_credentialed_targets(
    address: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["address"] = address

    with pytest.raises(ValueError, match="address"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DINKY",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "task_id",
    ["1842", "daily-job_7", "job.v2", "namespace:job", "job~stable"],
)
def test_dinky_task_id_accepts_safe_literal_identifiers(task_id: str) -> None:
    params = _canonical_params(task_id=task_id)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DINKY",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "task_id",
    [
        "",
        "   ",
        " job-7",
        "job-7 ",
        "daily job",
        "${job_id}",
        "$[yyyyMMdd]",
        "$(id)",
        "`id`",
        "job;id",
        "job&&id",
        "job\nnext",
        7,
        None,
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_task_id_rejects_nonliteral_or_nonstring_values(
    task_id: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["taskId"] = task_id

    with pytest.raises(ValueError, match="taskId"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DINKY",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("online", [True, False])
def test_dinky_online_accepts_only_real_booleans(*, online: bool) -> None:
    params = _canonical_params(online=online)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "DINKY",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize("online", [0, 1, "false", "true", "0", "1", None])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_online_rejects_bool_coercion(
    online: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["online"] = online

    with pytest.raises(ValueError, match="online"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DINKY",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("field", sorted(_REQUIRED_TYPED_FIELDS))
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_typed_create_and_edit_require_address_and_task_id(
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params.pop(field)

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DINKY",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("localParams", []),
        ("varPool", []),
        ("futureField", {"enabled": True}),
        ("localParams", None),
        ("varPool", None),
        ("futureField", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_dinky_typed_authoring_rejects_unowned_native_and_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "DINKY",
            params,
            intent=intent,
        )


def test_dinky_typed_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = _canonical_params()
    canonical["futureField"] = {"enabled": True}
    native = _canonical_params()
    native["localParams"] = []

    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encoded_task_params("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="localParams"):
        _decoded_task_params("3.4.2", native)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("address", "${DINKY_ADDRESS}"),
        ("address", "https://alice:secret@dinky.example.com"),
        ("taskId", "${job_id}"),
        ("online", "false"),
    ],
)
def test_dinky_typed_decode_rejects_unsafe_native_values(
    field: str,
    value: YamlValue,
) -> None:
    native = _canonical_params()
    native[field] = value

    with pytest.raises(TaskParameterProjectionError, match=field):
        _decoded_task_params("3.4.2", native)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("address", "${DINKY_ADDRESS}"),
        ("taskId", "job;id"),
        ("online", "false"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_dinky_invalid_canonical_create_does_not_downgrade_to_opaque(
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        _compiled_task_params("3.4.2", params)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("address", "${DINKY_ADDRESS}"),
        ("taskId", "job;id"),
        ("online", "false"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_dinky_invalid_canonical_edits_do_not_downgrade_to_opaque(
    input_mode: str,
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(UserInputError, match=field):
        _dinky_edit_plan("3.4.2", params, input_mode=input_mode)


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_safe_native_export_canonicalizes_and_reencodes_exactly(
    ds_version: str,
) -> None:
    native_params = _canonical_params(online=True)
    dag = _fake_dinky_dag(
        ds_version,
        native_params,
        workflow_name="dinky-safe-export",
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
    exported = document["tasks"][0]["task_params"]
    assert exported == native_params
    assert isinstance(exported, dict)

    normalized = catalog.normalize_task_params(
        "DINKY",
        cast("YamlObject", exported),
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    assert normalized == native_params
    assert _decoded_task_params(ds_version, native_params) == native_params
    assert _encoded_task_params(ds_version, normalized) == native_params
    assert _compiled_task_params(ds_version, normalized) == native_params


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_opaque_preserve_deep_copies_excluded_and_future_fields(
    ds_version: str,
) -> None:
    native = _opaque_native_params()
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "DINKY",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == expected
    assert preserved is not native
    local_params = native["localParams"]
    assert isinstance(local_params, list)
    local_param = local_params[0]
    assert isinstance(local_param, dict)
    local_param["value"] = "mutated"
    var_pool = native["varPool"]
    assert isinstance(var_pool, list)
    runtime = var_pool[0]
    assert isinstance(runtime, dict)
    runtime["value"] = {"status": "mutated"}
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_dinky_has_no_public_raw_opaque_create_or_edit_mode(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "DINKY",
            _opaque_native_params(),
            intent=intent,
        )

    assert str(captured.value) == (
        f"DINKY opaque authoring is unsupported for DolphinScheduler {ds_version}."
    )
    assert captured.value.details == {
        "selected_version": ds_version,
        "task_type": "DINKY",
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {ds_version} policy permits only opaque "
            "preservation for DINKY."
        ),
    }


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_CREATE),
        (TaskAuthoringIntent.TYPED_EDIT, TaskAuthoringIntent.TYPED_EDIT),
    ],
)
def test_dinky_excluded_fields_never_select_an_opaque_public_intent(
    ds_version: str,
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    selected = catalog.effective_authoring_intent(
        "DINKY",
        requested=requested,
        task_params=_opaque_native_params(),
    )

    assert selected is expected


@pytest.mark.parametrize("ds_version", _DINKY_VERSIONS)
def test_dinky_opaque_export_and_unchanged_patch_round_trip_losslessly(
    ds_version: str,
) -> None:
    native_params = _opaque_native_params()
    dag = _fake_dinky_dag(
        ds_version,
        native_params,
        workflow_name="dinky-opaque-roundtrip",
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


@pytest.mark.parametrize("ds_version", _DINKY_ABSENT_VERSIONS)
def test_dinky_runtime_surface_is_absent_before_3_1(ds_version: str) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithDinky",
        get_task_authoring_surface(ds_version),
    ).dinky

    assert surface.available is False
    assert surface.variable_forwarding is None
    assert surface.version_negotiation is False
    assert surface.task_params_logged is False
    assert surface.variables_logged is False
    assert surface.authenticated_request is False
    assert surface.explicit_http_timeout is False
    assert surface.failover_supported is False
    assert surface.retry_may_resubmit is False


@pytest.mark.parametrize(
    ("ds_version", "variable_forwarding", "variables_logged"),
    [
        ("3.1.0", "none", False),
        ("3.1.9", "none", False),
        ("3.2.0", "none", False),
        ("3.2.1", "workflow_globals_and_local_params", False),
        ("3.2.2", "workflow_globals_and_local_params", False),
        ("3.3.1", "workflow_globals_and_local_placeholders", False),
        ("3.3.2", "workflow_globals_and_local_placeholders", False),
        ("3.4.0", "workflow_globals_and_local_placeholders", False),
        ("3.4.1", "workflow_globals_and_local_placeholders", False),
        ("3.4.2", "all_prepared_params", True),
    ],
)
def test_dinky_runtime_surface_locks_variable_and_logging_epochs(
    ds_version: str,
    variable_forwarding: str,
    *,
    variables_logged: bool,
) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithDinky",
        get_task_authoring_surface(ds_version),
    ).dinky

    assert surface.available is True
    assert surface.variable_forwarding == variable_forwarding
    assert surface.version_negotiation is (ds_version >= "3.2.1")
    assert surface.task_params_logged is True
    assert surface.variables_logged is variables_logged
    assert surface.authenticated_request is False
    assert surface.explicit_http_timeout is False
    assert surface.failover_supported is False
    assert surface.retry_may_resubmit is True


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.9", "3.2.0"])
def test_dinky_legacy_guidance_promises_no_implicit_variable_forwarding(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "does not forward workflow globals" in guidance
    assert "task parameters are logged" in guidance
    assert "retry may resubmit" in guidance
    assert "failover" in guidance


@pytest.mark.parametrize("ds_version", ["3.2.1", "3.2.2"])
def test_dinky_v1_guidance_discloses_global_and_local_parameter_forwarding(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "dinky 1" in guidance
    assert "workflow global" in guidance
    assert "localparams" in guidance
    assert "secret" in guidance
    assert "retry may resubmit" in guidance


@pytest.mark.parametrize("ds_version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1"])
def test_dinky_placeholder_epoch_guidance_discloses_prepare_map_expansion(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "workflow global" in guidance
    assert "localparams" in guidance
    assert "placeholder" in guidance
    assert "prepared" in guidance
    assert "secret" in guidance


def test_dinky_3_4_2_guidance_discloses_full_prepared_map_externalization() -> None:
    guidance = _schema_guidance("3.4.2")

    for source in (
        "built-in",
        "project",
        "workflow",
        "task",
        "command",
        "business",
    ):
        assert source in guidance
    assert "all prepared" in guidance
    assert "sent to dinky" in guidance
    assert "info" in guidance
    assert "logged" in guidance
    assert "secret" in guidance
    assert "does not redact" in guidance
    assert "failover" in guidance
    assert "retry may resubmit" in guidance
