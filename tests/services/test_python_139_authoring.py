from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.services._task_authoring_prep import parameter_example_yaml

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.workflow_patch import validate_workflow_patch_document
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
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
    from dsctl.support.json_types import JsonObject


_VERSION = "1.3.9"
_TASK_TYPE = "PYTHON"
_FINGERPRINT = "sha256:10176f48106c53b2b3724f3d454bc9effc8a2cd477a8c31e2b57fbbab7e92bfa"
_RESOURCE_NAME = "/tenant/resources/scripts/helpers.py"
_CANONICAL_FIELDS = {"rawScript", "localParams", "resourceList"}
_CANONICAL_FIELD_PATHS = {
    "task_params.rawScript",
    "task_params.localParams[]",
    "task_params.localParams[].prop",
    "task_params.localParams[].direct",
    "task_params.localParams[].type",
    "task_params.localParams[].value",
    "task_params.resourceList[]",
}
_EXACT_DATA_TYPES = {
    "BOOLEAN",
    "DATE",
    "DOUBLE",
    "FLOAT",
    "INTEGER",
    "LONG",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
}
_REFS = TaskRefIndex.from_code_by_name({})


def _canonical(*, with_resource: bool = False) -> YamlObject:
    return {
        "rawScript": 'print("bizdate=${bizdate}")\n',
        "localParams": [
            {
                "prop": "bizdate",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "2026-08-20",
            }
        ],
        "resourceList": ([{"resourceName": _RESOURCE_NAME}] if with_resource else []),
    }


def _native(*, with_resource: bool = False, **overrides: YamlValue) -> YamlObject:
    payload = _canonical(with_resource=False)
    payload["resourceList"] = (
        [{"id": 0, "res": _RESOURCE_NAME}] if with_resource else []
    )
    payload.update(overrides)
    return payload


def _spec(params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(_VERSION)
    return validate_workflow_document(
        {
            "workflow": {"name": "python-139-authoring"},
            "tasks": [
                {
                    "name": "run-python",
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


def _compiled(params: YamlObject) -> YamlObject:
    payload = prepare_legacy_workflow_graph(
        _spec(params),
        task_id_factory=lambda _task_name: "tasks-python",
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])
    native_params = process_definition["tasks"][0]["params"]
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encode(params: YamlObject, *, task_type: str = _TASK_TYPE) -> YamlObject:
    projected = encode_task_parameters(
        version=_VERSION,
        task_type=task_type,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decode(
    params: YamlObject,
    *,
    task_type: str = _TASK_TYPE,
    source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=_VERSION,
        task_type=task_type,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _legacy_graph(
    native_params: YamlObject,
    *,
    task_type: str = _TASK_TYPE,
) -> DecodedLegacyWorkflowGraph:
    return decode_legacy_workflow_graph(
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "tasks-python",
                        "name": "run-python",
                        "type": task_type,
                        "description": "Original Python task",
                        "params": native_params,
                        "preTasks": [],
                    }
                ],
                "timeout": 0,
                "tenantId": -1,
            }
        ),
        locations=json.dumps(
            {
                "tasks-python": {
                    "name": "run-python",
                    "targetarr": "",
                    "nodenumber": 0,
                    "x": 0,
                    "y": 0,
                }
            }
        ),
        connects="[]",
    )


def _exported_params(graph: DecodedLegacyWorkflowGraph) -> YamlObject:
    document = graph.workflow_document(name="python-139-roundtrip")
    tasks = document["tasks"]
    assert isinstance(tasks, list)
    task = tasks[0]
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _recompiled_params(
    graph: DecodedLegacyWorkflowGraph,
    *,
    spec: WorkflowSpec | None = None,
) -> YamlObject:
    selected_spec = (
        graph.to_workflow_spec(name="python-139-roundtrip") if spec is None else spec
    )
    payload = prepare_legacy_workflow_graph(
        selected_spec,
        baseline=graph,
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])
    params = process_definition["tasks"][0]["params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _command_edited_params(
    graph: DecodedLegacyWorkflowGraph,
    *,
    command: str,
) -> YamlObject:
    patch = validate_workflow_patch_document(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "run-python"},
                            "set": {"command": command},
                        }
                    ]
                }
            }
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog(_VERSION),
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    ).patch
    plan = prepare_legacy_workflow_mutation_plan(
        graph,
        workflow_name="python-139-roundtrip",
        project_name="demo",
        description=None,
        release_state="OFFLINE",
        mutation=patch,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    process_definition = json.loads(plan.compilation.preview()["processDefinitionJson"])
    params = process_definition["tasks"][0]["params"]
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _assert_opaque_legacy_edits_are_lossless(
    native: YamlObject,
    *,
    task_type: str,
) -> None:
    graph = _legacy_graph(native, task_type=task_type)
    baseline_spec = graph.to_workflow_spec(name="python-139-roundtrip")
    current = baseline_spec.tasks[0]
    edited_spec = baseline_spec.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"description": "Metadata-only edit"},
                    deep=True,
                )
            ]
        },
        deep=True,
    )

    assert (
        graph.tasks[0].task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    assert current.task_params == native
    assert _recompiled_params(graph, spec=edited_spec) == native
    assert _command_edited_params(graph, command='print("changed")\n') == {
        **native,
        "rawScript": 'print("changed")\n',
    }


def _task_params_json_schema() -> YamlObject:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    return cast("YamlObject", task_params)


def test_python_139_exact_source_fact_has_reviewed_typed_membership() -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert _TASK_TYPE in catalog.upstream_task_types
    assert _TASK_TYPE in catalog.reviewed_typed_task_types
    assert _TASK_TYPE in catalog.legacy_typed_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review

    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.common.task.python.PythonParameters"
    )
    assert fact.registration_kind == "legacy_switch"
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.cli_model == "ScriptTaskParamsSpec"
    assert review.review == "legacy-1.3.9-python-no-resource-exact-subset"
    assert review.semantic_fingerprint == fact.semantic_fingerprint


def test_python_has_reviewed_typed_membership_on_all_exact_profiles() -> None:
    assert (
        tuple(
            version
            for version in TARGET_DS_VERSIONS
            if _TASK_TYPE
            in get_task_authoring_catalog(version).reviewed_typed_task_types
        )
        == TARGET_DS_VERSIONS
    )


def test_python_139_catalog_normalizes_the_exact_canonical_shape() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    canonical = _canonical()

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == canonical
    assert set(normalized) == _CANONICAL_FIELDS


def test_python_139_schema_exposes_only_exact_authored_fields() -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(_VERSION),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_paths = {path for path in fields if path.startswith("task_params.")}

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert task_param_paths == _CANONICAL_FIELD_PATHS
    assert fields["task_params.localParams[].direct"]["choices"] == ["IN"]
    assert set(fields["task_params.localParams[].type"]["choices"]) == (
        _EXACT_DATA_TYPES
    )
    assert "permission checks" in fields["task_params.resourceList[]"]["description"]


def test_python_139_json_schema_is_closed_and_requires_empty_resources() -> None:
    task_params = _task_params_json_schema()
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _CANONICAL_FIELDS
    assert set(required) == {"rawScript"}
    assert "varPool" not in properties
    resource_list = properties["resourceList"]
    assert isinstance(resource_list, dict)
    assert resource_list["items"] is False
    assert resource_list["maxItems"] == 0
    description = resource_list["description"]
    assert isinstance(description, str)
    assert "permission checks" in description


@pytest.mark.parametrize("blank", ["", "   ", "\t\n", "\x85\xa0\u3000"])
def test_python_139_schema_and_runtime_reject_blank_script_and_prop(
    blank: str,
) -> None:
    task_params = _task_params_json_schema()
    properties = task_params["properties"]
    definitions = task_params["$defs"]
    assert isinstance(properties, dict)
    assert isinstance(definitions, dict)
    raw_script = properties["rawScript"]
    global_param = definitions["GlobalParamSpec"]
    assert isinstance(raw_script, dict)
    assert isinstance(global_param, dict)
    parameter_properties = global_param["properties"]
    assert isinstance(parameter_properties, dict)
    prop = parameter_properties["prop"]
    assert isinstance(prop, dict)

    assert re.fullmatch(cast("str", raw_script["pattern"]), blank) is None
    assert re.fullmatch(cast("str", prop["pattern"]), blank) is None

    with pytest.raises(ValueError, match="rawScript"):
        _spec({**_canonical(), "rawScript": blank})
    params = _canonical()
    local_params = params["localParams"]
    assert isinstance(local_params, list)
    parameter = local_params[0]
    assert isinstance(parameter, dict)
    parameter["prop"] = blank
    with pytest.raises(ValueError, match="parameter names"):
        _spec(params)


def test_python_139_schema_and_runtime_accept_nonblank_bom_text() -> None:
    task_params = _task_params_json_schema()
    properties = task_params["properties"]
    assert isinstance(properties, dict)
    raw_script = properties["rawScript"]
    assert isinstance(raw_script, dict)

    value = "\ufeff"
    assert re.fullmatch(cast("str", raw_script["pattern"]), value) is not None
    assert _spec({**_canonical(), "rawScript": value}).tasks[0].task_params == {
        **_canonical(),
        "rawScript": value,
    }


def test_python_139_summary_and_mappings_publish_the_exact_surface() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    mapping_result = task_type_schema_result(
        _TASK_TYPE,
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(mapping_result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in mapping_result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {"name", "type"}
    assert summary["required_paths_by_payload_mode"] == {
        "command": ["command"],
        "task_params": ["task_params.rawScript"],
    }
    assert set(mappings) == _CANONICAL_FIELD_PATHS
    assert mappings["task_params.resourceList[]"] == (
        "processDefinitionJson.tasks[].params.resourceList"
    )


@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_python_139_templates_validate_and_compile_exactly(variant: str) -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_template_result(
        _TASK_TYPE,
        variant=None if variant in {"minimal", "params"} else variant,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    metadata = result.data["template"]
    assert isinstance(metadata, dict)
    document = yaml.safe_load(
        parameter_example_yaml(_TASK_TYPE, _VERSION)
        if variant == "params"
        else cast("str", result.data["yaml"])
    )
    assert isinstance(document, dict)

    assert metadata["kind"] == "typed"
    assert metadata["variants"] == []
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"python-139-{variant}"},
            "tasks": [document],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    payload = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda _task_name: "tasks-python",
    ).materialize()
    process_definition = json.loads(payload["processDefinitionJson"])
    native_params = process_definition["tasks"][0]["params"]

    assert "varPool" not in native_params
    assert all(item["direct"] == "IN" for item in native_params["localParams"])
    assert native_params["resourceList"] == []


def test_python_139_resource_template_is_not_publicly_authorable() -> None:
    with pytest.raises(UserInputError, match="Unsupported task template variant"):
        task_template_result(
            _TASK_TYPE,
            variant="resource",
            catalog=get_task_authoring_catalog(_VERSION),
        )


def test_python_139_projector_owns_the_no_resource_legacy_subset() -> None:
    canonical = _canonical()

    native = _encode(canonical)
    decoded, provenance = _decode(native)

    assert native == _native()
    assert decoded == canonical
    assert provenance is ProjectionSource.TYPED_AUTHORING


def test_shell_139_uses_the_same_no_resource_projection() -> None:
    shell_canonical: YamlObject = {
        "rawScript": "bash scripts/job.sh",
        "localParams": [],
        "resourceList": [],
    }

    assert _encode(shell_canonical, task_type="SHELL") == shell_canonical
    assert _decode(shell_canonical, task_type="SHELL") == (
        shell_canonical,
        ProjectionSource.TYPED_AUTHORING,
    )


@pytest.mark.parametrize("task_type", ["PYTHON", "SHELL"])
def test_legacy_script_typed_resource_authoring_is_fail_closed(
    task_type: str,
) -> None:
    params = _canonical(with_resource=True)

    with pytest.raises(TaskParameterProjectionError, match="permission checks"):
        _encode(params, task_type=task_type)


@pytest.mark.parametrize(
    ("update", "field"),
    [
        ({"direct": "OUT"}, "tasks[].task_params.localParams[].direct"),
        ({"type": "LIST"}, "tasks[].task_params.localParams[].type"),
        ({"type": "FILE"}, "tasks[].task_params.localParams[].type"),
    ],
    ids=["out", "list", "file"],
)
def test_python_139_typed_local_params_fail_the_exact_parameter_gate(
    update: YamlObject,
    field: str,
) -> None:
    params = _canonical()
    local_params = params["localParams"]
    assert isinstance(local_params, list)
    parameter = local_params[0]
    assert isinstance(parameter, dict)
    parameter.update(update)

    with pytest.raises(UnsupportedFeatureError) as captured:
        _spec(params)

    assert captured.value.details["field"] == field


def test_python_139_rejects_duplicate_local_parameter_names() -> None:
    params = _canonical()
    local_params = params["localParams"]
    assert isinstance(local_params, list)
    local_params.append(deepcopy(local_params[0]))

    with pytest.raises(ValueError, match="must be unique"):
        _spec(params)


@pytest.mark.parametrize(
    "invalid_params",
    [
        {**_canonical(), "varPool": []},
        {**_canonical(), "futureField": {"native": True}},
        {**_canonical(), "resourceList": [{"resourceName": _RESOURCE_NAME}]},
        {**_canonical(), "resourceList": [{"id": 12}]},
        {**_canonical(), "resourceList": [{"res": "scripts/job.py"}]},
        {
            **_canonical(),
            "resourceList": cast(
                "YamlValue",
                [{"resourceName": _RESOURCE_NAME, "futureField": True}],
            ),
        },
    ],
    ids=[
        "var-pool",
        "future-field",
        "resource-name",
        "native-id",
        "native-res",
        "resource-extra",
    ],
)
def test_python_139_invalid_typed_shapes_do_not_downgrade_to_opaque(
    invalid_params: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(_VERSION)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=invalid_params,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises((UnsupportedFeatureError, ValueError)):
        _spec(invalid_params)


def test_python_139_typed_resource_attachment_reports_permission_boundary() -> None:
    with pytest.raises(ValueError, match="permission checks"):
        _spec(_canonical(with_resource=True))


@pytest.mark.parametrize(
    "native",
    [
        _native(resourceList=[{"id": 0, "res": _RESOURCE_NAME}]),
        _native(resourceList=[{"id": 12}]),
        _native(resourceList=[{"id": 12, "res": "scripts/job.py"}]),
        _native(varPool=[]),
        _native(futureField={"nested": ["preserve", {"value": True}]}),
        _native(
            localParams=[
                {
                    "prop": "result",
                    "direct": "OUT",
                    "type": "VARCHAR",
                    "value": "",
                }
            ]
        ),
        _native(
            localParams=[
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "one",
                },
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "two",
                },
            ]
        ),
        _native(
            localParams=[
                {
                    "prop": " bizdate ",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "2026-08-20",
                }
            ]
        ),
        _native(
            localParams=[
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": None,
                }
            ]
        ),
    ],
    ids=[
        "resource-id-zero-res",
        "resource-id",
        "resource-id-res",
        "var-pool",
        "future",
        "out-param",
        "duplicate-local-param",
        "noncanonical-prop-spelling",
        "explicit-null-value",
    ],
)
def test_python_139_richer_native_shapes_keep_opaque_provenance(
    native: YamlObject,
) -> None:
    decoded, provenance = _decode(native)
    reencoded = encode_task_parameters(
        version=_VERSION,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(decoded)),
        refs=_REFS,
        source=provenance,
    )

    assert decoded == native
    assert provenance is ProjectionSource.OPAQUE_PRESERVE
    assert reencoded.task_params == native


def test_python_139_safe_legacy_export_is_canonical_and_recompiles_exactly() -> None:
    native = _native()
    graph = _legacy_graph(native)

    assert (
        graph.tasks[0].task_params_reencode_source is ProjectionSource.TYPED_AUTHORING
    )
    assert _exported_params(graph) == _canonical()
    assert _recompiled_params(graph) == native


def test_python_139_richer_native_metadata_edit_is_lossless() -> None:
    native = _native(
        resourceList=[{"id": 12}],
        futureField={"nested": ["preserve", {"value": True}]},
    )
    graph = _legacy_graph(native)
    baseline_spec = graph.to_workflow_spec(name="python-139-roundtrip")
    current = baseline_spec.tasks[0]
    edited_spec = baseline_spec.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"description": "Metadata-only edit"},
                    deep=True,
                )
            ]
        },
        deep=True,
    )

    assert (
        graph.tasks[0].task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    assert _exported_params(graph) == native
    assert _recompiled_params(graph, spec=edited_spec) == native


@pytest.mark.parametrize(
    "native",
    [
        _native(
            localParams=[
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "2026-08-20",
                    "future": {"keep": True},
                }
            ]
        ),
        _native(
            localParams=[
                {
                    "prop": " bizdate ",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "2026-08-20",
                }
            ]
        ),
        _native(
            localParams=[
                {
                    "prop": "bizdate",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": None,
                }
            ]
        ),
    ],
    ids=["nested-future", "noncanonical-prop-spelling", "explicit-null-value"],
)
def test_python_139_opaque_local_params_bypass_typed_normalization(
    native: YamlObject,
) -> None:
    graph = _legacy_graph(native)

    assert _exported_params(graph) == native
    assert _recompiled_params(graph) == native


@pytest.mark.parametrize("task_type", ["PYTHON", "SHELL"])
def test_legacy_script_139_opaque_nested_params_survive_edits_losslessly(
    task_type: str,
) -> None:
    native = _native(
        localParams=[
            {
                "prop": " x ",
                "direct": "FUTURE_DIRECTION",
                "type": "FUTURE_TYPE",
                "value": None,
                "futureNested": {"items": [None, {"keep": ' exact "'}]},
            }
        ],
        futureField={"keep": None},
    )
    _assert_opaque_legacy_edits_are_lossless(native, task_type=task_type)


@pytest.mark.parametrize("task_type", ["PYTHON", "SHELL"])
def test_legacy_script_139_opaque_scalar_spelling_survives_edits_losslessly(
    task_type: str,
) -> None:
    native = _native(
        localParams=[
            {
                "prop": " x ",
                "direct": "IN",
                "type": "VARCHAR",
                "value": None,
            }
        ],
    )

    _assert_opaque_legacy_edits_are_lossless(native, task_type=task_type)


def test_python_139_richer_native_command_edit_keeps_opaque_provenance() -> None:
    native = _native(
        resourceList=[{"id": 12}],
        futureField={"nested": ["preserve", {"value": True}]},
    )
    graph = _legacy_graph(native)

    assert _command_edited_params(graph, command='print("changed")\n') == {
        **native,
        "rawScript": 'print("changed")\n',
    }


def test_python_139_safe_native_command_edit_reencodes_exact_wire_shape() -> None:
    graph = _legacy_graph(_native())

    assert _command_edited_params(graph, command='print("changed")\n') == _native(
        rawScript='print("changed")\n'
    )


def test_python_139_id_zero_resource_command_edit_preserves_native_wire() -> None:
    graph = _legacy_graph(_native(with_resource=True))

    assert (
        graph.tasks[0].task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    )
    assert _command_edited_params(graph, command='print("changed")\n') == _native(
        with_resource=True,
        rawScript='print("changed")\n',
    )


def test_python_139_guidance_discloses_logging_runtime_and_recovery() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    schema = task_type_schema_result(_TASK_TYPE, catalog=catalog)
    template = task_template_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(schema.data, dict)
    assert isinstance(template.data, dict)
    field_guidance = " ".join(
        str(field.get("description", ""))
        for field in schema.data["fields"]
        if isinstance(field, dict)
        and isinstance(field.get("path"), str)
        and field["path"].startswith("task_params.")
    ).lower()
    template_guidance = cast("str", template.data["yaml"]).lower()

    for guidance in (field_guidance, template_guidance):
        for phrase in (
            "placeholder",
            "crlf",
            "utf-8",
            "python_home",
            "task params",
            "substituted script",
            "stdout",
            "info",
            "not secret storage",
            "does not redact",
            "local process",
            "no structured output",
            "failover",
            "retry",
            "reruns the whole script",
            "side effects",
            "resourcelist",
            "permission checks",
            "opaque",
        ):
            assert phrase in guidance
