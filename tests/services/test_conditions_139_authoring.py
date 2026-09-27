from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from dsctl.errors import UnsupportedFeatureError, UserInputError
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
from dsctl.services.workflow.create import (
    prepare_creation,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyTask,
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.services.workflow._types import WorkflowServiceRuntime
    from dsctl.support.json_types import JsonObject


_VERSION = "1.3.9"
_TASK_TYPE = "CONDITIONS"
_FINGERPRINT = "sha256:84e7f2acf8c0988681bdf910ec4bccd1946db1dda3b8d1d8aec9ec5b59e2179f"
_CANONICAL_PARAMS: YamlObject = {
    "dependence": {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "AND",
                "dependItemList": [
                    {"task": "upstream", "status": "SUCCESS"},
                ],
            }
        ],
    },
    "conditionResult": {
        "successNode": ["on-success"],
        "failedNode": ["on-failed"],
    },
}


def _canonical_params(
    *,
    predicate: str = "upstream",
    success: str = "on-success",
    failure: str = "on-failed",
) -> YamlObject:
    return {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {"task": predicate, "status": "SUCCESS"},
                    ],
                }
            ],
        },
        "conditionResult": {
            "successNode": [success],
            "failedNode": [failure],
        },
    }


def _workflow_spec(
    params: YamlObject,
    *,
    route_depends_on: list[str] | None = None,
    upstream_depends_on: list[str] | None = None,
    third_successor: bool = False,
) -> WorkflowSpec:
    tasks: list[YamlObject] = [
        {
            "name": "upstream",
            "type": "SHELL",
            "command": "echo upstream",
        },
        {
            "name": "route",
            "type": _TASK_TYPE,
            "task_params": params,
        },
        {
            "name": "on-success",
            "type": "SHELL",
            "command": "echo success",
        },
        {
            "name": "on-failed",
            "type": "SHELL",
            "command": "echo failed",
        },
    ]
    if upstream_depends_on is not None:
        tasks[0]["depends_on"] = cast("list[YamlValue]", upstream_depends_on)
    if route_depends_on is not None:
        tasks[1]["depends_on"] = cast("list[YamlValue]", route_depends_on)
    if third_successor:
        tasks.append(
            {
                "name": "third",
                "type": "SHELL",
                "command": "echo third",
                "depends_on": ["route"],
            }
        )
    catalog = get_task_authoring_catalog(_VERSION)
    return validate_workflow_document(
        {
            "workflow": {"name": "conditions-139"},
            "tasks": cast("list[YamlValue]", tasks),
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _safe_native_wire() -> JsonObject:
    prepared = prepare_legacy_workflow_graph(
        _workflow_spec(_canonical_params()),
        task_id_factory=lambda task_name: f"tasks-{task_name}",
    )
    payload = prepared.materialize()
    return {
        "processDefinitionJson": payload["processDefinitionJson"],
        "locations": payload["locations"],
        "connects": payload["connects"],
    }


def _decode_wire(wire: JsonObject) -> DecodedLegacyWorkflowGraph:
    return decode_legacy_workflow_graph(
        process_definition_json=cast("str", wire["processDefinitionJson"]),
        locations=cast("str", wire["locations"]),
        connects=cast("str", wire["connects"]),
    )


def _native_tasks(wire: JsonObject) -> list[JsonObject]:
    process = json.loads(cast("str", wire["processDefinitionJson"]))
    assert isinstance(process, dict)
    raw_tasks = process["tasks"]
    assert isinstance(raw_tasks, list)
    return cast("list[JsonObject]", raw_tasks)


def _native_route(wire: JsonObject) -> JsonObject:
    return next(task for task in _native_tasks(wire) if task["name"] == "route")


def _decoded_route(graph: DecodedLegacyWorkflowGraph) -> DecodedLegacyTask:
    return next(task for task in graph.tasks if task.name == "route")


def _serialize_process(wire: JsonObject, tasks: list[JsonObject]) -> None:
    process = json.loads(cast("str", wire["processDefinitionJson"]))
    assert isinstance(process, dict)
    process["tasks"] = tasks
    wire["processDefinitionJson"] = json.dumps(
        process,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _opaque_native_wire(kind: str) -> JsonObject:
    wire = _safe_native_wire()
    tasks = _native_tasks(wire)
    route = next(task for task in tasks if task["name"] == "route")
    if kind == "nonempty-params":
        route["params"] = {"localParams": []}
    elif kind == "richer-split":
        dependence = cast("JsonObject", route["dependence"])
        dependence["futureField"] = {"nested": ["preserve", None]}
        route["futureTaskField"] = {"keep": True}
    elif kind == "dependence-runtime":
        dependence = cast("JsonObject", route["dependence"])
        dependence["conditionSuccess"] = True
    elif kind == "result-runtime":
        condition_result = cast("JsonObject", route["conditionResult"])
        condition_result["conditionSuccess"] = False
    elif kind == "mismatched-graph":
        route["preTasks"] = []
    else:
        message = f"Unknown native test shape: {kind}"
        raise AssertionError(message)
    _serialize_process(wire, tasks)
    if kind == "mismatched-graph":
        locations = json.loads(cast("str", wire["locations"]))
        assert isinstance(locations, dict)
        upstream_location = locations["tasks-upstream"]
        route_location = locations["tasks-route"]
        assert isinstance(upstream_location, dict)
        assert isinstance(route_location, dict)
        upstream_location["nodenumber"] = 0
        route_location["targetarr"] = ""
        wire["locations"] = json.dumps(
            locations,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        connects = json.loads(cast("str", wire["connects"]))
        assert isinstance(connects, list)
        wire["connects"] = json.dumps(
            [
                edge
                for edge in connects
                if not (
                    isinstance(edge, dict)
                    and edge.get("endPointSourceId") == "tasks-upstream"
                    and edge.get("endPointTargetId") == "tasks-route"
                )
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return wire


def _rich_opaque_native_wire() -> JsonObject:
    wire = _safe_native_wire()
    tasks = _native_tasks(wire)
    route = next(task for task in tasks if task["name"] == "route")
    route["params"] = {
        "localParams": [{"prop": "runtime", "future": {"keep": None}}],
        "varPool": [{"prop": "result", "value": "opaque"}],
    }
    dependence = cast("JsonObject", route["dependence"])
    dependence["conditionSuccess"] = True
    dependence["futureField"] = {"nested": ["preserve", {"value": 1}]}
    condition_result = cast("JsonObject", route["conditionResult"])
    condition_result["conditionSuccess"] = False
    route["futureTaskField"] = {"keep": [None, True, " exact "]}
    _serialize_process(wire, tasks)
    return wire


def _compiled_route_from_graph(
    graph: DecodedLegacyWorkflowGraph,
    *,
    spec: WorkflowSpec | None = None,
) -> JsonObject:
    selected = (
        graph.to_workflow_spec(name="conditions-139-roundtrip", project="demo")
        if spec is None
        else spec
    )
    payload = prepare_legacy_workflow_graph(selected, baseline=graph).materialize()
    wire: JsonObject = {
        "processDefinitionJson": payload["processDefinitionJson"],
        "locations": payload["locations"],
        "connects": payload["connects"],
    }
    return _native_route(wire)


def test_conditions_139_catalog_has_reviewed_typed_membership_without_raw_opaque() -> (
    None
):
    catalog = get_task_authoring_catalog(_VERSION)
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review

    assert _TASK_TYPE in catalog.reviewed_typed_task_types
    assert len(catalog.reviewed_typed_task_types) == 11
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.cli_model == "ConditionsTaskParamsSpec"
    assert review.semantic_fingerprint == fact.semantic_fingerprint

    for intent in (
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params(
                _TASK_TYPE,
                _CANONICAL_PARAMS,
                intent=intent,
            )


def test_conditions_139_canonical_compile_owns_outer_task_node_wire_and_edges() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "conditions-139"},
            "tasks": [
                {"name": "upstream", "type": "SHELL", "command": "echo upstream"},
                {
                    "name": "route",
                    "type": _TASK_TYPE,
                    "task_params": _CANONICAL_PARAMS,
                },
                {
                    "name": "on-success",
                    "type": "SHELL",
                    "command": "echo success",
                },
                {
                    "name": "on-failed",
                    "type": "SHELL",
                    "command": "echo failed",
                },
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    prepared = prepare_legacy_workflow_graph(
        spec,
        task_id_factory=lambda task_name: f"tasks-{task_name}",
    )
    process_definition = json.loads(prepared.materialize()["processDefinitionJson"])
    tasks = {task["name"]: task for task in process_definition["tasks"]}
    route = tasks["route"]

    assert prepared.edges == (
        ("upstream", "route"),
        ("route", "on-success"),
        ("route", "on-failed"),
    )
    assert route["params"] == {}
    assert route["dependence"] == {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "AND",
                "dependItemList": [
                    {"depTasks": "upstream", "status": "SUCCESS"},
                ],
            }
        ],
    }
    assert route["conditionResult"] == {
        "successNode": ["on-success"],
        "failedNode": ["on-failed"],
    }
    assert route["preTasks"] == ["upstream"]
    assert tasks["on-success"]["preTasks"] == ["route"]
    assert tasks["on-failed"]["preTasks"] == ["route"]


def test_conditions_139_schema_and_templates_expose_only_the_closed_split_shape() -> (
    None
):
    catalog = get_task_authoring_catalog(_VERSION)
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=catalog,
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
    assert set(properties) == {"dependence", "conditionResult"}
    assert {"localParams", "varPool"}.isdisjoint(properties)

    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    assert summary["variants"] == []
    for variant in (None,):
        template = task_template_result(
            _TASK_TYPE,
            variant=variant,
            catalog=catalog,
        )
        assert isinstance(template.data, dict)
        document = yaml.safe_load(cast("str", template.data["yaml"]))
        assert isinstance(document, dict)
        authored_params = document["task_params"]
        assert isinstance(authored_params, dict)
        assert set(authored_params) == {"dependence", "conditionResult"}

    with pytest.raises(UserInputError, match="Unsupported task template variant"):
        task_template_result(_TASK_TYPE, variant="params", catalog=catalog)


@pytest.mark.parametrize(
    ("invalid_params", "message"),
    [
        ({}, r"dependence|conditionResult"),
        (
            {
                "dependence": {"relation": "AND", "dependTaskList": []},
                "conditionResult": {
                    "successNode": ["on-success"],
                    "failedNode": ["on-failed"],
                },
            },
            "dependTaskList",
        ),
        (
            {
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {"relation": "AND", "dependItemList": []},
                    ],
                },
                "conditionResult": {
                    "successNode": ["on-success"],
                    "failedNode": ["on-failed"],
                },
            },
            "dependItemList",
        ),
        ({**_CANONICAL_PARAMS, "futureField": True}, "unsupported fields"),
        ({**_CANONICAL_PARAMS, "localParams": []}, "localParams"),
        ({**_CANONICAL_PARAMS, "varPool": []}, "varPool"),
        (
            {
                **_CANONICAL_PARAMS,
                "conditionResult": {
                    "successNode": [],
                    "failedNode": ["on-failed"],
                },
            },
            "successNode",
        ),
        (
            {
                **_CANONICAL_PARAMS,
                "conditionResult": {
                    "successNode": ["on-success", "third"],
                    "failedNode": ["on-failed"],
                },
            },
            "successNode",
        ),
        (
            {
                **_CANONICAL_PARAMS,
                "conditionResult": {
                    "successNode": ["same"],
                    "failedNode": ["same"],
                },
            },
            "different tasks",
        ),
    ],
    ids=[
        "empty-root",
        "empty-depend-task-list",
        "empty-depend-item-list",
        "extra-field",
        "local-params",
        "var-pool",
        "empty-success-branch",
        "multiple-success-branches",
        "same-success-and-failure",
    ],
)
def test_conditions_139_typed_normalization_is_closed_and_fail_closed(
    invalid_params: YamlObject,
    message: str,
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
    with pytest.raises((TypeError, ValueError), match=message):
        catalog.normalize_task_params(
            _TASK_TYPE,
            invalid_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_conditions_139_typed_normalization_preserves_the_exact_canonical_shape() -> (
    None
):
    catalog = get_task_authoring_catalog(_VERSION)

    assert (
        catalog.normalize_task_params(
            _TASK_TYPE,
            _CANONICAL_PARAMS,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == _CANONICAL_PARAMS
    )


@pytest.mark.parametrize(
    ("params", "message"),
    [
        (_canonical_params(predicate="missing"), "unknown task 'missing'"),
        (_canonical_params(success="missing"), "unknown task 'missing'"),
        (_canonical_params(predicate="route"), "cannot reference itself"),
        (_canonical_params(success="route"), "cannot reference itself"),
    ],
    ids=[
        "unknown-predicate",
        "unknown-branch",
        "self-predicate",
        "self-branch",
    ],
)
def test_conditions_139_graph_references_fail_closed(
    params: YamlObject,
    message: str,
) -> None:
    with pytest.raises(LegacyWorkflowGraphError, match=message):
        prepare_legacy_workflow_graph(_workflow_spec(params))


def test_conditions_139_create_service_translates_graph_errors() -> None:
    runtime = cast(
        "WorkflowServiceRuntime",
        SimpleNamespace(
            domain=SimpleNamespace(
                workflows=SimpleNamespace(workflow_graph_family="legacy-json")
            )
        ),
    )

    with pytest.raises(UserInputError, match="unknown task 'missing'"):
        prepare_creation(
            runtime,
            spec=_workflow_spec(_canonical_params(predicate="missing")),
            catalog=get_task_authoring_catalog(_VERSION),
        )


@pytest.mark.parametrize(
    ("risk_type", "expected_command"),
    [
        ("workflow_full_edit_destructive_change", "workflow edit"),
        (
            "workflow_instance_full_edit_destructive_change",
            "workflow-instance edit",
        ),
    ],
)
def test_conditions_139_edit_service_translates_graph_errors(
    risk_type: str,
    expected_command: str,
) -> None:
    graph = _decode_wire(_safe_native_wire())
    catalog = get_task_authoring_catalog(_VERSION)
    patch = validate_workflow_patch_document(
        {
            "patch": {
                "tasks": {
                    "create": [
                        {
                            "name": "third",
                            "type": "SHELL",
                            "command": "echo third",
                            "depends_on": ["route"],
                        }
                    ]
                }
            }
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    ).patch

    with pytest.raises(UserInputError, match="extra successors: third") as exc_info:
        prepare_legacy_workflow_mutation_plan(
            graph,
            workflow_name="conditions-139-roundtrip",
            project_name="demo",
            description=None,
            release_state="OFFLINE",
            mutation=patch,
            catalog=catalog,
            risk_type=risk_type,
        )
    assert expected_command in (exc_info.value.suggestion or "")


def test_conditions_139_graph_cycle_fails_closed() -> None:
    spec = _workflow_spec(
        _canonical_params(),
        upstream_depends_on=["on-success"],
    )

    with pytest.raises(LegacyWorkflowGraphError, match="dependency cycle"):
        prepare_legacy_workflow_graph(spec)


def test_conditions_139_third_direct_successor_fails_closed() -> None:
    spec = _workflow_spec(_canonical_params(), third_successor=True)

    with pytest.raises(
        LegacyWorkflowGraphError,
        match=r"only to its success and failure branches.*third",
    ):
        prepare_legacy_workflow_graph(spec)


def test_conditions_139_safe_native_split_has_typed_provenance_and_json_roundtrip() -> (
    None
):
    wire = _safe_native_wire()
    graph = _decode_wire(wire)
    route = _decoded_route(graph)
    document = graph.workflow_document(name="conditions-139-roundtrip")
    exported_tasks = document["tasks"]
    assert isinstance(exported_tasks, list)
    exported_route = next(
        task
        for task in exported_tasks
        if isinstance(task, dict) and task.get("name") == "route"
    )

    assert route.task_params_reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported_route["task_params"] == _CANONICAL_PARAMS

    spec = graph.to_workflow_spec(
        name="conditions-139-roundtrip",
        project="demo",
    )
    recompiled = prepare_legacy_workflow_graph(spec, baseline=graph).materialize()
    for field in ("processDefinitionJson", "locations", "connects"):
        assert json.loads(recompiled[field]) == json.loads(cast("str", wire[field]))


@pytest.mark.parametrize(
    "kind",
    [
        "nonempty-params",
        "richer-split",
        "dependence-runtime",
        "result-runtime",
        "mismatched-graph",
    ],
)
def test_conditions_139_richer_or_graph_mismatched_native_stays_opaque(
    kind: str,
) -> None:
    wire = _opaque_native_wire(kind)
    original = _native_route(wire)
    graph = _decode_wire(wire)
    route = _decoded_route(graph)
    recompiled = _compiled_route_from_graph(graph)

    assert route.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    document = graph.workflow_document(name="conditions-139-opaque")
    exported_tasks = document["tasks"]
    assert isinstance(exported_tasks, list)
    exported_route = next(
        task
        for task in exported_tasks
        if isinstance(task, dict) and task.get("name") == "route"
    )
    assert exported_route["task_params"] == original["params"]
    for field in ("params", "dependence", "conditionResult"):
        assert recompiled[field] == original[field]
    if "futureTaskField" in original:
        assert recompiled["futureTaskField"] == original["futureTaskField"]


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_conditions_139_opaque_split_fields_survive_metadata_edits(
    input_mode: str,
) -> None:
    wire = _rich_opaque_native_wire()
    original = _native_route(wire)
    graph = _decode_wire(wire)
    catalog = get_task_authoring_catalog(_VERSION)

    mutation: WorkflowPatchSpec | WorkflowSpec
    if input_mode == "patch":
        mutation = validate_workflow_patch_document(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "route"},
                                "set": {"description": "metadata only"},
                            }
                        ]
                    }
                }
            },
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.TYPED_EDIT,
            ),
        ).patch
    else:
        baseline = graph.to_workflow_spec(
            name="conditions-139-roundtrip",
            project="demo",
        )
        mutation = baseline.model_copy(
            update={
                "tasks": [
                    task.model_copy(
                        update={"description": "metadata only"},
                        deep=True,
                    )
                    if task.name == "route"
                    else task
                    for task in baseline.tasks
                ]
            },
            deep=True,
        )

    plan = prepare_legacy_workflow_mutation_plan(
        graph,
        workflow_name="conditions-139-roundtrip",
        project_name="demo",
        description=None,
        release_state="OFFLINE",
        mutation=mutation,
        catalog=catalog,
    )
    compiled_wire: JsonObject = {
        "processDefinitionJson": plan.compilation.preview()["processDefinitionJson"],
    }
    edited = _native_route(compiled_wire)

    assert _decoded_route(graph).task_params_reencode_source is (
        ProjectionSource.OPAQUE_PRESERVE
    )
    assert edited["description"] == "metadata only"
    for field in ("params", "dependence", "conditionResult", "futureTaskField"):
        assert edited[field] == original[field]


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_conditions_139_opaque_split_fields_reject_topology_edits(
    input_mode: str,
) -> None:
    graph = _decode_wire(_rich_opaque_native_wire())
    catalog = get_task_authoring_catalog(_VERSION)

    mutation: WorkflowPatchSpec | WorkflowSpec
    if input_mode == "patch":
        mutation = validate_workflow_patch_document(
            {
                "patch": {
                    "tasks": {
                        "rename": [{"from": "on-success", "to": "renamed-success"}]
                    }
                }
            },
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.TYPED_EDIT,
            ),
        ).patch
    else:
        baseline = graph.to_workflow_spec(
            name="conditions-139-roundtrip",
            project="demo",
        )
        mutation = baseline.model_copy(
            update={
                "tasks": [
                    task.model_copy(update={"name": "renamed-success"}, deep=True)
                    if task.name == "on-success"
                    else task
                    for task in baseline.tasks
                ]
            },
            deep=True,
        )

    with pytest.raises(UserInputError, match=r"opaque.*CONDITIONS.*topology"):
        prepare_legacy_workflow_mutation_plan(
            graph,
            workflow_name="conditions-139-roundtrip",
            project_name="demo",
            description=None,
            release_state="OFFLINE",
            mutation=mutation,
            catalog=catalog,
        )


def test_conditions_139_direct_compiler_rejects_opaque_topology_edits() -> None:
    graph = _decode_wire(_rich_opaque_native_wire())
    baseline = graph.to_workflow_spec(
        name="conditions-139-roundtrip",
        project="demo",
    )
    renamed = baseline.model_copy(
        update={
            "tasks": [
                task.model_copy(update={"name": "renamed-success"}, deep=True)
                if task.name == "on-success"
                else task
                for task in baseline.tasks
            ]
        },
        deep=True,
    )

    with pytest.raises(
        LegacyWorkflowGraphError,
        match=r"opaque CONDITIONS.*task names and topology",
    ):
        prepare_legacy_workflow_graph(renamed, baseline=graph)


@pytest.mark.parametrize(
    "task_set",
    [
        {"type": "SHELL", "command": "echo changed"},
        {"task_params": _CANONICAL_PARAMS},
    ],
    ids=["type-change", "params-change"],
)
def test_conditions_139_opaque_split_fields_reject_payload_edits(
    task_set: YamlObject,
) -> None:
    graph = _decode_wire(_rich_opaque_native_wire())
    catalog = get_task_authoring_catalog(_VERSION)
    patch = validate_workflow_patch_document(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "route"},
                            "set": task_set,
                        }
                    ]
                }
            }
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    ).patch

    with pytest.raises(UserInputError, match=r"opaque CONDITIONS task.*payload"):
        prepare_legacy_workflow_mutation_plan(
            graph,
            workflow_name="conditions-139-roundtrip",
            project_name="demo",
            description=None,
            release_state="OFFLINE",
            mutation=patch,
            catalog=catalog,
        )


@pytest.mark.parametrize(
    ("version", "expected_success", "expected_failure"),
    [
        ("2.0.0", "7002", "7003"),
        ("3.4.2", 7002, 7003),
    ],
)
def test_conditions_139_support_does_not_change_modern_code_projection(
    version: str,
    expected_success: int | str,
    expected_failure: int | str,
) -> None:
    refs = TaskRefIndex.from_code_by_name(
        {
            "upstream": 7001,
            "on-success": 7002,
            "on-failed": 7003,
        }
    )
    canonical = cast("JsonObject", deepcopy(_CANONICAL_PARAMS))

    encoded = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    dependence = encoded.task_params["dependence"]
    assert isinstance(dependence, dict)
    groups = dependence["dependTaskList"]
    assert isinstance(groups, list)
    assert groups[0]["dependItemList"] == [{"depTaskCode": 7001, "status": "SUCCESS"}]
    assert encoded.task_params["conditionResult"] == {
        "successNode": [expected_success],
        "failedNode": [expected_failure],
    }

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=encoded.task_params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == canonical


def test_conditions_139_schema_and_templates_disclose_runtime_boundaries() -> None:
    catalog = get_task_authoring_catalog(_VERSION)
    schema = task_type_schema_result(_TASK_TYPE, catalog=catalog)
    assert isinstance(schema.data, dict)
    field_guidance = " ".join(
        str(field.get("description", ""))
        for field in schema.data["fields"]
        if isinstance(field, dict)
        and isinstance(field.get("path"), str)
        and field["path"].startswith("task_params.")
    ).lower()
    template_guidance = []
    for variant in (None,):
        template = task_template_result(
            _TASK_TYPE,
            variant=variant,
            catalog=catalog,
        )
        assert isinstance(template.data, dict)
        template_guidance.append(cast("str", template.data["yaml"]).lower())

    for guidance in (field_guidance, *template_guidance):
        for phrase in (
            "same-process task-name predicates",
            "master",
            "task names",
            "info",
            "not secret storage",
            "no worker",
            "structured output",
            "remote application id",
            "failover",
            "retry",
            "reevaluate",
            "persisted",
        ):
            assert phrase in guidance
