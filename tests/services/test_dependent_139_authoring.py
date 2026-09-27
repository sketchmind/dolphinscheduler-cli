from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING

import pytest

from dsctl.models.workflow_spec import WorkflowSpec, validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyDependentRefIndex,
    LegacyWorkflowGraphError,
    decode_legacy_workflow_graph,
    legacy_safe_dependent_targets,
    prepare_legacy_workflow_graph,
    prepare_legacy_workflow_lint_graph,
)
from dsctl.upstream.task_parameter_projection import ProjectionSource

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue

_BASE_MONTH_VALUES = (
    "thisMonth",
    "lastMonth",
    "lastMonthBegin",
    "lastMonthEnd",
)
_EXTENDED_MONTH_VALUES = (
    "thisMonth",
    "thisMonthBegin",
    "thisMonthEnd",
    "lastMonth",
    "lastMonthBegin",
    "lastMonthEnd",
)
_BASE_DATE_VERSIONS = ("1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.1.0")
_EXTENDED_DATE_VERSIONS = (
    "3.0.6",
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


def _dependent_params(
    *,
    version: str,
    cycle: str = "month",
    date_value: str = "thisMonth",
) -> YamlObject:
    item: YamlObject = {
        "dependentType": "DEPENDENT_ON_WORKFLOW",
        "cycle": cycle,
        "dateValue": date_value,
    }
    if version == "1.3.9":
        item.update(
            {
                "projectName": "analytics",
                "workflowName": "upstream-daily",
            }
        )
    else:
        item.update(
            {
                "projectCode": 7,
                "definitionCode": 101,
                "depTaskCode": 0,
            }
        )
    return {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [item],
                }
            ],
        }
    }


def _normalize(version: str, *, cycle: str, date_value: str) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    return catalog.normalize_task_params(
        "DEPENDENT",
        _dependent_params(version=version, cycle=cycle, date_value=date_value),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )


def _workflow_spec(*, task_target: bool = False) -> WorkflowSpec:
    item: YamlObject = {
        "dependentType": (
            "DEPENDENT_ON_TASK" if task_target else "DEPENDENT_ON_WORKFLOW"
        ),
        "projectName": "analytics",
        "workflowName": "upstream-daily",
        "cycle": "day",
        "dateValue": "last1Days",
    }
    if task_target:
        item["taskName"] = "extract"
    return validate_workflow_document(
        {
            "workflow": {"name": "downstream", "project": "orchestration"},
            "tasks": [
                {
                    "name": "wait-upstream",
                    "type": "DEPENDENT",
                    "task_params": {
                        "dependence": {
                            "relation": "AND",
                            "dependTaskList": [
                                {
                                    "relation": "OR",
                                    "dependItemList": [item],
                                }
                            ],
                        }
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("1.3.9"),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _dependent_refs(*, task_target: bool = False) -> LegacyDependentRefIndex:
    task_name = "extract" if task_target else None
    dep_tasks = "extract" if task_target else "ALL"
    return LegacyDependentRefIndex.from_native_by_selector(
        {
            ("analytics", "upstream-daily", task_name): (7, 101, dep_tasks),
        }
    )


def _yaml_object(value: object) -> YamlObject:
    assert isinstance(value, dict)
    return value


def _yaml_list(value: object) -> list[YamlValue]:
    assert isinstance(value, list)
    return value


def _sole_dependent_item(params: YamlObject) -> YamlObject:
    dependence = _yaml_object(params["dependence"])
    group = _yaml_object(_yaml_list(dependence["dependTaskList"])[0])
    return _yaml_object(_yaml_list(group["dependItemList"])[0])


def _opaque_dependent_graph() -> DecodedLegacyWorkflowGraph:
    payload = prepare_legacy_workflow_graph(
        _workflow_spec(task_target=True),
        task_id_factory=lambda _name: "tasks-wait",
        dependent_refs=_dependent_refs(task_target=True),
    ).materialize()
    process = json.loads(payload["processDefinitionJson"])
    process["tasks"][0]["dependence"]["dependTaskList"][0]["dependItemList"][0][
        "future"
    ] = {"keep": True}
    payload["processDefinitionJson"] = json.dumps(process)
    return decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
        dependent_refs=_dependent_refs(task_target=True),
    )


def test_dependent_139_is_reviewed_with_name_routed_closed_model() -> None:
    catalog = get_task_authoring_catalog("1.3.9")
    fact = catalog.task_type_facts["DEPENDENT"]
    review = fact.typed_authoring_review

    assert review is not None
    assert review.cli_model == "Dependent139TaskParamsSpec"
    assert "DEPENDENT" in catalog.entries
    assert len(catalog.reviewed_typed_task_types) == 11
    assert catalog.supports_typed_authoring("DEPENDENT") is True
    assert catalog.supports_opaque_authoring("DEPENDENT") is False

    normalized = _normalize("1.3.9", cycle="day", date_value="last1Days")
    item = _sole_dependent_item(normalized)
    assert item == {
        "dependentType": "DEPENDENT_ON_WORKFLOW",
        "projectName": "analytics",
        "workflowName": "upstream-daily",
        "cycle": "day",
        "dateValue": "last1Days",
    }


@pytest.mark.parametrize("version", _BASE_DATE_VERSIONS)
@pytest.mark.parametrize("date_value", _BASE_MONTH_VALUES)
def test_dependent_base_profiles_accept_exact_month_values(
    version: str,
    date_value: str,
) -> None:
    assert _normalize(version, cycle="month", date_value=date_value)


@pytest.mark.parametrize("version", _BASE_DATE_VERSIONS)
@pytest.mark.parametrize("date_value", ["thisMonthBegin", "thisMonthEnd"])
def test_dependent_base_profiles_reject_runtime_absent_month_values(
    version: str,
    date_value: str,
) -> None:
    with pytest.raises(ValueError, match="dateValue"):
        _normalize(version, cycle="month", date_value=date_value)


@pytest.mark.parametrize("version", _EXTENDED_DATE_VERSIONS)
@pytest.mark.parametrize("date_value", _EXTENDED_MONTH_VALUES)
def test_dependent_extended_profiles_accept_exact_month_values(
    version: str,
    date_value: str,
) -> None:
    assert _normalize(version, cycle="month", date_value=date_value)


@pytest.mark.parametrize("version", ["1.3.9", "3.4.2"])
def test_dependent_rejects_cycle_date_value_mismatch(version: str) -> None:
    with pytest.raises(ValueError, match="dateValue"):
        _normalize(version, cycle="day", date_value="thisMonth")


@pytest.mark.parametrize("version", ["1.3.9", "3.4.2"])
def test_dependent_rejects_unknown_date_value(version: str) -> None:
    with pytest.raises(ValueError, match="dateValue"):
        _normalize(version, cycle="day", date_value="notAWindow")


@pytest.mark.parametrize("task_target", [False, True])
def test_dependent_139_compiles_names_to_split_outer_wire(
    task_target: bool,  # noqa: FBT001
) -> None:
    prepared = prepare_legacy_workflow_graph(
        _workflow_spec(task_target=task_target),
        task_id_factory=lambda _name: "tasks-wait",
        dependent_refs=_dependent_refs(task_target=task_target),
    )

    process = json.loads(prepared.materialize()["processDefinitionJson"])
    task = process["tasks"][0]
    assert prepared.edges == ()
    assert task["type"] == "DEPENDENT"
    assert task["params"] == {}
    assert task["dependence"] == {
        "relation": "AND",
        "dependTaskList": [
            {
                "relation": "OR",
                "dependItemList": [
                    {
                        "projectId": 7,
                        "definitionId": 101,
                        "depTasks": "extract" if task_target else "ALL",
                        "cycle": "day",
                        "dateValue": "last1Days",
                    }
                ],
            }
        ],
    }


def test_dependent_139_lint_uses_local_only_synthetic_ids() -> None:
    prepared = prepare_legacy_workflow_lint_graph(_workflow_spec(task_target=True))
    process = json.loads(prepared.materialize()["processDefinitionJson"])
    item = process["tasks"][0]["dependence"]["dependTaskList"][0]["dependItemList"][0]

    assert item["projectId"] > 0
    assert item["definitionId"] > 0
    assert item["depTasks"] == "extract"


def test_dependent_139_safe_wire_reverse_binds_to_names() -> None:
    payload = prepare_legacy_workflow_graph(
        _workflow_spec(task_target=True),
        task_id_factory=lambda _name: "tasks-wait",
        dependent_refs=_dependent_refs(task_target=True),
    ).materialize()

    graph = decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
        dependent_refs=_dependent_refs(task_target=True),
    )

    task = graph.tasks[0]
    assert task.task_params_reencode_source is ProjectionSource.TYPED_AUTHORING
    document = graph.workflow_document(name="downstream", project="orchestration")
    document_tasks = document["tasks"]
    assert isinstance(document_tasks, list)
    document_task = document_tasks[0]
    assert isinstance(document_task, dict)
    assert (
        document_task["task_params"]
        == _workflow_spec(
            task_target=True,
        )
        .tasks[0]
        .task_params
    )


def test_dependent_139_safe_native_target_inventory_is_exact_and_deduplicated() -> None:
    payload = prepare_legacy_workflow_graph(
        _workflow_spec(task_target=True),
        task_id_factory=lambda _name: "tasks-wait",
        dependent_refs=_dependent_refs(task_target=True),
    ).materialize()
    graph = decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
    )

    assert legacy_safe_dependent_targets(graph) == ((7, 101, "extract"),)


@pytest.mark.parametrize("richer_field", ["future", "dependResult", "status"])
def test_dependent_139_richer_or_runtime_item_state_stays_opaque(
    richer_field: str,
) -> None:
    payload = prepare_legacy_workflow_graph(
        _workflow_spec(task_target=True),
        task_id_factory=lambda _name: "tasks-wait",
        dependent_refs=_dependent_refs(task_target=True),
    ).materialize()
    process = json.loads(payload["processDefinitionJson"])
    item = process["tasks"][0]["dependence"]["dependTaskList"][0]["dependItemList"][0]
    item[richer_field] = "SUCCESS" if richer_field != "future" else {"keep": True}
    payload["processDefinitionJson"] = json.dumps(process)

    graph = decode_legacy_workflow_graph(
        payload["processDefinitionJson"],
        payload["locations"],
        payload["connects"],
        dependent_refs=_dependent_refs(task_target=True),
    )

    task = graph.tasks[0]
    assert task.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert isinstance(task.document["task_params"], Mapping)
    assert task.document["task_params"] == {}
    assert legacy_safe_dependent_targets(graph) == ()


def test_dependent_139_opaque_outer_state_survives_metadata_recompile() -> None:
    graph = _opaque_dependent_graph()
    baseline = graph.to_workflow_spec(
        name="downstream",
        project="orchestration",
    )
    changed_task = baseline.tasks[0].model_copy(
        update={"description": "metadata only"},
    )
    desired = baseline.model_copy(update={"tasks": [changed_task]})

    reparsed = json.loads(
        prepare_legacy_workflow_graph(
            desired,
            baseline=graph,
        ).materialize()["processDefinitionJson"]
    )

    preserved = reparsed["tasks"][0]
    assert preserved["description"] == "metadata only"
    assert preserved["params"] == {}
    assert preserved["dependence"]["dependTaskList"][0]["dependItemList"][0][
        "future"
    ] == {"keep": True}


def test_dependent_139_opaque_outer_state_allows_unrelated_topology_addition() -> None:
    graph = _opaque_dependent_graph()
    baseline = graph.to_workflow_spec(
        name="downstream",
        project="orchestration",
    )
    unrelated = validate_workflow_document(
        {
            "workflow": {"name": "unrelated", "project": "orchestration"},
            "tasks": [
                {"name": "local-a", "type": "SHELL", "command": "echo a"},
                {
                    "name": "local-b",
                    "type": "SHELL",
                    "command": "echo b",
                    "depends_on": ["local-a"],
                },
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("1.3.9"),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    desired = baseline.model_copy(
        update={"tasks": [*baseline.tasks, *unrelated.tasks]},
    )

    prepared = prepare_legacy_workflow_graph(
        desired,
        baseline=graph,
        task_id_factory=lambda name: f"tasks-{name}",
    )
    process = json.loads(prepared.materialize()["processDefinitionJson"])

    assert prepared.edges == (("local-a", "local-b"),)
    assert process["tasks"][0]["dependence"]["dependTaskList"][0]["dependItemList"][0][
        "future"
    ] == {"keep": True}


@pytest.mark.parametrize(
    "mutation",
    ["delete", "rename", "type", "task_params", "command"],
)
def test_dependent_139_opaque_outer_state_rejects_destructive_task_change(
    mutation: str,
) -> None:
    graph = _opaque_dependent_graph()
    baseline = graph.to_workflow_spec(
        name="downstream",
        project="orchestration",
    )
    task = baseline.tasks[0]
    if mutation == "delete":
        desired = baseline.model_copy(update={"tasks": []})
    else:
        if mutation == "rename":
            changed_task = task.model_copy(update={"name": "renamed"})
        elif mutation == "type":
            changed_task = task.model_copy(update={"type": "SHELL"})
        elif mutation == "task_params":
            changed_task = task.model_copy(
                update={"task_params": {"changed": True}},
            )
        else:
            changed_task = task.model_copy(update={"command": "echo changed"})
        desired = baseline.model_copy(
            update={"tasks": [changed_task]},
        )

    with pytest.raises(LegacyWorkflowGraphError, match="opaque DEPENDENT"):
        prepare_legacy_workflow_graph(desired, baseline=graph)
