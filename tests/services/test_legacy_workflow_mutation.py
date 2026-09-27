from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dsctl.models.workflow_patch import validate_workflow_patch_document
from dsctl.models.workflow_spec import WorkflowSpec
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def _live_graph() -> DecodedLegacyWorkflowGraph:
    process_data = {
        "globalParams": [],
        "tasks": [
            {
                "id": "tasks-11",
                "name": "extract",
                "type": "SHELL",
                "description": "",
                "params": {
                    "rawScript": "echo extract",
                    "resourceList": [],
                    "localParams": [],
                },
                "preTasks": [],
                "runFlag": "NORMAL",
                "maxRetryTimes": 0,
                "retryInterval": 1,
                "taskInstancePriority": "MEDIUM",
                "workerGroup": "default",
                "futureTaskField": {"keep": True},
            },
            {
                "id": "tasks-22",
                "name": "load",
                "type": "SHELL",
                "description": "",
                "params": {
                    "rawScript": "echo load",
                    "resourceList": [],
                    "localParams": [],
                },
                "preTasks": ["extract"],
                "runFlag": "NORMAL",
                "maxRetryTimes": 0,
                "retryInterval": 1,
                "taskInstancePriority": "MEDIUM",
                "workerGroup": "default",
            },
        ],
        "timeout": 30,
        "tenantId": 7,
        "futureProcessField": "keep",
    }
    locations = {
        "tasks-11": {
            "name": "extract",
            "targetarr": "",
            "nodenumber": 1,
            "x": 17,
            "y": 23,
            "futureLocationField": "keep",
        },
        "tasks-22": {
            "name": "load",
            "targetarr": "tasks-11",
            "nodenumber": 0,
            "x": 317,
            "y": 23,
        },
    }
    connects = [
        {
            "endPointSourceId": "tasks-11",
            "endPointTargetId": "tasks-22",
        }
    ]
    return decode_legacy_workflow_graph(
        json.dumps(process_data),
        json.dumps(locations),
        json.dumps(connects),
    )


def test_patch_plan_preserves_renamed_identity_and_allocates_only_new_tasks() -> None:
    patch = validate_workflow_patch_document(
        {
            "patch": {
                "workflow": {"set": {"timeout": 45}},
                "tasks": {
                    "rename": [{"from": "extract", "to": "extract-v2"}],
                    "create": [
                        {
                            "name": "verify",
                            "type": "SHELL",
                            "command": "echo verify",
                            "depends_on": ["extract-v2"],
                        }
                    ],
                },
            }
        }
    ).patch
    allocated_ids: Iterator[str] = iter(("tasks-33",))
    allocated_names: list[str] = []

    def allocate(task_name: str) -> str:
        allocated_names.append(task_name)
        return next(allocated_ids)

    plan = prepare_legacy_workflow_mutation_plan(
        _live_graph(),
        workflow_name="daily-sync",
        project_name="etl-prod",
        description="nightly",
        release_state="OFFLINE",
        mutation=patch,
        catalog=get_task_authoring_catalog("1.3.9"),
        task_id_factory=allocate,
    )

    assert plan.input_mode == "patch"
    assert plan.has_changes is True
    assert plan.diff["renamed_tasks"] == [
        {"from_name": "extract", "to_name": "extract-v2"}
    ]
    assert plan.diff["added_tasks"] == ["verify"]
    assert plan.merged_spec.workflow.timeout == 45
    assert plan.compilation.required_task_id_count == 1
    assert plan.compilation.task_ids == (
        ("extract-v2", "tasks-11"),
        ("load", "tasks-22"),
        ("verify", "tasks-33"),
    )
    assert allocated_names == ["verify"]

    payload = plan.compilation.preview()
    process_data = json.loads(payload["processDefinitionJson"])
    task_by_name = {task["name"]: task for task in process_data["tasks"]}
    assert process_data["futureProcessField"] == "keep"
    assert task_by_name["extract-v2"]["futureTaskField"] == {"keep": True}
    assert task_by_name["load"]["preTasks"] == ["extract-v2"]
    locations = json.loads(payload["locations"])
    assert locations["tasks-11"] == {
        "name": "extract-v2",
        "targetarr": "",
        "nodenumber": 2,
        "x": 17,
        "y": 23,
        "futureLocationField": "keep",
    }


def test_file_plan_matches_by_name_and_preserves_unchanged_native_fields() -> None:
    live_graph = _live_graph()
    desired_document = live_graph.workflow_document(
        name="daily-sync",
        project="etl-prod",
        description="nightly",
        release_state="ONLINE",
    )
    desired_tasks = desired_document["tasks"]
    assert isinstance(desired_tasks, list)
    load_task = desired_tasks[1]
    assert isinstance(load_task, dict)
    load_params = load_task["task_params"]
    assert isinstance(load_params, dict)
    load_params["rawScript"] = "echo changed"
    desired_tasks.append(
        {
            "name": "verify",
            "type": "SHELL",
            "command": "echo verify",
            "depends_on": ["load"],
        }
    )
    desired = WorkflowSpec.model_validate(desired_document)

    plan = prepare_legacy_workflow_mutation_plan(
        live_graph,
        workflow_name="daily-sync",
        project_name="etl-prod",
        description="nightly",
        release_state="OFFLINE",
        mutation=desired,
        catalog=get_task_authoring_catalog("1.3.9"),
        task_id_factory=lambda task_name: f"new-{task_name}",
    )

    assert plan.input_mode == "file"
    assert plan.has_changes is True
    assert plan.diff["workflow_updated_fields"] == ["release_state"]
    assert plan.diff["updated_tasks"] == ["load"]
    assert plan.diff["added_tasks"] == ["verify"]
    assert plan.diff["renamed_tasks"] == []
    assert plan.compilation.task_ids == (
        ("extract", "tasks-11"),
        ("load", "tasks-22"),
        ("verify", "new-verify"),
    )
    process_data = json.loads(plan.compilation.preview()["processDefinitionJson"])
    task_by_name = {task["name"]: task for task in process_data["tasks"]}
    assert task_by_name["extract"]["futureTaskField"] == {"keep": True}
    assert task_by_name["load"]["params"]["rawScript"] == "echo changed"


def test_noop_patch_reports_no_changes_without_allocating_task_ids() -> None:
    patch = validate_workflow_patch_document(
        {"patch": {"workflow": {"set": {"timeout": 30}}}}
    ).patch

    def unexpected_allocation(task_name: str) -> str:
        message = f"No-op patch unexpectedly allocated an id for {task_name}"
        raise AssertionError(message)

    plan = prepare_legacy_workflow_mutation_plan(
        _live_graph(),
        workflow_name="daily-sync",
        project_name="etl-prod",
        description="nightly",
        release_state="OFFLINE",
        mutation=patch,
        catalog=get_task_authoring_catalog("1.3.9"),
        task_id_factory=unexpected_allocation,
    )

    assert plan.input_mode == "patch"
    assert plan.has_changes is False
    assert plan.diff["workflow_updated_fields"] == []
    assert plan.compilation.required_task_id_count == 0
    assert plan.compilation.task_ids == (
        ("extract", "tasks-11"),
        ("load", "tasks-22"),
    )


def test_destructive_full_file_plan_retains_confirmation_requirement() -> None:
    live_graph = _live_graph()
    desired_document = live_graph.workflow_document(
        name="daily-sync-v2",
        project="etl-prod",
        description="nightly",
        release_state="OFFLINE",
    )
    desired_tasks = desired_document["tasks"]
    assert isinstance(desired_tasks, list)
    desired_document["tasks"] = desired_tasks[:1]

    plan = prepare_legacy_workflow_mutation_plan(
        live_graph,
        workflow_name="daily-sync",
        project_name="etl-prod",
        description="nightly",
        release_state="OFFLINE",
        mutation=WorkflowSpec.model_validate(desired_document),
        catalog=get_task_authoring_catalog("1.3.9"),
        task_id_factory=lambda task_name: f"new-{task_name}",
    )

    assert plan.confirmation == {
        "risk_type": "workflow_full_edit_destructive_change",
        "risk_level": "high",
        "deleted_tasks": ["load"],
        "renamed_workflow": True,
        "old_workflow_name": "daily-sync",
        "new_workflow_name": "daily-sync-v2",
        "task_type_changes": [],
    }
