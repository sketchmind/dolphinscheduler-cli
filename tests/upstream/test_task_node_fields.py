"""One native field decision must reach every task-node projection consumer."""

import json
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.models.workflow_spec import WorkflowSpec
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream import task_settings, workflow_graph_preservation
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskResourceRefIndex,
    TaskWorkflowRefIndex,
)
from dsctl.upstream.workflow_graph import render_workflow_graph

if TYPE_CHECKING:
    from dsctl.upstream.serialization import TaskData


def test_one_native_binding_drives_wire_export_discovery_and_authorized_overlay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This is an in-memory maintenance probe, not a new exact-version claim.
    monkeypatch.setitem(
        task_settings._TASK_NODE_FIELDS,
        "delay",
        replace(task_settings._TASK_NODE_FIELDS["delay"], native_name="renamedDelay"),
    )
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "flow"},
            "tasks": [
                {"name": "task", "type": "SHELL", "command": "echo ok", "delay": 6}
            ],
        }
    )
    payload = render_workflow_graph(
        spec,
        task_codes={"task": 101},
        task_versions={"task": 1},
        task_cache_by_name={},
        edges=[],
        levels={"task": 0},
        task_cache_requested=False,
        profile_version="3.4.2",
        projection_sources={"task": ProjectionSource.TYPED_AUTHORING},
        resource_refs=TaskResourceRefIndex({}, {}, {}),
        workflow_refs=TaskWorkflowRefIndex({}, {}),
    )
    native = json.loads(payload["taskDefinitionJson"])[0]
    assert native["renamedDelay"] == 6
    assert "delayTime" not in native
    assert task_settings.decode_task_node_fields(cast("TaskData", native))["delay"] == 6

    result = task_type_schema_result(
        "SHELL", field="delay", catalog=get_task_authoring_catalog("3.4.2")
    )
    assert isinstance(result.data, dict)
    assert isinstance(result.data["fields"], list)
    assert (
        result.data["fields"][0]["compile_path"] == "taskDefinitionJson[].renamedDelay"
    )

    current = {**native, "renamedDelay": 0, "cpuQuota": -1, "future": "preserved"}
    merged = workflow_graph_preservation._merge_task_definitions(
        [current],
        compiled_tasks=[{**native, "cpuQuota": 8}],
        task_name="task",
        requested_fields=("delay", "cpu_quota"),
    )
    assert merged == [{**current, "renamedDelay": 6, "cpuQuota": 8}]
    delay_only = workflow_graph_preservation._merge_task_definitions(
        [current],
        compiled_tasks=[{**native, "cpuQuota": 8}],
        task_name="task",
        requested_fields=("delay",),
    )
    assert delay_only == [{**current, "renamedDelay": 6}]
