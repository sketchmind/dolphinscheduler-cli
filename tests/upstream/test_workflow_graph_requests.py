from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.upstream.workflow_graph_requests import (
    WorkflowGraphCounts,
    legacy_workflow_arguments,
    legacy_workflow_instance_update_arguments,
    workflow_create_arguments,
    workflow_graph_counts,
    workflow_instance_update_arguments,
    workflow_update_arguments,
)

if TYPE_CHECKING:
    from dsctl.upstream.workflow_graph import WorkflowUpdatePayload


def _payload() -> WorkflowUpdatePayload:
    return {
        "name": "夜间同步",
        "description": None,
        "globalParams": '[ {"prop":"run_day", "value":"2026-09-06"} ]',
        "locations": "[ ]",
        "timeout": 0,
        "taskRelationJson": '[{"preTaskCode":0,"postTaskCode":9101}]',
        "taskDefinitionJson": '[ {"code":9101,"name":"同步"} ]',
        "executionType": "PARALLEL",
        "releaseState": None,
    }


def test_legacy_arguments_preserve_all_three_string_graph_representations() -> None:
    assert legacy_workflow_arguments(
        {
            "processDefinitionJson": '{ "tasks":[{"id":"task-a"}] }',
            "locations": '{"task-a":{"x":80,"y":80}}',
            "connects": '[ {"startPointSource":""} ]',
        }
    ) == {
        "process_definition_json": '{ "tasks":[{"id":"task-a"}] }',
        "locations": '{"task-a":{"x":80,"y":80}}',
        "connects": '[ {"startPointSource":""} ]',
    }


def test_definition_arguments_preserve_compiled_text_and_explicit_nulls() -> None:
    payload = _payload()
    expected = {
        "name": "夜间同步",
        "description": None,
        "global_params": '[ {"prop":"run_day", "value":"2026-09-06"} ]',
        "locations": "[ ]",
        "timeout": 0,
        "task_relation_json": '[{"preTaskCode":0,"postTaskCode":9101}]',
        "task_definition_json": '[ {"code":9101,"name":"同步"} ]',
        "execution_type": "PARALLEL",
    }
    assert workflow_create_arguments(payload) == expected
    assert workflow_update_arguments(payload) == {
        **expected,
        "release_state": None,
        "tenant_code": None,
    }
    assert workflow_update_arguments(payload, tenant_code="tenant-a") == {
        **expected,
        "release_state": None,
        "tenant_code": "tenant-a",
    }
    assert payload == _payload()


@pytest.mark.parametrize("sync_definition", [False, True])
def test_legacy_instance_arguments_preserve_native_field_binding(
    *,
    sync_definition: bool,
) -> None:
    assert legacy_workflow_instance_update_arguments(
        {
            "processDefinitionJson": '{ "tasks":[{"id":"task-a"}] }',
            "locations": '{"task-a":{"x":80,"y":80}}',
            "connects": '[ {"startPointSource":""} ]',
        },
        sync_definition=sync_definition,
    ) == {
        "process_instance_json": '{ "tasks":[{"id":"task-a"}] }',
        "locations": '{"task-a":{"x":80,"y":80}}',
        "connects": '[ {"startPointSource":""} ]',
        "sync_define": sync_definition,
    }


@pytest.mark.parametrize("sync_definition", [False, True])
def test_instance_arguments_bind_only_the_supported_graph_fields(
    *,
    sync_definition: bool,
) -> None:
    payload = _payload()
    assert workflow_instance_update_arguments(
        payload, sync_definition=sync_definition
    ) == {
        "task_relation_json": '[{"preTaskCode":0,"postTaskCode":9101}]',
        "task_definition_json": '[ {"code":9101,"name":"同步"} ]',
        "sync_define": sync_definition,
        "global_params": '[ {"prop":"run_day", "value":"2026-09-06"} ]',
        "locations": "[ ]",
        "timeout": 0,
    }


def test_graph_counts_count_rendered_arrays() -> None:
    assert workflow_graph_counts(_payload()) == WorkflowGraphCounts(
        task_definitions=1, task_relations=1, global_parameters=1
    )


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        (None, TypeError, "taskDefinitionJson did not compile to JSON text"),
        ("{}", TypeError, "taskDefinitionJson did not compile to a JSON array"),
        ("[", json.JSONDecodeError, "Expecting value"),
    ],
)
def test_graph_counts_preserve_malformed_payload_errors(
    value: str | None,
    error: type[Exception],
    message: str,
) -> None:
    payload = cast("dict[str, object]", _payload())
    payload["taskDefinitionJson"] = value
    with pytest.raises(error, match=message):
        workflow_graph_counts(cast("WorkflowUpdatePayload", payload))
