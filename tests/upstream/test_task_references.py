from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pytest

from dsctl.models.workflow_spec import WorkflowTaskSpec
from dsctl.services._workflow.compile import workflow_edges
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    encode_task_parameters,
)
from dsctl.upstream.task_references import rename_task_references

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def test_switch_rename_preserves_opaque_policy_without_granting_typed_validity() -> (
    None
):
    payload: JsonObject = {
        "switchResult": {
            "dependTaskList": [None, {"condition": "old", "nextNode": "old"}],
            "nextNode": "old",
        },
        "nextBranch": "old",
        "future": {"nextNode": "old"},
    }
    original = deepcopy(payload)
    renamed = rename_task_references("SWITCH", payload, {"old": "new"})

    assert payload == original
    assert renamed == {
        "switchResult": {
            "dependTaskList": [{"condition": "old", "nextNode": "new"}],
            "nextNode": "new",
        },
        "nextBranch": "new",
        "future": {"nextNode": "old"},
    }
    # Persisted runtime state is rewritable but cannot authorize a new typed task.
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.1",
            task_type="SWITCH",
            task_params=renamed,
            refs=TaskRefIndex.from_code_by_name({"new": 101}),
            source=ProjectionSource.TYPED_AUTHORING,
        )
    assert captured.value.details["field"] == "task_params.nextBranch"


def test_graph_excludes_runtime_switch_reference_and_retains_numeric_task_names() -> (
    None
):
    tasks = [
        WorkflowTaskSpec.model_construct(
            name="route",
            type="SWITCH",
            task_params={
                "switchResult": {
                    "dependTaskList": [{"nextNode": "101"}, {"nextNode": 999}],
                    "nextNode": "101",
                },
                "nextBranch": "missing-runtime-target",
            },
        ),
        WorkflowTaskSpec.model_construct(name="101", type="SHELL", command="echo ok"),
    ]

    assert workflow_edges(tasks) == [("route", "101")]


def test_conditions_rename_preserves_unknown_rows_and_native_integer_values() -> None:
    payload: JsonObject = {
        "dependence": {
            "dependTaskList": [
                None,
                {"dependItemList": [False, {"task": "101", "status": "SUCCESS"}]},
            ]
        },
        "conditionResult": {"successNode": [101, "101"], "failedNode": ["other"]},
    }
    original = deepcopy(payload)

    assert rename_task_references("CONDITIONS", payload, {"101": "renamed"}) == {
        "dependence": {
            "dependTaskList": [
                None,
                {"dependItemList": [False, {"task": "renamed", "status": "SUCCESS"}]},
            ]
        },
        "conditionResult": {"successNode": [101, "renamed"], "failedNode": ["other"]},
    }
    assert payload == original
