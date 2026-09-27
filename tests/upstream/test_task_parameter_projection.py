from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Literal

import pytest

from dsctl.errors import UserInputError
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


_MODERN_EMR_VERSIONS = (
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


@pytest.fixture
def refs() -> TaskRefIndex:
    """Return one exact local-task name/code bijection."""
    return TaskRefIndex.from_code_by_name(
        {
            "upstream": 101,
            "success": 102,
            "failed": 103,
            "default": 104,
        }
    )


def test_ref_index_rejects_non_bijective_inputs() -> None:
    """Task references must be an immutable name/code bijection."""
    with pytest.raises(ValueError, match="bijection"):
        TaskRefIndex(
            code_by_name={"one": 101},
            name_by_code={101: "other"},
        )


def test_unowned_task_params_are_copied_without_projection(refs: TaskRefIndex) -> None:
    """The pure seam must isolate identity projections from caller mutation."""
    authored: JsonObject = {"rawScript": "echo ok", "resourceList": []}

    projected = encode_task_parameters(
        version="3.2.2",
        task_type="SHELL",
        task_params=authored,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    authored["rawScript"] = "mutated"
    first_copy = projected.task_params
    first_copy["rawScript"] = "also mutated"

    assert projected.task_type == "SHELL"
    assert projected.task_params == {
        "rawScript": "echo ok",
        "resourceList": [],
    }


@pytest.mark.parametrize(
    ("version", "expected_ref"),
    [
        ("2.0.0", "102"),
        ("2.0.9", 102),
        ("3.0.0", 102),
        ("3.1.9", 102),
        ("3.2.0", 102),
        ("3.2.2", 102),
        ("3.3.1", 102),
        ("3.4.2", 102),
    ],
)
def test_switch_refs_roundtrip_through_exact_wire_epochs(
    version: str,
    expected_ref: int | str,
    refs: TaskRefIndex,
) -> None:
    """SWITCH emits each epoch's native reference shape and decodes to names."""
    canonical: JsonObject = {
        "switchResult": {
            "dependTaskList": [
                {"condition": "${route} == 'yes'", "nextNode": "success"}
            ],
            "nextNode": "default",
        },
        "localParams": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="SWITCH",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    switch_result = encoded.task_params["switchResult"]
    assert isinstance(switch_result, dict)
    branches = switch_result["dependTaskList"]
    assert isinstance(branches, list)
    assert branches[0]["nextNode"] == expected_ref
    default_ref = "104" if version == "2.0.0" else 104
    assert switch_result["nextNode"] == default_ref
    assert (
        decode_task_parameters(
            version=version,
            task_type=encoded.task_type,
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


def test_switch_modern_next_branch_is_runtime_only(refs: TaskRefIndex) -> None:
    """Persisted modern nextBranch evidence is never typed authoring input."""
    native: JsonObject = {
        "switchResult": {"dependTaskList": [], "nextNode": "default"},
        "nextBranch": "success",
    }

    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.1",
            task_type="SWITCH",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.nextBranch"
    assert captured.value.details["reason"] == "runtime-only"
    preserved = encode_task_parameters(
        version="3.4.1",
        task_type="SWITCH",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert preserved.task_params == {
        "switchResult": {"dependTaskList": [], "nextNode": 104},
        "nextBranch": 102,
    }
    decoded = decode_task_parameters(
        version="3.4.1",
        task_type="SWITCH",
        task_params=preserved.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.task_params == native


def test_switch_default_only_injects_native_empty_branch_list(
    refs: TaskRefIndex,
) -> None:
    """Default-only canonical SWITCH avoids a null native dependTaskList."""
    encoded = encode_task_parameters(
        version="3.4.2",
        task_type="SWITCH",
        task_params={"switchResult": {"nextNode": "default"}},
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == {
        "switchResult": {"dependTaskList": [], "nextNode": 104}
    }


def test_switch_legacy_runtime_only_fields_fail_closed(refs: TaskRefIndex) -> None:
    """Typed authoring cannot persist legacy SWITCH runtime state."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.2.2",
            task_type="SWITCH",
            task_params={
                "switchResult": {"dependTaskList": [], "nextNode": "default"},
                "nextBranch": "success",
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert isinstance(captured.value, UserInputError)
    assert captured.value.details == {
        "version": "3.2.2",
        "direction": "encode",
        "task_type": "SWITCH",
        "field": "task_params.nextBranch",
        "reason": "runtime-only",
    }


def test_opaque_preserve_is_lossless_for_unrepresentable_switch_state(
    refs: TaskRefIndex,
) -> None:
    """Opaque preservation never guesses how to normalize native runtime fields."""
    native: JsonObject = {
        "switchResult": {
            "dependTaskList": [{"condition": "true", "nextNode": ["102", "103"]}],
            "nextNode": ["104"],
        },
        "nextBranch": 102,
        "resultConditionLocation": 0,
    }
    expected = deepcopy(native)

    decoded = decode_task_parameters(
        version="2.0.0",
        task_type="SWITCH",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    native["nextBranch"] = 999
    encoded = encode_task_parameters(
        version="2.0.0",
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert encoded.task_type == "SWITCH"
    assert encoded.task_params == expected


@pytest.mark.parametrize(
    ("version", "expected_outcomes"),
    [
        ("2.0.0", {"successNode": ["102"], "failedNode": ["103"]}),
        ("2.0.9", {"successNode": [102], "failedNode": [103]}),
        ("3.0.0", {"successNode": ["102"], "failedNode": ["103"]}),
        ("3.0.6", {"successNode": ["102"], "failedNode": ["103"]}),
        ("3.1.0", {"successNode": ["102"], "failedNode": ["103"]}),
        ("3.1.9", {"successNode": ["102"], "failedNode": ["103"]}),
        ("3.2.0", {"successNode": [102], "failedNode": [103]}),
        ("3.2.1", {"successNode": [102], "failedNode": [103]}),
        ("3.2.2", {"successNode": [102], "failedNode": [103]}),
        ("3.3.1", {"successNode": [102], "failedNode": [103]}),
        ("3.3.2", {"successNode": [102], "failedNode": [103]}),
        ("3.4.0", {"successNode": [102], "failedNode": [103]}),
        ("3.4.1", {"successNode": [102], "failedNode": [103]}),
        ("3.4.2", {"successNode": [102], "failedNode": [103]}),
    ],
)
def test_conditions_refs_roundtrip_in_every_code_based_epoch(
    version: str,
    expected_outcomes: dict[str, list[int | str]],
    refs: TaskRefIndex,
) -> None:
    """CONDITIONS predicates and both outcomes use the local task-code index."""
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "OR",
                    "dependItemList": [{"task": "upstream", "status": "SUCCESS"}],
                }
            ],
        },
        "conditionResult": {
            "successNode": ["success"],
            "failedNode": ["failed"],
        },
        "localParams": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="CONDITIONS",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    native = encoded.task_params
    dependence = native["dependence"]
    assert isinstance(dependence, dict)
    groups = dependence["dependTaskList"]
    assert isinstance(groups, list)
    assert groups[0]["dependItemList"] == [{"depTaskCode": 101, "status": "SUCCESS"}]
    assert native["conditionResult"] == expected_outcomes
    assert (
        decode_task_parameters(
            version=version,
            task_type="CONDITIONS",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize(
    "task_type",
    ["CONDITIONS", "DEPENDENT", "SUB_WORKFLOW", "SWITCH"],
)
def test_139_logic_and_nested_tasks_have_no_pure_code_projection(
    task_type: str,
    refs: TaskRefIndex,
) -> None:
    """The 1.3.9 native shapes need task names or database ids, not task codes."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="1.3.9",
            task_type=task_type,
            task_params={},
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["version"] == "1.3.9"
    assert captured.value.details["reason"] == "no-code-based-projection"


def test_139_opaque_name_based_switch_is_preserved(refs: TaskRefIndex) -> None:
    """A native 1.3.9 name-based logic task remains lossless despite no authoring."""
    native: JsonObject = {
        "switchResult": {
            "dependTaskList": [{"condition": "true", "nextNode": "success"}],
            "nextNode": "default",
        }
    }

    encoded = encode_task_parameters(
        version="1.3.9",
        task_type="SWITCH",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert encoded.task_params == native


def test_conditions_rejects_unknown_canonical_predecessor(refs: TaskRefIndex) -> None:
    """Unknown predicate names fail before a native workflow request exists."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="CONDITIONS",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {"task": "missing", "status": "SUCCESS"}
                            ],
                        }
                    ],
                },
                "conditionResult": {
                    "successNode": ["success"],
                    "failedNode": ["failed"],
                },
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == (
        "task_params.dependence.dependTaskList[0].dependItemList[0].task"
    )
    assert captured.value.details["reason"] == "unknown-task-name"


def test_conditions_rejects_unknown_native_outcome_code(refs: TaskRefIndex) -> None:
    """Typed export never guesses a task name for an unknown native code."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="3.2.2",
            task_type="CONDITIONS",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {"depTaskCode": 101, "status": "SUCCESS"}
                            ],
                        }
                    ],
                },
                "conditionResult": {
                    "successNode": [999],
                    "failedNode": [103],
                },
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == (
        "task_params.conditionResult.successNode[0]"
    )
    assert captured.value.details["reason"] == "unknown-task-code"


@pytest.mark.parametrize("status", ["RUNNING_EXECUTION", "PAUSE", "FORCED_SUCCESS"])
def test_conditions_rejects_non_predicate_statuses(
    status: str,
    refs: TaskRefIndex,
) -> None:
    """CONDITIONS supports only terminal success/failure predicate statuses."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.3.1",
            task_type="CONDITIONS",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [{"task": "upstream", "status": status}],
                        }
                    ],
                },
                "conditionResult": {
                    "successNode": ["success"],
                    "failedNode": ["failed"],
                },
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "unsupported-condition-status"


def test_conditions_runtime_result_is_not_typed_authoring(refs: TaskRefIndex) -> None:
    """conditionSuccess is runtime state even though modern JSON serializes it."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.1",
            task_type="CONDITIONS",
            task_params={
                "dependence": {"relation": "AND", "dependTaskList": []},
                "conditionResult": {
                    "conditionSuccess": False,
                    "successNode": ["success"],
                    "failedNode": ["failed"],
                },
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == (
        "task_params.conditionResult.conditionSuccess"
    )
    assert captured.value.details["reason"] == "runtime-only"


def test_switch_rejects_multi_target_native_reference(refs: TaskRefIndex) -> None:
    """A legacy list with several targets cannot become one canonical branch."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="2.0.0",
            task_type="SWITCH",
            task_params={
                "switchResult": {
                    "dependTaskList": [
                        {"condition": "true", "nextNode": ["102", "103"]}
                    ],
                    "nextNode": ["104"],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "unrepresentable-reference-shape"


@pytest.mark.parametrize(
    "version",
    [
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    ],
)
def test_sub_workflow_uses_legacy_sub_process_wire(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """The stable nested-workflow intent maps to SUB_PROCESS before 3.3.1."""
    canonical: JsonObject = {
        "workflowDefinitionCode": 9001,
        "localParams": [],
        "varPool": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="SUB_WORKFLOW",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "SUB_PROCESS"
    assert encoded.task_params == {
        "processDefinitionCode": 9001,
        "localParams": [],
        "varPool": [],
    }
    assert decode_task_parameters(
        version=version,
        task_type="SUB_PROCESS",
        task_params=encoded.task_params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    ) == encode_task_parameters(
        version="3.4.1",
        task_type="SUB_WORKFLOW",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"])
def test_sub_workflow_is_native_in_modern_epochs(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """SUB_WORKFLOW and workflowDefinitionCode are native from 3.3.1 onward."""
    canonical: JsonObject = {
        "workflowDefinitionCode": 9001,
        "localParams": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="SUB_WORKFLOW",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "SUB_WORKFLOW"
    assert encoded.task_params == canonical
    assert (
        decode_task_parameters(
            version=version,
            task_type=encoded.task_type,
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", ["2.0.0", "3.2.2", "3.4.2"])
def test_sub_workflow_nonempty_resource_list_fails_closed(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """SUB workflow plugins do not consume compatibility-only resourceList."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="SUB_WORKFLOW",
            task_params={
                "workflowDefinitionCode": 9001,
                "resourceList": [{"id": 7}],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.resourceList"
    assert captured.value.details["reason"] == "compatibility-only-field"


def test_sub_workflow_empty_resource_list_is_omitted(refs: TaskRefIndex) -> None:
    """An empty compatibility list does not leak onto the native task wire."""
    encoded = encode_task_parameters(
        version="3.2.2",
        task_type="SUB_WORKFLOW",
        task_params={"workflowDefinitionCode": 9001, "resourceList": []},
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == {"processDefinitionCode": 9001}


_DEPENDENT_BASE_DATE_WINDOWS = (
    ("hour", "currentHour"),
    ("hour", "last1Hour"),
    ("hour", "last2Hours"),
    ("hour", "last3Hours"),
    ("hour", "last24Hours"),
    ("day", "today"),
    ("day", "last1Days"),
    ("day", "last2Days"),
    ("day", "last3Days"),
    ("day", "last7Days"),
    ("week", "thisWeek"),
    ("week", "lastWeek"),
    ("week", "lastMonday"),
    ("week", "lastTuesday"),
    ("week", "lastWednesday"),
    ("week", "lastThursday"),
    ("week", "lastFriday"),
    ("week", "lastSaturday"),
    ("week", "lastSunday"),
    ("month", "thisMonth"),
    ("month", "lastMonth"),
    ("month", "lastMonthBegin"),
    ("month", "lastMonthEnd"),
)
_DEPENDENT_ITEM_FIELD = "task_params.dependence.dependTaskList[0].dependItemList[0]"


@pytest.mark.parametrize(
    ("version", "parameter_passing", "native_has_dependent_type"),
    [
        ("2.0.0", None, False),
        ("2.0.9", None, False),
        ("3.0.0", None, False),
        ("3.0.6", None, False),
        ("3.1.0", None, False),
        ("3.1.9", None, False),
        ("3.2.0", None, False),
        ("3.2.1", False, False),
        ("3.2.2", False, False),
        ("3.3.1", False, True),
        ("3.4.2", False, True),
    ],
)
def test_dependent_items_roundtrip_through_semantic_epochs(
    version: str,
    parameter_passing: Literal[False] | None,
    native_has_dependent_type: Literal[False, True],
    refs: TaskRefIndex,
) -> None:
    """DEPENDENT projects only fields consumed by each exact plugin epoch."""
    item: JsonObject = {
        "dependentType": "DEPENDENT_ON_WORKFLOW",
        "projectCode": 7001,
        "definitionCode": 8001,
        "depTaskCode": 0,
        "cycle": "day",
        "dateValue": "today",
    }
    if parameter_passing is not None:
        item["parameterPassing"] = parameter_passing
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [{"relation": "OR", "dependItemList": [item]}],
        },
        "localParams": [],
        "varPool": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="DEPENDENT",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    native_dependence = encoded.task_params["dependence"]
    assert isinstance(native_dependence, dict)
    native_groups = native_dependence["dependTaskList"]
    assert isinstance(native_groups, list)
    native_item = native_groups[0]["dependItemList"][0]
    assert ("dependentType" in native_item) is native_has_dependent_type
    if version == "3.2.1":
        assert native_item["parameterPassing"] is False
    assert (
        decode_task_parameters(
            version=version,
            task_type="DEPENDENT",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", ["2.0.0", "3.4.2"])
@pytest.mark.parametrize(("cycle", "date_value"), _DEPENDENT_BASE_DATE_WINDOWS)
def test_dependent_base_date_windows_project_unchanged(
    version: str,
    cycle: str,
    date_value: str,
    refs: TaskRefIndex,
) -> None:
    """The 23 date windows common to every runtime retain exact spelling."""
    encoded = encode_task_parameters(
        version=version,
        task_type="DEPENDENT",
        task_params={
            "dependence": {
                "relation": "AND",
                "dependTaskList": [
                    {
                        "relation": "AND",
                        "dependItemList": [
                            {
                                "dependentType": "DEPENDENT_ON_WORKFLOW",
                                "projectCode": 7001,
                                "definitionCode": 8001,
                                "depTaskCode": 0,
                                "cycle": cycle,
                                "dateValue": date_value,
                            }
                        ],
                    }
                ],
            }
        },
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    dependence = encoded.task_params["dependence"]
    assert isinstance(dependence, dict)
    groups = dependence["dependTaskList"]
    assert isinstance(groups, list)
    group = groups[0]
    assert isinstance(group, dict)
    items = group["dependItemList"]
    assert isinstance(items, list)
    item = items[0]
    assert isinstance(item, dict)
    assert (item["cycle"], item["dateValue"]) == (cycle, date_value)


@pytest.mark.parametrize("version", ["2.0.0", "2.0.9", "3.0.0", "3.1.0"])
@pytest.mark.parametrize("date_value", ["thisMonthBegin", "thisMonthEnd"])
def test_dependent_base_date_epochs_reject_extended_month_values(
    version: str,
    date_value: str,
    refs: TaskRefIndex,
) -> None:
    """Base-only runtimes must reject a date value absent from their switch."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "month",
                                    "dateValue": date_value,
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "encode",
        "task_type": "DEPENDENT",
        "field": (
            "task_params.dependence.dependTaskList[0].dependItemList[0].dateValue"
        ),
        "reason": "field-absent-in-version",
    }


def test_dependent_typed_decode_rejects_date_absent_from_exact_runtime(
    refs: TaskRefIndex,
) -> None:
    """Typed reads cannot canonicalize a date the selected runtime cannot run."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="3.0.0",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "month",
                                    "dateValue": "thisMonthBegin",
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["direction"] == "decode"
    assert captured.value.details["reason"] == "field-absent-in-version"


@pytest.mark.parametrize(
    ("version", "parameter_passing"),
    [
        ("3.0.6", None),
        ("3.1.9", None),
        ("3.2.0", None),
        ("3.2.1", False),
        ("3.2.2", False),
        ("3.3.1", False),
        ("3.3.2", False),
        ("3.4.0", False),
        ("3.4.1", False),
        ("3.4.2", False),
    ],
)
@pytest.mark.parametrize("date_value", ["thisMonthBegin", "thisMonthEnd"])
def test_dependent_extended_date_epochs_roundtrip_month_boundaries(
    version: str,
    parameter_passing: Literal[False] | None,
    date_value: str,
    refs: TaskRefIndex,
) -> None:
    """Every runtime with the extended switch accepts both month boundaries."""
    item: JsonObject = {
        "dependentType": "DEPENDENT_ON_WORKFLOW",
        "projectCode": 7001,
        "definitionCode": 8001,
        "depTaskCode": 0,
        "cycle": "month",
        "dateValue": date_value,
    }
    if parameter_passing is not None:
        item["parameterPassing"] = parameter_passing
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [{"relation": "AND", "dependItemList": [item]}],
        }
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="DEPENDENT",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert (
        decode_task_parameters(
            version=version,
            task_type="DEPENDENT",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


def test_dependent_date_value_must_match_cycle(refs: TaskRefIndex) -> None:
    """A valid DS date value cannot be authored under a different cycle."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "hour",
                                    "dateValue": "today",
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == f"{_DEPENDENT_ITEM_FIELD}.dateValue"
    assert captured.value.details["reason"] == "dependent-date-cycle-mismatch"


def test_dependent_rejects_unknown_cycle(refs: TaskRefIndex) -> None:
    """The direct boundary accepts only cycles implemented by the runtime UI."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "quarter",
                                    "dateValue": "today",
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == f"{_DEPENDENT_ITEM_FIELD}.cycle"
    assert captured.value.details["reason"] == "unsupported-dependent-cycle"


def test_dependent_rejects_unknown_date_value(refs: TaskRefIndex) -> None:
    """Unknown date values cannot fall through to the runtime's empty switch."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": "day",
                                    "dateValue": "tomorrow",
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == f"{_DEPENDENT_ITEM_FIELD}.dateValue"
    assert captured.value.details["reason"] == "unsupported-dependent-date-value"


@pytest.mark.parametrize(
    ("cycle", "date_value", "field"),
    [("${cycle}", "today", "cycle"), ("day", "$[date]", "dateValue")],
)
def test_dependent_rejects_date_window_placeholders(
    cycle: str,
    date_value: str,
    field: str,
    refs: TaskRefIndex,
) -> None:
    """DEPENDENT identity windows are literal and never parameter-substituted."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_WORKFLOW",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 0,
                                    "cycle": cycle,
                                    "dateValue": date_value,
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == f"{_DEPENDENT_ITEM_FIELD}.{field}"
    assert captured.value.details["reason"] == "parameter-placeholder-not-supported"


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.4.2"])
def test_dependent_policy_fields_are_supported_from_320(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """3.2.0 introduced the dependent polling and failure-policy fields."""
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "checkInterval": 30,
            "failurePolicy": "DEPENDENT_FAILURE_WAITING",
            "failureWaitingTime": 5,
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 7001,
                            "definitionCode": 8001,
                            "depTaskCode": 42,
                            "cycle": "day",
                            "dateValue": "today",
                            **(
                                {"parameterPassing": False}
                                if version >= "3.2.1"
                                else {}
                            ),
                        }
                    ],
                }
            ],
        }
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="DEPENDENT",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert (
        decode_task_parameters(
            version=version,
            task_type="DEPENDENT",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


def test_dependent_rejects_pre_320_policy_fields(refs: TaskRefIndex) -> None:
    """Legacy plugins must not receive polling fields they do not define."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.1.9",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "checkInterval": 30,
                    "dependTaskList": [],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.dependence.checkInterval"
    assert captured.value.details["reason"] == "field-absent-in-version"


def test_dependent_rejects_pre_321_parameter_passing(refs: TaskRefIndex) -> None:
    """parameterPassing does not exist before the 3.2.1 plugin model."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.2.0",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": "DEPENDENT_ON_TASK",
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": 42,
                                    "cycle": "day",
                                    "dateValue": "today",
                                    "parameterPassing": True,
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "field-absent-in-version"


@pytest.mark.parametrize(
    ("dependent_type", "dep_task_code"),
    [("DEPENDENT_ON_WORKFLOW", 42), ("DEPENDENT_ON_TASK", 0)],
)
def test_dependent_type_and_code_must_agree(
    dependent_type: str,
    dep_task_code: int,
    refs: TaskRefIndex,
) -> None:
    """Workflow and task dependency sentinels cannot be interchanged."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "dependentType": dependent_type,
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": dep_task_code,
                                    "cycle": "day",
                                    "dateValue": "today",
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "dependent-type-code-mismatch"


def test_dependent_all_tasks_sentinel_is_unrepresentable_typed_export(
    refs: TaskRefIndex,
) -> None:
    """The legacy -1 all-tasks sentinel has no current canonical model value."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="3.2.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [
                        {
                            "relation": "AND",
                            "dependItemList": [
                                {
                                    "projectCode": 7001,
                                    "definitionCode": 8001,
                                    "depTaskCode": -1,
                                    "cycle": "day",
                                    "dateValue": "today",
                                    "parameterPassing": False,
                                }
                            ],
                        }
                    ],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "unrepresentable-all-tasks-sentinel"


@pytest.mark.parametrize("runtime_field", ["dependResult", "status"])
def test_dependent_runtime_item_fields_fail_closed(
    runtime_field: str,
    refs: TaskRefIndex,
) -> None:
    """Runtime evaluation state is not part of typed DEPENDENT authoring."""
    item: JsonObject = {
        "dependentType": "DEPENDENT_ON_TASK",
        "projectCode": 7001,
        "definitionCode": 8001,
        "depTaskCode": 42,
        "cycle": "day",
        "dateValue": "today",
        runtime_field: "SUCCESS",
    }
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {
                    "relation": "AND",
                    "dependTaskList": [{"relation": "AND", "dependItemList": [item]}],
                }
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "runtime-only"


def test_dependent_nonempty_resource_list_fails_closed(refs: TaskRefIndex) -> None:
    """DEPENDENT does not consume compatibility-only resourceList entries."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.2",
            task_type="DEPENDENT",
            task_params={
                "dependence": {"relation": "AND", "dependTaskList": []},
                "resourceList": [{"id": 7}],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "compatibility-only-field"


def _canonical_procedure_params(*, include_var_pool: bool = True) -> JsonObject:
    params: JsonObject = {
        "type": "MYSQL",
        "datasource": 7,
        "method": "{call reporting.refresh_daily(?,?)}",
        "localParams": [
            {
                "prop": "biz_date",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "2026-08-19",
            },
            {
                "prop": "tenant_id",
                "direct": "IN",
                "type": "INTEGER",
                "value": "42",
            },
        ],
    }
    if include_var_pool:
        params["varPool"] = []
    return params


@pytest.mark.parametrize(
    ("version", "native_method", "include_var_pool"),
    [
        ("1.3.9", "reporting.refresh_daily", False),
        ("2.0.0", "{call reporting.refresh_daily(?,?)}", True),
        (
            "2.0.9",
            "{call reporting.refresh_daily(${biz_date},${tenant_id})}",
            True,
        ),
    ],
)
def test_procedure_method_roundtrips_through_three_exact_epochs(
    version: str,
    native_method: str,
    *,
    include_var_pool: bool,
    refs: TaskRefIndex,
) -> None:
    """Canonical JDBC calls project to each native method epoch and back."""
    canonical = _canonical_procedure_params(include_var_pool=include_var_pool)

    encoded = encode_task_parameters(
        version=version,
        task_type="PROCEDURE",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    decoded = decode_task_parameters(
        version=version,
        task_type=encoded.task_type,
        task_params=encoded.task_params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "PROCEDURE"
    assert encoded.task_params == {**canonical, "method": native_method}
    assert decoded.task_params == canonical


def test_procedure_rejects_duplicate_local_param_names(refs: TaskRefIndex) -> None:
    """Named-placeholder epochs require one unambiguous prop-to-slot mapping."""
    params = _canonical_procedure_params()
    local_params = params["localParams"]
    assert isinstance(local_params, list)
    duplicate = local_params[1]
    assert isinstance(duplicate, dict)
    duplicate["prop"] = "biz_date"

    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version="3.4.0",
            task_type="PROCEDURE",
            task_params=params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["reason"] == "duplicate-local-param-name"


@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_procedure_typed_projection_rejects_runtime_out_property(
    direction: Literal["encode", "decode"],
    refs: TaskRefIndex,
) -> None:
    """Derived outProperty state is never accepted as typed authoring input."""
    params = _canonical_procedure_params()
    params["outProperty"] = {
        "result": {
            "prop": "result",
            "direct": "OUT",
            "type": "VARCHAR",
            "value": "ready",
        }
    }
    if direction == "decode":
        params["method"] = "{call reporting.refresh_daily(${biz_date},${tenant_id})}"
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(TaskParameterProjectionError) as captured:
        projector(
            version="2.0.9",
            task_type="PROCEDURE",
            task_params=params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": "2.0.9",
        "direction": direction,
        "task_type": "PROCEDURE",
        "field": "task_params.outProperty",
        "reason": "runtime-only-field",
    }


@pytest.mark.parametrize("version", ["2.0.9", "3.4.0"])
def test_procedure_opaque_out_property_preserves_native_method_and_runtime_state(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """Opaque repair keeps the inseparable method and derived state native."""
    native = _canonical_procedure_params()
    native["method"] = "{call reporting.refresh_daily(${biz_date},${tenant_id})}"
    native["outProperty"] = {
        "result": {
            "prop": "result",
            "direct": "OUT",
            "type": "VARCHAR",
            "value": "ready",
        }
    }
    expected_native = deepcopy(native)

    decoded = decode_task_parameters(
        version=version,
        task_type="PROCEDURE",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    native["method"] = "mutated after decode"
    encoded = encode_task_parameters(
        version=version,
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_type == "PROCEDURE"
    assert decoded.task_params == expected_native
    assert encoded.task_type == "PROCEDURE"
    assert encoded.task_params == expected_native


def test_procedure_opaque_unrepresentable_native_method_remains_byte_stable(
    refs: TaskRefIndex,
) -> None:
    """Opaque repair must not reinterpret a positional call in a named epoch."""
    native = _canonical_procedure_params()
    native["method"] = "{call reporting.refresh_daily(?,?)}"
    native["outProperty"] = {"result": {"value": "ready"}}
    expected = deepcopy(native)

    decoded = decode_task_parameters(
        version="3.4.0",
        task_type="PROCEDURE",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    encoded = encode_task_parameters(
        version="3.4.0",
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == expected
    assert encoded.task_params == expected


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_emr_run_job_flow_roundtrips_without_legacy_native_program_type(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """The 3.0 EMR wire implies RUN_JOB_FLOW instead of storing its mode."""
    canonical: JsonObject = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
        "localParams": [],
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="EMR",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    decoded = decode_task_parameters(
        version=version,
        task_type=encoded.task_type,
        task_params=encoded.task_params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "EMR"
    assert encoded.task_params == {
        "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
        "localParams": [],
    }
    assert decoded.task_type == "EMR"
    assert decoded.task_params == canonical


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_legacy_emr_rejects_add_job_flow_steps(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """ADD_JOB_FLOW_STEPS has no execution class in the 3.0 EMR plugin."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="EMR",
            task_params={
                "programType": "ADD_JOB_FLOW_STEPS",
                "stepsDefineJson": '{"JobFlowId":"j-123"}',
                "localParams": [],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "encode",
        "task_type": "EMR",
        "field": "task_params.programType",
        "reason": "unsupported-program-type",
    }


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_legacy_emr_decode_rejects_native_add_steps_shape(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """A 3.0 EMR wire cannot contain the later steps JSON field."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version=version,
            task_type="EMR",
            task_params={
                "stepsDefineJson": '{"JobFlowId":"j-123"}',
                "localParams": [],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "decode",
        "task_type": "EMR",
        "field": "task_params.stepsDefineJson",
        "reason": "field-absent-in-version",
    }


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_legacy_emr_decode_rejects_a_stored_program_type(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """The 3.0 discriminator is canonical-only and must not appear on its wire."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version=version,
            task_type="EMR",
            task_params={
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
                "localParams": [],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": "decode",
        "task_type": "EMR",
        "field": "task_params.programType",
        "reason": "field-absent-in-version",
    }


@pytest.mark.parametrize("version", _MODERN_EMR_VERSIONS)
@pytest.mark.parametrize(
    ("program_type", "inactive_field"),
    [
        ("RUN_JOB_FLOW", "stepsDefineJson"),
        ("ADD_JOB_FLOW_STEPS", "jobFlowDefineJson"),
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_modern_emr_typed_projection_rejects_inactive_mode_json(
    version: str,
    program_type: str,
    inactive_field: str,
    direction: Literal["encode", "decode"],
    refs: TaskRefIndex,
) -> None:
    """Typed EMR keeps the selected program mode and JSON payload unambiguous."""
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )
    params: JsonObject = {
        "programType": program_type,
        "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
        "stepsDefineJson": '{"JobFlowId":"j-123"}',
        "localParams": [],
    }

    with pytest.raises(TaskParameterProjectionError) as captured:
        projector(
            version=version,
            task_type="EMR",
            task_params=params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": direction,
        "task_type": "EMR",
        "field": f"task_params.{inactive_field}",
        "reason": "inactive-program-type-field",
    }


@pytest.mark.parametrize("version", _MODERN_EMR_VERSIONS)
@pytest.mark.parametrize(
    ("program_type", "active_field"),
    [
        ("RUN_JOB_FLOW", "jobFlowDefineJson"),
        ("ADD_JOB_FLOW_STEPS", "stepsDefineJson"),
    ],
)
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_modern_emr_typed_projection_requires_active_mode_json(
    version: str,
    program_type: str,
    active_field: str,
    direction: Literal["encode", "decode"],
    refs: TaskRefIndex,
) -> None:
    """Each typed EMR mode requires its corresponding native JSON field."""
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(TaskParameterProjectionError) as captured:
        projector(
            version=version,
            task_type="EMR",
            task_params={"programType": program_type, "localParams": []},
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": direction,
        "task_type": "EMR",
        "field": f"task_params.{active_field}",
        "reason": "missing-active-program-type-field",
    }


@pytest.mark.parametrize("version", _MODERN_EMR_VERSIONS)
@pytest.mark.parametrize(
    "canonical",
    [
        {
            "programType": "RUN_JOB_FLOW",
            "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
            "localParams": [],
        },
        {
            "programType": "ADD_JOB_FLOW_STEPS",
            "stepsDefineJson": '{"JobFlowId":"j-123"}',
            "localParams": [],
        },
    ],
)
def test_modern_emr_modes_are_exact_wire_identities(
    version: str,
    canonical: JsonObject,
    refs: TaskRefIndex,
) -> None:
    """Both reviewed modern EMR modes already use the canonical wire names."""
    encoded = encode_task_parameters(
        version=version,
        task_type="EMR",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    decoded = decode_task_parameters(
        version=version,
        task_type=encoded.task_type,
        task_params=encoded.task_params,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_type == "EMR"
    assert encoded.task_params == canonical
    assert decoded.task_type == "EMR"
    assert decoded.task_params == canonical


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_opaque_legacy_emr_keeps_the_native_implied_mode_shape(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """Opaque 3.0 payloads stay native because programType has no provenance."""
    native: JsonObject = {
        "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
        "localParams": [],
    }
    expected_native = deepcopy(native)

    decoded = decode_task_parameters(
        version=version,
        task_type="EMR",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    native["jobFlowDefineJson"] = "mutated after decode"
    encoded = encode_task_parameters(
        version=version,
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == expected_native
    assert encoded.task_type == "EMR"
    assert encoded.task_params == expected_native


@pytest.mark.parametrize("version", ["3.0.0", "3.0.6"])
def test_opaque_legacy_emr_preserves_an_unknown_native_program_type(
    version: str,
    refs: TaskRefIndex,
) -> None:
    native: JsonObject = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": '{"Name":"native"}',
        "futureField": {"keep": True},
        "localParams": [],
    }
    expected = deepcopy(native)

    decoded = decode_task_parameters(
        version=version,
        task_type="EMR",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    encoded = encode_task_parameters(
        version=version,
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == expected
    assert encoded.task_params == expected


@pytest.mark.parametrize("version", _MODERN_EMR_VERSIONS)
def test_opaque_modern_emr_preserves_ambiguous_and_unknown_native_fields(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """Opaque repair keeps upstream-valid dual JSON and future fields lossless."""
    native: JsonObject = {
        "programType": "RUN_JOB_FLOW",
        "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
        "stepsDefineJson": '{"JobFlowId":"j-123"}',
        "futureField": {"nested": [1, 2]},
        "localParams": [],
    }
    expected_native = deepcopy(native)

    decoded = decode_task_parameters(
        version=version,
        task_type="EMR",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    future_field = native["futureField"]
    assert isinstance(future_field, dict)
    future_field["nested"] = []
    encoded = encode_task_parameters(
        version=version,
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == expected_native
    assert encoded.task_type == "EMR"
    assert encoded.task_params == expected_native


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
@pytest.mark.parametrize("direction", ["encode", "decode"])
def test_absent_emr_versions_fail_closed_for_typed_projection(
    version: str,
    direction: Literal["encode", "decode"],
    refs: TaskRefIndex,
) -> None:
    """The pure seam cannot silently accept EMR before the plugin exists."""
    projector = (
        encode_task_parameters if direction == "encode" else decode_task_parameters
    )

    with pytest.raises(TaskParameterProjectionError) as captured:
        projector(
            version=version,
            task_type="EMR",
            task_params={
                "programType": "RUN_JOB_FLOW",
                "jobFlowDefineJson": '{"Name":"nightly-cluster"}',
                "localParams": [],
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details == {
        "version": version,
        "direction": direction,
        "task_type": "EMR",
        "field": "task.type",
        "reason": "task-type-absent-in-version",
    }


@pytest.mark.parametrize(
    ("version", "source"),
    [
        (version, source)
        for version in (
            "2.0.0",
            "2.0.9",
            "3.0.0",
            "3.0.6",
            "3.1.0",
            "3.1.9",
            "3.2.0",
            "3.2.1",
            "3.2.2",
        )
        for source in (
            ProjectionSource.TYPED_AUTHORING,
            ProjectionSource.OPAQUE_PRESERVE,
        )
    ],
)
def test_legacy_http_injects_native_socket_timeout(
    version: str,
    source: ProjectionSource,
    refs: TaskRefIndex,
) -> None:
    """Legacy HTTP's primitive socketTimeout receives the UI authoring default."""
    canonical: JsonObject = {
        "url": "https://example.test/health",
        "httpMethod": "GET",
        "httpParams": [],
        "httpCheckCondition": "STATUS_CODE_DEFAULT",
        "connectTimeout": 60000,
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="HTTP",
        task_params=canonical,
        refs=refs,
        source=source,
    )

    assert encoded.task_params["socketTimeout"] == 60000
    assert (
        decode_task_parameters(
            version=version,
            task_type="HTTP",
            task_params=encoded.task_params,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params
        == canonical
    )


@pytest.mark.parametrize("version", ["2.0.0", "3.0.6", "3.2.0"])
def test_http_body_fails_closed_before_321(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """httpBody did not exist in the HTTP plugin before 3.2.1."""
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="HTTP",
            task_params={
                "url": "https://example.test",
                "httpMethod": "POST",
                "httpBody": "{}",
                "connectTimeout": 60000,
            },
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert captured.value.details["field"] == "task_params.httpBody"
    assert captured.value.details["reason"] == "field-absent-in-version"


@pytest.mark.parametrize("version", ["3.2.1", "3.2.2"])
def test_http_body_is_supported_in_late_32(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """3.2.1 and 3.2.2 support body plus the legacy socket timeout."""
    canonical: JsonObject = {
        "url": "https://example.test",
        "httpMethod": "POST",
        "httpBody": "{}",
        "connectTimeout": 60000,
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="HTTP",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == {**canonical, "socketTimeout": 60000}


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"])
def test_modern_http_does_not_emit_removed_socket_timeout(
    version: str,
    refs: TaskRefIndex,
) -> None:
    """The rewritten modern HTTP model has no socketTimeout field."""
    canonical: JsonObject = {
        "url": "https://example.test",
        "httpMethod": "POST",
        "httpBody": "{}",
        "connectTimeout": 60000,
    }

    encoded = encode_task_parameters(
        version=version,
        task_type="HTTP",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == canonical


def test_nondefault_legacy_socket_timeout_is_opaque_only(refs: TaskRefIndex) -> None:
    """Opaque mode preserves a native timeout that typed YAML cannot express."""
    native: JsonObject = {
        "url": "https://example.test",
        "httpMethod": "GET",
        "connectTimeout": 60000,
        "socketTimeout": 12345,
    }

    with pytest.raises(TaskParameterProjectionError) as captured:
        decode_task_parameters(
            version="3.2.2",
            task_type="HTTP",
            task_params=native,
            refs=refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    preserved = decode_task_parameters(
        version="3.2.2",
        task_type="HTTP",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert captured.value.details["reason"] == "unrepresentable-native-value"
    assert preserved.task_params == native
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type="HTTP",
            task_params=preserved.task_params,
            refs=refs,
            source=ProjectionSource.OPAQUE_PRESERVE,
        ).task_params
        == native
    )


@pytest.mark.parametrize(
    ("parameter_update", "field", "reason"),
    [
        (
            {"direct": "OUT"},
            "task_params.localParams[0].direct",
            "unsupported-parameter-direction",
        ),
        (
            {"type": "LIST"},
            "task_params.localParams[0].type",
            "unsupported-parameter-data-type",
        ),
        (
            {"type": "FILE"},
            "task_params.localParams[0].type",
            "unsupported-parameter-data-type",
        ),
    ],
    ids=["out", "list", "file"],
)
def test_http_139_rejects_local_params_outside_exact_parameter_semantics(
    parameter_update: JsonObject,
    field: str,
    reason: str,
    refs: TaskRefIndex,
) -> None:
    """Typed HTTP cannot author values absent from the exact 1.3.9 profile."""
    canonical: JsonObject = {
        "url": "https://example.test/health",
        "httpMethod": "GET",
        "httpParams": [],
        "connectTimeout": 60000,
        "localParams": [
            {
                "prop": "status",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "ready",
                **parameter_update,
            }
        ],
    }
    native = {**canonical, "socketTimeout": 60000}

    for direction, projector, task_params in (
        ("encode", encode_task_parameters, canonical),
        ("decode", decode_task_parameters, native),
    ):
        with pytest.raises(TaskParameterProjectionError) as captured:
            projector(
                version="1.3.9",
                task_type="HTTP",
                task_params=task_params,
                refs=refs,
                source=ProjectionSource.TYPED_AUTHORING,
            )

        assert captured.value.details == {
            "version": "1.3.9",
            "direction": direction,
            "task_type": "HTTP",
            "field": field,
            "reason": reason,
        }

    preserved = decode_task_parameters_with_provenance(
        version="1.3.9",
        task_type="HTTP",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert preserved.task.task_params == native
    assert preserved.reencode_source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize(
    "data_type",
    [
        "BOOLEAN",
        "DATE",
        "DOUBLE",
        "FLOAT",
        "INTEGER",
        "LONG",
        "TIME",
        "TIMESTAMP",
        "VARCHAR",
    ],
)
def test_http_139_safe_export_reencodes_through_exact_parameter_semantics(
    data_type: str,
    refs: TaskRefIndex,
) -> None:
    """Every reviewed 1.3.9 IN type remains typed across export and create."""
    canonical: JsonObject = {
        "url": "https://example.test/health",
        "httpMethod": "GET",
        "httpParams": [],
        "connectTimeout": 60000,
        "localParams": [
            {
                "prop": "status",
                "direct": "IN",
                "type": data_type,
                "value": "ready",
            }
        ],
    }
    native: JsonObject = {**canonical, "socketTimeout": 60000}

    exported = decode_task_parameters_with_provenance(
        version="1.3.9",
        task_type="HTTP",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    recreated = encode_task_parameters(
        version="1.3.9",
        task_type="HTTP",
        task_params=exported.task.task_params,
        refs=refs,
        source=exported.reencode_source,
    )

    assert exported.task.task_params == canonical
    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert recreated.task_params == native


def test_opaque_mixed_mutation_projects_canonical_conditions(
    refs: TaskRefIndex,
) -> None:
    """A typed CONDITIONS patch still projects inside a preserved compilation."""
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [{"task": "upstream", "status": "SUCCESS"}],
                }
            ],
        },
        "conditionResult": {
            "successNode": ["success"],
            "failedNode": ["failed"],
        },
    }

    encoded = encode_task_parameters(
        version="3.2.2",
        task_type="CONDITIONS",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    dependence = encoded.task_params["dependence"]
    assert isinstance(dependence, dict)
    groups = dependence["dependTaskList"]
    assert isinstance(groups, list)
    assert groups[0]["dependItemList"][0]["depTaskCode"] == 101


def test_opaque_representable_native_sub_process_roundtrips(
    refs: TaskRefIndex,
) -> None:
    """Representable native baselines can be canonicalized then projected again."""
    native: JsonObject = {
        "processDefinitionCode": 9001,
        "localParams": [],
        "varPool": [],
    }

    decoded = decode_task_parameters(
        version="3.2.2",
        task_type="SUB_PROCESS",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    encoded = encode_task_parameters(
        version="3.2.2",
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_type == "SUB_WORKFLOW"
    assert decoded.task_params["workflowDefinitionCode"] == 9001
    assert encoded.task_type == "SUB_PROCESS"
    assert encoded.task_params == native


def test_opaque_runtime_dependent_state_is_lossless(refs: TaskRefIndex) -> None:
    """Runtime-only DEPENDENT fields remain untouched in opaque repair paths."""
    native: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 7001,
                            "definitionCode": 8001,
                            "depTaskCode": 42,
                            "cycle": "day",
                            "dateValue": "today",
                            "dependResult": "SUCCESS",
                            "parameterPassing": False,
                        }
                    ],
                }
            ],
        }
    }

    decoded = decode_task_parameters(
        version="3.4.2",
        task_type="DEPENDENT",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    encoded = encode_task_parameters(
        version="3.4.2",
        task_type=decoded.task_type,
        task_params=decoded.task_params,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task_params == native
    assert encoded.task_params == native


def test_opaque_mixed_mutation_projects_canonical_switch(refs: TaskRefIndex) -> None:
    """A named SWITCH patch projects even when the compilation preserves peers."""
    canonical: JsonObject = {
        "switchResult": {
            "dependTaskList": [{"condition": "true", "nextNode": "success"}],
            "nextNode": "default",
        }
    }

    encoded = encode_task_parameters(
        version="2.0.0",
        task_type="SWITCH",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    switch_result = encoded.task_params["switchResult"]
    assert isinstance(switch_result, dict)
    branches = switch_result["dependTaskList"]
    assert isinstance(branches, list)
    assert branches[0]["nextNode"] == "102"
    assert switch_result["nextNode"] == "104"


def test_opaque_mixed_mutation_projects_canonical_dependent(
    refs: TaskRefIndex,
) -> None:
    """A canonical dependentType marker projects inside preserved mutation."""
    canonical: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 7001,
                            "definitionCode": 8001,
                            "depTaskCode": 42,
                            "cycle": "day",
                            "dateValue": "today",
                        }
                    ],
                }
            ],
        }
    }

    encoded = encode_task_parameters(
        version="3.2.0",
        task_type="DEPENDENT",
        task_params=canonical,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    dependence = encoded.task_params["dependence"]
    assert isinstance(dependence, dict)
    groups = dependence["dependTaskList"]
    assert isinstance(groups, list)
    assert "dependentType" not in groups[0]["dependItemList"][0]


def test_opaque_modern_all_tasks_sentinel_is_lossless(refs: TaskRefIndex) -> None:
    """Ambiguous modern native DEPENDENT state remains opaque when unrepresentable."""
    native: JsonObject = {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 7001,
                            "definitionCode": 8001,
                            "depTaskCode": -1,
                            "cycle": "day",
                            "dateValue": "today",
                            "parameterPassing": False,
                        }
                    ],
                }
            ],
        }
    }

    encoded = encode_task_parameters(
        version="3.4.2",
        task_type="DEPENDENT",
        task_params=native,
        refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert encoded.task_params == native
