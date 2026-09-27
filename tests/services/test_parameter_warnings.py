from typing import cast

import pytest

from dsctl.models import WorkflowSpec
from dsctl.services._parameter_warnings import (
    ParameterExpressionWarningDetail,
    workflow_parameter_warnings,
)
from dsctl.upstream.parameter_semantics import get_parameter_semantics


def test_parameter_warnings_detect_week_year_and_calendar_week_patterns() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-etl",
                "project": "analytics",
                "global_params": {
                    "week_key": "$[yyyyww]",
                    "safe_date": "$[yyyyMMdd-1]",
                    "week_year_date": "$[YYYYMMdd]",
                },
            },
            "tasks": [
                {
                    "name": "extract",
                    "type": "SHELL",
                    "command": "echo $[yyyy-MM-dd]",
                },
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(spec)
    expression_details = [
        cast("ParameterExpressionWarningDetail", detail) for detail in details
    ]

    assert len(warnings) == 2
    assert [detail["code"] for detail in details] == [
        "parameter_time_format_calendar_year_with_week",
        "parameter_time_format_week_year_token",
    ]
    assert expression_details[0]["field"] == "workflow.global_params.week_key"
    assert expression_details[0]["expression"] == "$[yyyyww]"
    assert expression_details[1]["field"] == "workflow.global_params.week_year_date"
    assert expression_details[1]["expression"] == "$[YYYYMMdd]"


def test_parameter_warnings_scan_nested_task_params_safely() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-etl",
                "project": "analytics",
                "global_params": {
                    "safe_week": "$[year_week(yyyy-MM-dd)]",
                },
            },
            "tasks": [
                {
                    "name": "extract",
                    "type": "CUSTOM",
                    "task_params": {
                        "localParams": [
                            {
                                "prop": "week_key",
                                "direct": "IN",
                                "type": "VARCHAR",
                                "value": "$[yyyy-ww]",
                            }
                        ],
                    },
                },
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(spec)

    assert len(warnings) == 1
    assert details == [
        {
            "code": "parameter_time_format_calendar_year_with_week",
            "message": warnings[0],
            "field": "tasks[0].task_params.localParams[0].value",
            "expression": "$[yyyy-ww]",
            "pattern": "yyyy-ww",
            "suggestion": (
                "Use DS year_week(...) when week-of-year output is intended, or "
                "choose yyyy versus YYYY deliberately before applying the workflow."
            ),
        }
    ]


def test_parameter_warnings_detect_self_referential_local_parameter() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-etl",
                "project": "analytics",
                "global_params": {"run_label": "DEFAULT"},
            },
            "tasks": [
                {
                    "name": "report",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo ${run_label}",
                        "localParams": [
                            {
                                "prop": "run_label",
                                "type": "VARCHAR",
                                "value": "prefix-${run_label}",
                            }
                        ],
                    },
                }
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(spec)

    assert len(warnings) == 1
    assert details == [
        {
            "code": "parameter_local_self_reference",
            "message": warnings[0],
            "field": "tasks[0].task_params.localParams[0].value",
            "parameter": "run_label",
            "expression": "${run_label}",
            "suggestion": (
                "Remove this localParams entry to consume the same-name workflow "
                "global, or give it a concrete fallback. Use a different prop name "
                "when an explicit local alias is intended."
            ),
        }
    ]


def test_parameter_warnings_detect_self_referential_workflow_global() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-etl",
                "project": "analytics",
                "global_params": {"run_label": "prefix-${run_label}"},
            },
            "tasks": [
                {
                    "name": "report",
                    "type": "SHELL",
                    "command": "echo ${run_label}",
                }
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(spec)

    assert len(warnings) == 1
    assert details == [
        {
            "code": "parameter_global_self_reference",
            "message": warnings[0],
            "field": "workflow.global_params.run_label",
            "parameter": "run_label",
            "expression": "${run_label}",
            "suggestion": (
                "Replace the self-reference with a concrete workflow default, "
                "or omit the global and require a workflow startup parameter."
            ),
        }
    ]


def test_parameter_warnings_scan_list_form_workflow_globals() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {
                "name": "daily-etl",
                "global_params": [
                    {
                        "prop": "run_label",
                        "value": "${run_label}",
                        "direct": "OUT",
                    }
                ],
            },
            "tasks": [{"name": "report", "type": "SHELL", "command": "true"}],
        }
    )

    _, details = workflow_parameter_warnings(spec)

    assert [detail["code"] for detail in details] == ["parameter_global_self_reference"]
    assert details[0]["field"] == "workflow.global_params[0].value"


def test_parameter_warnings_reject_sub_workflow_local_params_as_child_inputs() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "parent", "project": "analytics"},
            "tasks": [
                {
                    "name": "invoke-child",
                    "type": "SUB_WORKFLOW",
                    "task_params": {
                        "workflowDefinitionCode": 123456789,
                        "localParams": [
                            {
                                "prop": "run_label",
                                "direct": "IN",
                                "type": "VARCHAR",
                                "value": "FROM_PARENT",
                            }
                        ],
                    },
                }
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(spec)

    assert len(warnings) == 1
    assert details == [
        {
            "code": "sub_workflow_local_params_not_child_inputs",
            "message": warnings[0],
            "field": "tasks[0].task_params.localParams",
            "task": "invoke-child",
            "parameter_names": ["run_label"],
            "suggestion": (
                "Move values supplied by this parent to workflow.global_params or "
                "pass them as parent startup parameters. Define reusable standalone "
                "fallbacks in the child workflow.global_params."
            ),
        }
    ]


def test_sub_workflow_warning_names_the_selected_exact_version() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "parent", "project": "analytics"},
            "tasks": [
                {
                    "name": "invoke-child",
                    "type": "SUB_WORKFLOW",
                    "task_params": {
                        "workflowDefinitionCode": 123456789,
                        "localParams": [{"prop": "run_label", "value": "value"}],
                    },
                }
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(
        spec,
        parameter_semantics=get_parameter_semantics("3.3.1"),
    )

    assert len(details) == 1
    assert "DS 3.3.1" in warnings[0]
    assert "DS 3.4.1" not in warnings[0]


@pytest.mark.parametrize(
    ("ds_version", "script", "expected_fragments"),
    [
        (
            "1.3.9",
            "${setValue(result=ok)}",
            ("does not support task output capture",),
        ),
        (
            "2.0.0",
            "echo ${setValue(result=ok)}\n#{setValue(other=ok)}",
            ("start at column 0", "does not recognize #{setValue"),
        ),
        (
            "3.0.0",
            "echo ${setValue(result=ok)}\n#{setValue(other=ok)}",
            ("start at column 0",),
        ),
        (
            "3.2.1",
            "echo ${setValue(result=ok)} and #{setValue(other=ok)}",
            (),
        ),
    ],
)
def test_set_value_warnings_follow_exact_parser_epoch(
    ds_version: str,
    script: str,
    expected_fragments: tuple[str, ...],
) -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "output-demo", "project": "analytics"},
            "tasks": [{"name": "produce", "type": "SHELL", "command": script}],
        }
    )

    warnings, details = workflow_parameter_warnings(
        spec,
        parameter_semantics=get_parameter_semantics(ds_version),
    )

    assert [detail["code"] for detail in details] == [
        "task_output_set_value_syntax_incompatible"
    ] * len(expected_fragments)
    assert len(warnings) == len(expected_fragments)
    for expected, warning in zip(expected_fragments, warnings, strict=True):
        assert expected in warning


def test_declared_in_binding_warns_when_direct_downstream_omits_matching_in() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "output-demo", "project": "analytics"},
            "tasks": [
                {
                    "name": "produce",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo result",
                        "localParams": [
                            {
                                "prop": "row_count",
                                "direct": "OUT",
                                "type": "VARCHAR",
                                "value": "",
                            }
                        ],
                    },
                },
                {
                    "name": "consume",
                    "type": "SHELL",
                    "command": "echo ${row_count}",
                    "depends_on": ["produce"],
                },
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(
        spec,
        parameter_semantics=get_parameter_semantics("3.3.1"),
    )

    assert details == [
        {
            "code": "task_output_missing_downstream_in",
            "message": warnings[0],
            "field": "tasks[1].task_params.localParams",
            "parameter": "row_count",
            "upstream_tasks": ["produce"],
            "downstream_task": "consume",
            "ds_version": "3.3.1",
            "suggestion": (
                "Declare an IN localParams entry with prop 'row_count' on task "
                "'consume', or remove the dependency if that output is not consumed."
            ),
        }
    ]
    assert "requires a matching Direct.IN declaration" in warnings[0]


def test_implicit_binding_epoch_does_not_require_downstream_in() -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "output-demo", "project": "analytics"},
            "tasks": [
                {
                    "name": "produce",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo result",
                        "localParams": [
                            {"prop": "row_count", "direct": "OUT", "value": ""}
                        ],
                    },
                },
                {
                    "name": "consume",
                    "type": "SHELL",
                    "command": "echo ${row_count}",
                    "depends_on": ["produce"],
                },
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(
        spec,
        parameter_semantics=get_parameter_semantics("3.2.2"),
    )

    assert warnings == []
    assert details == []


@pytest.mark.parametrize(
    ("ds_version", "winner", "winner_text"),
    [
        ("3.2.2", "earliest-nonempty", "earliest non-empty"),
        ("3.3.1", "latest-nonempty", "latest non-empty"),
    ],
)
def test_same_name_direct_upstream_outputs_warn_with_exact_runtime_winner(
    ds_version: str,
    winner: str,
    winner_text: str,
) -> None:
    spec = WorkflowSpec.model_validate(
        {
            "workflow": {"name": "output-demo", "project": "analytics"},
            "tasks": [
                {
                    "name": "first",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo first",
                        "localParams": [
                            {"prop": "row_count", "direct": "OUT", "value": ""}
                        ],
                    },
                },
                {
                    "name": "second",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo second",
                        "localParams": [
                            {"prop": "row_count", "direct": "OUT", "value": ""}
                        ],
                    },
                },
                {
                    "name": "join",
                    "type": "SHELL",
                    "task_params": {
                        "rawScript": "echo ${row_count}",
                        "localParams": [
                            {"prop": "row_count", "direct": "IN", "value": ""}
                        ],
                    },
                    "depends_on": ["first", "second"],
                },
            ],
        }
    )

    warnings, details = workflow_parameter_warnings(
        spec,
        parameter_semantics=get_parameter_semantics(ds_version),
    )

    assert details == [
        {
            "code": "task_output_same_name_upstream_collision",
            "message": warnings[0],
            "field": "tasks[2].depends_on",
            "parameter": "row_count",
            "upstream_tasks": ["first", "second"],
            "downstream_task": "join",
            "ds_version": ds_version,
            "winner": winner,
            "suggestion": (
                "Give each upstream OUT parameter a distinct prop, or add one "
                "merge task so the selected value is explicit."
            ),
        }
    ]
    assert winner_text in warnings[0]
