from typing import cast

from dsctl.models import WorkflowSpec
from dsctl.services._parameter_warnings import (
    ParameterExpressionWarningDetail,
    workflow_parameter_warnings,
)


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
