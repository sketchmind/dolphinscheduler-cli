from dsctl.models import WorkflowSpec
from dsctl.services._parameter_warnings import (
    workflow_parameter_expression_warnings,
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

    warnings, details = workflow_parameter_expression_warnings(spec)

    assert len(warnings) == 2
    assert [detail["code"] for detail in details] == [
        "parameter_time_format_calendar_year_with_week",
        "parameter_time_format_week_year_token",
    ]
    assert details[0]["field"] == "workflow.global_params.week_key"
    assert details[0]["expression"] == "$[yyyyww]"
    assert details[1]["field"] == "workflow.global_params.week_year_date"
    assert details[1]["expression"] == "$[YYYYMMdd]"


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

    warnings, details = workflow_parameter_expression_warnings(spec)

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
