from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    from dsctl.models.common import YamlValue
    from dsctl.models.workflow_spec import WorkflowSpec

_TIME_PLACEHOLDER_RE = re.compile(r"\$\[([^\]]+)\]")
_UPPERCASE_WEEK_YEAR_RE = re.compile(r"Y{2,}")
_CALENDAR_YEAR_WITH_WEEK_RE = re.compile(
    r"(?:"
    r"y{2,4}[MmdDHEhHmsSaS:/_.\-\s]*[wW]{1,2}"
    r"|"
    r"[wW]{1,2}[MmdDHEhHmsSaS:/_.\-\s]*y{2,4}"
    r")"
)


class ParameterExpressionWarningDetail(TypedDict):
    """Structured warning for risky DS dynamic parameter time expressions."""

    code: Literal[
        "parameter_time_format_week_year_token",
        "parameter_time_format_calendar_year_with_week",
    ]
    message: str
    field: str
    expression: str
    pattern: str
    suggestion: str


def workflow_parameter_expression_warnings(
    spec: WorkflowSpec,
) -> tuple[list[str], list[ParameterExpressionWarningDetail]]:
    """Return warnings for risky DS runtime parameter expressions in YAML specs."""
    details = list(_workflow_parameter_expression_warning_details(spec))
    return [detail["message"] for detail in details], details


def _workflow_parameter_expression_warning_details(
    spec: WorkflowSpec,
) -> Iterator[ParameterExpressionWarningDetail]:
    seen: set[tuple[str, str, str]] = set()
    for field, value in _iter_workflow_strings(spec):
        for expression_match in _TIME_PLACEHOLDER_RE.finditer(value):
            expression = expression_match.group(0)
            pattern = expression_match.group(1)
            for detail in _expression_warning_details(
                field=field,
                expression=expression,
                pattern=pattern,
            ):
                key = (detail["field"], detail["expression"], detail["code"])
                if key in seen:
                    continue
                seen.add(key)
                yield detail


def _iter_workflow_strings(spec: WorkflowSpec) -> Iterator[tuple[str, str]]:
    global_params = spec.workflow.global_params
    if isinstance(global_params, Mapping):
        for name, value in global_params.items():
            yield f"workflow.global_params.{name}", value
    elif global_params is not None:
        for index, parameter in enumerate(global_params):
            if parameter.value is not None:
                yield f"workflow.global_params[{index}].value", parameter.value

    for index, task in enumerate(spec.tasks):
        task_prefix = f"tasks[{index}]"
        if task.command is not None:
            yield f"{task_prefix}.command", task.command
        if task.task_params is not None:
            yield from _iter_yaml_strings(
                task.task_params,
                field=f"{task_prefix}.task_params",
            )


def _iter_yaml_strings(
    value: YamlValue,
    *,
    field: str,
) -> Iterator[tuple[str, str]]:
    if isinstance(value, str):
        yield field, value
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield from _iter_yaml_strings(child, field=f"{field}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        for index, child in enumerate(value):
            yield from _iter_yaml_strings(child, field=f"{field}[{index}]")


def _expression_warning_details(
    *,
    field: str,
    expression: str,
    pattern: str,
) -> Iterator[ParameterExpressionWarningDetail]:
    if _UPPERCASE_WEEK_YEAR_RE.search(pattern):
        yield _uppercase_week_year_warning(
            field=field,
            expression=expression,
            pattern=pattern,
        )
    if _CALENDAR_YEAR_WITH_WEEK_RE.search(pattern):
        yield _calendar_year_with_week_warning(
            field=field,
            expression=expression,
            pattern=pattern,
        )


def _uppercase_week_year_warning(
    *,
    field: str,
    expression: str,
    pattern: str,
) -> ParameterExpressionWarningDetail:
    message = (
        f"{field} contains {expression}: uppercase year tokens such as YYYY use "
        "week-based year semantics in DS Java-style time patterns, not calendar "
        "year semantics."
    )
    return {
        "code": "parameter_time_format_week_year_token",
        "message": message,
        "field": field,
        "expression": expression,
        "pattern": pattern,
        "suggestion": (
            "Use lowercase yyyy for calendar year; keep uppercase YYYY only when "
            "week-based year semantics are intended. Run `dsctl template params "
            "--topic time` for examples."
        ),
    }


def _calendar_year_with_week_warning(
    *,
    field: str,
    expression: str,
    pattern: str,
) -> ParameterExpressionWarningDetail:
    message = (
        f"{field} contains {expression}: combining calendar-year tokens such as "
        "yyyy with week tokens such as ww can be wrong near year boundaries."
    )
    return {
        "code": "parameter_time_format_calendar_year_with_week",
        "message": message,
        "field": field,
        "expression": expression,
        "pattern": pattern,
        "suggestion": (
            "Use DS year_week(...) when week-of-year output is intended, or choose "
            "yyyy versus YYYY deliberately before applying the workflow."
        ),
    }


__all__ = [
    "ParameterExpressionWarningDetail",
    "workflow_parameter_expression_warnings",
]
