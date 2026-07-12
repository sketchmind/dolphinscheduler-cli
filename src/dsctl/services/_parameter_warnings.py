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


class ParameterLocalSelfReferenceWarningDetail(TypedDict):
    """Structured warning for a local parameter that references itself."""

    code: Literal["parameter_local_self_reference"]
    message: str
    field: str
    parameter: str
    expression: str
    suggestion: str


class ParameterGlobalSelfReferenceWarningDetail(TypedDict):
    """Structured warning for a workflow global that references itself."""

    code: Literal["parameter_global_self_reference"]
    message: str
    field: str
    parameter: str
    expression: str
    suggestion: str


class SubWorkflowLocalParamsWarningDetail(TypedDict):
    """Structured warning for SUB_WORKFLOW localParams mistaken as child inputs."""

    code: Literal["sub_workflow_local_params_not_child_inputs"]
    message: str
    field: str
    task: str
    parameter_names: list[str]
    suggestion: str


ParameterWarningDetail = (
    ParameterExpressionWarningDetail
    | ParameterGlobalSelfReferenceWarningDetail
    | ParameterLocalSelfReferenceWarningDetail
    | SubWorkflowLocalParamsWarningDetail
)


def workflow_parameter_warnings(
    spec: WorkflowSpec,
) -> tuple[list[str], list[ParameterWarningDetail]]:
    """Return bounded warnings for risky workflow parameter authoring."""
    details: list[ParameterWarningDetail] = [
        *_workflow_parameter_expression_warning_details(spec),
        *_workflow_parameter_semantic_warning_details(spec),
    ]
    return [detail["message"] for detail in details], details


def _workflow_parameter_semantic_warning_details(
    spec: WorkflowSpec,
) -> Iterator[
    ParameterGlobalSelfReferenceWarningDetail
    | ParameterLocalSelfReferenceWarningDetail
    | SubWorkflowLocalParamsWarningDetail
]:
    yield from _workflow_global_self_reference_warning_details(spec)
    for task_index, task in enumerate(spec.tasks):
        task_params = task.task_params
        if not isinstance(task_params, Mapping):
            continue
        local_params = task_params.get("localParams")
        if not isinstance(local_params, Sequence) or isinstance(
            local_params,
            (bytes, bytearray, str),
        ):
            continue
        if task.type == "SUB_WORKFLOW" and local_params:
            field = f"tasks[{task_index}].task_params.localParams"
            parameter_names = [
                prop
                for parameter in local_params
                if isinstance(parameter, Mapping)
                and isinstance((prop := parameter.get("prop")), str)
            ]
            message = (
                f"{field} does not configure child workflow inputs in DS "
                f"3.4.1; SUB_WORKFLOW task '{task.name}' ignores these local "
                "parameters when it starts the child."
            )
            yield {
                "code": "sub_workflow_local_params_not_child_inputs",
                "message": message,
                "field": field,
                "task": task.name,
                "parameter_names": parameter_names,
                "suggestion": (
                    "Move values supplied by this parent to workflow.global_params "
                    "or pass them as parent startup parameters. Define reusable "
                    "standalone fallbacks in the child workflow.global_params."
                ),
            }
        for parameter_index, parameter in enumerate(local_params):
            if not isinstance(parameter, Mapping):
                continue
            prop = parameter.get("prop")
            value = parameter.get("value")
            if not isinstance(prop, str) or not isinstance(value, str):
                continue
            expression = f"${{{prop}}}"
            if expression not in value:
                continue
            field = (
                f"tasks[{task_index}].task_params.localParams[{parameter_index}].value"
            )
            message = (
                f"{field} contains the self-reference {expression}: the local "
                f"parameter '{prop}' shadows the same-name workflow value. "
                "Unless a higher-priority startup or upstream value replaces "
                "it, DS resolves it as a circular placeholder."
            )
            yield {
                "code": "parameter_local_self_reference",
                "message": message,
                "field": field,
                "parameter": prop,
                "expression": expression,
                "suggestion": (
                    "Remove this localParams entry to consume the same-name "
                    "workflow global, or give it a concrete fallback. Use a "
                    "different prop name when an explicit local alias is intended."
                ),
            }


def _workflow_global_self_reference_warning_details(
    spec: WorkflowSpec,
) -> Iterator[ParameterGlobalSelfReferenceWarningDetail]:
    global_params = spec.workflow.global_params
    entries: Iterator[tuple[str, str, str | None]]
    if isinstance(global_params, Mapping):
        entries = (
            (f"workflow.global_params.{name}", name, value)
            for name, value in global_params.items()
        )
    elif global_params is None:
        return
    else:
        entries = (
            (f"workflow.global_params[{index}].value", parameter.prop, parameter.value)
            for index, parameter in enumerate(global_params)
        )
    for field, parameter, value in entries:
        if value is None:
            continue
        expression = f"${{{parameter}}}"
        if expression not in value:
            continue
        message = (
            f"{field} contains the self-reference {expression}. Unless a "
            "higher-priority startup value replaces it, DS resolves the "
            f"workflow global '{parameter}' as a circular placeholder."
        )
        yield {
            "code": "parameter_global_self_reference",
            "message": message,
            "field": field,
            "parameter": parameter,
            "expression": expression,
            "suggestion": (
                "Replace the self-reference with a concrete workflow default, "
                "or omit the global and require a workflow startup parameter."
            ),
        }


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
    "ParameterGlobalSelfReferenceWarningDetail",
    "ParameterLocalSelfReferenceWarningDetail",
    "ParameterWarningDetail",
    "SubWorkflowLocalParamsWarningDetail",
    "workflow_parameter_warnings",
]
