from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.upstream.parameter_semantics import (
    ParameterSemanticsProfile,
    get_parameter_semantics,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlValue
    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec

_TIME_PLACEHOLDER_RE = re.compile(r"\$\[([^\]]+)\]")
_UPPERCASE_WEEK_YEAR_RE = re.compile(r"Y{2,}")
_CALENDAR_YEAR_WITH_WEEK_RE = re.compile(
    r"(?:"
    r"y{2,4}[MmdDHEhHmsSaS:/_.\-\s]*[wW]{1,2}"
    r"|"
    r"[wW]{1,2}[MmdDHEhHmsSaS:/_.\-\s]*y{2,4}"
    r")"
)
_SET_VALUE_RE = re.compile(r"(?P<prefix>[$#])\{setValue\(")


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


class TaskOutputSetValueWarningDetail(TypedDict):
    """Structured warning for output syntax unsupported by one DS parser epoch."""

    code: Literal["task_output_set_value_syntax_incompatible"]
    message: str
    field: str
    task: str
    ds_version: str
    parser: str
    marker: str
    suggestion: str


class TaskOutputMissingInputWarningDetail(TypedDict):
    """Structured warning for a downstream task missing a required IN binding."""

    code: Literal["task_output_missing_downstream_in"]
    message: str
    field: str
    parameter: str
    upstream_tasks: list[str]
    downstream_task: str
    ds_version: str
    suggestion: str


class TaskOutputCollisionWarningDetail(TypedDict):
    """Structured warning for ambiguous same-name direct-upstream outputs."""

    code: Literal["task_output_same_name_upstream_collision"]
    message: str
    field: str
    parameter: str
    upstream_tasks: list[str]
    downstream_task: str
    ds_version: str
    winner: Literal["earliest-nonempty", "latest-nonempty"]
    suggestion: str


ParameterWarningDetail = (
    ParameterExpressionWarningDetail
    | ParameterGlobalSelfReferenceWarningDetail
    | ParameterLocalSelfReferenceWarningDetail
    | SubWorkflowLocalParamsWarningDetail
    | TaskOutputCollisionWarningDetail
    | TaskOutputMissingInputWarningDetail
    | TaskOutputSetValueWarningDetail
)


def workflow_parameter_warnings(
    spec: WorkflowSpec,
    *,
    parameter_semantics: ParameterSemanticsProfile | None = None,
) -> tuple[list[str], list[ParameterWarningDetail]]:
    """Return bounded warnings for risky workflow parameter authoring."""
    if parameter_semantics is None:
        parameter_semantics = get_parameter_semantics("3.4.1")
    details: list[ParameterWarningDetail] = [
        *_workflow_parameter_expression_warning_details(spec),
        *_workflow_parameter_semantic_warning_details(
            spec,
            parameter_semantics=parameter_semantics,
        ),
        *_task_output_graph_warning_details(
            spec,
            parameter_semantics=parameter_semantics,
        ),
        *_task_output_set_value_warning_details(
            spec,
            parameter_semantics=parameter_semantics,
        ),
    ]
    return [detail["message"] for detail in details], details


def _task_output_graph_warning_details(
    spec: WorkflowSpec,
    *,
    parameter_semantics: ParameterSemanticsProfile,
) -> Iterator[TaskOutputCollisionWarningDetail | TaskOutputMissingInputWarningDetail]:
    output = parameter_semantics.output
    if not output.var_pool_transport:
        return
    tasks_by_name = {task.name: task for task in spec.tasks}
    for task_index, downstream in enumerate(spec.tasks):
        upstream_tasks_by_parameter: dict[str, list[str]] = {}
        for upstream_name in downstream.depends_on:
            upstream = tasks_by_name.get(upstream_name)
            if upstream is None:
                continue
            for parameter in _task_local_parameter_names(upstream, direct="OUT"):
                upstream_tasks_by_parameter.setdefault(parameter, []).append(
                    upstream_name
                )
        downstream_inputs = set(_task_local_parameter_names(downstream, direct="IN"))
        for parameter, upstream_tasks in upstream_tasks_by_parameter.items():
            field = f"tasks[{task_index}].task_params.localParams"
            upstream_labels = ", ".join(f"'{name}'" for name in upstream_tasks)
            if (
                output.downstream_binding == "declared-in-only"
                and parameter not in downstream_inputs
            ):
                message = (
                    f"{field} omits prop '{parameter}': DS "
                    f"{parameter_semantics.version} requires a matching Direct.IN "
                    f"declaration on direct downstream task '{downstream.name}' "
                    f"before it can consume the OUT value published by "
                    f"{upstream_labels}."
                )
                yield {
                    "code": "task_output_missing_downstream_in",
                    "message": message,
                    "field": field,
                    "parameter": parameter,
                    "upstream_tasks": upstream_tasks,
                    "downstream_task": downstream.name,
                    "ds_version": parameter_semantics.version,
                    "suggestion": (
                        f"Declare an IN localParams entry with prop '{parameter}' "
                        f"on task '{downstream.name}', or remove the dependency if "
                        "that output is not consumed."
                    ),
                }
            winner = output.same_name_upstream_winner
            if len(upstream_tasks) < 2 or winner == "n/a":
                continue
            winner_text = (
                "earliest non-empty"
                if winner == "earliest-nonempty"
                else "latest non-empty"
            )
            collision_field = f"tasks[{task_index}].depends_on"
            message = (
                f"{collision_field} names direct upstream tasks {upstream_labels}, "
                f"which all declare OUT prop '{parameter}': DS "
                f"{parameter_semantics.version} keeps the {winner_text} runtime "
                f"value for downstream task '{downstream.name}'."
            )
            yield {
                "code": "task_output_same_name_upstream_collision",
                "message": message,
                "field": collision_field,
                "parameter": parameter,
                "upstream_tasks": upstream_tasks,
                "downstream_task": downstream.name,
                "ds_version": parameter_semantics.version,
                "winner": winner,
                "suggestion": (
                    "Give each upstream OUT parameter a distinct prop, or add one "
                    "merge task so the selected value is explicit."
                ),
            }


def _task_local_parameter_names(
    task: WorkflowTaskSpec,
    *,
    direct: str,
) -> tuple[str, ...]:
    task_params = getattr(task, "task_params", None)
    if not isinstance(task_params, Mapping):
        return ()
    local_params = task_params.get("localParams")
    if not isinstance(local_params, Sequence) or isinstance(
        local_params,
        (bytes, bytearray, str),
    ):
        return ()
    names: list[str] = []
    for parameter in local_params:
        if not isinstance(parameter, Mapping):
            continue
        direction = parameter.get("direct")
        prop = parameter.get("prop")
        if direction != direct or not isinstance(prop, str) or not prop:
            continue
        if prop not in names:
            names.append(prop)
    return tuple(names)


def _workflow_parameter_semantic_warning_details(
    spec: WorkflowSpec,
    *,
    parameter_semantics: ParameterSemanticsProfile,
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
        nested_semantics = parameter_semantics.nested_workflow
        if (
            task.type == nested_semantics.task_type
            and nested_semantics.task_local_params_role == "ignored"
            and local_params
        ):
            field = f"tasks[{task_index}].task_params.localParams"
            parameter_names = [
                prop
                for parameter in local_params
                if isinstance(parameter, Mapping)
                and isinstance((prop := parameter.get("prop")), str)
            ]
            message = (
                f"{field} does not configure child workflow inputs in DS "
                f"{parameter_semantics.version}; {task.type} task '{task.name}' "
                "ignores these local parameters when it starts the child."
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


def _task_output_set_value_warning_details(
    spec: WorkflowSpec,
    *,
    parameter_semantics: ParameterSemanticsProfile,
) -> Iterator[TaskOutputSetValueWarningDetail]:
    parser = parameter_semantics.output.set_value_parser
    if parser == "dollar-or-hash-stream":
        return
    for task_index, task in enumerate(spec.tasks):
        for field, script in _task_output_scripts(task_index, task):
            for match in _SET_VALUE_RE.finditer(script):
                prefix = match.group("prefix")
                column = match.start() - script.rfind("\n", 0, match.start()) - 1
                marker = f"{prefix}{{setValue"
                reason: str | None = None
                suggestion: str
                if parser == "absent":
                    reason = (
                        f"DS {parameter_semantics.version} does not support task "
                        "output capture through setValue syntax"
                    )
                    suggestion = (
                        "Remove the setValue expression or run this workflow on a "
                        "DS version whose task-output parser supports it."
                    )
                elif parser == "dollar-line-start" and prefix == "#":
                    reason = (
                        f"DS {parameter_semantics.version} does not recognize "
                        f"{marker}(...) output expressions"
                    )
                    suggestion = (
                        "Use ${setValue(name=value)} and place it at column 0 on "
                        "its own output line."
                    )
                elif column != 0:
                    reason = (
                        f"DS {parameter_semantics.version} requires {marker}(...) "
                        "output expressions to start at column 0"
                    )
                    supported_markers = (
                        "${setValue(name=value)}"
                        if parser == "dollar-line-start"
                        else "${setValue(name=value)} or #{setValue(name=value)}"
                    )
                    suggestion = (
                        f"Emit {supported_markers} at column 0 on its own output line."
                    )
                else:
                    continue
                message = f"{field} contains {marker}(...): {reason}."
                yield {
                    "code": "task_output_set_value_syntax_incompatible",
                    "message": message,
                    "field": field,
                    "task": task.name,
                    "ds_version": parameter_semantics.version,
                    "parser": parser,
                    "marker": marker,
                    "suggestion": suggestion,
                }


def _task_output_scripts(
    task_index: int,
    task: WorkflowTaskSpec,
) -> Iterator[tuple[str, str]]:
    command = getattr(task, "command", None)
    if isinstance(command, str):
        yield f"tasks[{task_index}].command", command
    task_params = getattr(task, "task_params", None)
    if not isinstance(task_params, Mapping):
        return
    raw_script = task_params.get("rawScript")
    if isinstance(raw_script, str):
        yield f"tasks[{task_index}].task_params.rawScript", raw_script


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
            if value is not None:
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
    "TaskOutputCollisionWarningDetail",
    "TaskOutputMissingInputWarningDetail",
    "TaskOutputSetValueWarningDetail",
    "workflow_parameter_warnings",
]
