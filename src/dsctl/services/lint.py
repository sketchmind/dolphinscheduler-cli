from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict

from dsctl.errors import UserInputError
from dsctl.models.workflow_spec import load_workflow_spec
from dsctl.output import CommandResult, require_json_object
from dsctl.services._parameter_warnings import (
    ParameterExpressionWarningDetail,
    workflow_parameter_expression_warnings,
)
from dsctl.services._workflow_compile import (
    compile_workflow_create_payload,
    workflow_edges,
)
from dsctl.services._workflow_validation import (
    require_schedule_block_create_compatible,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services._workflow_compile import WorkflowCreatePayload


class LintCheckData(TypedDict):
    """One successful local lint stage."""

    code: str
    status: Literal["pass"]
    message: str


class LintDiagnosticData(TypedDict):
    """One machine-readable lint diagnostic."""

    code: str
    status: Literal["pass", "warning", "error"]
    message: str
    field: str | None
    suggestion: str | None


class WorkflowLintSummaryData(TypedDict):
    """Compact local workflow summary emitted by `lint workflow`."""

    name: str
    project: str | None
    releaseState: str
    executionType: str
    taskCount: int
    edgeCount: int
    taskTypeCounts: dict[str, int]
    rootTasks: list[str]
    leafTasks: list[str]
    hasSchedule: bool


class WorkflowLintCompilationData(TypedDict):
    """Stable compile summary derived from the DS create payload."""

    taskDefinitionCount: int
    taskRelationCount: int
    globalParamCount: int


class WorkflowLintData(TypedDict):
    """Structured payload returned by `lint workflow`."""

    kind: Literal["workflow"]
    valid: Literal[True]
    summary: WorkflowLintSummaryData
    compilation: WorkflowLintCompilationData
    checks: list[LintCheckData]
    diagnostics: list[LintDiagnosticData]


class WorkflowProjectSelectionWarningDetail(TypedDict):
    """Structured warning emitted for locally valid but externalized inputs."""

    code: Literal["workflow_project_selection_external"]
    message: str
    field: str
    suggestion: str
    accepted_sources: list[str]


WorkflowLintWarningDetail: TypeAlias = (
    WorkflowProjectSelectionWarningDetail | ParameterExpressionWarningDetail
)


def lint_workflow_result(*, file: str | Path) -> CommandResult:
    """Lint one workflow YAML file using local spec and compile checks."""
    path = _normalized_path(file)
    spec = _load_workflow_spec_or_error(path)
    require_schedule_block_create_compatible(spec)
    try:
        payload = compile_workflow_create_payload(spec)
    except UserInputError as error:
        raise _lint_compile_error(error) from error
    edges = workflow_edges(spec.tasks)
    warnings, warning_details = _workflow_lint_warnings(spec)
    checks = _workflow_lint_checks(spec)
    data = WorkflowLintData(
        kind="workflow",
        valid=True,
        summary=_workflow_lint_summary(spec, edges=edges),
        compilation=_workflow_lint_compilation(payload),
        checks=checks,
        diagnostics=_workflow_lint_diagnostics(checks, warning_details),
    )
    return CommandResult(
        data=require_json_object(data, label="workflow lint data"),
        resolved={"kind": "workflow", "file": str(path)},
        warnings=warnings,
        warning_details=warning_details,
    )


def _normalized_path(file: str | Path) -> Path:
    if isinstance(file, Path):
        return file
    return Path(file)


def _load_workflow_spec_or_error(path: Path) -> WorkflowSpec:
    try:
        return load_workflow_spec(path)
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=(
                "Run `dsctl template workflow` to inspect the stable YAML surface."
            ),
        ) from exc


def _lint_compile_error(error: UserInputError) -> UserInputError:
    if error.suggestion is not None:
        return error
    return UserInputError(
        error.message,
        details=error.details,
        suggestion=(
            "Fix the workflow DAG or task references in the YAML file and retry."
        ),
    )


def _workflow_lint_summary(
    spec: WorkflowSpec,
    *,
    edges: list[tuple[str, str]],
) -> WorkflowLintSummaryData:
    upstream_by_task: dict[str, set[str]] = {task.name: set() for task in spec.tasks}
    downstream_by_task: dict[str, set[str]] = {task.name: set() for task in spec.tasks}
    for predecessor, successor in edges:
        upstream_by_task[successor].add(predecessor)
        downstream_by_task[predecessor].add(successor)
    return {
        "name": spec.workflow.name,
        "project": spec.workflow.project,
        "releaseState": spec.workflow.release_state.value,
        "executionType": spec.workflow.execution_type.value,
        "taskCount": len(spec.tasks),
        "edgeCount": len(edges),
        "taskTypeCounts": dict(
            sorted(Counter(task.type for task in spec.tasks).items())
        ),
        "rootTasks": [
            task.name for task in spec.tasks if not upstream_by_task[task.name]
        ],
        "leafTasks": [
            task.name for task in spec.tasks if not downstream_by_task[task.name]
        ],
        "hasSchedule": spec.schedule is not None,
    }


def _workflow_lint_compilation(
    payload: WorkflowCreatePayload,
) -> WorkflowLintCompilationData:
    return {
        "taskDefinitionCount": _json_array_length(
            payload["taskDefinitionJson"],
            label="taskDefinitionJson",
        ),
        "taskRelationCount": _json_array_length(
            payload["taskRelationJson"],
            label="taskRelationJson",
        ),
        "globalParamCount": _json_array_length(
            payload["globalParams"],
            label="globalParams",
        ),
    }


def _workflow_lint_checks(spec: WorkflowSpec) -> list[LintCheckData]:
    schedule_message = (
        "Workflow schedule block is locally compatible with workflow create."
        if spec.schedule is not None
        else (
            "No schedule block declared; no local schedule-create compatibility "
            "check was needed."
        )
    )
    return [
        {
            "code": "workflow_spec_model_valid",
            "status": "pass",
            "message": "Workflow YAML matches the stable workflow spec.",
        },
        {
            "code": "workflow_schedule_contract_valid",
            "status": "pass",
            "message": schedule_message,
        },
        {
            "code": "workflow_compiles_for_create",
            "status": "pass",
            "message": "Workflow DAG compiles to the DS workflow-create payload.",
        },
    ]


def _workflow_lint_diagnostics(
    checks: list[LintCheckData],
    warning_details: list[WorkflowLintWarningDetail],
) -> list[LintDiagnosticData]:
    diagnostics: list[LintDiagnosticData] = [
        {
            "code": check["code"],
            "status": check["status"],
            "message": check["message"],
            "field": None,
            "suggestion": None,
        }
        for check in checks
    ]
    diagnostics.extend(
        {
            "code": detail["code"],
            "status": "warning",
            "message": detail["message"],
            "field": detail["field"],
            "suggestion": detail["suggestion"],
        }
        for detail in warning_details
    )
    return diagnostics


def _workflow_lint_warnings(
    spec: WorkflowSpec,
) -> tuple[list[str], list[WorkflowLintWarningDetail]]:
    warnings: list[str] = []
    details: list[WorkflowLintWarningDetail] = []
    if spec.workflow.project is None:
        warning = (
            "workflow.project is not set in the file; workflow create will need "
            "--project or stored project context."
        )
        warnings.append(warning)
        details.append(
            {
                "code": "workflow_project_selection_external",
                "message": warning,
                "field": "workflow.project",
                "suggestion": (
                    "Pass --project when creating the workflow or set project "
                    "context before retrying."
                ),
                "accepted_sources": [
                    "--project",
                    "context.project",
                ],
            }
        )

    parameter_warnings, parameter_warning_details = (
        workflow_parameter_expression_warnings(spec)
    )
    warnings.extend(parameter_warnings)
    details.extend(parameter_warning_details)
    return warnings, details


def _json_array_length(value: str | int | None, *, label: str) -> int:
    if not isinstance(value, str):
        message = f"{label} did not compile to JSON text"
        raise TypeError(message)
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        message = f"{label} did not compile to a JSON array"
        raise TypeError(message)
    return len(parsed)


__all__ = ["lint_workflow_result"]
