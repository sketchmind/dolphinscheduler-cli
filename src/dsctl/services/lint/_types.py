from __future__ import annotations

from typing import Literal, TypeAlias, TypedDict

from dsctl.services._parameter_warnings import ParameterWarningDetail

LintSeverity = Literal["info", "warning", "error"]


class LintDiagnosticData(TypedDict):
    """One machine-readable lint finding."""

    severity: LintSeverity
    code: str
    path: str | None
    message: str


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
    """Stable counts from the CLI's canonical local authoring plan."""

    taskDefinitionCount: int
    taskRelationCount: int
    globalParamCount: int


class WorkflowLintData(TypedDict, total=False):
    """Structured payload returned by `lint workflow`."""

    kind: Literal["workflow"]
    valid: bool
    summary: WorkflowLintSummaryData
    compilation: WorkflowLintCompilationData
    diagnostics: list[LintDiagnosticData]


class PatchLintSummaryData(TypedDict):
    """Locally knowable operation counts for one patch document."""

    workflowFields: list[str]
    createCount: int
    updateCount: int
    renameCount: int
    deleteCount: int


class PatchBaselineCheckData(TypedDict):
    """One check intentionally deferred until edit resolves a live baseline."""

    code: str
    path: str
    message: str


class PatchBaselineData(TypedDict):
    """Boundary between offline patch lint and baseline-aware edit preview."""

    requiredForCompleteValidation: Literal[True]
    unchecked: list[PatchBaselineCheckData]


class WorkflowPatchLintData(TypedDict, total=False):
    """Structured payload returned by either patch lint command."""

    kind: Literal["workflow-patch", "workflow-instance-patch"]
    valid: bool
    summary: PatchLintSummaryData
    baseline: PatchBaselineData
    diagnostics: list[LintDiagnosticData]


class WorkflowProjectSelectionWarningDetail(TypedDict):
    """Structured warning emitted for locally valid but externalized inputs."""

    code: Literal["workflow_project_selection_external"]
    message: str
    field: str
    suggestion: str
    accepted_sources: list[str]


WorkflowLintWarningDetail: TypeAlias = (
    WorkflowProjectSelectionWarningDetail | ParameterWarningDetail
)
