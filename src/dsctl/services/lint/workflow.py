from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.errors import UserInputError
from dsctl.models.common import model_validation_issues
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.output import CommandResult, require_json_object
from dsctl.services._parameter_warnings import workflow_parameter_warnings
from dsctl.services._workflow.validation import require_schedule_block_create_compatible
from dsctl.services.lint._context import (
    _selected_catalog,
    _structural_authoring_context,
)
from dsctl.services.lint._diagnostics import (
    _append_skipped_workflow_stages,
    _diagnostic,
    _invalid_lint_result,
    _load_yaml_mapping,
    _model_issue_diagnostics,
    _normalized_path,
    _warning_diagnostics,
)
from dsctl.services.lint._graph import (
    _workflow_lint_summary,
    _workflow_plan_stage,
)
from dsctl.services.lint._tasks import _validate_workflow_semantics
from dsctl.services.lint._types import WorkflowLintData

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services.lint._types import (
        LintDiagnosticData,
        WorkflowLintWarningDetail,
    )
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


def lint_workflow_result(
    *,
    file: str | Path,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | Path | None = None,
) -> CommandResult:
    """Lint one workflow YAML while retaining every completed local finding."""
    selected = _selected_catalog(catalog=catalog, env_file=env_file)
    path = _normalized_path(file)
    diagnostics: list[LintDiagnosticData] = []
    document = _load_yaml_mapping(
        path,
        label="Workflow YAML",
        diagnostics=diagnostics,
    )
    if document is None:
        _append_skipped_workflow_stages(diagnostics, after="yaml")
        return _invalid_lint_result(
            kind="workflow",
            path=path,
            data=WorkflowLintData(
                kind="workflow",
                valid=False,
                diagnostics=diagnostics,
            ),
            diagnostics=diagnostics,
        )

    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_yaml_loaded",
            None,
            "Workflow YAML is a readable mapping.",
        )
    )
    try:
        spec = validate_workflow_document(
            document,
            authoring_context=_structural_authoring_context(selected),
        )
    except ValidationError as error:
        diagnostics.extend(
            _model_issue_diagnostics(
                model_validation_issues(error),
                code_prefix="workflow_model",
            )
        )
        _append_skipped_workflow_stages(diagnostics, after="model")
        return _invalid_lint_result(
            kind="workflow",
            path=path,
            data=WorkflowLintData(
                kind="workflow",
                valid=False,
                diagnostics=diagnostics,
            ),
            diagnostics=diagnostics,
        )

    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_spec_model_valid",
            None,
            "Workflow YAML matches the stable workflow spec.",
        )
    )
    spec, semantic_diagnostics, semantic_failure = _validate_workflow_semantics(
        spec,
        catalog=selected,
    )
    diagnostics.extend(semantic_diagnostics)

    schedule_diagnostic, schedule_failure = _workflow_schedule_stage(spec)
    diagnostics.append(schedule_diagnostic)
    edges, compilation_data, plan_diagnostics, graph_failure = _workflow_plan_stage(
        spec,
        catalog=selected,
        compile_allowed=semantic_failure is None,
    )
    diagnostics.extend(plan_diagnostics)
    warnings, warning_details = _workflow_lint_warnings(spec, catalog=selected)
    diagnostics.extend(_warning_diagnostics(warning_details))

    valid = not any(item["severity"] == "error" for item in diagnostics)
    data = WorkflowLintData(
        kind="workflow",
        valid=valid,
        diagnostics=diagnostics,
    )
    if edges is not None:
        data["summary"] = _workflow_lint_summary(spec, edges=edges)
    if compilation_data is not None:
        data["compilation"] = compilation_data
    if valid:
        return CommandResult(
            data=require_json_object(data, label="workflow lint data"),
            resolved={"kind": "workflow", "file": str(path)},
            warnings=warnings,
            warning_details=warning_details,
        )
    return _invalid_lint_result(
        kind="workflow",
        path=path,
        data=data,
        diagnostics=diagnostics,
        failure=semantic_failure or schedule_failure or graph_failure,
        warnings=warnings,
        warning_details=warning_details,
    )


def _workflow_schedule_stage(
    spec: WorkflowSpec,
) -> tuple[LintDiagnosticData, UserInputError | None]:
    try:
        require_schedule_block_create_compatible(spec)
    except UserInputError as error:
        return (
            _diagnostic(
                "error",
                "workflow_schedule_contract_invalid",
                "schedule",
                error.message,
            ),
            error,
        )
    message = (
        "Workflow schedule block is locally compatible with workflow create."
        if spec.schedule is not None
        else "No schedule block requires schedule-create validation."
    )
    return (
        _diagnostic(
            "info",
            "workflow_schedule_contract_valid",
            "schedule" if spec.schedule is not None else None,
            message,
        ),
        None,
    )


def _workflow_lint_warnings(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
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
                "accepted_sources": ["--project", "context.project"],
            }
        )
    parameter_warnings, parameter_warning_details = workflow_parameter_warnings(
        spec,
        parameter_semantics=catalog.parameter_semantics,
    )
    warnings.extend(parameter_warnings)
    details.extend(parameter_warning_details)
    return warnings, details
