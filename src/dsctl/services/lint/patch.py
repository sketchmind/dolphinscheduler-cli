from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import ValidationError

from dsctl.models.common import model_validation_issues
from dsctl.models.workflow_patch import validate_workflow_patch_document
from dsctl.output import CommandResult, require_json_object
from dsctl.services.lint._context import (
    _selected_catalog,
    _structural_authoring_context,
)
from dsctl.services.lint._diagnostics import (
    _diagnostic,
    _invalid_lint_result,
    _load_yaml_mapping,
    _model_issue_diagnostics,
    _normalized_path,
)
from dsctl.services.lint._tasks import _patch_semantic_diagnostics
from dsctl.services.lint._types import WorkflowPatchLintData

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.services.lint._types import (
        LintDiagnosticData,
        PatchBaselineCheckData,
        PatchBaselineData,
        PatchLintSummaryData,
    )
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


def lint_workflow_patch_result(
    *,
    file: str | Path,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | Path | None = None,
) -> CommandResult:
    """Lint one workflow definition patch without loading a remote baseline."""
    return _lint_patch_result(
        file=file,
        catalog=catalog,
        env_file=env_file,
        workflow_instance=False,
    )


def lint_workflow_instance_patch_result(
    *,
    file: str | Path,
    catalog: TaskAuthoringCatalog | None = None,
    env_file: str | Path | None = None,
) -> CommandResult:
    """Lint one workflow-instance patch without loading a remote baseline."""
    return _lint_patch_result(
        file=file,
        catalog=catalog,
        env_file=env_file,
        workflow_instance=True,
    )


def _lint_patch_result(
    *,
    file: str | Path,
    catalog: TaskAuthoringCatalog | None,
    env_file: str | Path | None,
    workflow_instance: bool,
) -> CommandResult:
    selected = _selected_catalog(catalog=catalog, env_file=env_file)
    path = _normalized_path(file)
    kind: Literal["workflow-patch", "workflow-instance-patch"] = (
        "workflow-instance-patch" if workflow_instance else "workflow-patch"
    )
    diagnostics: list[LintDiagnosticData] = []
    baseline = _patch_baseline_data(workflow_instance=workflow_instance)
    document = _load_yaml_mapping(
        path,
        label="Workflow patch YAML",
        diagnostics=diagnostics,
    )
    if document is None:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_patch_model_check_skipped",
                None,
                "Patch model checks require a readable YAML mapping.",
            )
        )
        return _invalid_lint_result(
            kind=kind,
            path=path,
            data=WorkflowPatchLintData(
                kind=kind,
                valid=False,
                baseline=baseline,
                diagnostics=diagnostics,
            ),
            diagnostics=diagnostics,
        )
    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_patch_yaml_loaded",
            None,
            "Workflow patch YAML is a readable mapping.",
        )
    )

    try:
        patch = validate_workflow_patch_document(
            document,
            authoring_context=_structural_authoring_context(selected),
        ).patch
    except ValidationError as error:
        diagnostics.extend(
            _model_issue_diagnostics(
                model_validation_issues(error),
                code_prefix="workflow_patch_model",
            )
        )
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_patch_semantic_checks_skipped",
                "patch",
                "Patch semantic checks require a valid patch model.",
            )
        )
        return _invalid_lint_result(
            kind=kind,
            path=path,
            data=WorkflowPatchLintData(
                kind=kind,
                valid=False,
                baseline=baseline,
                diagnostics=diagnostics,
            ),
            diagnostics=diagnostics,
        )

    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_patch_model_valid",
            "patch",
            "Patch YAML matches the stable patch document model.",
        )
    )
    semantic_diagnostics, semantic_failure = _patch_semantic_diagnostics(
        patch,
        catalog=selected,
        workflow_instance=workflow_instance,
    )
    diagnostics.extend(semantic_diagnostics)
    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_patch_baseline_checks_deferred",
            "patch",
            (
                "Live task matching, preservation, and final DAG validation are "
                "deferred to the baseline-aware edit dry run."
            ),
        )
    )
    valid = not any(item["severity"] == "error" for item in diagnostics)
    data = WorkflowPatchLintData(
        kind=kind,
        valid=valid,
        summary=_patch_lint_summary(patch),
        baseline=baseline,
        diagnostics=diagnostics,
    )
    if valid:
        return CommandResult(
            data=require_json_object(data, label=f"{kind} lint data"),
            resolved={"kind": kind, "file": str(path)},
        )
    return _invalid_lint_result(
        kind=kind,
        path=path,
        data=data,
        diagnostics=diagnostics,
        failure=semantic_failure,
    )


def _patch_lint_summary(patch: WorkflowPatchSpec) -> PatchLintSummaryData:
    task_patch = patch.tasks
    return {
        "workflowFields": (
            []
            if patch.workflow is None
            else sorted(patch.workflow.set.model_fields_set)
        ),
        "createCount": 0 if task_patch is None else len(task_patch.create),
        "updateCount": 0 if task_patch is None else len(task_patch.update),
        "renameCount": 0 if task_patch is None else len(task_patch.rename),
        "deleteCount": 0 if task_patch is None else len(task_patch.delete),
    }


def _patch_baseline_data(*, workflow_instance: bool) -> PatchBaselineData:
    unchecked: list[PatchBaselineCheckData] = [
        {
            "code": "patch_live_task_matches_unchecked",
            "path": "patch.tasks",
            "message": (
                "Update, rename, and delete selectors are not checked against live "
                "task names."
            ),
        },
        {
            "code": "patch_live_name_collisions_unchecked",
            "path": "patch.tasks",
            "message": (
                "Create names and rename targets are not checked against unchanged "
                "live tasks."
            ),
        },
        {
            "code": "patch_partial_task_merge_unchecked",
            "path": "patch.tasks.update",
            "message": (
                "Partial task updates that omit type or payload fields require the "
                "live task before their merged model can be checked."
            ),
        },
        {
            "code": "patch_final_graph_unchecked",
            "path": "patch.tasks",
            "message": (
                "Final dependencies, logical-task references, and cycles require "
                "the complete live DAG."
            ),
        },
        {
            "code": "patch_native_preservation_unchecked",
            "path": "patch.tasks",
            "message": (
                "Opaque native task preservation and identity reuse require the "
                "exported live workflow state."
            ),
        },
    ]
    if workflow_instance:
        unchecked.append(
            {
                "code": "workflow_instance_state_unchecked",
                "path": "patch",
                "message": (
                    "The instance final state and editable dagData are checked only "
                    "after the target instance is loaded."
                ),
            }
        )
    return {"requiredForCompleteValidation": True, "unchecked": unchecked}
