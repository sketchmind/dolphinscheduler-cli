from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import yaml

from dsctl.errors import DsctlError, UserInputError
from dsctl.models.common import is_yaml_object, yaml_value_validation_issue
from dsctl.output import CommandResult, require_json_object

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.models.common import ModelValidationIssue, YamlObject
    from dsctl.services.lint._types import (
        LintDiagnosticData,
        LintSeverity,
        WorkflowLintData,
        WorkflowLintWarningDetail,
        WorkflowPatchLintData,
    )


def _load_yaml_mapping(
    path: Path,
    *,
    label: str,
    diagnostics: list[LintDiagnosticData],
) -> YamlObject | None:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                "yaml_file_unreadable",
                None,
                f"Could not read {label.lower()}: {error}",
            )
        )
        return None
    except yaml.YAMLError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                "yaml_syntax_invalid",
                None,
                f"{label} is invalid: {error}",
            )
        )
        return None
    if not isinstance(document, Mapping):
        diagnostics.append(
            _diagnostic(
                "error",
                "yaml_root_not_mapping",
                None,
                f"{label} root must be a mapping.",
            )
        )
        return None
    if is_yaml_object(document):
        return document
    issue = yaml_value_validation_issue(document)
    if issue is None:
        message = "YAML boundary rejected a document without a validation issue"
        raise RuntimeError(message)
    diagnostics.append(
        _diagnostic(
            "error",
            issue.code,
            issue.path,
            issue.message,
        )
    )
    return None


def _normalized_path(file: str | Path) -> Path:
    return file if isinstance(file, Path) else Path(file)


def _warning_diagnostics(
    details: Sequence[WorkflowLintWarningDetail],
) -> list[LintDiagnosticData]:
    return [
        _diagnostic(
            "warning",
            detail["code"],
            detail["field"],
            detail["message"],
        )
        for detail in details
    ]


def _model_issue_diagnostics(
    issues: Sequence[ModelValidationIssue],
    *,
    code_prefix: str,
) -> list[LintDiagnosticData]:
    return [
        _diagnostic(
            "error",
            f"{code_prefix}_{_normalized_issue_code(issue.code)}",
            issue.path,
            issue.message,
        )
        for issue in issues
    ]


def _normalized_issue_code(code: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "_" for character in code
    )
    return normalized.strip("_").lower() or "invalid"


def _diagnostic(
    severity: LintSeverity,
    code: str,
    path: str | None,
    message: str,
) -> LintDiagnosticData:
    return {
        "severity": severity,
        "code": code,
        "path": path,
        "message": message,
    }


def _append_skipped_workflow_stages(
    diagnostics: list[LintDiagnosticData],
    *,
    after: Literal["yaml", "model"],
) -> None:
    if after == "yaml":
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_model_check_skipped",
                None,
                "Workflow model checks require a readable YAML mapping.",
            )
        )
    diagnostics.extend(
        (
            _diagnostic(
                "info",
                "workflow_semantic_checks_skipped",
                None,
                "Exact task checks require a valid workflow model.",
            ),
            _diagnostic(
                "info",
                "workflow_schedule_check_skipped",
                "schedule",
                "Schedule checks require a valid workflow model.",
            ),
            _diagnostic(
                "info",
                "workflow_graph_check_skipped",
                "tasks",
                "Graph checks require a valid workflow model.",
            ),
            _diagnostic(
                "info",
                "workflow_compilation_skipped",
                None,
                "Local compilation requires a valid workflow model.",
            ),
        )
    )


def _invalid_lint_result(
    *,
    kind: str,
    path: Path,
    data: WorkflowLintData | WorkflowPatchLintData,
    diagnostics: Sequence[LintDiagnosticData],
    failure: UserInputError | None = None,
    warnings: list[str] | None = None,
    warning_details: Sequence[WorkflowLintWarningDetail] | None = None,
) -> CommandResult:
    error_count = sum(item["severity"] == "error" for item in diagnostics)
    resolved_failure = failure or UserInputError(
        f"{kind} lint found {error_count} blocking diagnostic(s).",
        details={"file": str(path), "diagnostic_count": error_count},
        suggestion=(
            "Fix the reported error diagnostics and rerun the same lint command."
        ),
    )
    if failure is not None:
        resolved_failure = UserInputError(
            failure.message,
            details={
                **failure.details,
                "file": str(path),
                "diagnostic_count": error_count,
            },
            suggestion=(
                failure.suggestion
                or "Fix the reported error diagnostics and rerun the same lint command."
            ),
        )
    return CommandResult(
        data=require_json_object(data, label=f"{kind} lint data"),
        resolved={"kind": kind, "file": str(path)},
        warnings=[] if warnings is None else warnings,
        warning_details=[] if warning_details is None else warning_details,
        failure=resolved_failure,
    )


def _as_user_input_error(error: DsctlError) -> UserInputError:
    return UserInputError(
        error.message,
        details=error.details,
        suggestion=error.suggestion,
    )
