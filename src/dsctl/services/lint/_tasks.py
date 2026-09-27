from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.errors import DsctlError, UserInputError
from dsctl.models.common import (
    ModelValidationError,
    prefixed_model_validation_issues,
)
from dsctl.models.task_spec import canonical_task_type
from dsctl.models.workflow_spec import COMMAND_TASK_TYPES
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.mutation import workflow_patch_intrinsic_issues
from dsctl.services.lint._diagnostics import (
    _as_user_input_error,
    _diagnostic,
    _model_issue_diagnostics,
)
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.models.workflow_patch import (
        WorkflowPatchSpec,
        WorkflowPatchTaskUpdateSpec,
    )
    from dsctl.models.workflow_spec import (
        WorkflowAuthoringContext,
        WorkflowSpec,
        WorkflowTaskSpec,
    )
    from dsctl.services.lint._types import LintDiagnosticData
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


def _validate_workflow_semantics(
    spec: WorkflowSpec,
    *,
    catalog: TaskAuthoringCatalog,
) -> tuple[WorkflowSpec, list[LintDiagnosticData], UserInputError | None]:
    exact = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    diagnostics, first_failure = _workflow_global_parameter_diagnostics(
        spec,
        exact=exact,
    )
    normalized_tasks = []
    for index, task in enumerate(spec.tasks):
        normalized, task_diagnostics, failure = _workflow_task_diagnostics(
            task,
            index=index,
            exact=exact,
        )
        normalized_tasks.append(normalized)
        diagnostics.extend(task_diagnostics)
        first_failure = first_failure or failure
    normalized_spec = spec.model_copy(update={"tasks": normalized_tasks})
    return normalized_spec, diagnostics, first_failure


def _workflow_global_parameter_diagnostics(
    spec: WorkflowSpec,
    *,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    global_params = spec.workflow.global_params
    if not isinstance(global_params, list):
        return [], None
    diagnostics: list[LintDiagnosticData] = []
    first_failure: UserInputError | None = None
    for index, parameter in enumerate(global_params):
        try:
            exact.validate_global_params([parameter])
        except DsctlError as error:
            first_failure = first_failure or _as_user_input_error(error)
            diagnostics.append(
                _diagnostic(
                    "error",
                    f"workflow_parameter_{error.error_type}",
                    f"workflow.global_params[{index}]",
                    error.message,
                )
            )
    return diagnostics, first_failure


def _workflow_task_diagnostics(
    task: WorkflowTaskSpec,
    *,
    index: int,
    exact: WorkflowAuthoringContext,
) -> tuple[WorkflowTaskSpec, list[LintDiagnosticData], UserInputError | None]:
    path = f"tasks[{index}]"
    diagnostics: list[LintDiagnosticData] = []
    normalized = task
    failure: UserInputError | None = None
    try:
        if task.task_params is None:
            exact.authorize_task_type(task.type)
        else:
            params = exact.normalize_task_params(task.type, task.task_params)
            normalized = task.model_copy(update={"task_params": params})
    except ModelValidationError as error:
        diagnostics.extend(
            _model_issue_diagnostics(
                prefixed_model_validation_issues(error.issues, prefix=path),
                code_prefix="workflow_task_model",
            )
        )
        failure = UserInputError(str(error))
    except DsctlError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                f"workflow_task_{error.error_type}",
                f"{path}.task_params",
                error.message,
            )
        )
        failure = _as_user_input_error(error)
    except ValueError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_task_parameters_invalid",
                f"{path}.task_params",
                str(error),
            )
        )
        failure = UserInputError(str(error))
    identity_diagnostic, identity_failure = _workflow_task_identity_diagnostic(
        task,
        path=path,
        exact=exact,
    )
    if identity_diagnostic is not None:
        diagnostics.append(identity_diagnostic)
    failure = failure or identity_failure
    if failure is None:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_task_valid",
                path,
                f"Task '{task.name}' is valid for the selected exact profile.",
            )
        )
    return normalized, diagnostics, failure


def _workflow_task_identity_diagnostic(
    task: WorkflowTaskSpec,
    *,
    path: str,
    exact: WorkflowAuthoringContext,
) -> tuple[LintDiagnosticData | None, UserInputError | None]:
    try:
        exact.validate_task_identity(task.type, task.name)
    except (DsctlError, ValueError) as error:
        message = error.message if isinstance(error, DsctlError) else str(error)
        return (
            _diagnostic(
                "error",
                "workflow_task_identity_invalid",
                f"{path}.name",
                message,
            ),
            UserInputError(message),
        )
    return None, None


def _patch_semantic_diagnostics(
    patch: WorkflowPatchSpec,
    *,
    catalog: TaskAuthoringCatalog,
    workflow_instance: bool,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    diagnostics = [
        _diagnostic("error", issue["code"], issue["path"], issue["message"])
        for issue in workflow_patch_intrinsic_issues(
            patch,
            workflow_instance=workflow_instance,
        )
    ]
    first_failure = (
        None if not diagnostics else UserInputError(diagnostics[0]["message"])
    )
    exact = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    for extra_diagnostics, failure in (
        _patch_global_parameter_diagnostics(patch, exact=exact),
        _patch_create_task_diagnostics(patch, exact=exact),
        _patch_update_task_diagnostics(patch, exact=exact),
    ):
        diagnostics.extend(extra_diagnostics)
        first_failure = first_failure or failure
    if not diagnostics:
        diagnostics.append(
            _diagnostic(
                "info",
                "workflow_patch_intrinsic_semantics_valid",
                "patch",
                "All patch constraints independent of a live baseline are valid.",
            )
        )
    return diagnostics, first_failure


def _patch_global_parameter_diagnostics(
    patch: WorkflowPatchSpec,
    *,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    global_params = None if patch.workflow is None else patch.workflow.set.global_params
    if not isinstance(global_params, list):
        return [], None
    diagnostics: list[LintDiagnosticData] = []
    first_failure: UserInputError | None = None
    for index, parameter in enumerate(global_params):
        try:
            exact.validate_global_params([parameter])
        except DsctlError as error:
            diagnostics.append(
                _diagnostic(
                    "error",
                    f"workflow_patch_parameter_{error.error_type}",
                    f"patch.workflow.set.global_params[{index}]",
                    error.message,
                )
            )
            first_failure = first_failure or _as_user_input_error(error)
    return diagnostics, first_failure


def _patch_create_task_diagnostics(
    patch: WorkflowPatchSpec,
    *,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    if patch.tasks is None:
        return [], None
    diagnostics: list[LintDiagnosticData] = []
    first_failure: UserInputError | None = None
    for index, task in enumerate(patch.tasks.create):
        task_diagnostics, failure = _validate_patch_task_payload(
            task.type,
            task.name,
            task.task_params,
            path=f"patch.tasks.create[{index}]",
            exact=exact,
        )
        diagnostics.extend(task_diagnostics)
        first_failure = first_failure or failure
    return diagnostics, first_failure


def _patch_update_task_diagnostics(
    patch: WorkflowPatchSpec,
    *,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    if patch.tasks is None:
        return [], None
    diagnostics: list[LintDiagnosticData] = []
    first_failure: UserInputError | None = None
    for index, update in enumerate(patch.tasks.update):
        task_diagnostics, failure = _one_patch_update_task_diagnostics(
            update,
            index=index,
            exact=exact,
        )
        diagnostics.extend(task_diagnostics)
        first_failure = first_failure or failure
    return diagnostics, first_failure


def _one_patch_update_task_diagnostics(
    update: WorkflowPatchTaskUpdateSpec,
    *,
    index: int,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    task_type = update.set.type
    if task_type is None:
        return [], None
    normalized_type = canonical_task_type(task_type)
    provided = update.set.model_fields_set
    if "command" in provided and update.set.command is not None:
        return _patch_command_update_diagnostics(
            normalized_type,
            index=index,
            exact=exact,
        )
    if "task_params" not in provided or update.set.task_params is None:
        return [], None
    return _validate_patch_task_payload(
        normalized_type,
        update.match.name,
        update.set.task_params,
        path=f"patch.tasks.update[{index}].set",
        exact=exact,
    )


def _patch_command_update_diagnostics(
    task_type: str,
    *,
    index: int,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    if task_type not in COMMAND_TASK_TYPES:
        message = (
            "Command shorthand is available only for SHELL and PYTHON task updates"
        )
        return [
            _diagnostic(
                "error",
                "workflow_patch_command_type_invalid",
                f"patch.tasks.update[{index}].set.command",
                message,
            )
        ], UserInputError(message)
    try:
        exact.authorize_task_type(task_type)
    except DsctlError as error:
        return [
            _diagnostic(
                "error",
                f"workflow_patch_task_{error.error_type}",
                f"patch.tasks.update[{index}].set.type",
                error.message,
            )
        ], _as_user_input_error(error)
    return [], None


def _validate_patch_task_payload(
    task_type: str,
    task_name: str,
    task_params: YamlObject | None,
    *,
    path: str,
    exact: WorkflowAuthoringContext,
) -> tuple[list[LintDiagnosticData], UserInputError | None]:
    diagnostics: list[LintDiagnosticData] = []
    try:
        if task_params is None:
            exact.authorize_task_type(task_type)
        else:
            exact.normalize_task_params(task_type, task_params)
        exact.validate_task_identity(task_type, task_name)
    except ModelValidationError as error:
        diagnostics.extend(
            _model_issue_diagnostics(
                prefixed_model_validation_issues(error.issues, prefix=path),
                code_prefix="workflow_patch_task_model",
            )
        )
        return diagnostics, UserInputError(str(error))
    except DsctlError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                f"workflow_patch_task_{error.error_type}",
                path,
                error.message,
            )
        )
        return diagnostics, _as_user_input_error(error)
    except ValueError as error:
        diagnostics.append(
            _diagnostic(
                "error",
                "workflow_patch_task_invalid",
                path,
                str(error),
            )
        )
        return diagnostics, UserInputError(str(error))
    diagnostics.append(
        _diagnostic(
            "info",
            "workflow_patch_task_valid",
            path,
            f"Task '{task_name}' is valid without baseline-derived fields.",
        )
    )
    return diagnostics, None
