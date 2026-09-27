from __future__ import annotations

from dataclasses import dataclass
from shlex import quote
from typing import TYPE_CHECKING, cast

from dsctl.cli_surface import (
    WORKFLOW_RESOURCE,
)
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConflictError,
    UserInputError,
)
from dsctl.services.selection import (
    SelectedValue,
    require_project_selection,
    require_workflow_selection,
    with_selection_source,
)
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)
from dsctl.upstream.resolver import (
    ResolvedProject,
    ResolvedTask,
    ResolvedWorkflow,
    raise_workflow_read_api_error,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import (
        WorkflowSpec,
    )
    from dsctl.services.selection import SelectionData
    from dsctl.services.workflow._types import (
        WorkflowServiceRuntime,
    )
    from dsctl.upstream.protocol import (
        TaskPayloadRecord,
        WorkflowPayloadRecord,
    )


@dataclass(frozen=True)
class _ResolvedWorkflowTarget:
    """One fully resolved project/workflow target for existing workflow actions."""

    selected_project: SelectedValue
    selected_workflow: SelectedValue
    scope: WorkflowScope
    workflow_payload: WorkflowPayloadRecord | None

    @property
    def project(self) -> ProjectRef:
        """Return the exact project identity without inventing an alias."""
        return self.scope.project

    @property
    def workflow(self) -> WorkflowRef:
        """Return the exact workflow identity without inventing an alias."""
        return self.scope.workflow

    @property
    def resolved_project(self) -> ResolvedProject:
        """Bridge code-native authoring helpers after their support gate."""
        return _code_native_project(self.scope.project)

    @property
    def resolved_workflow(self) -> ResolvedWorkflow:
        """Bridge code-native authoring helpers after their support gate."""
        return _code_native_workflow(self.scope.workflow)


def _native_code(project_or_workflow: ProjectRef | WorkflowRef, *, label: str) -> int:
    native = project_or_workflow.native
    if isinstance(native, NativeCode):
        return native.value
    message = f"The selected {label} is not code-native in this DS version"
    raise ApiTransportError(
        message,
        details={"resource": WORKFLOW_RESOURCE, "identity": native.value},
    )


def _legacy_native_id(
    project_or_workflow: ProjectRef | WorkflowRef,
    *,
    label: str,
) -> int:
    """Return one proved positive native ID on the exact legacy profile."""
    native = project_or_workflow.native
    if isinstance(native, NativeId) and native.value > 0:
        return native.value
    message = f"The selected {label} is not id-native in this DS version"
    raise ApiTransportError(
        message,
        details={"resource": WORKFLOW_RESOURCE, "identity": native.value},
    )


def _required_name(value: str | None, *, label: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    message = f"Resolved {label} payload was missing its required name"
    raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})


def _code_native_project(project: ProjectRef) -> ResolvedProject:
    return ResolvedProject(
        code=_native_code(project, label="project"),
        name=_required_name(project.name, label="project"),
        description=project.description,
    )


def _code_native_workflow(workflow: WorkflowRef) -> ResolvedWorkflow:
    return ResolvedWorkflow(
        code=_native_code(workflow, label="workflow"),
        name=_required_name(workflow.name, label="workflow"),
        version=workflow.version,
    )


def _code_native_task(task: TaskPayloadRecord) -> ResolvedTask:
    return ResolvedTask(
        code=task.code,
        name=_required_name(task.name, label="task"),
        version=task.version,
    )


def _project_selector(project: ProjectRef) -> str:
    return project.name or str(project.native.value)


def _identity_field(
    project_or_workflow: ProjectRef | WorkflowRef,
    *,
    prefix: str = "",
) -> str:
    suffix = "code" if isinstance(project_or_workflow.native, NativeCode) else "id"
    return f"{prefix}{suffix}"


def _resolve_create_project(
    runtime: WorkflowServiceRuntime,
    *,
    project: str | None,
    spec: WorkflowSpec,
) -> tuple[ProjectRef, dict[str, int | str | None]]:
    operations = runtime.domain.workflows
    if project is not None:
        selected_project = require_project_selection(project, runtime=runtime)
        resolved_project = operations.resolve_project(selected_project.value)
        return resolved_project, _resolved_project_selection(
            resolved_project,
            selected_project,
        )
    if spec.workflow.project is not None:
        resolved_project = operations.resolve_project(spec.workflow.project)
        return resolved_project, {
            **cast("dict[str, int | str | None]", resolved_project.to_data()),
            "source": "file",
        }
    selected_project = require_project_selection(None, runtime=runtime)
    resolved_project = operations.resolve_project(selected_project.value)
    return resolved_project, _resolved_project_selection(
        resolved_project,
        selected_project,
    )


def _resolved_file_workflow_data(
    workflow: ResolvedWorkflow | WorkflowRef,
) -> dict[str, int | str | None]:
    return {
        **cast("dict[str, int | str | None]", workflow.to_data()),
        "source": "file",
    }


def _resolved_project_selection(
    project: ResolvedProject | ProjectRef,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", project.to_data()), selection)


def _resolve_workflow_target(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    action: str,
    include_payload: bool = False,
) -> _ResolvedWorkflowTarget:
    operations = runtime.domain.workflows
    operations.require_action(action)
    selected_project = require_project_selection(project, runtime=runtime)
    selected_workflow = require_workflow_selection(
        workflow,
        input_form="argument",
    )
    scope = operations.resolve_workflow(
        selected_project.value,
        selected_workflow.value,
    )
    return _ResolvedWorkflowTarget(
        selected_project=selected_project,
        selected_workflow=selected_workflow,
        scope=scope,
        workflow_payload=(
            _load_workflow_detail(runtime, scope=scope, action=action)
            if include_payload
            else None
        ),
    )


def _load_workflow_detail(
    runtime: WorkflowServiceRuntime,
    *,
    scope: WorkflowScope,
    action: str,
) -> WorkflowPayloadRecord:
    """Keep pre-mutation detail reads under the shared workflow error contract."""
    try:
        return runtime.domain.workflows.detail(scope, action=action)
    except ApiResultError as error:
        raise_workflow_read_api_error(
            error,
            project_code=_native_code(scope.project, label="project"),
            workflow_code=_native_code(scope.workflow, label="workflow"),
        )


def _resolve_workflow_edit_target(
    runtime: WorkflowServiceRuntime,
    *,
    workflow: str | None,
    project: str | None,
    spec: WorkflowSpec | None,
) -> _ResolvedWorkflowTarget:
    operations = runtime.domain.workflows
    operations.require_action("workflow.edit")
    include_payload = operations.workflow_graph_family != "legacy-json"
    if spec is None:
        return _resolve_workflow_target(
            runtime,
            workflow=workflow,
            project=project,
            action="workflow.edit",
            include_payload=include_payload,
        )
    selected_project = _resolve_workflow_file_project_selection(
        runtime,
        project=project,
        spec=spec,
    )
    resolved_project = operations.resolve_project(selected_project.value)
    _validate_workflow_file_project(
        explicit_project=project,
        resolved_project=resolved_project,
        spec=spec,
    )
    selected_workflow = require_workflow_selection(
        workflow,
        input_form="argument",
    )
    scope = operations.resolve_workflow(
        selected_project.value,
        selected_workflow.value,
    )
    return _ResolvedWorkflowTarget(
        selected_project=selected_project,
        selected_workflow=selected_workflow,
        scope=scope,
        workflow_payload=(
            _load_workflow_detail(runtime, scope=scope, action="workflow.edit")
            if include_payload
            else None
        ),
    )


def _resolve_workflow_file_project_selection(
    runtime: WorkflowServiceRuntime,
    *,
    project: str | None,
    spec: WorkflowSpec,
) -> SelectedValue:
    if project is not None:
        return require_project_selection(project, runtime=runtime)
    if spec.workflow.project is not None:
        return SelectedValue(
            value=spec.workflow.project,
            source="file",
        )
    return require_project_selection(None, runtime=runtime)


def _validate_workflow_file_project(
    *,
    explicit_project: str | None,
    resolved_project: ProjectRef,
    spec: WorkflowSpec,
) -> None:
    if explicit_project is None or spec.workflow.project is None:
        return
    if str(resolved_project.native.value) == spec.workflow.project:
        return
    if resolved_project.name == spec.workflow.project:
        return
    message = (
        "workflow edit --file cannot target a different project than "
        "workflow.project in the YAML file."
    )
    raise UserInputError(
        message,
        details={
            "project": resolved_project.name,
            "project_identity": resolved_project.native.value,
            "workflow_project": spec.workflow.project,
        },
        suggestion=(
            "Use matching --project and workflow.project values; moving a "
            "workflow between projects is not supported by edit."
        ),
    )


def _resolved_workflow_selection(
    workflow: ResolvedWorkflow | WorkflowRef,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source(cast("SelectionData", workflow.to_data()), selection)


def _resolved_task_data(task: ResolvedTask) -> dict[str, int | str | None]:
    return cast("dict[str, int | str | None]", task.to_data())


def _resolved_selected_workflow_data(
    *,
    code: int,
    name: str,
    selection: SelectedValue,
) -> dict[str, int | str | None]:
    return with_selection_source({"code": code, "name": name}, selection)


def _require_new_workflow_name(
    runtime: WorkflowServiceRuntime, *, project: ProjectRef, workflow_name: str
) -> None:
    """Reject an observed same-project name conflict before preparing creation."""
    existing = runtime.domain.workflows.find_workflow_ref_by_name(
        project,
        workflow_name,
    )
    if existing is None:
        return
    message = f"Workflow {workflow_name!r} already exists in project {project.name!r}."
    raise ConflictError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "project": project.to_data(),
            "workflow": existing.to_data(),
            "mutation_applied": False,
        },
        suggestion=(
            "Choose a new workflow name, or inspect the existing definition with "
            f"`dsctl workflow get {quote(workflow_name)} "
            f"--project {quote(_project_selector(project))}` before editing it."
        ),
    )
