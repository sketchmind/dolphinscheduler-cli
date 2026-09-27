from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeGuard

from dsctl.errors import ApiResultError, DsctlError
from dsctl.models.task_spec import (
    Dependent139TaskParamsSpec,
    contains_ds_parameter_placeholder,
)
from dsctl.upstream.definition_models import (
    NativeId,
    NativeIdentity,
    ProjectRef,
    WorkflowScope,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyDependentRefIndex,
    LegacyWorkflowGraphError,
    legacy_safe_dependent_targets,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.upstream.definition_models import WorkflowRef
    from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot


class LegacyDependentReferenceOperations(Protocol):
    """Narrow read seam for exact 1.3.9 DEPENDENT name identities."""

    def resolve_project_by_name(self, project_name: str) -> ProjectRef:
        """Resolve one literal project name without numeric reinterpretation."""
        ...

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        """Return the complete visible project identity inventory."""
        ...

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        """Resolve one literal workflow name inside a proved project."""
        ...

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        """Return the project's complete visible workflow inventory."""
        ...

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        """Read one exact legacy definition when a task name must be verified."""
        ...


@dataclass(frozen=True, slots=True)
class LegacyDependentSelector:
    """One canonical exact-name DEPENDENT target."""

    project_name: str
    workflow_name: str
    task_name: str | None


class LegacyDependentTargetResolutionError(Exception):
    """One canonical DEPENDENT selector could not be bound safely."""

    def __init__(
        self,
        selector: LegacyDependentSelector,
        cause: BaseException,
    ) -> None:
        """Retain the failed selector and native error for service translation."""
        super().__init__(str(cause))
        self.project_name = selector.project_name
        self.workflow_name = selector.workflow_name
        self.task_name = selector.task_name
        self.cause = cause


class LegacyDependentReferenceResolver:
    """Bind exact 1.3.9 canonical names to split-wire native identities."""

    def __init__(
        self,
        operations: LegacyDependentReferenceOperations,
        *,
        action: str,
    ) -> None:
        """Freeze the exact read seam and calling service action."""
        self._operations = operations
        self._action = action

    def resolve_authoring(self, spec: WorkflowSpec) -> LegacyDependentRefIndex:
        """Resolve every typed target and verify literal task membership."""
        return _LegacyDependentAuthoringBinder(
            self._operations,
            action=self._action,
        ).bind(_dependent_selectors(spec))

    def reverse_bind(
        self,
        graph: DecodedLegacyWorkflowGraph,
    ) -> LegacyDependentRefIndex:
        """Best-effort bind exact native packages without failing normal reads."""
        return _LegacyDependentReverseBinder(
            self._operations,
            action=self._action,
        ).bind(legacy_safe_dependent_targets(graph))


class _LegacyDependentAuthoringBinder:
    """Resolve one authoring document with per-operation identity caches."""

    def __init__(
        self,
        operations: LegacyDependentReferenceOperations,
        *,
        action: str,
    ) -> None:
        self._operations = operations
        self._action = action
        self._projects: dict[str, ProjectRef] = {}
        self._workflows: dict[tuple[str, str], WorkflowScope] = {}
        self._task_names: dict[tuple[int, int], frozenset[str]] = {}

    def bind(
        self,
        selectors: tuple[LegacyDependentSelector, ...],
    ) -> LegacyDependentRefIndex:
        native_by_selector = {}
        for selector in selectors:
            try:
                native_by_selector[
                    (
                        selector.project_name,
                        selector.workflow_name,
                        selector.task_name,
                    )
                ] = self._resolve(selector)
            except (ApiResultError, DsctlError, LegacyWorkflowGraphError) as error:
                raise LegacyDependentTargetResolutionError(selector, error) from error
        return LegacyDependentRefIndex.from_native_by_selector(native_by_selector)

    def _resolve(
        self,
        selector: LegacyDependentSelector,
    ) -> tuple[int, int, str]:
        project = self._project(selector.project_name)
        scope = self._workflow(project, selector)
        project_id = _required_native_id(project.native, label="project")
        definition_id = _required_native_id(
            scope.workflow.native,
            label="workflow",
        )
        if selector.task_name is not None:
            _require_target_task(
                selector,
                task_names=self._target_task_names(
                    project_id,
                    definition_id,
                    scope,
                ),
            )
        dep_tasks = "ALL" if selector.task_name is None else selector.task_name
        return project_id, definition_id, dep_tasks

    def _project(self, project_name: str) -> ProjectRef:
        cached = self._projects.get(project_name)
        if cached is not None:
            return cached
        resolved = _required_project(
            self._operations.resolve_project_by_name(project_name),
            expected_name=project_name,
        )
        self._projects[project_name] = resolved
        return resolved

    def _workflow(
        self,
        project: ProjectRef,
        selector: LegacyDependentSelector,
    ) -> WorkflowScope:
        key = (selector.project_name, selector.workflow_name)
        cached = self._workflows.get(key)
        if cached is not None:
            return cached
        resolved = _required_workflow_scope(
            self._operations.resolve_workflow_by_name(
                project,
                selector.workflow_name,
            ),
            project=project,
            expected_name=selector.workflow_name,
        )
        self._workflows[key] = resolved
        return resolved

    def _target_task_names(
        self,
        project_id: int,
        definition_id: int,
        scope: WorkflowScope,
    ) -> frozenset[str]:
        key = (project_id, definition_id)
        cached = self._task_names.get(key)
        if cached is not None:
            return cached
        snapshot = self._operations.legacy_definition(
            scope,
            action=self._action,
        )
        names = _literal_task_names(snapshot)
        self._task_names[key] = names
        return names


class _LegacyDependentReverseBinder:
    """Best-effort reverse binder whose failures remain local to one target."""

    def __init__(
        self,
        operations: LegacyDependentReferenceOperations,
        *,
        action: str,
    ) -> None:
        self._operations = operations
        self._action = action
        self._workflow_scopes: dict[tuple[int, int], WorkflowScope | None] = {}
        self._task_names: dict[tuple[int, int], frozenset[str] | None] = {}

    def bind(
        self,
        targets: tuple[tuple[int, int, str], ...],
    ) -> LegacyDependentRefIndex:
        if not targets:
            return LegacyDependentRefIndex.empty()
        try:
            inventory = self._operations.visible_project_refs()
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return LegacyDependentRefIndex.empty()
        by_project = _targets_by_project(targets)
        projects = _unambiguous_projects_by_id(
            inventory,
            requested_ids=set(by_project),
        )
        bindings = {}
        for project_id, project_targets in by_project.items():
            project = projects.get(project_id)
            if project is not None:
                bindings.update(self._bind_project(project, project_targets))
        try:
            return LegacyDependentRefIndex.from_native_by_selector(bindings)
        except LegacyWorkflowGraphError:
            return LegacyDependentRefIndex.empty()

    def _bind_project(
        self,
        project: ProjectRef,
        targets: tuple[tuple[int, int, str], ...],
    ) -> dict[tuple[str, str, str | None], tuple[int, int, str]]:
        resolved_project = self._reresolve_project(project)
        if resolved_project is None:
            return {}
        project = resolved_project
        requested_ids = {definition_id for _, definition_id, _ in targets}
        try:
            inventory = self._operations.visible_workflow_refs(project)
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return {}
        workflows = _unambiguous_workflows_by_id(
            inventory,
            requested_ids=requested_ids,
        )
        bindings = {}
        for target in targets:
            binding = self._bind_target(project, workflows, target)
            if binding is not None:
                selector, native = binding
                bindings[selector] = native
        return bindings

    def _bind_target(
        self,
        project: ProjectRef,
        workflows: dict[int, WorkflowRef],
        target: tuple[int, int, str],
    ) -> tuple[tuple[str, str, str | None], tuple[int, int, str]] | None:
        project_id, definition_id, dep_tasks = target
        workflow_ref = workflows.get(definition_id)
        if (
            not _is_safe_literal_name(project.name)
            or workflow_ref is None
            or not _is_safe_literal_name(workflow_ref.name)
        ):
            return None
        scope = self._target_scope(project, workflow_ref)
        if scope is None:
            return None
        task_name = None if dep_tasks == "ALL" else dep_tasks
        if task_name is not None and (
            not _is_safe_literal_name(task_name)
            or not self._target_contains_task(
                scope,
                task_name,
            )
        ):
            return None
        return (
            (project.name, workflow_ref.name, task_name),
            (project_id, definition_id, dep_tasks),
        )

    def _reresolve_project(self, project: ProjectRef) -> ProjectRef | None:
        project_name = project.name
        if not _is_safe_literal_name(project_name):
            return None
        try:
            resolved = _required_project(
                self._operations.resolve_project_by_name(project_name),
                expected_name=project_name,
            )
            _require_same_native_project(resolved, project)
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return None
        else:
            return resolved

    def _target_scope(
        self,
        project: ProjectRef,
        workflow_ref: WorkflowRef,
    ) -> WorkflowScope | None:
        project_id = _required_native_id(project.native, label="project")
        definition_id = _required_native_id(
            workflow_ref.native,
            label="workflow",
        )
        key = (project_id, definition_id)
        if key not in self._workflow_scopes:
            self._workflow_scopes[key] = self._read_target_scope(
                project,
                workflow_ref,
            )
        return self._workflow_scopes[key]

    def _read_target_scope(
        self,
        project: ProjectRef,
        workflow_ref: WorkflowRef,
    ) -> WorkflowScope | None:
        try:
            scope = _required_workflow_scope(
                self._operations.resolve_workflow_by_name(
                    project,
                    workflow_ref.name or "",
                ),
                project=project,
                expected_name=workflow_ref.name or "",
            )
            _require_same_native_workflow(scope, workflow_ref)
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return None
        else:
            return scope

    def _target_contains_task(
        self,
        scope: WorkflowScope,
        task_name: str,
    ) -> bool:
        definition_id = _required_native_id(
            scope.workflow.native,
            label="workflow",
        )
        project_id = _required_native_id(scope.project.native, label="project")
        key = (project_id, definition_id)
        if key not in self._task_names:
            self._task_names[key] = self._read_target_task_names(
                scope,
            )
        task_names = self._task_names[key]
        return task_names is not None and task_name in task_names

    def _read_target_task_names(
        self,
        scope: WorkflowScope,
    ) -> frozenset[str] | None:
        try:
            return _literal_task_names(
                self._operations.legacy_definition(
                    scope,
                    action=self._action,
                )
            )
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return None


def _dependent_selectors(spec: WorkflowSpec) -> tuple[LegacyDependentSelector, ...]:
    selectors: list[LegacyDependentSelector] = []
    seen: set[tuple[str, str, str | None]] = set()
    for task in spec.tasks:
        if task.type != "DEPENDENT" or task.task_params is None:
            continue
        dependence = task.task_params.get("dependence")
        if not isinstance(dependence, Mapping):
            # A richer server-originated split DEPENDENT is deliberately kept
            # opaque by the legacy graph decoder.  It has no canonical names
            # to resolve, and metadata-only edits must preserve its frozen
            # native identity instead of trying to reinterpret that state.
            continue
        params = Dependent139TaskParamsSpec.model_validate(task.task_params)
        for group in params.dependence.depend_task_list:
            for item in group.depend_item_list:
                key = (item.project_name, item.workflow_name, item.task_name)
                if key in seen:
                    continue
                seen.add(key)
                selectors.append(LegacyDependentSelector(*key))
    return tuple(selectors)


def _required_project(
    project: ProjectRef,
    *,
    expected_name: str,
) -> ProjectRef:
    if project.name != expected_name:
        message = f"Project name {expected_name!r} resolved as {project.name!r}"
        raise LegacyWorkflowGraphError(message)
    _required_native_id(project.native, label="project")
    return project


def _required_workflow_scope(
    scope: WorkflowScope,
    *,
    project: ProjectRef,
    expected_name: str,
) -> WorkflowScope:
    if scope.project != project:
        message = f"Workflow {expected_name!r} resolved in a different project"
        raise LegacyWorkflowGraphError(message)
    if scope.workflow.name != expected_name:
        message = f"Workflow name {expected_name!r} resolved as {scope.workflow.name!r}"
        raise LegacyWorkflowGraphError(message)
    _required_native_id(scope.workflow.native, label="workflow")
    return scope


def _required_native_id(native: NativeIdentity, *, label: str) -> int:
    if not isinstance(native, NativeId) or native.value <= 0:
        message = f"Legacy DEPENDENT {label} resolution must return a positive ID"
        raise LegacyWorkflowGraphError(message)
    return native.value


def _require_target_task(
    selector: LegacyDependentSelector,
    *,
    task_names: frozenset[str],
) -> None:
    if selector.task_name is None or selector.task_name in task_names:
        return
    message = (
        f"Task '{selector.task_name}' does not exist in target workflow "
        f"'{selector.workflow_name}'"
    )
    raise LegacyWorkflowGraphError(message)


def _require_same_native_workflow(
    scope: WorkflowScope,
    expected: WorkflowRef,
) -> None:
    if scope.workflow.native == expected.native:
        return
    message = "Target workflow identity changed during reverse binding"
    raise LegacyWorkflowGraphError(message)


def _require_same_native_project(
    project: ProjectRef,
    expected: ProjectRef,
) -> None:
    if project.native == expected.native:
        return
    message = "Target project identity changed during reverse binding"
    raise LegacyWorkflowGraphError(message)


def _targets_by_project(
    targets: tuple[tuple[int, int, str], ...],
) -> dict[int, tuple[tuple[int, int, str], ...]]:
    grouped: dict[int, list[tuple[int, int, str]]] = {}
    for target in targets:
        grouped.setdefault(target[0], []).append(target)
    return {
        project_id: tuple(project_targets)
        for project_id, project_targets in grouped.items()
    }


def _unambiguous_projects_by_id(
    refs: tuple[ProjectRef, ...],
    *,
    requested_ids: set[int],
) -> dict[int, ProjectRef]:
    refs_by_id: dict[int, list[ProjectRef]] = {}
    ids_by_name: dict[str, set[int]] = {}
    for ref in refs:
        if not isinstance(ref.native, NativeId) or ref.native.value <= 0:
            continue
        project_id = ref.native.value
        refs_by_id.setdefault(project_id, []).append(ref)
        if _is_safe_literal_name(ref.name):
            ids_by_name.setdefault(ref.name, set()).add(project_id)
    return {
        project_id: project_refs[0]
        for project_id in requested_ids
        if (project_refs := refs_by_id.get(project_id))
        and _is_safe_literal_name(project_refs[0].name)
        and all(ref == project_refs[0] for ref in project_refs)
        and ids_by_name.get(project_refs[0].name) == {project_id}
    }


def _unambiguous_workflows_by_id(
    refs: tuple[WorkflowRef, ...],
    *,
    requested_ids: set[int],
) -> dict[int, WorkflowRef]:
    refs_by_id: dict[int, list[WorkflowRef]] = {}
    ids_by_name: dict[str, set[int]] = {}
    for ref in refs:
        if not isinstance(ref.native, NativeId) or ref.native.value <= 0:
            continue
        definition_id = ref.native.value
        refs_by_id.setdefault(definition_id, []).append(ref)
        if _is_safe_literal_name(ref.name):
            ids_by_name.setdefault(ref.name, set()).add(definition_id)
    return {
        definition_id: workflow_refs[0]
        for definition_id in requested_ids
        if (workflow_refs := refs_by_id.get(definition_id))
        and _is_safe_literal_name(workflow_refs[0].name)
        and all(ref == workflow_refs[0] for ref in workflow_refs)
        and ids_by_name.get(workflow_refs[0].name) == {definition_id}
    }


def _is_safe_literal_name(value: str | None) -> TypeGuard[str]:
    return bool(
        value
        and value == value.strip()
        and not contains_ds_parameter_placeholder(value)
    )


def _literal_task_names(
    snapshot: LegacyWorkflowDefinitionSnapshot,
) -> frozenset[str]:
    try:
        document = json.loads(snapshot.process_definition_json)
    except (TypeError, ValueError) as error:
        message = "Target workflow processDefinitionJson is not valid JSON"
        raise LegacyWorkflowGraphError(message) from error
    if not isinstance(document, dict):
        message = "Target workflow processDefinitionJson must be an object"
        raise LegacyWorkflowGraphError(message)
    tasks = document.get("tasks")
    if not isinstance(tasks, list):
        message = "Target workflow processDefinitionJson.tasks must be an array"
        raise LegacyWorkflowGraphError(message)

    names: set[str] = set()
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            message = f"Target workflow task {index} must be an object"
            raise LegacyWorkflowGraphError(message)
        task_name = task.get("name")
        if (
            not isinstance(task_name, str)
            or not task_name
            or task_name != task_name.strip()
        ):
            message = f"Target workflow task {index} has an invalid name"
            raise LegacyWorkflowGraphError(message)
        if task_name in names:
            message = f"Target workflow task name {task_name!r} is duplicated"
            raise LegacyWorkflowGraphError(message)
        names.add(task_name)
    return frozenset(names)


__all__ = [
    "LegacyDependentReferenceOperations",
    "LegacyDependentReferenceResolver",
    "LegacyDependentSelector",
    "LegacyDependentTargetResolutionError",
]
