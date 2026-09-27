from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, TypeGuard

from dsctl.errors import ApiResultError, DsctlError
from dsctl.models.task_spec import contains_ds_parameter_placeholder
from dsctl.upstream.definition_models import NativeId, ProjectRef, WorkflowScope
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    LegacyWorkflowGraphError,
    LegacyWorkflowRefIndex,
    PreparedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    legacy_runtime_nested_workflow_definition_ids,
    legacy_safe_sub_process_definition_ids,
    prepared_legacy_runtime_nested_workflow_definition_ids,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.upstream.definition_models import WorkflowRef
    from dsctl.upstream.workflows import LegacyWorkflowDefinitionSnapshot


MAX_LEGACY_DESCENDANT_WORKFLOWS = 1_000


class LegacyWorkflowReferenceOperations(Protocol):
    """Narrow exact-wire read seam for legacy nested-workflow identities."""

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        """Resolve one exact workflow name inside the selected project."""
        ...

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        """Read the selected project's visible identity inventory once."""
        ...

    def resolve_workflow_by_id(
        self,
        project: ProjectRef,
        workflow_id: int,
    ) -> WorkflowScope:
        """Resolve one exact native ID inside the selected project."""
        ...

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        """Read one exact legacy three-document workflow definition."""
        ...


class LegacyChildWorkflowResolutionError(Exception):
    """One canonical child selector could not be bound safely."""

    def __init__(self, selector: str, cause: BaseException) -> None:
        """Retain the failed canonical selector and its native cause."""
        super().__init__(str(cause))
        self.selector = selector
        self.cause = cause


class LegacyDescendantWorkflowError(Exception):
    """One native descendant could not be read or decoded safely."""

    def __init__(self, workflow_id: int | None, cause: BaseException) -> None:
        """Retain the failed descendant identity and its native cause."""
        super().__init__(str(cause))
        self.workflow_id = workflow_id
        self.cause = cause


class LegacyRecursiveWorkflowError(Exception):
    """A canonical selector resolves directly back to its containing workflow."""

    def __init__(self, selector: str) -> None:
        """Retain the direct recursive selector."""
        super().__init__(selector)
        self.selector = selector


class LegacyNestedWorkflowCycleError(Exception):
    """The exact runtime traversal contains a direct or indirect cycle."""

    def __init__(self, cycle: tuple[int, ...]) -> None:
        """Retain the ordered cycle including its repeated closing ID."""
        super().__init__(" -> ".join(str(workflow_id) for workflow_id in cycle))
        self.cycle = cycle


class LegacyNestedWorkflowLimitError(Exception):
    """The exact runtime traversal exceeded its bounded read closure."""

    def __init__(self, limit: int) -> None:
        """Retain the configured closure safety limit."""
        super().__init__(str(limit))
        self.limit = limit


@dataclass(frozen=True, slots=True)
class LegacyWorkflowAuthoringResolution:
    """Frozen canonical name bindings plus their already-proved scopes."""

    refs: LegacyWorkflowRefIndex
    scopes_by_id: Mapping[int, WorkflowScope]

    @classmethod
    def empty(cls) -> LegacyWorkflowAuthoringResolution:
        """Return one authoring resolution with no remote identities."""
        return cls(
            refs=LegacyWorkflowRefIndex.empty(),
            scopes_by_id=MappingProxyType({}),
        )


class LegacyWorkflowReferenceResolver:
    """Own exact 1.3.9 name/ID binding and runtime-equivalent graph walks."""

    def __init__(
        self,
        operations: LegacyWorkflowReferenceOperations,
        *,
        project: ProjectRef,
        action: str,
    ) -> None:
        """Bind exact read operations and stable mutation context."""
        self._operations = operations
        self._project = project
        self._action = action

    def resolve_authoring(
        self,
        spec: WorkflowSpec,
        *,
        containing_workflow_id: int | None,
    ) -> LegacyWorkflowAuthoringResolution:
        """Bind explicit child names without compiling or walking remote graphs."""
        selectors = _child_workflow_selectors(spec)
        if not selectors:
            return LegacyWorkflowAuthoringResolution.empty()
        normalized_containing_id = _optional_positive_workflow_id(
            containing_workflow_id,
            label="containing workflow",
        )
        scopes_by_id: dict[int, WorkflowScope] = {}
        id_by_name: dict[str, int] = {}
        for selector in selectors:
            if normalized_containing_id is None and selector == spec.workflow.name:
                raise LegacyRecursiveWorkflowError(selector)
            try:
                child_scope = self._operations.resolve_workflow_by_name(
                    self._project,
                    selector,
                )
                child_id = _required_native_id(child_scope, label="child workflow")
            except (ApiResultError, DsctlError, LegacyWorkflowGraphError) as error:
                raise LegacyChildWorkflowResolutionError(selector, error) from error
            if (
                normalized_containing_id is not None
                and child_id == normalized_containing_id
            ):
                raise LegacyRecursiveWorkflowError(selector)
            id_by_name[selector] = child_id
            scopes_by_id[child_id] = child_scope
        return LegacyWorkflowAuthoringResolution(
            refs=LegacyWorkflowRefIndex.from_id_by_name(id_by_name),
            scopes_by_id=MappingProxyType(dict(scopes_by_id)),
        )

    def audit_compilation(
        self,
        compilation: PreparedLegacyWorkflowGraph,
        *,
        containing_workflow_id: int | None,
        resolution: LegacyWorkflowAuthoringResolution,
    ) -> None:
        """Audit every final native reference exactly as the 1.3.9 runtime walks it."""
        try:
            root_ids = prepared_legacy_runtime_nested_workflow_definition_ids(
                compilation
            )
            containing_id = _optional_positive_workflow_id(
                containing_workflow_id,
                label="containing workflow",
            )
        except LegacyWorkflowGraphError as error:
            raise LegacyDescendantWorkflowError(None, error) from error
        if not root_ids:
            return
        auditor = _LegacyNestedWorkflowAuditor(
            self._operations,
            project=self._project,
            action=self._action,
            scopes_by_id=resolution.scopes_by_id,
        )
        auditor.audit_ids(root_ids, containing_id=containing_id)

    def reverse_bind_immediate(
        self,
        graph: DecodedLegacyWorkflowGraph,
        *,
        containing_workflow_id: int | None,
    ) -> LegacyWorkflowRefIndex:
        """Best-effort bind only exact immediate SUB_PROCESS packages for reads."""
        child_ids = legacy_safe_sub_process_definition_ids(graph)
        if not child_ids:
            return LegacyWorkflowRefIndex.empty()
        try:
            containing_id = _optional_positive_workflow_id(
                containing_workflow_id,
                label="containing workflow",
            )
        except LegacyWorkflowGraphError:
            return LegacyWorkflowRefIndex.empty()

        try:
            visible_refs = self._operations.visible_workflow_refs(self._project)
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError):
            return LegacyWorkflowRefIndex.empty()
        requested_ids = set(child_ids)
        if containing_id is not None:
            requested_ids.discard(containing_id)
        if not requested_ids:
            return LegacyWorkflowRefIndex.empty()
        names_by_id = _safe_visible_workflow_names_by_id(
            visible_refs,
            requested_ids=requested_ids,
        )
        id_by_name = _unambiguous_workflow_ids_by_name(names_by_id)
        try:
            return LegacyWorkflowRefIndex.from_id_by_name(id_by_name)
        except LegacyWorkflowGraphError:
            return LegacyWorkflowRefIndex.empty()


class _LegacyNestedWorkflowAuditor:
    """Iteratively validate one same-project legacy descendant closure."""

    def __init__(
        self,
        operations: LegacyWorkflowReferenceOperations,
        *,
        project: ProjectRef,
        action: str,
        scopes_by_id: Mapping[int, WorkflowScope] | None = None,
    ) -> None:
        self._operations = operations
        self._project = project
        self._action = action
        self._scopes_by_id = {} if scopes_by_id is None else dict(scopes_by_id)
        self._graphs_by_id: dict[int, DecodedLegacyWorkflowGraph] = {}
        self._visited: set[int] = set()
        self._loaded_count = 0

    def audit_ids(
        self,
        roots: tuple[int, ...],
        *,
        containing_id: int | None,
    ) -> None:
        """Reject missing descendants, cycles, and an overlarge closure."""
        for root_id in roots:
            if root_id in self._visited:
                continue
            self._audit_root(root_id, containing_id=containing_id)

    def _audit_root(self, root_id: int, *, containing_id: int | None) -> None:
        path = [] if containing_id is None else [containing_id]
        active = {workflow_id: index for index, workflow_id in enumerate(path)}
        stack: list[tuple[bool, int]] = [(True, root_id)]
        while stack:
            entering, workflow_id = stack.pop()
            if not entering:
                if path and path[-1] == workflow_id:
                    path.pop()
                active.pop(workflow_id, None)
                self._visited.add(workflow_id)
                continue
            if workflow_id in active:
                cycle_start = active[workflow_id]
                raise LegacyNestedWorkflowCycleError((*path[cycle_start:], workflow_id))
            if workflow_id in self._visited:
                continue
            scope = self._scope_for_id(workflow_id)
            graph = self._graph(scope)
            active[workflow_id] = len(path)
            path.append(workflow_id)
            stack.append((False, workflow_id))
            try:
                child_ids = legacy_runtime_nested_workflow_definition_ids(graph)
            except LegacyWorkflowGraphError as error:
                raise LegacyDescendantWorkflowError(workflow_id, error) from error
            for child_id in reversed(child_ids):
                if child_id in active:
                    cycle_start = active[child_id]
                    raise LegacyNestedWorkflowCycleError(
                        (*path[cycle_start:], child_id)
                    )
                if child_id not in self._visited:
                    stack.append((True, child_id))

    def _scope_for_id(self, workflow_id: int) -> WorkflowScope:
        cached = self._scopes_by_id.get(workflow_id)
        if cached is not None:
            return cached
        try:
            scope = self._operations.resolve_workflow_by_id(
                self._project,
                workflow_id,
            )
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError) as error:
            raise LegacyDescendantWorkflowError(workflow_id, error) from error
        try:
            resolved_id = _required_native_id(scope, label="descendant workflow")
        except LegacyWorkflowGraphError as error:
            raise LegacyDescendantWorkflowError(workflow_id, error) from error
        if resolved_id != workflow_id:
            message = (
                f"Descendant workflow id {workflow_id} resolved as native id "
                f"{resolved_id}"
            )
            identity_error = LegacyWorkflowGraphError(message)
            raise LegacyDescendantWorkflowError(
                workflow_id,
                identity_error,
            ) from identity_error
        self._scopes_by_id[workflow_id] = scope
        return scope

    def _graph(self, scope: WorkflowScope) -> DecodedLegacyWorkflowGraph:
        workflow_id = _required_native_id(scope, label="descendant workflow")
        cached = self._graphs_by_id.get(workflow_id)
        if cached is not None:
            return cached
        if self._loaded_count >= MAX_LEGACY_DESCENDANT_WORKFLOWS:
            raise LegacyNestedWorkflowLimitError(MAX_LEGACY_DESCENDANT_WORKFLOWS)
        self._loaded_count += 1
        try:
            snapshot = self._operations.legacy_definition(
                scope,
                action=self._action,
            )
            graph = decode_legacy_workflow_graph(
                snapshot.process_definition_json,
                snapshot.locations,
                snapshot.connects,
            )
        except (ApiResultError, DsctlError, LegacyWorkflowGraphError) as error:
            raise LegacyDescendantWorkflowError(workflow_id, error) from error
        self._graphs_by_id[workflow_id] = graph
        return graph


def _safe_visible_workflow_names_by_id(
    visible_refs: tuple[WorkflowRef, ...],
    *,
    requested_ids: set[int],
) -> dict[int, str]:
    """Keep only literal, unambiguous names for requested native IDs."""
    names_by_id: dict[int, str] = {}
    ambiguous_ids: set[int] = set()
    for workflow_ref in visible_refs:
        native = workflow_ref.native
        if not isinstance(native, NativeId) or native.value not in requested_ids:
            continue
        child_id = native.value
        child_name = workflow_ref.name
        if not _is_safe_child_workflow_name(child_name):
            ambiguous_ids.add(child_id)
            names_by_id.pop(child_id, None)
            continue
        previous_name = names_by_id.get(child_id)
        if previous_name is not None and previous_name != child_name:
            ambiguous_ids.add(child_id)
            names_by_id.pop(child_id, None)
            continue
        if child_id not in ambiguous_ids:
            names_by_id[child_id] = child_name
    return names_by_id


def _is_safe_child_workflow_name(value: str | None) -> TypeGuard[str]:
    return bool(
        value
        and value.strip()
        and value == value.strip()
        and not contains_ds_parameter_placeholder(value)
    )


def _unambiguous_workflow_ids_by_name(
    names_by_id: Mapping[int, str],
) -> dict[str, int]:
    id_by_name: dict[str, int] = {}
    ambiguous_names: set[str] = set()
    for child_id, child_name in names_by_id.items():
        previous_id = id_by_name.get(child_name)
        if previous_id is not None and previous_id != child_id:
            id_by_name.pop(child_name, None)
            ambiguous_names.add(child_name)
        elif child_name not in ambiguous_names:
            id_by_name[child_name] = child_id
    return id_by_name


def _child_workflow_selectors(spec: WorkflowSpec) -> tuple[str, ...]:
    selectors: list[str] = []
    seen: set[str] = set()
    for task in spec.tasks:
        if task.type != "SUB_WORKFLOW" or task.task_params is None:
            continue
        selector = task.task_params.get("childWorkflowName")
        if isinstance(selector, str) and selector not in seen:
            seen.add(selector)
            selectors.append(selector)
    return tuple(selectors)


def _optional_positive_workflow_id(value: int | None, *, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        message = f"Legacy {label} must use a positive native ID"
        raise LegacyWorkflowGraphError(message)
    return value


def _required_native_id(scope: WorkflowScope, *, label: str) -> int:
    native = scope.workflow.native
    if not isinstance(native, NativeId) or native.value <= 0:
        message = f"Legacy {label} resolution returned a non-positive ID identity"
        raise LegacyWorkflowGraphError(message)
    return native.value


__all__ = [
    "MAX_LEGACY_DESCENDANT_WORKFLOWS",
    "LegacyChildWorkflowResolutionError",
    "LegacyDescendantWorkflowError",
    "LegacyNestedWorkflowCycleError",
    "LegacyNestedWorkflowLimitError",
    "LegacyRecursiveWorkflowError",
    "LegacyWorkflowAuthoringResolution",
    "LegacyWorkflowReferenceOperations",
    "LegacyWorkflowReferenceResolver",
]
