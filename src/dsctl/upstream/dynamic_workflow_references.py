from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, TypeGuard

from dsctl.errors import ApiResultError, DsctlError
from dsctl.models.task_spec import contains_ds_parameter_placeholder
from dsctl.upstream.definition_models import NativeCode, ProjectRef, WorkflowScope
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_parameter_projection import TaskWorkflowRefIndex

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.definition_models import WorkflowRef
    from dsctl.upstream.protocol import WorkflowDagRecord


MAX_DYNAMIC_DESCENDANT_WORKFLOWS = 1_000

_NESTED_TASK_CODE_FIELDS = {
    "DYNAMIC": "processDefinitionCode",
    "SUB_PROCESS": "processDefinitionCode",
}


class DynamicWorkflowReferenceOperations(Protocol):
    """Narrow code-native read seam for exact DYNAMIC closure audits."""

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        """Read the selected project's visible workflow inventory once."""
        ...

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        """Resolve one exact workflow code inside an already-proved project."""
        ...

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        """Read one exact code-native workflow DAG."""
        ...


class DynamicWorkflowCycleError(Exception):
    """The exact runtime traversal contains a direct or indirect cycle."""

    def __init__(self, cycle: tuple[int, ...]) -> None:
        """Retain the ordered cycle including its repeated closing code."""
        super().__init__(" -> ".join(str(workflow_code) for workflow_code in cycle))
        self.cycle = cycle


class DynamicWorkflowLimitError(Exception):
    """The exact runtime traversal exceeded its bounded read closure."""

    def __init__(self, limit: int) -> None:
        """Retain the configured closure safety limit."""
        super().__init__(str(limit))
        self.limit = limit


class DynamicWorkflowReadError(Exception):
    """One native descendant could not be read safely."""

    def __init__(self, workflow_code: int, cause: BaseException) -> None:
        """Retain the failed descendant code and its native cause."""
        super().__init__(str(cause))
        self.workflow_code = workflow_code
        self.cause = cause


class DynamicWorkflowShapeError(Exception):
    """One descendant carries an unsafe nested-workflow reference shape."""

    def __init__(self, workflow_code: int, reason: str) -> None:
        """Retain the containing descendant code and exact rejected reason."""
        super().__init__(reason)
        self.workflow_code = workflow_code
        self.reason = reason


class DynamicChildWorkflowResolutionError(Exception):
    """One canonical child selector could not be bound safely."""

    def __init__(self, selector: str, cause: BaseException) -> None:
        """Retain the failed literal selector and its native cause."""
        super().__init__(str(cause))
        self.selector = selector
        self.cause = cause


class DynamicWorkflowReferenceResolver:
    """Bind exact 3.2.2 child-workflow names and codes."""

    def __init__(
        self,
        operations: DynamicWorkflowReferenceOperations,
        *,
        project: ProjectRef,
    ) -> None:
        """Bind the code-native read seam and selected project."""
        self._operations = operations
        self._project = project

    def resolve_authoring(self, names: Sequence[str]) -> TaskWorkflowRefIndex:
        """Resolve literal canonical names without interpreting numeric text."""
        selectors = tuple(dict.fromkeys(names))
        if not selectors:
            return TaskWorkflowRefIndex.from_code_by_name({})
        try:
            visible_refs = self._operations.visible_workflow_refs(self._project)
        except (ApiResultError, DsctlError, ValueError, TypeError) as error:
            raise DynamicChildWorkflowResolutionError(selectors[0], error) from error
        code_by_name: dict[str, int] = {}
        resolved = TaskWorkflowRefIndex.from_code_by_name({})
        for name in selectors:
            try:
                code_by_name[name] = _required_visible_child_code(
                    visible_refs,
                    selector=name,
                )
                resolved = TaskWorkflowRefIndex.from_code_by_name(code_by_name)
            except (ApiResultError, DsctlError, ValueError, TypeError) as error:
                raise DynamicChildWorkflowResolutionError(name, error) from error
        return resolved

    def reverse_bind(self, codes: Sequence[int]) -> TaskWorkflowRefIndex:
        """Best-effort bind requested codes from one visible identity inventory."""
        requested_codes = {
            code
            for code in codes
            if isinstance(code, int) and not isinstance(code, bool) and code > 0
        }
        if not requested_codes:
            return TaskWorkflowRefIndex.from_code_by_name({})
        try:
            visible_refs = self._operations.visible_workflow_refs(self._project)
        except (ApiResultError, DsctlError, ValueError, TypeError):
            return TaskWorkflowRefIndex.from_code_by_name({})
        code_by_name = _unambiguous_visible_code_by_name(
            visible_refs,
            requested_codes=requested_codes,
        )
        try:
            return TaskWorkflowRefIndex.from_code_by_name(code_by_name)
        except ValueError:
            return TaskWorkflowRefIndex.from_code_by_name({})


class DynamicWorkflowReferenceAuditor:
    """Iteratively inspect one exact 3.2.2 DYNAMIC/SUB_PROCESS closure."""

    def __init__(
        self,
        operations: DynamicWorkflowReferenceOperations,
        *,
        project: ProjectRef,
        action: str,
    ) -> None:
        """Bind the exact read seam and mutation context for one audit."""
        self._operations = operations
        self._project = project
        self._action = action
        self._scopes: dict[int, WorkflowScope] = {}
        self._children: dict[int, tuple[int, ...]] = {}
        self._visited: set[int] = set()

    def audit(
        self,
        roots: Sequence[int],
        *,
        containing_workflow_code: int | None,
    ) -> None:
        """Reject missing descendants, cycles, and an overlarge closure."""
        for root in roots:
            if root in self._visited:
                continue
            self._audit_root(root, containing_workflow_code=containing_workflow_code)

    def _audit_root(
        self,
        root: int,
        *,
        containing_workflow_code: int | None,
    ) -> None:
        path = [] if containing_workflow_code is None else [containing_workflow_code]
        active = {code: index for index, code in enumerate(path)}
        stack: list[tuple[bool, int]] = [(True, root)]
        while stack:
            entering, workflow_code = stack.pop()
            if not entering:
                if path and path[-1] == workflow_code:
                    path.pop()
                active.pop(workflow_code, None)
                self._visited.add(workflow_code)
                continue
            if workflow_code in active:
                cycle_start = active[workflow_code]
                raise DynamicWorkflowCycleError((*path[cycle_start:], workflow_code))
            if workflow_code in self._visited:
                continue
            children = self._child_codes(workflow_code)
            active[workflow_code] = len(path)
            path.append(workflow_code)
            stack.append((False, workflow_code))
            for child_code in reversed(children):
                if child_code in active:
                    cycle_start = active[child_code]
                    raise DynamicWorkflowCycleError((*path[cycle_start:], child_code))
                if child_code not in self._visited:
                    stack.append((True, child_code))

    def _child_codes(self, workflow_code: int) -> tuple[int, ...]:
        cached = self._children.get(workflow_code)
        if cached is not None:
            return cached
        if len(self._children) >= MAX_DYNAMIC_DESCENDANT_WORKFLOWS:
            raise DynamicWorkflowLimitError(MAX_DYNAMIC_DESCENDANT_WORKFLOWS)
        scope = self._scope(workflow_code)
        try:
            dag = self._operations.dag(scope, action=self._action)
            children = _nested_workflow_codes(
                dag,
                containing_workflow_code=workflow_code,
            )
        except DynamicWorkflowShapeError:
            raise
        except (ApiResultError, DsctlError, ValueError, TypeError) as error:
            raise DynamicWorkflowReadError(workflow_code, error) from error
        self._children[workflow_code] = children
        return children

    def _scope(self, workflow_code: int) -> WorkflowScope:
        cached = self._scopes.get(workflow_code)
        if cached is not None:
            return cached
        try:
            scope = self._operations.resolve_workflow_by_code(
                self._project,
                workflow_code,
            )
        except (ApiResultError, DsctlError, ValueError, TypeError) as error:
            raise DynamicWorkflowReadError(workflow_code, error) from error
        native = scope.workflow.native
        if not isinstance(native, NativeCode) or native.value != workflow_code:
            reason = "resolved child workflow code changed during preflight"
            raise DynamicWorkflowShapeError(workflow_code, reason)
        self._scopes[workflow_code] = scope
        return scope


def dynamic_workflow_codes_from_dag(dag: WorkflowDagRecord) -> tuple[int, ...]:
    """Collect positive exact-3.2.2 DYNAMIC child codes for read prefiltering."""
    codes: list[int] = []
    seen: set[int] = set()
    for task in dag.taskDefinitionList or ():
        if enum_value(task.taskType) != "DYNAMIC":
            continue
        params = _optional_json_object(task.taskParams)
        if params is None:
            continue
        code = params.get("processDefinitionCode")
        if (
            not isinstance(code, int)
            or isinstance(code, bool)
            or code <= 0
            or code in seen
        ):
            continue
        seen.add(code)
        codes.append(code)
    return tuple(codes)


def _nested_workflow_codes(
    dag: WorkflowDagRecord,
    *,
    containing_workflow_code: int,
) -> tuple[int, ...]:
    codes: list[int] = []
    seen: set[int] = set()
    for task in dag.taskDefinitionList or ():
        task_type = enum_value(task.taskType)
        if task_type not in _NESTED_TASK_CODE_FIELDS:
            continue
        raw_params = task.taskParams
        try:
            decoded_params = (
                json.loads(raw_params) if isinstance(raw_params, str) else raw_params
            )
            params = _require_json_object(
                decoded_params,
                label=f"{task_type} taskParams",
            )
        except (json.JSONDecodeError, TypeError) as error:
            reason = f"{task_type}.taskParams is not a JSON object"
            raise DynamicWorkflowShapeError(
                containing_workflow_code,
                reason,
            ) from error
        field_name = _NESTED_TASK_CODE_FIELDS[task_type]
        code = params.get(field_name)
        if not isinstance(code, int) or isinstance(code, bool) or code <= 0:
            reason = f"{task_type}.{field_name} is not a positive workflow code"
            raise DynamicWorkflowShapeError(
                containing_workflow_code,
                reason,
            )
        if code not in seen:
            seen.add(code)
            codes.append(code)
    return tuple(codes)


def _require_json_object(value: JsonValue, *, label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        message = f"{label} must be a JSON object"
        raise TypeError(message)
    if not all(isinstance(key, str) for key in value):
        message = f"{label} must use string keys"
        raise TypeError(message)
    return dict(value)


def _optional_json_object(value: JsonValue) -> JsonObject | None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        return None
    return dict(value)


def _required_visible_child_code(
    visible_refs: Sequence[WorkflowRef],
    *,
    selector: str,
) -> int:
    matches = [workflow for workflow in visible_refs if workflow.name == selector]
    if len(matches) != 1:
        message = (
            f"DYNAMIC child workflow name {selector!r} must resolve to exactly one "
            "visible workflow"
        )
        raise ValueError(message)
    native = matches[0].native
    if not isinstance(native, NativeCode) or native.value <= 0:
        message = (
            f"DYNAMIC child workflow name {selector!r} has no positive native code"
        )
        raise ValueError(message)
    return native.value


def _unambiguous_visible_code_by_name(
    visible_refs: Sequence[WorkflowRef],
    *,
    requested_codes: set[int],
) -> dict[str, int]:
    names_by_code: dict[int, str] = {}
    ambiguous_codes: set[int] = set()
    for workflow in visible_refs:
        native = workflow.native
        if not isinstance(native, NativeCode) or native.value not in requested_codes:
            continue
        code = native.value
        name = workflow.name
        if not _is_safe_child_workflow_name(name):
            names_by_code.pop(code, None)
            ambiguous_codes.add(code)
            continue
        previous_name = names_by_code.get(code)
        if previous_name is not None and previous_name != name:
            names_by_code.pop(code, None)
            ambiguous_codes.add(code)
        elif code not in ambiguous_codes:
            names_by_code[code] = name

    code_by_name: dict[str, int] = {}
    ambiguous_names: set[str] = set()
    for code, name in names_by_code.items():
        previous_code = code_by_name.get(name)
        if previous_code is not None and previous_code != code:
            code_by_name.pop(name, None)
            ambiguous_names.add(name)
        elif name not in ambiguous_names:
            code_by_name[name] = code
    return code_by_name


def _is_safe_child_workflow_name(value: str | None) -> TypeGuard[str]:
    return bool(
        value
        and value.strip()
        and value == value.strip()
        and not contains_ds_parameter_placeholder(value)
    )


__all__ = [
    "MAX_DYNAMIC_DESCENDANT_WORKFLOWS",
    "DynamicChildWorkflowResolutionError",
    "DynamicWorkflowCycleError",
    "DynamicWorkflowLimitError",
    "DynamicWorkflowReadError",
    "DynamicWorkflowReferenceAuditor",
    "DynamicWorkflowReferenceOperations",
    "DynamicWorkflowReferenceResolver",
    "DynamicWorkflowShapeError",
    "dynamic_workflow_codes_from_dag",
]
