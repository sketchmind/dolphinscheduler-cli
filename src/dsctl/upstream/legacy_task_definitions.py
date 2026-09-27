from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, cast

from dsctl.cli_surface import TASK_RESOURCE
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.models.workflow_patch import (
    WorkflowPatchSpec,
    WorkflowPatchTaskSetSpec,
    validate_workflow_patch_document,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyTask,
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
)
from dsctl.upstream.task_definitions import MutationOutcome

if TYPE_CHECKING:
    from collections.abc import Callable

    from dsctl.models.workflow_spec import WorkflowSpec, WorkflowTaskSpec
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.definition_models import WorkflowScope
    from dsctl.upstream.legacy_workflow_graph import PreparedLegacyWorkflowGraph
    from dsctl.upstream.wire import WireRequest
    from dsctl.upstream.workflows import (
        LegacyWorkflowDefinitionSnapshot,
        PreparedLegacyWorkflowUpdate,
    )


CatalogT = TypeVar("CatalogT")
CatalogT_contra = TypeVar("CatalogT_contra", contravariant=True)


class LegacyTaskMutationPlan(Protocol):
    """Structural result required from the injected legacy graph compiler."""

    @property
    def merged_spec(self) -> WorkflowSpec:
        """Return the merged stable workflow spec."""

    @property
    def compilation(self) -> PreparedLegacyWorkflowGraph:
        """Return the frozen native graph compilation."""

    @property
    def has_changes(self) -> bool:
        """Return whether the merged spec differs from live state."""


class LegacyTaskUpdateCompiler(Protocol[CatalogT_contra]):
    """Pure legacy workflow mutation compiler injected above upstream code."""

    def __call__(
        self,
        graph: DecodedLegacyWorkflowGraph,
        *,
        workflow_name: str,
        project_name: str,
        description: str | None,
        release_state: str | None,
        mutation: WorkflowPatchSpec | WorkflowSpec,
        catalog: CatalogT_contra,
        task_id_factory: Callable[[str], str],
    ) -> LegacyTaskMutationPlan:
        """Compile one task patch into an exact whole-workflow graph."""


class LegacyWorkflowOperations(Protocol):
    """Narrow workflow seam needed to adapt embedded legacy tasks."""

    @property
    def ds_version(self) -> str:
        """Return the exact bound profile version."""

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        """Resolve one id-native workflow."""

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        """Read the three native legacy graph strings."""

    def prepare_legacy_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> PreparedLegacyWorkflowUpdate:
        """Capture one exact legacy whole-workflow update."""

    def apply_update(self, prepared: PreparedLegacyWorkflowUpdate) -> None:
        """Execute the captured update exactly once."""


@dataclass(frozen=True, slots=True)
class LegacyWorkflowSelector:
    """Stable project/workflow selector for one embedded task collection."""

    project: str
    workflow: str


@dataclass(frozen=True, slots=True)
class LegacyTaskSelector:
    """Stable selector whose task component is an exact task name."""

    project: str
    workflow: str
    task: str


@dataclass(frozen=True, slots=True)
class LegacyTaskRef:
    """One DS 1.3 string-native task identity without invented code/version."""

    id: str
    name: str

    def to_data(self) -> JsonObject:
        """Render only the identity fields native to DS 1.3."""
        return {"id": self.id, "name": self.name}


@dataclass(frozen=True, slots=True)
class LegacyTaskView:
    """Caller-facing projection of one embedded TaskNode."""

    _data: JsonObject = field(repr=False)

    def to_data(self) -> JsonObject:
        """Return an isolated output value."""
        return _thaw_object(self._data)


@dataclass(frozen=True, slots=True)
class LegacyTaskScope:
    """Resolved containing workflow plus one exact-name task."""

    workflow: WorkflowScope
    task: LegacyTaskRef


@dataclass(frozen=True, slots=True)
class LegacyTaskRead:
    """One legacy task view and its resolved identity-native scope."""

    scope: LegacyTaskScope
    view: LegacyTaskView


@dataclass(frozen=True, slots=True)
class LegacyTaskSummary:
    """Compact id-native task list row."""

    ref: LegacyTaskRef

    def to_data(self) -> JsonObject:
        """Return the native task identity summary."""
        return self.ref.to_data()


@dataclass(frozen=True, slots=True)
class LegacyTaskListing:
    """One resolved workflow and all of its embedded tasks."""

    scope: WorkflowScope
    tasks: tuple[LegacyTaskSummary, ...]


@dataclass(frozen=True, slots=True)
class PreparedLegacyTaskUpdate:
    """One guarded whole-workflow update prepared from stable task intent."""

    profile_version: str
    current: LegacyTaskRead
    requested_fields: tuple[str, ...]
    updated_fields: tuple[str, ...]
    no_change: bool
    request: WireRequest
    initial_projection: JsonObject
    expected_projection: JsonObject
    _project_selector: str = field(repr=False, compare=False)
    _workflow_selector: str = field(repr=False, compare=False)
    _initial_snapshot: tuple[str | None, ...] = field(repr=False, compare=False)
    _expected_projection: tuple[tuple[str, JsonValue], ...] = field(
        repr=False,
        compare=False,
    )
    _prepared_update: PreparedLegacyWorkflowUpdate = field(
        repr=False,
        compare=False,
    )
    _operations: LegacyWorkflowOperations = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class LegacyTaskDefinitions(Generic[CatalogT]):
    """Adapt DS 1.3 embedded TaskNodes behind stable task behavior."""

    profile_version: str
    operations: LegacyWorkflowOperations
    catalog: CatalogT
    compile_update: LegacyTaskUpdateCompiler[CatalogT]

    def __post_init__(self) -> None:
        """Reject accidental cross-profile composition."""
        if self.profile_version != "1.3.9":
            message = "Legacy task definitions only support DolphinScheduler 1.3.9"
            raise ValueError(message)
        if self.operations.ds_version != self.profile_version:
            message = "Legacy task operations do not match the selected profile"
            raise ValueError(message)

    def list(self, selector: LegacyWorkflowSelector) -> LegacyTaskListing:
        """List string-native tasks inside one selected workflow."""
        scope, _snapshot, graph = self._load(
            project=selector.project,
            workflow=selector.workflow,
            action="task.list",
        )
        return LegacyTaskListing(
            scope=scope,
            tasks=tuple(LegacyTaskSummary(_task_ref(task)) for task in graph.tasks),
        )

    def get(self, selector: LegacyTaskSelector) -> LegacyTaskRead:
        """Get one embedded task by exact name."""
        scope, _snapshot, graph = self._load(
            project=selector.project,
            workflow=selector.workflow,
            action="task.get",
        )
        task = _task_by_exact_name(graph, selector.task)
        return _task_read(scope, task)

    def prepare_update(
        self,
        selector: LegacyTaskSelector,
        *,
        patch: WorkflowPatchTaskSetSpec,
        requested_fields: tuple[str, ...],
    ) -> PreparedLegacyTaskUpdate:
        """Compile a task patch into one exact prepared workflow update."""
        _require_supported_update_fields(requested_fields)
        scope, snapshot, graph = self._load(
            project=selector.project,
            workflow=selector.workflow,
            action="task.update",
        )
        _require_offline(scope, task=selector.task)
        current_task = _task_by_exact_name(graph, selector.task)
        mutation = validate_workflow_patch_document(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": current_task.name},
                                "set": patch.model_dump(
                                    mode="python",
                                    exclude_unset=True,
                                ),
                            }
                        ]
                    }
                }
            }
        ).patch
        workflow_name = snapshot.name or scope.workflow.name
        project_name = scope.project.name
        if workflow_name is None or project_name is None:
            message = "Legacy task update scope was missing workflow or project name"
            raise ValueError(message)
        plan = self.compile_update(
            graph,
            workflow_name=workflow_name,
            project_name=project_name,
            description=snapshot.description,
            release_state=snapshot.release_state,
            mutation=mutation,
            catalog=self.catalog,
            task_id_factory=_unexpected_task_id,
        )
        desired_task = next(
            task for task in plan.merged_spec.tasks if task.name == current_task.name
        )
        current_spec = graph.to_workflow_spec(
            name=workflow_name,
            project=project_name,
            description=snapshot.description,
            release_state=snapshot.release_state or "OFFLINE",
        )
        current_spec_task = next(
            task for task in current_spec.tasks if task.name == current_task.name
        )
        updated_fields = tuple(
            field_name
            for field_name in requested_fields
            if _task_field(current_spec_task, field_name)
            != _task_field(desired_task, field_name)
        )
        initial_projection: JsonObject = {
            field_name: _task_field(current_spec_task, field_name)
            for field_name in requested_fields
        }
        expected_projection: JsonObject = {
            field_name: _task_field(desired_task, field_name)
            for field_name in requested_fields
        }
        payload = plan.compilation.preview()
        prepared_update = self.operations.prepare_legacy_update(
            scope,
            name=plan.merged_spec.workflow.name,
            description=plan.merged_spec.workflow.description,
            process_definition_json=payload["processDefinitionJson"],
            locations=payload["locations"],
            connects=payload["connects"],
        )
        return PreparedLegacyTaskUpdate(
            profile_version=self.profile_version,
            current=_task_read(scope, current_task),
            requested_fields=requested_fields,
            updated_fields=updated_fields,
            no_change=not updated_fields,
            request=prepared_update.request,
            initial_projection=initial_projection,
            expected_projection=expected_projection,
            _project_selector=selector.project,
            _workflow_selector=selector.workflow,
            _initial_snapshot=_snapshot_fingerprint(snapshot),
            _expected_projection=tuple(expected_projection.items()),
            _prepared_update=prepared_update,
            _operations=self.operations,
        )

    def apply(
        self,
        prepared: PreparedLegacyTaskUpdate,
    ) -> MutationOutcome[LegacyTaskRead]:
        """Reject stale/online state, apply once, then read back by task name."""
        if (
            prepared.profile_version != self.profile_version
            or prepared._operations is not self.operations
        ):
            message = "Prepared legacy task update belongs to another bound runtime"
            raise ValueError(message)
        if prepared.no_change:
            return MutationOutcome(value=prepared.current, mutation_applied=False)
        scope, snapshot, _graph = self._load(
            project=prepared._project_selector,
            workflow=prepared._workflow_selector,
            action="task.update",
        )
        _require_offline(scope, task=prepared.current.scope.task.name)
        if _snapshot_fingerprint(snapshot) != prepared._initial_snapshot:
            message = "Prepared legacy task update is stale and was not sent"
            raise ConflictError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "task": prepared.current.scope.task.name,
                    "reason": "legacy_whole_workflow_changed",
                },
                suggestion="Retry the task update from a fresh read.",
            )
        self.operations.apply_update(prepared._prepared_update)
        refreshed_scope, refreshed_snapshot, refreshed_graph = self._load(
            project=prepared._project_selector,
            workflow=prepared._workflow_selector,
            action="task.update",
        )
        task_name = prepared.current.scope.task.name
        try:
            refreshed_task = _task_by_exact_name(refreshed_graph, task_name)
        except NotFoundError as exc:
            message = "Legacy task update was accepted but readback lost the task"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "task": task_name,
                    "mutation_applied": True,
                    "phase": "mutation_readback",
                },
                suggestion="Read the containing workflow before retrying the update.",
            ) from exc
        refreshed_workflow_name = (
            refreshed_snapshot.name or refreshed_scope.workflow.name
        )
        refreshed_project_name = refreshed_scope.project.name
        if refreshed_workflow_name is None or refreshed_project_name is None:
            message = "Legacy task update readback scope was missing names"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "task": task_name,
                    "mutation_applied": True,
                    "phase": "mutation_readback",
                },
            )
        refreshed_spec = refreshed_graph.to_workflow_spec(
            name=refreshed_workflow_name,
            project=refreshed_project_name,
            description=refreshed_snapshot.description,
            release_state=refreshed_snapshot.release_state or "OFFLINE",
        )
        refreshed_spec_task = next(
            task for task in refreshed_spec.tasks if task.name == task_name
        )
        actual_projection = tuple(
            (field_name, _task_field(refreshed_spec_task, field_name))
            for field_name, _expected in prepared._expected_projection
        )
        if actual_projection != prepared._expected_projection:
            message = "Legacy task update readback did not match the requested state"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "task": task_name,
                    "mutation_applied": True,
                    "phase": "mutation_readback",
                    "expected": dict(prepared._expected_projection),
                    "actual": dict(actual_projection),
                },
                suggestion="Inspect the task and containing workflow before retrying.",
            )
        refreshed = _task_read(refreshed_scope, refreshed_task)
        return MutationOutcome(value=refreshed, mutation_applied=True)

    def _load(
        self,
        *,
        project: str,
        workflow: str,
        action: str,
    ) -> tuple[
        WorkflowScope,
        LegacyWorkflowDefinitionSnapshot,
        DecodedLegacyWorkflowGraph,
    ]:
        scope = self.operations.resolve_workflow(project, workflow)
        snapshot = self.operations.legacy_definition(scope, action=action)
        graph = decode_legacy_workflow_graph(
            snapshot.process_definition_json,
            snapshot.locations,
            snapshot.connects,
        )
        return scope, snapshot, graph


def _task_by_exact_name(
    graph: DecodedLegacyWorkflowGraph,
    selector: str,
) -> DecodedLegacyTask:
    normalized = selector.strip()
    if not normalized:
        message = "Task name is required"
        raise UserInputError(
            message,
            details={"resource": TASK_RESOURCE},
            suggestion="Pass the exact task name returned by `dsctl task list`.",
        )
    for task in graph.tasks:
        if task.name == normalized:
            return task
    message = f"Task {normalized!r} was not found in the selected workflow."
    raise NotFoundError(
        message,
        details={"resource": TASK_RESOURCE, "selector": normalized},
        suggestion="Run `dsctl task list` in the selected workflow.",
    )


def _task_ref(task: DecodedLegacyTask) -> LegacyTaskRef:
    return LegacyTaskRef(id=task.id, name=task.name)


def _task_read(scope: WorkflowScope, task: DecodedLegacyTask) -> LegacyTaskRead:
    return LegacyTaskRead(
        scope=LegacyTaskScope(workflow=scope, task=_task_ref(task)),
        view=LegacyTaskView(_task_data(task)),
    )


def _task_data(task: DecodedLegacyTask) -> JsonObject:
    native = task.native_fields
    timeout, timeout_flag, timeout_strategy = _timeout_fields(native.get("timeout"))
    retry_times = _non_negative_int(native.get("maxRetryTimes", 0), default=0)
    retry_interval = _non_negative_int(native.get("retryInterval", 0), default=0)
    run_flag = native.get("runFlag", "NORMAL")
    flag = "NO" if run_flag == "FORBIDDEN" else "YES"
    description = native.get("description", native.get("desc"))
    if not isinstance(description, str):
        description = None
    task_params = _embedded_object(native.get("params"))
    return {
        "id": task.id,
        "name": task.name,
        "description": description,
        "taskType": task.type,
        "taskParams": task_params,
        "workerGroup": _text_or_none(native.get("workerGroup")),
        "failRetryTimes": retry_times,
        "failRetryInterval": retry_interval,
        "timeout": timeout,
        "timeoutFlag": timeout_flag,
        "timeoutNotifyStrategy": timeout_strategy,
        "taskPriority": _text_or_none(native.get("taskInstancePriority")),
        "flag": flag,
        "dependsOn": list(task.depends_on),
    }


_SUPPORTED_UPDATE_FIELDS = frozenset(
    {
        "command",
        "depends_on",
        "description",
        "flag",
        "priority",
        "retry.interval",
        "retry.times",
        "timeout",
        "timeout_notify_strategy",
        "worker_group",
    }
)


def _require_supported_update_fields(requested_fields: tuple[str, ...]) -> None:
    unsupported = sorted(set(requested_fields).difference(_SUPPORTED_UPDATE_FIELDS))
    if not unsupported:
        return
    message = "DolphinScheduler 1.3.9 cannot represent the requested task update fields"
    raise UnsupportedFeatureError(
        message,
        details={
            "resource": TASK_RESOURCE,
            "ds_version": "1.3.9",
            "unsupported_fields": unsupported,
            "reason": "upstream_capability_absent",
        },
        suggestion="Omit these fields or use a supported workflow edit.",
    )


def _require_offline(scope: WorkflowScope, *, task: str) -> None:
    if scope.view.release_state != "ONLINE":
        return
    message = "Task update requires the containing workflow to be offline"
    raise InvalidStateError(
        message,
        details={
            "resource": TASK_RESOURCE,
            "task": task,
            "workflow": scope.workflow.native.value,
            "release_state": "ONLINE",
        },
        suggestion="Bring the workflow offline before retrying `task update`.",
    )


def _snapshot_fingerprint(
    snapshot: LegacyWorkflowDefinitionSnapshot,
) -> tuple[str | None, ...]:
    return (
        snapshot.name,
        snapshot.description,
        snapshot.release_state,
        snapshot.process_definition_json,
        snapshot.locations,
        snapshot.connects,
    )


def _unexpected_task_id(task_name: str) -> str:
    message = f"Task update unexpectedly allocated a native id for {task_name!r}"
    raise RuntimeError(message)


def _task_field(task: WorkflowTaskSpec, field_name: str) -> JsonValue:
    if field_name == "command":
        if task.command is not None:
            return task.command
        if task.task_params is not None:
            return task.task_params.get("rawScript")
        return None
    if field_name == "retry.times":
        return task.retry.times
    if field_name == "retry.interval":
        return task.retry.interval
    value = getattr(task, field_name)
    return cast("JsonValue", getattr(value, "value", value))


def _timeout_fields(value: JsonValue | None) -> tuple[int, str, str | None]:
    timeout = _embedded_object(value)
    if timeout.get("enable") is not True:
        return 0, "CLOSE", None
    interval = _non_negative_int(timeout.get("interval"), default=0)
    strategy = _text_or_none(timeout.get("strategy")) or "WARN"
    if strategy == "WARN,FAILED":
        strategy = "WARNFAILED"
    return interval, "OPEN", strategy


def _embedded_object(value: JsonValue | None) -> JsonObject:
    if isinstance(value, Mapping):
        return _thaw_object(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(decoded, Mapping):
            return _thaw_object(decoded)
    return {}


def _non_negative_int(value: JsonValue | None, *, default: int) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return default


def _text_or_none(value: JsonValue | None) -> str | None:
    return value if isinstance(value, str) else None


def _thaw_object(value: Mapping[str, JsonValue]) -> JsonObject:
    return {key: _thaw(item) for key, item in value.items()}


def _thaw(value: JsonValue) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw(item) for item in value]
    return value


__all__ = [
    "LegacyTaskDefinitions",
    "LegacyTaskListing",
    "LegacyTaskRead",
    "LegacyTaskRef",
    "LegacyTaskSelector",
    "LegacyWorkflowSelector",
]
