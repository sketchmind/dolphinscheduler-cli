from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Protocol, TypeGuard, TypeVar, cast

from dsctl.cli_surface import TASK_RESOURCE
from dsctl.errors import (
    ApiTransportError,
    ConflictError,
    DsctlError,
    NotFoundError,
    PermissionDeniedError,
    UnsupportedFeatureError,
)
from dsctl.output import require_json_object
from dsctl.upstream.definition_models import NativeCode
from dsctl.upstream.resolver import ResolvedProject, ResolvedTask, ResolvedWorkflow
from dsctl.upstream.resolver import task as resolve_task
from dsctl.upstream.serialization import (
    TaskData,
    TaskListItem,
    serialize_task,
    serialize_task_ref,
)
from dsctl.upstream.wire import (
    WireContractError,
    WireResponseDecodeError,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.models.workflow_patch import WorkflowPatchTaskSetSpec
    from dsctl.support.yaml_io import JsonObject
    from dsctl.upstream.definition_models import (
        WorkflowScope as DefinitionWorkflowScope,
    )
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        TaskOperations,
        TaskPayloadRecord,
        WorkflowDagRecord,
        WorkflowTaskRelationRecord,
    )
    from dsctl.upstream.task_definition_wire import (
        TaskTopLevelFieldPolicy,
        TaskUpdateWirePolicy,
    )
    from dsctl.upstream.task_update import TaskUpdateCompilation
    from dsctl.upstream.wire import (
        PreparedWireCallToken,
        WireExecution,
        WireRequest,
    )


ValueT = TypeVar("ValueT")
_BINDING_DIAGNOSTIC_LIMIT = 8


@dataclass(frozen=True)
class TaskSelector:
    """Canonical project/workflow/task selector accepted by the deep module."""

    project: str
    workflow: str
    task: str


@dataclass(frozen=True)
class WorkflowSelector:
    """Canonical project/workflow selector used for task discovery."""

    project: str
    workflow: str


@dataclass(frozen=True)
class WorkflowScope:
    """Resolved project and workflow identities for one task collection."""

    project: ResolvedProject
    workflow: ResolvedWorkflow


@dataclass(frozen=True)
class TaskScope:
    """Resolved identities for one project-scoped task definition."""

    project: ResolvedProject
    workflow: ResolvedWorkflow
    task: ResolvedTask


@dataclass(frozen=True)
class TaskSummary:
    """Stable task-list projection independent of generated DS models."""

    _data: TaskListItem = field(repr=False)

    @property
    def code(self) -> int:
        """Return the stable task code."""
        return self._data["code"]

    @property
    def name(self) -> str | None:
        """Return the stable task name when present."""
        return self._data["name"]

    def to_data(self) -> TaskListItem:
        """Return an isolated value suitable for the output boundary."""
        return deepcopy(self._data)


@dataclass(frozen=True)
class TaskListing:
    """Resolved workflow scope plus its task-definition summaries."""

    scope: WorkflowScope
    tasks: tuple[TaskSummary, ...]


@dataclass(frozen=True)
class TaskView:
    """Stable CLI projection that does not expose a generated DS model."""

    _data: TaskData = field(repr=False)

    @property
    def code(self) -> int:
        """Return the stable task code."""
        return self._data["code"]

    @property
    def name(self) -> str | None:
        """Return the stable task name when present."""
        return self._data["name"]

    def to_data(self) -> TaskData:
        """Return an isolated copy suitable for the output boundary."""
        return deepcopy(self._data)


@dataclass(frozen=True)
class TaskRead:
    """One canonical task view plus its fully resolved scope."""

    scope: TaskScope
    view: TaskView


@dataclass(frozen=True)
class NativeTaskSnapshot:
    """Typed projection plus untouched logical JSON held below the boundary."""

    record: TaskPayloadRecord
    raw: JsonObject


@dataclass(frozen=True)
class TaskUpdateIntent:
    """One canonical task patch and the fields explicitly requested by the user."""

    selector: TaskSelector
    patch: WorkflowPatchTaskSetSpec
    requested_fields: tuple[str, ...]


@dataclass(frozen=True)
class PreparedTaskUpdate:
    """Profile-bound, non-mutating task update plan used by preview and apply."""

    profile_version: str
    recipe_fingerprint: str
    current: TaskRead
    initial_task_version: int
    initial_upstream_codes: tuple[int, ...]
    expected_upstream_codes: tuple[int, ...]
    initial_projection: JsonObject
    expected_projection: JsonObject
    requested_fields: tuple[str, ...]
    updated_fields: tuple[str, ...]
    no_change: bool
    request: WireRequest
    _update_spec: WorkflowPatchTaskSetSpec = field(repr=False, compare=False)
    _wire_call: PreparedWireCallToken | None = field(repr=False, compare=False)
    _whole_workflow_call: PreparedWholeWorkflowTaskMutation | None = field(
        repr=False,
        compare=False,
    )


@dataclass(frozen=True)
class MutationOutcome(Generic[ValueT]):
    """Mutation truth plus the canonical readback value."""

    value: ValueT
    mutation_applied: bool


@dataclass
class _WorkflowBindingProof:
    """Exhaustive evidence with bounded public diagnostics."""

    workflow_count: int = 0
    workflow_code_sample: list[int] = field(default_factory=list)
    membership_codes: set[int] = field(default_factory=set)
    relation_codes: set[int] = field(default_factory=set)
    issue_count: int = 0
    issues: list[JsonObject] = field(default_factory=list)

    def record_workflow(self, code: int | None) -> None:
        """Count every returned workflow while retaining a small stable sample."""
        self.workflow_count += 1
        if (
            code is not None
            and len(self.workflow_code_sample) < _BINDING_DIAGNOSTIC_LIMIT
        ):
            self.workflow_code_sample.append(code)

    def record_issue(self, issue: JsonObject) -> None:
        """Retain only a bounded sample while preserving the exact issue count."""
        self.issue_count += 1
        if len(self.issues) < _BINDING_DIAGNOSTIC_LIMIT:
            self.issues.append(issue)


class TaskUpdateCompiler(Protocol):
    """Canonical task-patch compiler owned by the task-definition recipe."""

    def __call__(
        self,
        *,
        current_task: TaskPayloadRecord,
        dag: WorkflowDagRecord,
        update_spec: WorkflowPatchTaskSetSpec,
        requested_fields: Sequence[str],
        task_code: int,
    ) -> TaskUpdateCompilation:
        """Compile one canonical patch into owned task-update values."""
        ...


class TaskDefinitionWire(Protocol):
    """Exact-profile wire programs consumed by ``TaskDefinitions``."""

    @property
    def ds_version(self) -> str:
        """Return the exact DolphinScheduler profile version."""
        ...

    @property
    def recipe_fingerprint(self) -> str:
        """Return the exact task recipe identity."""
        ...

    @property
    def top_level_field_policy(self) -> TaskTopLevelFieldPolicy:
        """Return the exact profile's explicit task-field classification."""
        ...

    @property
    def update_policy(self) -> TaskUpdateWirePolicy:
        """Return exact pre-transport update constraints."""
        ...

    def describe(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> WireExecution[WorkflowDagRecord]:
        """Execute the exact workflow DAG read used by stable task actions."""
        ...

    def get(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> WireExecution[TaskPayloadRecord]:
        """Execute project-scoped task detail through generated wire."""
        ...

    def prepare_update(
        self,
        *,
        project_code: int,
        task_code: int,
        task_definition_json: str,
        upstream_codes: Sequence[int],
    ) -> PreparedWireCallToken:
        """Prepare the exact generated update request without HTTP."""
        ...

    def apply_update(
        self,
        prepared: PreparedWireCallToken,
    ) -> WireExecution[int | None]:
        """Apply a previously prepared generated update request once."""
        ...


class PreparedWholeWorkflowTaskMutation(Protocol):
    """Opaque whole-definition call captured for one task update."""

    @property
    def request(self) -> WireRequest:
        """Return the exact workflow update request exposed by dry-run."""
        ...

    @property
    def initial_graph_fingerprint(self) -> str:
        """Return the complete graph snapshot identity used by the stale guard."""
        ...

    @property
    def has_changes(self) -> bool:
        """Return whether the workflow compiler found a persistent change."""
        ...


class WholeWorkflowTaskUpdate(Protocol):
    """Exact-profile fallback for task epochs without a coherent task endpoint."""

    @property
    def profile_version(self) -> str:
        """Return the exact DolphinScheduler profile version."""
        ...

    def prepare(
        self,
        scope: DefinitionWorkflowScope,
        *,
        dag: WorkflowDagRecord,
        dag_raw: JsonObject,
        project: ResolvedProject,
        task_name: str,
        patch: WorkflowPatchTaskSetSpec,
        requested_fields: Sequence[str],
    ) -> PreparedWholeWorkflowTaskMutation:
        """Compile and capture one whole-workflow task mutation without HTTP."""
        ...

    def fingerprint(
        self,
        dag: WorkflowDagRecord,
        *,
        dag_raw: JsonObject,
    ) -> str:
        """Fingerprint every available field in one complete DAG snapshot."""
        ...

    def apply(self, prepared: PreparedWholeWorkflowTaskMutation) -> None:
        """Execute one previously captured whole-workflow mutation exactly once."""
        ...


@dataclass(frozen=True)
class TaskDefinitions:
    """Resolve, inspect, prepare, mutate, and read back task definitions."""

    profile_version: str
    definitions: DefinitionReads
    wire: TaskDefinitionWire
    compile_update: TaskUpdateCompiler
    whole_workflow_update: WholeWorkflowTaskUpdate | None = None

    def __post_init__(self) -> None:
        """Reject composition across exact profile boundaries."""
        if self.wire.ds_version != self.profile_version:
            message = (
                "Task-definition wire version does not match the selected profile: "
                f"{self.wire.ds_version!r} != {self.profile_version!r}"
            )
            raise ValueError(message)
        if (
            self.whole_workflow_update is not None
            and self.whole_workflow_update.profile_version != self.profile_version
        ):
            message = (
                "Whole-workflow task update version does not match the selected profile"
            )
            raise ValueError(message)

    def get(self, selector: TaskSelector) -> TaskRead:
        """Resolve a scoped selector and return one canonical task view."""
        scope, _dag, _dag_raw, _definition_scope = self._resolve_scope(selector)
        native = self._get_native(
            project_code=scope.project.code,
            task_code=scope.task.code,
        )
        self._validate_native(native, scope=scope)
        return TaskRead(scope=scope, view=_task_view(native.record))

    def list(self, selector: WorkflowSelector) -> TaskListing:
        """Resolve one workflow and list its DAG task definitions."""
        scope, dag, _dag_raw, _definition_scope = self._resolve_workflow_scope(selector)
        return TaskListing(
            scope=scope,
            tasks=tuple(
                TaskSummary(serialize_task_ref(task))
                for task in dag.taskDefinitionList or ()
            ),
        )

    def prepare_update(self, intent: TaskUpdateIntent) -> PreparedTaskUpdate:
        """Read and compile an exact request plan without mutating DolphinScheduler."""
        self._validate_update_intent(intent)
        scope, dag, dag_raw, definition_scope = self._resolve_scope(intent.selector)
        native = self._get_native(
            project_code=scope.project.code,
            task_code=scope.task.code,
        )
        self._validate_native(native, scope=scope)
        self._validate_snapshot_consistency(
            native,
            dag=dag,
            scope=scope,
            phase="prepare",
            mutation_applied=False,
        )
        compilation = self.compile_update(
            current_task=native.record,
            dag=dag,
            update_spec=intent.patch,
            requested_fields=intent.requested_fields,
            task_code=scope.task.code,
        )
        whole_workflow_update = self._selected_whole_workflow_update()
        if whole_workflow_update is None:
            self._reject_unexpressible_dependency_clear(
                compilation,
                intent=intent,
                scope=scope,
            )
        if not compilation.no_change:
            self._validate_mutation_shape(native)
            self._validate_unique_workflow_binding(
                scope=scope,
                phase="prepare",
            )
        wire_call: PreparedWireCallToken | None = None
        whole_workflow_call: PreparedWholeWorkflowTaskMutation | None = None
        if whole_workflow_update is not None:
            whole_workflow_call = whole_workflow_update.prepare(
                definition_scope,
                dag=dag,
                dag_raw=dag_raw,
                project=scope.project,
                task_name=scope.task.name,
                patch=intent.patch,
                requested_fields=intent.requested_fields,
            )
            if whole_workflow_call.has_changes != bool(compilation.updated_fields):
                message = (
                    "Task detail and whole-workflow compilation disagreed about "
                    "whether the update changes persistent state"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "resource": TASK_RESOURCE,
                        "selected_version": self.profile_version,
                        "project_code": scope.project.code,
                        "workflow_code": scope.workflow.code,
                        "code": scope.task.code,
                        "mutation_applied": False,
                    },
                )
            request = whole_workflow_call.request
        else:
            task_definition_json = _json_text(
                _profile_update_payload(
                    compilation.payload,
                    raw=native.raw,
                    policy=self.wire.top_level_field_policy,
                )
            )
            wire_call = self.wire.prepare_update(
                project_code=scope.project.code,
                task_code=scope.task.code,
                task_definition_json=task_definition_json,
                upstream_codes=compilation.updated_upstream_codes,
            )
            request = wire_call.request
        return PreparedTaskUpdate(
            profile_version=self.profile_version,
            recipe_fingerprint=self.wire.recipe_fingerprint,
            current=TaskRead(scope=scope, view=_task_view(native.record)),
            initial_task_version=_task_version(native.record),
            initial_upstream_codes=_normalized_codes(
                compilation.current_upstream_codes
            ),
            expected_upstream_codes=_normalized_codes(
                compilation.updated_upstream_codes
            ),
            initial_projection=deepcopy(compilation.current_projection),
            expected_projection=deepcopy(compilation.expected_projection),
            requested_fields=tuple(intent.requested_fields),
            updated_fields=compilation.updated_fields,
            no_change=compilation.no_change,
            request=request,
            _update_spec=intent.patch.model_copy(deep=True),
            _wire_call=wire_call,
            _whole_workflow_call=whole_workflow_call,
        )

    def apply(
        self,
        prepared: PreparedTaskUpdate,
    ) -> MutationOutcome[TaskRead]:
        """Apply one profile-bound plan exactly once and own its readback."""
        self._validate_prepared(prepared)
        if prepared.no_change:
            return MutationOutcome(
                value=prepared.current,
                mutation_applied=False,
            )

        task_code = prepared.current.scope.task.code
        scope = prepared.current.scope
        self._verify_prepared_state(prepared)
        try:
            returned_code = self._apply_prepared_call(prepared)
        except WireResponseDecodeError as exc:
            message = (
                "DolphinScheduler accepted the task update, but its response "
                "did not match the exact profile"
            )
            raise ApiTransportError(
                message,
                details={
                    **exc.details,
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": task_code,
                    "mutation_applied": True,
                },
                source=exc.source,
                suggestion=_reconciliation_suggestion(scope),
            ) from exc
        except ApiTransportError as exc:
            details = dict(exc.details)
            details.pop("mutation_applied", None)
            details.update(
                {
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": task_code,
                    "mutation_may_have_applied": True,
                    "phase": "mutation_request",
                }
            )
            message = (
                "Task update transport failed after dispatch; the mutation may "
                "have been applied"
            )
            raise ApiTransportError(
                message,
                details=details,
                source=exc.source,
                suggestion=_reconciliation_suggestion(scope),
            ) from exc
        if returned_code is not None and returned_code != task_code:
            message = "DolphinScheduler returned a different task code after update"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "expected_code": task_code,
                    "returned_code": returned_code,
                    "mutation_applied": True,
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion=_reconciliation_suggestion(scope),
            )

        try:
            refreshed, refreshed_dag, _refreshed_dag_raw = self._read_scope_state(scope)
        except (DsctlError, TypeError, WireContractError) as exc:
            message = "Task update succeeded, but its required readback failed"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": task_code,
                    "mutation_applied": True,
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion=_reconciliation_suggestion(scope),
            ) from exc
        self._validate_snapshot_consistency(
            refreshed,
            dag=refreshed_dag,
            scope=scope,
            phase="readback",
            mutation_applied=True,
        )
        try:
            verification = self.compile_update(
                current_task=refreshed.record,
                dag=refreshed_dag,
                update_spec=prepared._update_spec,
                requested_fields=prepared.requested_fields,
                task_code=task_code,
            )
        except (DsctlError, TypeError, WireContractError) as exc:
            message = "Task update succeeded, but its readback could not be verified"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": task_code,
                    "mutation_applied": True,
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion=_reconciliation_suggestion(scope),
            ) from exc
        actual_upstream_codes = _normalized_codes(verification.current_upstream_codes)
        refreshed_version = _task_version(refreshed.record)
        mismatched_fields = list(verification.updated_fields)
        dependencies_match = actual_upstream_codes == prepared.expected_upstream_codes
        version_advance_required = prepared._whole_workflow_call is None or any(
            field != "depends_on" for field in prepared.updated_fields
        )
        version_advanced = refreshed_version > prepared.initial_task_version
        if (
            mismatched_fields
            or not dependencies_match
            or (version_advance_required and not version_advanced)
        ):
            message = (
                "DolphinScheduler reported task update success, but readback did "
                "not persist the requested state"
            )
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": task_code,
                    "mutation_applied": True,
                    "mismatched_fields": mismatched_fields,
                    "expected_projection": prepared.expected_projection,
                    "actual_projection": verification.current_projection,
                    "expected_upstream_codes": list(prepared.expected_upstream_codes),
                    "actual_upstream_codes": list(actual_upstream_codes),
                    "initial_task_version": prepared.initial_task_version,
                    "readback_task_version": refreshed_version,
                    "task_version_advanced": version_advanced,
                    "task_version_advance_required": version_advance_required,
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion=_reconciliation_suggestion(scope),
            )
        return MutationOutcome(
            value=TaskRead(scope=scope, view=_task_view(refreshed.record)),
            mutation_applied=True,
        )

    def _apply_prepared_call(self, prepared: PreparedTaskUpdate) -> int | None:
        whole_workflow_call = prepared._whole_workflow_call
        if whole_workflow_call is not None:
            whole_workflow_update = self._selected_whole_workflow_update()
            if whole_workflow_update is None:
                message = "Prepared whole-workflow task update lost its recipe"
                raise WireContractError(message)
            whole_workflow_update.apply(whole_workflow_call)
            return None
        wire_call = prepared._wire_call
        if wire_call is None:
            message = "Prepared task update omitted its exact mutation call"
            raise WireContractError(message)
        return self.wire.apply_update(wire_call).payload

    def _get_native(
        self,
        *,
        project_code: int,
        task_code: int,
    ) -> NativeTaskSnapshot:
        execution = self.wire.get(
            project_code=project_code,
            task_code=task_code,
        )
        return NativeTaskSnapshot(
            record=execution.payload,
            raw=require_json_object(
                execution.raw_payload,
                label="native task snapshot",
            ),
        )

    def _read_scope_state(
        self,
        scope: TaskScope,
    ) -> tuple[NativeTaskSnapshot, WorkflowDagRecord, JsonObject]:
        execution = self.wire.describe(
            project_code=scope.project.code,
            workflow_code=scope.workflow.code,
        )
        dag = execution.payload
        dag_raw = require_json_object(
            execution.raw_payload,
            label="native workflow DAG snapshot",
        )
        native = self._get_native(
            project_code=scope.project.code,
            task_code=scope.task.code,
        )
        self._validate_native(native, scope=scope)
        return native, dag, dag_raw

    def _verify_prepared_state(self, prepared: PreparedTaskUpdate) -> None:
        scope = prepared.current.scope
        try:
            current, dag, dag_raw = self._read_scope_state(scope)
        except (DsctlError, TypeError, WireContractError) as exc:
            message = "Task update was not sent because current state could not be read"
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": scope.task.code,
                    "mutation_applied": False,
                    "phase": "stale_check",
                },
                source=getattr(exc, "source", None),
                suggestion="Retry the task update from a fresh read.",
            ) from exc
        self._validate_snapshot_consistency(
            current,
            dag=dag,
            scope=scope,
            phase="stale_check",
            mutation_applied=False,
        )
        whole_workflow_call = prepared._whole_workflow_call
        if whole_workflow_call is not None:
            whole_workflow_update = self._selected_whole_workflow_update()
            if whole_workflow_update is None:
                message = "Prepared whole-workflow task update lost its recipe"
                raise WireContractError(message)
            current_graph_fingerprint = whole_workflow_update.fingerprint(
                dag,
                dag_raw=dag_raw,
            )
            if (
                current_graph_fingerprint
                != whole_workflow_call.initial_graph_fingerprint
            ):
                message = "Prepared whole workflow task update is stale"
                raise ConflictError(
                    message,
                    details={
                        "resource": TASK_RESOURCE,
                        "project_code": scope.project.code,
                        "workflow_code": scope.workflow.code,
                        "code": scope.task.code,
                        "phase": "stale_check",
                        "reason": "whole_workflow_graph_changed",
                        "mutation_applied": False,
                    },
                    suggestion="Retry the task update from a fresh read.",
                )
        self._validate_unique_workflow_binding(
            scope=scope,
            phase="stale_check",
        )
        try:
            current_compilation = self.compile_update(
                current_task=current.record,
                dag=dag,
                update_spec=prepared._update_spec,
                requested_fields=prepared.requested_fields,
                task_code=scope.task.code,
            )
        except (DsctlError, TypeError, WireContractError) as exc:
            message = "Prepared task update no longer matches current remote state"
            raise ConflictError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": scope.task.code,
                    "mutation_applied": False,
                    "phase": "stale_check",
                },
                suggestion="Retry the task update from a fresh read.",
            ) from exc

        current_version = _task_version(current.record)
        current_upstream_codes = _normalized_codes(
            current_compilation.current_upstream_codes
        )
        stale_fields = sorted(
            field
            for field in prepared.requested_fields
            if current_compilation.current_projection.get(field)
            != prepared.initial_projection.get(field)
        )
        if (
            current_version == prepared.initial_task_version
            and current_upstream_codes == prepared.initial_upstream_codes
            and not stale_fields
        ):
            return
        message = "Prepared task update is stale and was not sent"
        raise ConflictError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "project_code": scope.project.code,
                "workflow_code": scope.workflow.code,
                "code": scope.task.code,
                "mutation_applied": False,
                "phase": "stale_check",
                "prepared_task_version": prepared.initial_task_version,
                "current_task_version": current_version,
                "prepared_upstream_codes": list(prepared.initial_upstream_codes),
                "current_upstream_codes": list(current_upstream_codes),
                "stale_fields": stale_fields,
            },
            suggestion="Retry the task update from a fresh read.",
        )

    def _reject_unexpressible_dependency_clear(
        self,
        compilation: TaskUpdateCompilation,
        *,
        intent: TaskUpdateIntent,
        scope: TaskScope,
    ) -> None:
        if (
            "depends_on" not in intent.patch.model_fields_set
            or not compilation.current_upstream_codes
            or compilation.updated_upstream_codes
        ):
            return
        message = (
            "The selected DolphinScheduler task update API cannot remove the "
            "final upstream dependency"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "selected_version": self.profile_version,
                "project_code": scope.project.code,
                "workflow_code": scope.workflow.code,
                "code": scope.task.code,
                "current_upstream_codes": list(
                    _normalized_codes(compilation.current_upstream_codes)
                ),
                "requested_upstream_codes": [],
                "reason": "main_api_cannot_clear_all_upstream_relations",
                "mutation_applied": False,
            },
            suggestion=(
                "Use `dsctl workflow edit` for this structural DAG change; do not "
                "retry it through `dsctl task update`."
            ),
        )

    def _validate_unique_workflow_binding(
        self,
        *,
        scope: TaskScope,
        phase: str,
    ) -> None:
        """Prove guarded standalone updates bind only the selected workflow."""
        if not self.wire.update_policy.requires_unique_workflow_binding:
            return
        try:
            proof = self._collect_workflow_binding_proof(scope)
        except (ApiTransportError, NotFoundError, PermissionDeniedError) as exc:
            exc.details["mutation_applied"] = False
            raise
        except (DsctlError, TypeError, WireContractError) as exc:
            message = (
                "Task update was not sent because its workflow binding inventory "
                "could not be read"
            )
            raise ApiTransportError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "selected_version": self.profile_version,
                    "project_code": scope.project.code,
                    "workflow_code": scope.workflow.code,
                    "code": scope.task.code,
                    "phase": phase,
                    "reason": "standalone_task_update_workflow_binding_unreadable",
                    "mutation_applied": False,
                },
                source=getattr(exc, "source", None),
                suggestion="Retry from a fresh read before updating this task.",
            ) from exc

        expected_codes = {scope.workflow.code}
        details: JsonObject = {
            "resource": TASK_RESOURCE,
            "selected_version": self.profile_version,
            "project_code": scope.project.code,
            "workflow_code": scope.workflow.code,
            "code": scope.task.code,
            "phase": phase,
            "workflow_inventory_count": proof.workflow_count,
            "workflow_inventory_codes_sample": proof.workflow_code_sample,
            "workflow_inventory_truncated": (
                proof.workflow_count > len(proof.workflow_code_sample)
            ),
            "membership_workflow_count": len(proof.membership_codes),
            "membership_workflow_codes_sample": sorted(proof.membership_codes)[
                :_BINDING_DIAGNOSTIC_LIMIT
            ],
            "membership_workflow_truncated": (
                len(proof.membership_codes) > _BINDING_DIAGNOSTIC_LIMIT
            ),
            "relation_workflow_count": len(proof.relation_codes),
            "relation_workflow_codes_sample": sorted(proof.relation_codes)[
                :_BINDING_DIAGNOSTIC_LIMIT
            ],
            "relation_workflow_truncated": (
                len(proof.relation_codes) > _BINDING_DIAGNOSTIC_LIMIT
            ),
            "issue_count": proof.issue_count,
            "issues": proof.issues,
            "issues_truncated": proof.issue_count > len(proof.issues),
            "mutation_applied": False,
        }
        if proof.issue_count or proof.membership_codes != proof.relation_codes:
            message = (
                "Task update was not sent because its workflow binding inventory "
                "was inconsistent"
            )
            raise ApiTransportError(
                message,
                details={
                    **details,
                    "reason": (
                        "standalone_task_update_workflow_inventory_inconsistent"
                    ),
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion="Retry from a fresh read before updating this task.",
            )
        if (
            proof.membership_codes == expected_codes
            and proof.relation_codes == expected_codes
        ):
            return
        if phase == "stale_check":
            message = "Prepared task update workflow binding changed before apply"
            raise ConflictError(
                message,
                details={
                    **details,
                    "reason": "prepared_task_update_workflow_binding_changed",
                },
                suggestion="Retry the task update from a fresh read.",
            )
        if (
            not proof.membership_codes
            or scope.workflow.code not in proof.membership_codes
        ):
            message = (
                "Task update was not sent because its workflow binding inventory "
                "contradicted the selected workflow"
            )
            raise ApiTransportError(
                message,
                details={
                    **details,
                    "reason": (
                        "standalone_task_update_workflow_inventory_inconsistent"
                    ),
                },
                source={
                    "kind": "remote",
                    "system": "dolphinscheduler",
                    "layer": "response",
                },
                suggestion="Retry from a fresh read before updating this task.",
            )
        message = (
            "The selected DolphinScheduler task endpoint cannot prove a unique "
            "selected workflow binding"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                **details,
                "reason": ("standalone_task_update_workflow_binding_not_unique"),
            },
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "response",
            },
            suggestion=(
                "Use `dsctl workflow edit` so the complete workflow DAG remains "
                "the mutation boundary."
            ),
        )

    def _collect_workflow_binding_proof(
        self,
        scope: TaskScope,
    ) -> _WorkflowBindingProof:
        proof = _WorkflowBindingProof()
        refs = self.definitions.workflow_refs(str(scope.project.code))
        seen_codes: set[int] = set()
        for index, ref in enumerate(refs):
            native = ref.native
            if not isinstance(native, NativeCode) or not _is_positive_int(native.value):
                proof.record_workflow(None)
                proof.record_issue(
                    {
                        "kind": "invalid_workflow_reference",
                        "workflow_index": index,
                    }
                )
                continue
            workflow_code = native.value
            proof.record_workflow(workflow_code)
            if workflow_code in seen_codes:
                proof.record_issue(
                    {
                        "kind": "duplicate_workflow_reference",
                        "workflow_code": workflow_code,
                    }
                )
                continue
            seen_codes.add(workflow_code)
            dag = self.wire.describe(
                project_code=scope.project.code,
                workflow_code=workflow_code,
            ).payload
            _inspect_workflow_task_binding(
                proof,
                dag=dag,
                expected_project_code=scope.project.code,
                expected_workflow_code=workflow_code,
                task_code=scope.task.code,
            )
        return proof

    def _validate_snapshot_consistency(
        self,
        native: NativeTaskSnapshot,
        *,
        dag: WorkflowDagRecord,
        scope: TaskScope,
        phase: str,
        mutation_applied: bool,
    ) -> None:
        issues = _snapshot_consistency_issues(
            native,
            dag=dag,
            scope=scope,
        )
        if not issues:
            return
        details = {
            "resource": TASK_RESOURCE,
            "project_code": scope.project.code,
            "workflow_code": scope.workflow.code,
            "code": scope.task.code,
            "phase": phase,
            "issues": issues,
            "mutation_applied": mutation_applied,
        }
        if phase == "stale_check":
            message = "Prepared task update no longer has a coherent DAG snapshot"
            raise ConflictError(
                message,
                details=details,
                suggestion="Retry the task update from a fresh read.",
            )
        message = "Task detail and workflow DAG versions are inconsistent"
        raise ApiTransportError(
            message,
            details=details,
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "response",
            },
            suggestion=(
                _reconciliation_suggestion(scope)
                if mutation_applied
                else "Retry after DolphinScheduler returns a coherent DAG snapshot."
            ),
        )

    def _resolve_scope(
        self,
        selector: TaskSelector,
    ) -> tuple[
        TaskScope,
        WorkflowDagRecord,
        JsonObject,
        DefinitionWorkflowScope,
    ]:
        workflow_scope, dag, dag_raw, definition_scope = self._resolve_workflow_scope(
            WorkflowSelector(
                project=selector.project,
                workflow=selector.workflow,
            )
        )
        task_lookup = _DagTaskLookup(tuple(dag.taskDefinitionList or ()))
        resolved_task = resolve_task(
            selector.task,
            adapter=cast("TaskOperations", task_lookup),
            project_code=workflow_scope.project.code,
            workflow_code=workflow_scope.workflow.code,
        )
        return (
            TaskScope(
                project=workflow_scope.project,
                workflow=workflow_scope.workflow,
                task=resolved_task,
            ),
            dag,
            dag_raw,
            definition_scope,
        )

    def _resolve_workflow_scope(
        self,
        selector: WorkflowSelector,
    ) -> tuple[
        WorkflowScope,
        WorkflowDagRecord,
        JsonObject,
        DefinitionWorkflowScope,
    ]:
        resolved = self.definitions.resolve_workflow(
            selector.project,
            selector.workflow,
        )
        project_native = resolved.project.native
        workflow_native = resolved.workflow.native
        if not isinstance(project_native, NativeCode) or not isinstance(
            workflow_native,
            NativeCode,
        ):
            message = "Stable task definitions require native integer code identities"
            raise WireContractError(message)
        project_name = resolved.project.name
        workflow_name = resolved.workflow.name
        if project_name is None or workflow_name is None:
            message = "Resolved task scope omitted project or workflow name"
            raise WireContractError(message)
        resolved_project = ResolvedProject(
            code=project_native.value,
            name=project_name,
            description=resolved.project.description,
        )
        resolved_workflow = ResolvedWorkflow(
            code=workflow_native.value,
            name=workflow_name,
            version=resolved.workflow.version,
        )
        execution = self.wire.describe(
            project_code=resolved_project.code,
            workflow_code=resolved_workflow.code,
        )
        dag = execution.payload
        dag_raw = require_json_object(
            execution.raw_payload,
            label="native workflow DAG snapshot",
        )
        return (
            WorkflowScope(
                project=resolved_project,
                workflow=resolved_workflow,
            ),
            dag,
            dag_raw,
            resolved,
        )

    def _validate_update_intent(self, intent: TaskUpdateIntent) -> None:
        whole_workflow_update = self._selected_whole_workflow_update()
        if (
            not self.wire.update_policy.update_available
            and whole_workflow_update is None
        ):
            message = (
                f"DolphinScheduler {self.profile_version} cannot coherently update "
                "a workflow task through its standalone task endpoint"
            )
            raise UnsupportedFeatureError(
                message,
                details={
                    "resource": TASK_RESOURCE,
                    "selected_version": self.profile_version,
                    "reason": (
                        "standalone_task_update_does_not_advance_"
                        "workflow_relation_versions"
                    ),
                    "upstream_capability_limited": True,
                    "mutation_applied": False,
                },
                suggestion=(
                    "Update the complete process definition in DolphinScheduler "
                    "or use a profile whose task endpoint updates workflow "
                    "relation versions."
                ),
            )
        requested = set(intent.requested_fields)
        unsupported = sorted(
            requested.intersection(self.wire.update_policy.unsupported_patch_fields)
        )
        dependency_requested = "depends_on" in intent.patch.model_fields_set
        dependency_update_available = self.wire.update_policy.dependency_update
        if not unsupported and (
            not dependency_requested or dependency_update_available
        ):
            return
        if dependency_requested and not dependency_update_available:
            unsupported.append("depends_on")
        unsupported = sorted(set(unsupported))
        message = (
            f"DolphinScheduler {self.profile_version} cannot express the requested "
            "task update fields"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "selected_version": self.profile_version,
                "unsupported_fields": unsupported,
                "dependency_update": dependency_update_available,
                "mutation_applied": False,
            },
            suggestion=(
                "Remove depends_on; DS 2.0.3 cannot safely edit existing dependencies. "
                "Use a reviewed version with dependency editing support."
                if self.profile_version == "2.0.3" and dependency_requested
                else "Remove the unsupported fields and retry. Use `dsctl workflow "
                "edit` for dependency changes that this task endpoint cannot "
                "represent."
            ),
        )

    def _selected_whole_workflow_update(self) -> WholeWorkflowTaskUpdate | None:
        """Select the fallback only when the standalone recipe is terminal."""
        if self.wire.update_policy.update_available:
            return None
        return self.whole_workflow_update

    def _validate_native(
        self,
        native: NativeTaskSnapshot,
        *,
        scope: TaskScope,
    ) -> None:
        if (
            native.record.code == scope.task.code
            and native.record.projectCode == scope.project.code
        ):
            return
        message = "Project-scoped task detail returned a mismatched task identity"
        raise ApiTransportError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "expected_project_code": scope.project.code,
                "returned_project_code": native.record.projectCode,
                "expected_code": scope.task.code,
                "returned_code": native.record.code,
            },
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "response",
            },
        )

    def _validate_mutation_shape(self, native: NativeTaskSnapshot) -> None:
        unexpected_fields = sorted(
            set(native.raw).difference(
                self.wire.top_level_field_policy.classified_fields
            )
        )
        if not unexpected_fields:
            return
        message = (
            "Task payload contains unreviewed top-level fields; refusing a "
            "potentially lossy update"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": TASK_RESOURCE,
                "selected_version": self.profile_version,
                "unexpected_fields": unexpected_fields,
                "mutation_applied": False,
            },
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "response",
            },
            suggestion=(
                "Verify DS_VERSION matches the server. Do not retry this task "
                "mutation until the selected profile reviews these fields."
            ),
        )

    def _validate_prepared(self, prepared: PreparedTaskUpdate) -> None:
        call_count = int(prepared._wire_call is not None) + int(
            prepared._whole_workflow_call is not None
        )
        if (
            prepared.profile_version == self.profile_version
            and prepared.recipe_fingerprint == self.wire.recipe_fingerprint
            and call_count == 1
            and (
                prepared._whole_workflow_call is None
                or self._selected_whole_workflow_update() is not None
            )
        ):
            return
        message = "Prepared task update does not belong to this exact profile recipe"
        raise ApiTransportError(
            message,
            details={
                "prepared_version": prepared.profile_version,
                "selected_version": self.profile_version,
                "mutation_applied": False,
            },
        )


@dataclass(frozen=True)
class _DagTaskLookup:
    tasks: tuple[TaskPayloadRecord, ...]

    def list(
        self,
        *,
        project_code: int,
        workflow_code: int,
    ) -> tuple[TaskPayloadRecord, ...]:
        del project_code, workflow_code
        return self.tasks


def _inspect_workflow_task_binding(
    proof: _WorkflowBindingProof,
    *,
    dag: WorkflowDagRecord,
    expected_project_code: int,
    expected_workflow_code: int,
    task_code: int,
) -> None:
    _inspect_binding_workflow_identity(
        proof,
        dag=dag,
        expected_project_code=expected_project_code,
        expected_workflow_code=expected_workflow_code,
    )
    matching_tasks = [
        task for task in dag.taskDefinitionList or () if task.code == task_code
    ]
    task_version = _inspect_binding_task_membership(
        proof,
        matching_tasks=matching_tasks,
        workflow_code=expected_workflow_code,
    )
    _inspect_binding_relations(
        proof,
        relations=tuple(dag.workflowTaskRelationList or ()),
        matching_task_count=len(matching_tasks),
        task_code=task_code,
        task_version=task_version,
        workflow_code=expected_workflow_code,
    )


def _inspect_binding_workflow_identity(
    proof: _WorkflowBindingProof,
    *,
    dag: WorkflowDagRecord,
    expected_project_code: int,
    expected_workflow_code: int,
) -> None:
    workflow = dag.workflowDefinition
    if workflow is None:
        proof.record_issue(
            {
                "kind": "workflow_definition_missing",
                "workflow_code": expected_workflow_code,
            }
        )
    elif (
        workflow.code != expected_workflow_code
        or workflow.projectCode != expected_project_code
    ):
        proof.record_issue(
            {
                "kind": "workflow_identity_mismatch",
                "workflow_code": expected_workflow_code,
                "actual_workflow_code": workflow.code,
                "actual_project_code": workflow.projectCode,
            }
        )


def _inspect_binding_task_membership(
    proof: _WorkflowBindingProof,
    *,
    matching_tasks: Sequence[TaskPayloadRecord],
    workflow_code: int,
) -> int | None:
    if len(matching_tasks) > 1:
        proof.record_issue(
            {
                "kind": "duplicate_task_membership",
                "workflow_code": workflow_code,
                "count": len(matching_tasks),
            }
        )
        return None
    if not matching_tasks:
        return None
    proof.membership_codes.add(workflow_code)
    candidate_version = matching_tasks[0].version
    if _is_task_version(candidate_version):
        return candidate_version
    proof.record_issue(
        {
            "kind": "invalid_task_version",
            "workflow_code": workflow_code,
        }
    )
    return None


def _inspect_binding_relations(
    proof: _WorkflowBindingProof,
    *,
    relations: Sequence[WorkflowTaskRelationRecord],
    matching_task_count: int,
    task_code: int,
    task_version: int | None,
    workflow_code: int,
) -> None:
    matching_relations = []
    relation_keys: set[tuple[int, int, int, int]] = set()
    for index, relation in enumerate(relations):
        relation_key = (
            relation.preTaskCode,
            relation.preTaskVersion,
            relation.postTaskCode,
            relation.postTaskVersion,
        )
        if relation_key in relation_keys:
            proof.record_issue(
                {
                    "kind": "duplicate_relation",
                    "workflow_code": workflow_code,
                    "relation_index": index,
                }
            )
        relation_keys.add(relation_key)
        if task_code in {relation.preTaskCode, relation.postTaskCode}:
            matching_relations.append((index, relation))

    if matching_relations:
        proof.relation_codes.add(workflow_code)
    if matching_task_count and not any(
        relation.postTaskCode == task_code for _index, relation in matching_relations
    ):
        proof.record_issue(
            {
                "kind": "missing_incoming_task_relation",
                "workflow_code": workflow_code,
            }
        )
    if bool(matching_task_count) is not bool(matching_relations):
        proof.record_issue(
            {
                "kind": "task_relation_membership_mismatch",
                "workflow_code": workflow_code,
            }
        )
    for index, relation in matching_relations:
        _inspect_binding_relation_version(
            proof,
            relation=relation,
            relation_index=index,
            task_code=task_code,
            task_version=task_version,
            workflow_code=workflow_code,
        )


def _inspect_binding_relation_version(
    proof: _WorkflowBindingProof,
    *,
    relation: WorkflowTaskRelationRecord,
    relation_index: int,
    task_code: int,
    task_version: int | None,
    workflow_code: int,
) -> None:
    pre_matches = relation.preTaskCode == task_code
    post_matches = relation.postTaskCode == task_code
    if pre_matches and post_matches:
        proof.record_issue(
            {
                "kind": "self_relation",
                "workflow_code": workflow_code,
                "relation_index": relation_index,
            }
        )
    if task_version is None:
        return
    if pre_matches and relation.preTaskVersion != task_version:
        proof.record_issue(
            {
                "kind": "relation_task_version_mismatch",
                "workflow_code": workflow_code,
                "relation_index": relation_index,
                "side": "pre",
            }
        )
    if post_matches and relation.postTaskVersion != task_version:
        proof.record_issue(
            {
                "kind": "relation_task_version_mismatch",
                "workflow_code": workflow_code,
                "relation_index": relation_index,
                "side": "post",
            }
        )


def _snapshot_consistency_issues(
    native: NativeTaskSnapshot,
    *,
    dag: WorkflowDagRecord,
    scope: TaskScope,
) -> list[JsonObject]:
    issues = _workflow_identity_issues(dag, scope=scope)
    matching_tasks = [
        task for task in dag.taskDefinitionList or () if task.code == scope.task.code
    ]
    if len(matching_tasks) != 1:
        issues.append(
            {
                "kind": "dag_task_membership",
                "expected_count": 1,
                "actual_count": len(matching_tasks),
            }
        )
        return issues

    detail_version = native.record.version
    dag_version = matching_tasks[0].version
    if not _is_task_version(detail_version):
        issues.append(
            {
                "kind": "invalid_detail_task_version",
                "actual": detail_version,
            }
        )
        return issues
    if not _is_task_version(dag_version):
        issues.append(
            {
                "kind": "invalid_dag_task_version",
                "actual": dag_version,
            }
        )
        return issues
    if dag_version != detail_version:
        issues.append(
            {
                "kind": "task_version_mismatch",
                "detail_version": detail_version,
                "dag_version": dag_version,
            }
        )
    issues.extend(
        _relation_version_issues(
            dag,
            task_code=scope.task.code,
            task_version=dag_version,
        )
    )
    return issues


def _workflow_identity_issues(
    dag: WorkflowDagRecord,
    *,
    scope: TaskScope,
) -> list[JsonObject]:
    issues: list[JsonObject] = []
    workflow = dag.workflowDefinition
    if workflow is None:
        issues.append({"kind": "workflow_definition_missing"})
    else:
        if workflow.code != scope.workflow.code:
            issues.append(
                {
                    "kind": "workflow_code_mismatch",
                    "expected": scope.workflow.code,
                    "actual": workflow.code,
                }
            )
        if workflow.projectCode != scope.project.code:
            issues.append(
                {
                    "kind": "workflow_project_mismatch",
                    "expected": scope.project.code,
                    "actual": workflow.projectCode,
                }
            )
    return issues


def _relation_version_issues(
    dag: WorkflowDagRecord,
    *,
    task_code: int,
    task_version: int,
) -> list[JsonObject]:
    issues: list[JsonObject] = []
    for index, relation in enumerate(dag.workflowTaskRelationList or ()):
        if (
            relation.preTaskCode == task_code
            and relation.preTaskVersion != task_version
        ):
            issues.append(
                {
                    "kind": "relation_task_version_mismatch",
                    "relation_index": index,
                    "side": "pre",
                    "task_version": task_version,
                    "relation_version": relation.preTaskVersion,
                }
            )
        if (
            relation.postTaskCode == task_code
            and relation.postTaskVersion != task_version
        ):
            issues.append(
                {
                    "kind": "relation_task_version_mismatch",
                    "relation_index": index,
                    "side": "post",
                    "task_version": task_version,
                    "relation_version": relation.postTaskVersion,
                }
            )
    return issues


def _task_version(record: TaskPayloadRecord) -> int:
    version = record.version
    if _is_task_version(version):
        return version
    message = "Task payload did not contain a valid positive version"
    raise ApiTransportError(
        message,
        details={
            "resource": TASK_RESOURCE,
            "code": record.code,
            "version": version,
        },
        source={
            "kind": "remote",
            "system": "dolphinscheduler",
            "layer": "response",
        },
    )


def _is_task_version(value: int | None) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_positive_int(value: int) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _normalized_codes(codes: Sequence[int]) -> tuple[int, ...]:
    return tuple(sorted(set(codes)))


def _reconciliation_suggestion(scope: TaskScope) -> str:
    return (
        f"Run `dsctl task get {scope.task.code} --project {scope.project.code} "
        f"--workflow {scope.workflow.code}` and compare the requested fields "
        "before retrying the mutation."
    )


def _task_view(record: TaskPayloadRecord) -> TaskView:
    return TaskView(serialize_task(record))


def _profile_update_payload(
    compiled: JsonObject,
    *,
    raw: JsonObject,
    policy: TaskTopLevelFieldPolicy,
) -> JsonObject:
    """Project compiler output to exact request fields and preserve opaque values."""
    payload: JsonObject = {
        key: deepcopy(value)
        for key, value in compiled.items()
        if key in policy.request_payload
    }
    for key in policy.opaque_preservation.difference({"taskParams"}):
        if key in raw:
            payload[key] = deepcopy(raw[key])
    return require_json_object(payload, label="profile task update payload")


def _json_text(value: JsonObject) -> str:
    return json.dumps(
        require_json_object(value, label="task update payload"),
        ensure_ascii=False,
        separators=(",", ":"),
    )


__all__ = [
    "MutationOutcome",
    "NativeTaskSnapshot",
    "PreparedTaskUpdate",
    "TaskDefinitionWire",
    "TaskDefinitions",
    "TaskListing",
    "TaskRead",
    "TaskScope",
    "TaskSelector",
    "TaskSummary",
    "TaskUpdateCompiler",
    "TaskUpdateIntent",
    "TaskView",
    "WorkflowScope",
    "WorkflowSelector",
]
