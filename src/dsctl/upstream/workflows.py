from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Literal, cast

from dsctl.cli_surface import TASK_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import (
    ApiTransportError,
    NotFoundError,
    ResolutionError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.workflow_profiles import WORKFLOW_PROFILE_FACTS
from dsctl.upstream._compiled_workflow_runtime import (
    WORKFLOW_PROGRAMS,
    WorkflowPrimitive,
)
from dsctl.upstream._selector_resolution import (
    normalize_identifier,
    parse_numeric_identifier,
)
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.data_quality import (
    bind_data_quality_authoring_inspector,
)
from dsctl.upstream.datasources import DataSourceAdapter
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
)
from dsctl.upstream.execution_receipts import WorkflowExecutionReceipt
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from dsctl.upstream.identity import IdentityAdapter
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.project_preferences import (
    ProjectPreferenceAdapter,
)
from dsctl.upstream.resources import bind_task_resource_resolver
from dsctl.upstream.response_projection import (
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.schedules import (
    ScheduleAdapter,
    ScheduleOperations,
)
from dsctl.upstream.task_definition_wire import (
    WorkflowDagProjectionError,
    bind_task_definition_wire,
    project_exact_workflow_dag,
)
from dsctl.upstream.wire import (
    PreparedCompiledWireCall,
    WireContractError,
    WireRequest,
)
from dsctl.upstream.workflow_graph_requests import (
    legacy_workflow_arguments,
    workflow_create_arguments,
    workflow_update_arguments,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.legacy_workflow_graph import LegacyWorkflowGraphPayload
    from dsctl.upstream.protocol import (
        CurrentUserOperations,
        DataQualityAuthoringInspector,
        DataSourceOperations,
        DependentLineageTaskRecord,
        ProjectPreferenceOperations,
        TaskPayloadRecord,
        TaskResourceResolver,
        WorkflowDagRecord,
        WorkflowLineageDetailRecord,
        WorkflowLineageRecord,
        WorkflowLineageRelationRecord,
        WorkflowPayloadRecord,
        WorkflowTaskRelationRecord,
    )
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire
    from dsctl.upstream.wire import CompiledWireProfile
    from dsctl.upstream.workflow_graph import (
        WorkflowCreatePayload,
        WorkflowUpdatePayload,
    )


_DefinitionFamily = Literal["legacy-json", "process", "workflow"]
_ExecutionResult = Literal["none", "trigger-code", "id-list"]
ExecutionScheduleTimeShape = Literal["comma-range", "json"]
_LineageProjection = Literal["absent", "legacy-direct", "canonical"]
_DependentProjection = Literal[
    "absent",
    "task-main-info",
    "dependent-lineage-task",
]


@dataclass(frozen=True)
class _WorkflowRecipe:
    family: _DefinitionFamily
    native_identity: Literal["id", "code"]
    create_supported: bool
    dag_supported: bool
    task_scope_supported: bool
    definition_tenant_code: bool
    execution_tenant_code: bool
    create_other_params: bool
    update_other_params: bool
    execution_type: bool
    execution_result: _ExecutionResult
    environment_code: bool
    start_params: bool
    execution_dry_run: bool
    expected_parallelism_number: bool
    complement_dependent_mode: bool
    all_level_dependent: bool
    execution_order: bool
    test_flag: bool
    definition_version: bool
    definition_delete_lineage_guard: bool
    timeout: bool
    warning_group_omission: Literal["zero", "omit"]
    execution_schedule_time_shape: ExecutionScheduleTimeShape
    lineage_projection: _LineageProjection
    dependent_projection: _DependentProjection

    @property
    def start_node_identity(self) -> Literal["name", "code"]:
        """Return the exact task identity accepted by executor startNodeList."""
        return "name" if self.family == "legacy-json" else "code"


_RECIPE_FIELDS = frozenset(_WorkflowRecipe.__dataclass_fields__)
_RECIPE_STRING_VALUES: dict[str, frozenset[str]] = {
    "family": frozenset({"legacy-json", "process", "workflow"}),
    "native_identity": frozenset({"id", "code"}),
    "execution_result": frozenset({"none", "trigger-code", "id-list"}),
    "lineage_projection": frozenset({"absent", "legacy-direct", "canonical"}),
    "dependent_projection": frozenset(
        {"absent", "task-main-info", "dependent-lineage-task"}
    ),
    "warning_group_omission": frozenset({"zero", "omit"}),
    "execution_schedule_time_shape": frozenset({"comma-range", "json"}),
}


def _recipe_from_generated_fact(
    version: str, value: OpaqueGeneratedValue
) -> _WorkflowRecipe:
    if not isinstance(value, dict) or set(value) != _RECIPE_FIELDS:
        message = f"Generated workflow profile {version} has an invalid field set"
        raise WireContractError(message)
    for field, allowed in _RECIPE_STRING_VALUES.items():
        if value[field] not in allowed:
            message = f"Generated workflow profile {version} has invalid {field}"
            raise WireContractError(message)
    for field in _RECIPE_FIELDS - _RECIPE_STRING_VALUES.keys():
        if not isinstance(value[field], bool):
            message = f"Generated workflow profile {version} has non-boolean {field}"
            raise WireContractError(message)
    return _WorkflowRecipe(**cast("dict[str, Any]", value))


_RECIPE_BY_VERSION: dict[str, _WorkflowRecipe] = {
    version: _recipe_from_generated_fact(version, fact)
    for version, fact in WORKFLOW_PROFILE_FACTS.items()
}


def supports_workflow_execution_type(ds_version: str) -> bool:
    """Expose the exact definition execution-mode capability without transport."""
    return _RECIPE_BY_VERSION[ds_version].execution_type


def workflow_execution_schedule_time_shape(
    ds_version: str,
) -> ExecutionScheduleTimeShape:
    """Expose the exact executor scheduleTime consumer shape without transport."""
    return _RECIPE_BY_VERSION[ds_version].execution_schedule_time_shape


_PREFERENCE_VERSIONS = frozenset(
    {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)


@dataclass(frozen=True)
class _WorkflowBindings:
    compiled: CompiledWireProfile
    recipe: _WorkflowRecipe


@dataclass(frozen=True)
class WorkflowDagSnapshot:
    """Canonical DAG names above process/workflow generated DTO dialects."""

    workflowDefinition: WorkflowPayloadRecord | None  # noqa: N815
    workflowTaskRelationList: tuple[WorkflowTaskRelationRecord, ...]  # noqa: N815
    taskDefinitionList: tuple[TaskPayloadRecord, ...]  # noqa: N815


@dataclass(frozen=True)
class LegacyWorkflowDefinitionSnapshot:
    """Exact DS 1.3 definition strings kept below canonical graph decoding."""

    scope: WorkflowScope
    name: str | None
    description: str | None
    release_state: str | None
    process_definition_json: str
    locations: str
    connects: str


@dataclass(frozen=True)
class WorkflowLineageSnapshot:
    """Canonical graph names above legacy and current lineage responses."""

    workFlowRelationList: tuple[WorkflowLineageRelationRecord, ...]  # noqa: N815
    workFlowRelationDetailList: tuple[  # noqa: N815
        WorkflowLineageDetailRecord,
        ...,
    ]


class WorkflowDeleteLineageError(RuntimeError):
    """Whole-definition deletion would leave owned lineage on this profile."""


@dataclass(frozen=True)
class DependentLineageTaskSnapshot:
    """Stable dependent-task row projected from either upstream DTO."""

    projectCode: int  # noqa: N815
    workflowDefinitionCode: int  # noqa: N815
    workflowDefinitionName: str | None  # noqa: N815
    taskDefinitionCode: int  # noqa: N815
    taskDefinitionName: str | None  # noqa: N815


@dataclass(frozen=True)
class PreparedWorkflowCreate:
    """One exact generated workflow-create request captured before mutation."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall


@dataclass(frozen=True)
class PreparedLegacyWorkflowCreate:
    """One exact DS 1.3 string-native workflow-create request."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall


@dataclass(frozen=True)
class PreparedWorkflowUpdate:
    """One exact generated workflow-update request captured before mutation."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall


@dataclass(frozen=True)
class PreparedLegacyWorkflowUpdate:
    """One exact DS 1.3 string-native workflow-update request."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall


@dataclass(frozen=True)
class PreparedWorkflowRelease:
    """One exact generated workflow-release request captured before mutation."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall | None


@dataclass(frozen=True)
class PreparedLegacyWorkflowRelease:
    """One exact DS 1.3 id-native workflow-release request."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall | None


@dataclass(frozen=True, slots=True)
class _WorkflowExecutionArgs:
    scope: WorkflowScope
    schedule_time: str
    command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"]
    worker_group: str
    tenant_code: str
    start_node_list: tuple[int, ...] | tuple[str, ...] | None
    task_scope: str | None
    failure_strategy: str
    warning_type: str
    workflow_instance_priority: str
    warning_group_id: int | None
    environment_code: int | None
    start_params: str | None
    execution_dry_run: bool
    run_mode: str | None
    expected_parallelism_number: int | None
    complement_dependent_mode: str | None
    all_level_dependent: bool | None
    execution_order: str | None


@dataclass(frozen=True)
class PreparedWorkflowExecution:
    """One exact generated workflow-execution request captured before mutation."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall
    _action: str
    _operation: Literal["backfill", "run"]
    _project: ProjectRef


@dataclass(frozen=True)
class WorkflowDefinitionWire:
    """Prepare and execute source-compiled definition mutations."""

    ds_version: str
    _recipe: _WorkflowRecipe
    _programs: BoundCompiledPrograms[WorkflowPrimitive]

    def _require_dialect(self, *, legacy: bool, operation: str) -> None:
        if (self._recipe.family == "legacy-json") != legacy:
            if operation == "release":
                dialect = "legacy" if legacy else "code-native"
                msg = f"DS {self.ds_version} has no {dialect} workflow-release program"
            else:
                dialect = "legacy " if legacy else ""
                msg = (
                    f"DS {self.ds_version} has no {dialect}"
                    f"workflow-{operation} wire program"
                )
            raise WireContractError(msg)

    def prepare_create(
        self,
        *,
        project_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        tenant_code: str | None,
    ) -> PreparedWorkflowCreate:
        """Capture the selected-version create request without HTTP."""
        self._require_dialect(legacy=False, operation="create")
        values: JsonObject = {
            "projectCode": project_code,
            "name": name,
            "description": description,
            "globalParams": global_params,
            "locations": locations,
            "timeout": timeout,
            "taskRelationJson": task_relation_json,
            "taskDefinitionJson": task_definition_json,
        }
        if self._recipe.definition_tenant_code:
            values["tenantCode"] = tenant_code
        if self._recipe.create_other_params:
            values["otherParamsJson"] = None
        if self._recipe.execution_type:
            values["executionType"] = execution_type
        call = self._programs.prepare("definition_create", values)
        return PreparedWorkflowCreate(request=call.request, _wire_call=call)

    def prepare_legacy_create(
        self,
        *,
        project_name: str,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> PreparedLegacyWorkflowCreate:
        """Capture the exact string-native create request."""
        self._require_dialect(legacy=True, operation="create")
        call = self._programs.prepare(
            "definition_create",
            {
                "projectName": project_name,
                "name": name,
                "description": description,
                "processDefinitionJson": process_definition_json,
                "locations": locations,
                "connects": connects,
            },
        )
        return PreparedLegacyWorkflowCreate(request=call.request, _wire_call=call)

    def apply_create(
        self, prepared: PreparedWorkflowCreate | PreparedLegacyWorkflowCreate
    ) -> None:
        """Execute the detached create request exactly once."""
        self._require_dialect(
            legacy=isinstance(prepared, PreparedLegacyWorkflowCreate),
            operation="create",
        )
        mutation_call(
            lambda: self._programs.execute("definition_create", prepared._wire_call),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="create",
        )

    def prepare_update(
        self,
        *,
        project_code: int,
        workflow_code: int,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> PreparedWorkflowUpdate:
        """Capture the selected-version update request without HTTP."""
        self._require_dialect(legacy=False, operation="update")
        values: JsonObject = {
            "projectCode": project_code,
            "code": workflow_code,
            "name": name,
            "description": description,
            "globalParams": global_params,
            "locations": locations,
            "timeout": timeout,
            "taskRelationJson": task_relation_json,
            "taskDefinitionJson": task_definition_json,
            "releaseState": release_state,
        }
        if self._recipe.definition_tenant_code:
            values["tenantCode"] = tenant_code
        if self._recipe.update_other_params:
            values["otherParamsJson"] = None
        if self._recipe.execution_type:
            values["executionType"] = execution_type
        call = self._programs.prepare("definition_update", values)
        return PreparedWorkflowUpdate(request=call.request, _wire_call=call)

    def prepare_legacy_update(
        self,
        *,
        project_name: str,
        workflow_id: int,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> PreparedLegacyWorkflowUpdate:
        """Capture the exact string-native update request."""
        self._require_dialect(legacy=True, operation="update")
        call = self._programs.prepare(
            "definition_update",
            {
                "projectName": project_name,
                "id": workflow_id,
                "name": name,
                "description": description,
                "processDefinitionJson": process_definition_json,
                "locations": locations,
                "connects": connects,
            },
        )
        return PreparedLegacyWorkflowUpdate(request=call.request, _wire_call=call)

    def apply_update(
        self, prepared: PreparedWorkflowUpdate | PreparedLegacyWorkflowUpdate
    ) -> None:
        """Execute the detached update request exactly once."""
        self._require_dialect(
            legacy=isinstance(prepared, PreparedLegacyWorkflowUpdate),
            operation="update",
        )
        mutation_call(
            lambda: self._programs.execute("definition_update", prepared._wire_call),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="update",
        )

    def prepare_release(
        self,
        *,
        project_code: int,
        workflow_code: int | str,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> PreparedWorkflowRelease:
        """Prepare executable identities; retain display-only creation previews."""
        self._require_dialect(legacy=False, operation="release")
        preview = isinstance(workflow_code, str)
        call = self._programs.prepare(
            "definition_release",
            {
                "projectCode": project_code,
                "code": 0 if preview else workflow_code,
                "releaseState": state,
            },
        )
        if preview:
            request = replace(
                call.request,
                path=call.request.path.removesuffix("/0/release")
                + f"/{workflow_code}/release",
            )
            return PreparedWorkflowRelease(request=request, _wire_call=None)
        return PreparedWorkflowRelease(request=call.request, _wire_call=call)

    def prepare_legacy_release(
        self,
        *,
        project_name: str,
        workflow_id: int | str,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> PreparedLegacyWorkflowRelease:
        """Capture the exact legacy release request or its display-only preview."""
        release_state: Literal[0, 1] = 1 if state == "ONLINE" else 0
        if isinstance(workflow_id, str):
            return PreparedLegacyWorkflowRelease(
                request=WireRequest(
                    method="POST",
                    path=f"/projects/{project_name}/process/release",
                    query=None,
                    form={"processId": workflow_id, "releaseState": release_state},
                    json=None,
                    content=None,
                ),
                _wire_call=None,
            )
        self._require_dialect(legacy=True, operation="release")
        call = self._programs.prepare(
            "definition_release",
            {
                "projectName": project_name,
                "processId": workflow_id,
                "releaseState": release_state,
            },
        )
        return PreparedLegacyWorkflowRelease(request=call.request, _wire_call=call)

    def apply_release(
        self, prepared: PreparedWorkflowRelease | PreparedLegacyWorkflowRelease
    ) -> None:
        """Execute a previously prepared release, never a preview identity."""
        legacy = isinstance(prepared, PreparedLegacyWorkflowRelease)
        call = prepared._wire_call
        if call is None:
            identity = "id" if legacy else "code"
            msg = f"A preview workflow {identity} cannot be applied to DolphinScheduler"
            raise WireContractError(msg)
        self._require_dialect(legacy=legacy, operation="release")
        mutation_call(
            lambda: self._programs.execute("definition_release", call),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="release",
        )


@dataclass(frozen=True)
class WorkflowExecutionWire:
    """Exact executor request and result policy above the compiled exchange."""

    ds_version: str
    result_kind: _ExecutionResult
    _recipe: _WorkflowRecipe
    _programs: BoundCompiledPrograms[WorkflowPrimitive]

    def prepare(self, args: _WorkflowExecutionArgs) -> PreparedWorkflowExecution:
        """Capture one selected-version execution request without HTTP."""
        values = _execution_form_values(
            self._recipe,
            scope=args.scope,
            schedule_time=args.schedule_time,
            command_type=args.command_type,
            worker_group=args.worker_group,
            tenant_code=args.tenant_code,
            start_node_list=args.start_node_list,
            task_scope=args.task_scope,
            failure_strategy=args.failure_strategy,
            warning_type=args.warning_type,
            workflow_instance_priority=args.workflow_instance_priority,
            warning_group_id=args.warning_group_id,
            environment_code=args.environment_code,
            start_params=args.start_params,
            execution_dry_run=args.execution_dry_run,
            run_mode=args.run_mode,
            expected_parallelism_number=args.expected_parallelism_number,
            complement_dependent_mode=args.complement_dependent_mode,
            all_level_dependent=args.all_level_dependent,
            execution_order=args.execution_order,
        )
        if self._recipe.family == "legacy-json":
            values["projectName"] = _project_name(
                args.scope.project, ds_version=self.ds_version
            )
        else:
            values["projectCode"] = _native_code(
                args.scope.project.native, ds_version=self.ds_version
            )
        call = self._programs.prepare("workflow_execute", values)
        return PreparedWorkflowExecution(
            request=call.request,
            _wire_call=call,
            _project=args.scope.project,
            _action=_execution_action(
                command_type=args.command_type, start_node_list=args.start_node_list
            ),
            _operation="backfill" if args.command_type == "COMPLEMENT_DATA" else "run",
        )

    def apply(self, prepared: PreparedWorkflowExecution) -> WorkflowExecutionReceipt:
        """Execute exactly the captured request and project its exact result."""
        operation = prepared._operation
        result = mutation_call(
            lambda: (
                self._programs.execute("workflow_execute", prepared._wire_call).payload
            ),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation=operation,
        )
        receipt = verify_mutation(
            lambda: _execution_receipt(
                result, result_kind=self.result_kind, ds_version=self.ds_version
            ),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation=operation,
            phase="mutation_response",
        )
        trigger_code = receipt.trigger_code
        if trigger_code is None:
            return receipt
        try:
            ids = verify_mutation(
                lambda: self.trigger_instance_ids(prepared._project, trigger_code),
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                operation=operation,
                phase="instance_resolution",
            )
        except ApiTransportError as error:
            error.details["execution"] = receipt.to_data()
            error.details["project"] = prepared._project.to_data()
            raise
        return replace(
            receipt,
            workflow_instance_ids=ids,
            instance_resolution="resolved" if ids else "pending",
        )

    def trigger_instance_ids(
        self, project: ProjectRef, trigger_code: int
    ) -> tuple[int, ...]:
        """Read only identities explicitly associated with one server trigger."""
        call = self._programs.prepare(
            "instance_trigger",
            {
                "projectCode": _native_code(project.native, ds_version=self.ds_version),
                "triggerCode": trigger_code,
            },
        )
        rows = self._programs.execute("instance_trigger", call).payload
        if not isinstance(rows, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                field="workflowInstanceIds",
                reason="trigger result is not a list",
            )
        ids: list[int] = []
        for row in rows:
            identity = response_field(
                row, "id", ds_version=self.ds_version, resource=WORKFLOW_RESOURCE
            )
            if (
                not isinstance(identity, int)
                or isinstance(identity, bool)
                or identity <= 0
            ):
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=WORKFLOW_RESOURCE,
                    field="workflowInstanceIds",
                    reason="trigger instance identity is not a positive integer",
                )
            ids.append(identity)
        return tuple(ids)


@dataclass(frozen=True)
class WorkflowDomain:
    """Deep workflow surface owning exact wire, resolution, and projections."""

    workflows: WorkflowOperations
    task_resource_resolver: TaskResourceResolver | None = None
    data_quality_authoring_inspector: DataQualityAuthoringInspector | None = None
    task_datasources: DataSourceOperations | None = None
    task_definitions: TaskDefinitionWire | None = None

    @property
    def definitions(self) -> DefinitionReads:
        """Expose the shared caller-oriented definition reader."""
        return self.workflows.definitions

    @property
    def schedules(self) -> ScheduleOperations:
        """Expose schedule composition used by workflow authoring."""
        return self.workflows.schedules

    def prepare_definition_create(
        self,
        project: ProjectRef,
        *,
        payload: WorkflowCreatePayload | LegacyWorkflowGraphPayload,
        name: str,
        description: str | None,
    ) -> PreparedWorkflowCreate | PreparedLegacyWorkflowCreate:
        """Select the exact definition dialect for a compiled authoring graph."""
        if self.workflows.workflow_graph_family == "legacy-json":
            return self.workflows.prepare_legacy_create(
                project,
                name=name,
                description=description,
                **legacy_workflow_arguments(
                    cast("LegacyWorkflowGraphPayload", payload)
                ),
            )
        return self.workflows.prepare_create(
            project, **workflow_create_arguments(cast("WorkflowCreatePayload", payload))
        )

    def prepare_definition_update(
        self,
        scope: WorkflowScope,
        *,
        payload: WorkflowUpdatePayload | LegacyWorkflowGraphPayload,
        name: str,
        description: str | None,
    ) -> PreparedWorkflowUpdate | PreparedLegacyWorkflowUpdate:
        """Keep native graph request projection inside the selected domain."""
        if self.workflows.workflow_graph_family == "legacy-json":
            return self.workflows.prepare_legacy_update(
                scope,
                name=name,
                description=description,
                **legacy_workflow_arguments(
                    cast("LegacyWorkflowGraphPayload", payload)
                ),
            )
        return self.workflows.prepare_update(
            scope, **workflow_update_arguments(cast("WorkflowUpdatePayload", payload))
        )


class WorkflowAdapter:
    """Bind one reviewed exact package to the workflow domain."""

    def __init__(self, ds_version: str, *, version_slug: str | None = None) -> None:
        """Load one exact generated package and reviewed workflow recipe."""
        self._bindings = _load_bindings(ds_version, version_slug=version_slug)
        self.ds_version = ds_version
        self.version_slug = f"ds_{ds_version.replace('.', '_')}"

    @classmethod
    def for_version(cls, ds_version: str) -> WorkflowAdapter:
        """Return the exact adapter for one reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> WorkflowDomain:
        """Bind workflow operations and their caller-owned dependencies."""
        read_adapter = (
            IdNativeReadAdapter()
            if self.ds_version == "1.3.9"
            else CodeNativeReadAdapter.for_version(self.ds_version)
        )
        definitions = read_adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions
        current_user = IdentityAdapter.for_version(self.ds_version).bind_identity(
            profile, http_client=http_client
        )
        schedules = (
            ScheduleAdapter.for_version(self.ds_version)
            .bind(
                profile,
                http_client=http_client,
            )
            .schedules
        )
        preferences = (
            ProjectPreferenceAdapter.for_version(self.ds_version)
            .bind(profile, http_client=http_client)
            .preferences
            if self.ds_version in _PREFERENCE_VERSIONS
            else None
        )
        return WorkflowDomain(
            workflows=WorkflowOperations(
                programs=WORKFLOW_PROGRAMS.bind(
                    self._bindings.compiled, profile, http_client=http_client
                ),
                bindings=self._bindings,
                definition_wire=bind_workflow_definition_wire(
                    http_client,
                    bindings=self._bindings,
                ),
                execution_wire=bind_workflow_execution_wire(
                    http_client,
                    bindings=self._bindings,
                ),
                definitions=definitions,
                current_user=current_user,
                schedules=schedules,
                project_preferences=preferences,
            ),
            task_resource_resolver=bind_task_resource_resolver(
                self.ds_version,
                profile,
                http_client=http_client,
            ),
            data_quality_authoring_inspector=(
                bind_data_quality_authoring_inspector(
                    self.ds_version,
                    profile,
                    http_client=http_client,
                )
            ),
            task_datasources=(
                DataSourceAdapter.for_version(self.ds_version)
                .bind(profile, http_client=http_client)
                .datasources
            ),
            task_definitions=(
                bind_task_definition_wire(http_client)
                if self.ds_version == "3.1.0"
                else None
            ),
        )


WORKFLOW_DOMAIN = BoundDomain[WorkflowDomain](
    name=WORKFLOW_RESOURCE,
    adapter_for_version=WorkflowAdapter.for_version,
)


@dataclass(frozen=True)
class WorkflowOperations:
    """Caller-cohesive workflow operations backed by one exact package."""

    programs: BoundCompiledPrograms[WorkflowPrimitive]
    bindings: _WorkflowBindings
    definition_wire: WorkflowDefinitionWire
    execution_wire: WorkflowExecutionWire
    definitions: DefinitionReads
    current_user: CurrentUserOperations
    schedules: ScheduleOperations
    project_preferences: ProjectPreferenceOperations | None

    @property
    def ds_version(self) -> str:
        """Return the exact generated package version."""
        return self.bindings.compiled.ds_version

    @property
    def workflow_graph_family(self) -> Literal["legacy-json", "code-native"]:
        """Expose only the graph epoch needed to select canonical compilation."""
        if self.bindings.recipe.family == "legacy-json":
            return "legacy-json"
        return "code-native"

    @property
    def execution_schedule_time_shape(self) -> Literal["comma-range", "json"]:
        """Expose the exact executor scheduleTime consumer epoch."""
        return self.bindings.recipe.execution_schedule_time_shape

    def require_action(self, action: str) -> None:
        """Reject terminally unsupported actions before any HTTP request."""
        recipe = self.bindings.recipe
        if action in {"workflow.create", "workflow.edit"} and not (
            recipe.create_supported
        ):
            self._unsupported(
                action,
                reason="upstream_capability_limited",
                constraint=(
                    "DS 1.3.9 stores string-native tasks in "
                    "processDefinitionJson; the stable code/version-native "
                    "authoring contract cannot be projected without inventing "
                    "task identities."
                ),
            )

        if (
            action
            in {
                "workflow.describe",
                "workflow.digest",
                "workflow.export",
            }
            and not recipe.dag_supported
        ):
            self._unsupported(
                action,
                reason="upstream_capability_limited",
                constraint=(
                    "DS 1.3.9 exposes a string-native legacy graph that cannot "
                    "be losslessly represented by the stable workflow DAG."
                ),
            )
        if action == "workflow.run-task" and not recipe.task_scope_supported:
            self._unsupported(
                action,
                reason="upstream_capability_limited",
                constraint=(
                    "DS 1.3.9 addresses start nodes by legacy string task name; "
                    "the stable task selector resolves code-native task identities."
                ),
            )
        if action in {"workflow.lineage.list", "workflow.lineage.get"} and (
            recipe.lineage_projection == "absent"
        ):
            self._unsupported(
                action,
                reason="upstream_capability_absent",
                constraint="This DolphinScheduler release has no lineage controller.",
            )
        if action == "workflow.lineage.dependent-tasks" and (
            recipe.dependent_projection == "absent"
        ):
            self._unsupported(
                action,
                reason="upstream_capability_absent",
                constraint=(
                    "This exact lineage controller exposes graph reads but no "
                    "dependent-task query."
                ),
            )

    def normalize_expected_parallelism_number(
        self,
        value: int | None,
    ) -> int | None:
        """Apply the exact executor parameter boundary before any resolution."""
        if self.bindings.recipe.expected_parallelism_number:
            return 2 if value is None else value
        if value is None:
            return None
        message = (
            "--expected-parallelism-number is not available in "
            f"DolphinScheduler {self.ds_version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "action": "workflow.backfill",
                "parameter": "expectedParallelismNumber",
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
            },
            suggestion=(
                "Omit --expected-parallelism-number for this exact DS version."
            ),
        )

    def require_execution_options(
        self,
        *,
        action: str,
        tenant_code: str | None = None,
        environment_code: int | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> None:
        """Reject explicit executor options absent from the exact recipe."""
        del run_mode  # runMode is part of every exact executor recipe.
        recipe = self.bindings.recipe
        options = (
            (
                recipe.execution_tenant_code,
                "tenantCode",
                tenant_code is not None,
                "--tenant",
            ),
            (
                recipe.environment_code,
                "environmentCode",
                environment_code is not None,
                "--environment-code",
            ),
            (
                recipe.execution_dry_run,
                "dryRun",
                execution_dry_run,
                "--execution-dry-run",
            ),
            (
                recipe.expected_parallelism_number,
                "expectedParallelismNumber",
                expected_parallelism_number is not None,
                "--expected-parallelism-number",
            ),
            (
                recipe.complement_dependent_mode,
                "complementDependentMode",
                complement_dependent_mode is not None,
                "--complement-dependent-mode",
            ),
            (
                recipe.all_level_dependent,
                "allLevelDependent",
                bool(all_level_dependent),
                "--all-level-dependent",
            ),
            (
                recipe.execution_order,
                "executionOrder",
                execution_order is not None,
                "--execution-order",
            ),
        )
        for supported, parameter, explicit, flag in options:
            if supported or not explicit:
                continue
            message = f"{flag} is not available in DolphinScheduler {self.ds_version}."
            raise UnsupportedFeatureError(
                message,
                details={
                    "resource": WORKFLOW_RESOURCE,
                    "action": action,
                    "parameter": parameter,
                    "ds_version": self.ds_version,
                    "reason": "upstream_capability_absent",
                },
                suggestion=f"Omit {flag} for this exact DS version.",
            )

    def normalize_start_params(
        self,
        value: str | None,
        *,
        action: str,
    ) -> str | None:
        """Apply the exact start-parameter boundary before any resolution."""
        if self.bindings.recipe.start_params or value is None:
            return value
        message = (
            "--param is not available in DolphinScheduler "
            f"{self.ds_version} because its workflow executor has no "
            "startParams field."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "action": action,
                "parameter": "startParams",
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
            },
            suggestion="Omit --param for this exact DS version.",
        )

    def resolve_project(self, selector: str) -> ProjectRef:
        """Resolve one project without exposing the generated dialect."""
        return self.definitions.resolve_project(selector)

    def resolve_project_by_name(self, project_name: str) -> ProjectRef:
        """Resolve an exact project name without numeric-selector reinterpretation."""
        return self.definitions.resolve_project_by_name(project_name)

    def visible_project_refs(self) -> tuple[ProjectRef, ...]:
        """Read the complete visible project identity inventory once."""
        return self.definitions.visible_project_refs()

    def resolve_workflow(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> WorkflowScope:
        """Resolve one project-scoped workflow using its native identity."""
        return self.definitions.resolve_workflow(project_selector, workflow_selector)

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        """Resolve an explicit workflow name inside an already-resolved project."""
        return self.definitions.resolve_workflow_by_name(project, workflow_name)

    def find_workflow_ref_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowRef | None:
        """Find one exact-name workflow ref without loading its detail."""
        return self.definitions.find_workflow_ref_by_name(project, workflow_name)

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        """Read the project's identity inventory once for batch reverse binding."""
        return self.definitions.visible_workflow_refs(project)

    def resolve_workflow_by_id(
        self,
        project: ProjectRef,
        workflow_id: int,
    ) -> WorkflowScope:
        """Resolve one exact legacy workflow id inside a proved project."""
        return self.definitions.resolve_workflow_by_id(project, workflow_id)

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        """Resolve one exact workflow code inside a proved code-native project."""
        return self.definitions.resolve_workflow_by_code(project, workflow_code)

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        """Load and canonicalize a code-native workflow DAG."""
        self.require_action(action)
        project_code = _native_code(scope.project.native, ds_version=self.ds_version)
        workflow_code = _native_code(scope.workflow.native, ds_version=self.ds_version)
        payload = self.programs.call(
            "definition_get", {"projectCode": project_code, "code": workflow_code}
        )
        try:
            return project_exact_workflow_dag(
                payload,
                ds_version=self.ds_version,
                expected_project_code=project_code,
                expected_workflow_code=workflow_code,
            )
        except WorkflowDagProjectionError as exc:
            raise projection_error(
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                field=exc.field,
                reason=exc.reason,
            ) from exc
        except WireContractError as exc:
            raise projection_error(
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                field="data",
                reason="generated workflow DAG violated its exact contract",
            ) from exc

    def legacy_definition(
        self,
        scope: WorkflowScope,
        *,
        action: str,
    ) -> LegacyWorkflowDefinitionSnapshot:
        """Load one exact DS 1.3 definition without decoding native JSON."""
        self.require_action(action)
        if self.workflow_graph_family != "legacy-json":
            message = (
                f"DS {self.ds_version} does not expose legacy-json workflow detail"
            )
            raise WireContractError(message)
        project_name = _project_name(scope.project, ds_version=self.ds_version)
        workflow_id = _native_id(scope.workflow.native, ds_version=self.ds_version)
        payload = self.programs.call(
            "definition_get", {"projectName": project_name, "processId": workflow_id}
        )
        return LegacyWorkflowDefinitionSnapshot(
            scope=scope,
            name=_optional_workflow_text_field(
                payload,
                "name",
                ds_version=self.ds_version,
            ),
            description=_optional_workflow_text_field(
                payload,
                "description",
                ds_version=self.ds_version,
            ),
            release_state=scope.view.release_state,
            process_definition_json=_required_legacy_json_string(
                payload,
                "processDefinitionJson",
                ds_version=self.ds_version,
            ),
            locations=_required_legacy_json_string(
                payload,
                "locations",
                ds_version=self.ds_version,
            ),
            connects=_required_legacy_json_string(
                payload,
                "connects",
                ds_version=self.ds_version,
            ),
        )

    def detail(self, scope: WorkflowScope, *, action: str) -> WorkflowPayloadRecord:
        """Return the definition embedded in one canonical DAG response."""
        dag = self.dag(scope, action=action)
        payload = dag.workflowDefinition
        if payload is None:
            message = "Workflow DAG payload was missing its definition"
            raise ApiTransportError(
                message,
                details={"ds_version": self.ds_version},
            )
        return payload

    def resolve_task(
        self,
        scope: WorkflowScope,
        selector: str,
        *,
        action: str,
    ) -> TaskPayloadRecord:
        """Resolve one code-native task within the already selected DAG."""
        self.require_action(action)
        dag = self.dag(scope, action=action)
        normalized = normalize_identifier(selector, label="Task")
        numeric = parse_numeric_identifier(normalized)
        tasks = list(dag.taskDefinitionList or ())
        if numeric is not None:
            matches = [task for task in tasks if task.code == numeric]
        else:
            matches = [task for task in tasks if task.name == normalized]
        if len(matches) == 1:
            return matches[0]
        details = {
            "resource": TASK_RESOURCE,
            "selector": normalized,
            "project": scope.project.native.value,
            "workflow": scope.workflow.native.value,
        }
        if not matches:
            message = f"Task {normalized!r} was not found in the selected workflow."
            raise NotFoundError(
                message,
                details=details,
            )
        message = f"Task selector {normalized!r} matched more than one task."
        raise ResolutionError(
            message,
            details={**details, "match_count": len(matches)},
        )

    def allocate_task_codes(self, project: ProjectRef, count: int) -> tuple[int, ...]:
        """Allocate exact server-owned task codes for workflow authoring."""
        self.require_action("workflow.create")
        if count <= 0:
            return ()
        if self.bindings.recipe.family == "legacy-json":
            message = "Exact workflow package has no task-code allocator"
            raise WireContractError(message)
        project_code = _native_code(project.native, ds_version=self.ds_version)
        payload = self.programs.call(
            "task_code_allocate", {"projectCode": project_code, "genNum": count}
        )
        if not isinstance(payload, list) or any(
            not isinstance(item, int) or isinstance(item, bool) or item <= 0
            for item in payload
        ):
            raise projection_error(
                ds_version=self.ds_version,
                resource=TASK_RESOURCE,
                field="taskCodes",
                reason="task-code allocator did not return positive integers",
            )
        if len(payload) != count or len(set(payload)) != len(payload):
            raise projection_error(
                ds_version=self.ds_version,
                resource=TASK_RESOURCE,
                field="taskCodes",
                reason="task-code allocator count or uniqueness did not match",
            )
        return tuple(payload)

    def prepare_create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> PreparedWorkflowCreate:
        """Prepare one exact create request without mutating DolphinScheduler."""
        self.require_action("workflow.create")
        project_code = _native_code(project.native, ds_version=self.ds_version)
        return self.definition_wire.prepare_create(
            project_code=project_code,
            name=name,
            description=description,
            global_params=global_params,
            locations=locations,
            timeout=timeout,
            task_relation_json=task_relation_json,
            task_definition_json=task_definition_json,
            execution_type=execution_type,
            tenant_code=(
                self._current_tenant_code()
                if self.bindings.recipe.definition_tenant_code
                else None
            ),
        )

    def apply_create(
        self,
        prepared: PreparedWorkflowCreate | PreparedLegacyWorkflowCreate,
    ) -> None:
        """Apply one profile-bound create plan without reconstructing it."""
        self.require_action("workflow.create")
        self.definition_wire.apply_create(prepared)

    def prepare_legacy_create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        process_definition_json: str,
        locations: str,
        connects: str,
    ) -> PreparedLegacyWorkflowCreate:
        """Prepare one exact DS 1.3 string-native create request."""
        self.require_action("workflow.create")
        project_name = _project_name(project, ds_version=self.ds_version)
        return self.definition_wire.prepare_legacy_create(
            project_name=project_name,
            name=name,
            description=description,
            process_definition_json=process_definition_json,
            locations=locations,
            connects=connects,
        )

    def create(
        self,
        project: ProjectRef,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
    ) -> None:
        """Create through the same prepared request exposed to dry-run callers."""
        prepared = self.prepare_create(
            project,
            name=name,
            description=description,
            global_params=global_params,
            locations=locations,
            timeout=timeout,
            task_relation_json=task_relation_json,
            task_definition_json=task_definition_json,
            execution_type=execution_type,
        )
        self.apply_create(prepared)

    def prepare_update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> PreparedWorkflowUpdate:
        """Prepare one exact update request without mutating DolphinScheduler."""
        self.require_action("workflow.edit")
        project_code = _native_code(scope.project.native, ds_version=self.ds_version)
        workflow_code = _native_code(scope.workflow.native, ds_version=self.ds_version)
        return self.definition_wire.prepare_update(
            project_code=project_code,
            workflow_code=workflow_code,
            name=name,
            description=description,
            global_params=global_params,
            locations=locations,
            timeout=timeout,
            task_relation_json=task_relation_json,
            task_definition_json=task_definition_json,
            execution_type=execution_type,
            release_state=release_state,
            tenant_code=(
                tenant_code or self._current_tenant_code()
                if self.bindings.recipe.definition_tenant_code
                else None
            ),
        )

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
        """Prepare one exact DS 1.3 string-native update request."""
        self.require_action("workflow.edit")
        project_name = _project_name(scope.project, ds_version=self.ds_version)
        workflow_id = _native_id(scope.workflow.native, ds_version=self.ds_version)
        return self.definition_wire.prepare_legacy_update(
            project_name=project_name,
            workflow_id=workflow_id,
            name=name,
            description=description,
            process_definition_json=process_definition_json,
            locations=locations,
            connects=connects,
        )

    def apply_update(
        self,
        prepared: PreparedWorkflowUpdate | PreparedLegacyWorkflowUpdate,
    ) -> None:
        """Apply one profile-bound update plan without reconstructing it."""
        self.require_action("workflow.edit")
        self.definition_wire.apply_update(prepared)

    def update(
        self,
        scope: WorkflowScope,
        *,
        name: str,
        description: str | None,
        global_params: str,
        locations: str,
        timeout: int,
        task_relation_json: str,
        task_definition_json: str,
        execution_type: str | None,
        release_state: str | None,
        tenant_code: str | None,
    ) -> None:
        """Update through the same prepared request exposed to dry-run callers."""
        prepared = self.prepare_update(
            scope,
            name=name,
            description=description,
            global_params=global_params,
            locations=locations,
            timeout=timeout,
            task_relation_json=task_relation_json,
            task_definition_json=task_definition_json,
            execution_type=execution_type,
            release_state=release_state,
            tenant_code=tenant_code,
        )
        self.apply_update(prepared)

    def delete(self, scope: WorkflowScope) -> None:
        """Delete one definition with the exact id/code route."""
        if self.bindings.recipe.definition_delete_lineage_guard:
            self._check_delete_lineage(scope)
        args: JsonObject
        if self.bindings.recipe.family == "legacy-json":
            project_name = _project_name(scope.project, ds_version=self.ds_version)
            args = {
                "projectName": project_name,
                "processDefinitionId": scope.workflow.native.value,
            }
        else:
            args = {
                "projectCode": _native_code(
                    scope.project.native, ds_version=self.ds_version
                ),
                "code": _native_code(scope.workflow.native, ds_version=self.ds_version),
            }
        prepared = self.programs.prepare("definition_delete", args)
        mutation_call(
            lambda: self.programs.execute("definition_delete", prepared),
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
            operation="delete",
        )

    def _check_delete_lineage(self, scope: WorkflowScope) -> None:
        payload = self._lineage_get_payload(scope)
        relations = response_field(
            payload,
            "workFlowRelationList",
            ds_version=self.ds_version,
            resource=WORKFLOW_RESOURCE,
        )
        # A null graph is not evidence that the workflow owns no lineage.
        if not isinstance(relations, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                field="workFlowRelationList",
                reason="workflow deletion requires a lineage relation list",
            )
        for relation in relations:
            source = _required_int_field(
                relation, "sourceWorkFlowCode", ds_version=self.ds_version
            )
            target = _required_int_field(
                relation, "targetWorkFlowCode", ds_version=self.ds_version
            )
            if source < 0 or target <= 0:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=WORKFLOW_RESOURCE,
                    field="workFlowRelationList",
                    reason="lineage relation has an invalid workflow code",
                )
            # Exact 3.3.x emits every edge, including synthetic roots, from an
            # owned lineage row. Even a root without its companion edge blocks.
            if target == scope.workflow.native.value:
                message = "Workflow owns lineage that native deletion would retain"
                raise WorkflowDeleteLineageError(message)

    def release(
        self,
        scope: WorkflowScope,
        *,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> None:
        """Set the definition release state through the exact route vocabulary."""
        prepared = self.prepare_release(
            scope.project,
            workflow_code=scope.workflow.native.value,
            state=state,
        )
        self.apply_release(prepared)

    def prepare_release(
        self,
        project: ProjectRef,
        *,
        workflow_code: int | str,
        state: Literal["ONLINE", "OFFLINE"],
    ) -> PreparedWorkflowRelease | PreparedLegacyWorkflowRelease:
        """Prepare one exact profile-native release request without HTTP."""
        if self.workflow_graph_family == "legacy-json":
            return self.definition_wire.prepare_legacy_release(
                project_name=_project_name(project, ds_version=self.ds_version),
                workflow_id=workflow_code,
                state=state,
            )
        return self.definition_wire.prepare_release(
            project_code=_native_code(project.native, ds_version=self.ds_version),
            workflow_code=workflow_code,
            state=state,
        )

    def apply_release(
        self,
        prepared: PreparedWorkflowRelease | PreparedLegacyWorkflowRelease,
    ) -> None:
        """Apply one profile-bound release plan without reconstructing it."""
        self.definition_wire.apply_release(prepared)

    def run(
        self,
        scope: WorkflowScope,
        *,
        schedule_time: str,
        command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> WorkflowExecutionReceipt:
        """Trigger or backfill a workflow using exact executor parameters."""
        prepared = self.prepare_execution(
            scope,
            schedule_time=schedule_time,
            command_type=command_type,
            worker_group=worker_group,
            tenant_code=tenant_code,
            start_node_list=start_node_list,
            task_scope=task_scope,
            failure_strategy=failure_strategy,
            warning_type=warning_type,
            workflow_instance_priority=workflow_instance_priority,
            warning_group_id=warning_group_id,
            environment_code=environment_code,
            start_params=start_params,
            execution_dry_run=execution_dry_run,
            run_mode=run_mode,
            expected_parallelism_number=expected_parallelism_number,
            complement_dependent_mode=complement_dependent_mode,
            all_level_dependent=all_level_dependent,
            execution_order=execution_order,
        )
        return self.apply_execution(prepared)

    def prepare_execution(
        self,
        scope: WorkflowScope,
        *,
        schedule_time: str,
        command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
        worker_group: str,
        tenant_code: str,
        start_node_list: Sequence[int] | Sequence[str] | None = None,
        task_scope: str | None = None,
        failure_strategy: str = "CONTINUE",
        warning_type: str = "NONE",
        workflow_instance_priority: str = "MEDIUM",
        warning_group_id: int | None = None,
        environment_code: int | None = None,
        start_params: str | None = None,
        execution_dry_run: bool = False,
        run_mode: str | None = None,
        expected_parallelism_number: int | None = None,
        complement_dependent_mode: str | None = None,
        all_level_dependent: bool | None = None,
        execution_order: str | None = None,
    ) -> PreparedWorkflowExecution:
        """Prepare one exact executor request without mutating DolphinScheduler."""
        action = _execution_action(
            command_type=command_type,
            start_node_list=start_node_list,
        )
        self.require_action(action)
        recipe = self.bindings.recipe
        if start_node_list is not None and not recipe.task_scope_supported:
            self.require_action("workflow.run-task")
        normalized_start_nodes = _normalize_start_nodes(
            start_node_list,
            identity=recipe.start_node_identity,
            ds_version=self.ds_version,
        )
        return self.execution_wire.prepare(
            _WorkflowExecutionArgs(
                scope=scope,
                schedule_time=schedule_time,
                command_type=command_type,
                worker_group=worker_group,
                tenant_code=tenant_code,
                start_node_list=normalized_start_nodes,
                task_scope=task_scope,
                failure_strategy=failure_strategy,
                warning_type=warning_type,
                workflow_instance_priority=workflow_instance_priority,
                warning_group_id=warning_group_id,
                environment_code=environment_code,
                start_params=start_params,
                execution_dry_run=execution_dry_run,
                run_mode=run_mode,
                expected_parallelism_number=expected_parallelism_number,
                complement_dependent_mode=complement_dependent_mode,
                all_level_dependent=all_level_dependent,
                execution_order=execution_order,
            )
        )

    def apply_execution(
        self,
        prepared: PreparedWorkflowExecution,
    ) -> WorkflowExecutionReceipt:
        """Apply one profile-bound executor plan without reconstructing it."""
        self.require_action(prepared._action)
        return self.execution_wire.apply(prepared)

    def lineage_list(
        self,
        project_selector: str,
    ) -> tuple[ProjectRef, WorkflowLineageRecord]:
        """Resolve one project and load its canonical lineage graph."""
        self.require_action("workflow.lineage.list")
        project = self.definitions.resolve_project(project_selector)
        return project, self.lineage_list_resolved(project)

    def lineage_list_resolved(
        self,
        project: ProjectRef,
    ) -> WorkflowLineageRecord:
        """Load lineage for an already resolved project reference."""
        self.require_action("workflow.lineage.list")
        payload = self.programs.call(
            "lineage_list",
            {"projectCode": _native_code(project.native, ds_version=self.ds_version)},
        )
        return self._project_lineage(payload)

    def lineage_get(
        self,
        project_selector: str,
        workflow_selector: str,
    ) -> tuple[WorkflowScope, WorkflowLineageRecord]:
        """Resolve one workflow and load its canonical lineage graph."""
        self.require_action("workflow.lineage.get")
        scope = self.definitions.resolve_workflow(
            project_selector,
            workflow_selector,
        )
        return scope, self.lineage_get_resolved(scope)

    def lineage_get_resolved(
        self,
        scope: WorkflowScope,
    ) -> WorkflowLineageRecord:
        """Load lineage for an already resolved workflow scope."""
        return self._project_lineage(self._lineage_get_payload(scope))

    def _lineage_get_payload(self, scope: WorkflowScope) -> OpaqueGeneratedValue:
        self.require_action("workflow.lineage.get")
        return self.programs.call(
            "lineage_get",
            {
                "projectCode": _native_code(
                    scope.project.native, ds_version=self.ds_version
                ),
                "workFlowCode": _native_code(
                    scope.workflow.native, ds_version=self.ds_version
                ),
            },
        )

    def dependent_tasks(
        self,
        project_selector: str,
        workflow_selector: str,
        *,
        task_selector: str | None,
    ) -> tuple[
        WorkflowScope,
        TaskPayloadRecord | None,
        tuple[DependentLineageTaskRecord, ...],
    ]:
        """Return exact dependent tasks after terminal support checks."""
        action = "workflow.lineage.dependent-tasks"
        self.require_action(action)
        scope = self.definitions.resolve_workflow(
            project_selector,
            workflow_selector,
        )
        task, dependent = self.dependent_tasks_resolved(
            scope,
            task_selector=task_selector,
        )
        return scope, task, dependent

    def dependent_tasks_resolved(
        self,
        scope: WorkflowScope,
        *,
        task_selector: str | None,
    ) -> tuple[TaskPayloadRecord | None, tuple[DependentLineageTaskRecord, ...]]:
        """Load dependent tasks for an already resolved workflow scope."""
        action = "workflow.lineage.dependent-tasks"
        self.require_action(action)
        task = (
            None
            if task_selector is None
            else self.resolve_task(scope, task_selector, action=action)
        )
        payload = self.programs.call(
            "lineage_dependent_tasks",
            {
                "projectCode": _native_code(
                    scope.project.native, ds_version=self.ds_version
                ),
                "workFlowCode": _native_code(
                    scope.workflow.native, ds_version=self.ds_version
                ),
                "taskCode": None if task is None else task.code,
            },
        )
        if not isinstance(payload, list):
            raise projection_error(
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
                field="dependentTasks",
                reason="dependent-task response is not a list",
            )
        projected = tuple(self._project_dependent_task(item) for item in payload)
        return task, cast("tuple[DependentLineageTaskRecord, ...]", projected)

    def _project_lineage(self, payload: OpaqueGeneratedValue) -> WorkflowLineageRecord:
        if self.bindings.recipe.lineage_projection == "canonical":
            relations = sequence_field(
                payload,
                "workFlowRelationList",
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
            )
            details = sequence_field(
                payload,
                "workFlowRelationDetailList",
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
            )
        else:
            relations = sequence_field(
                payload,
                "workFlowRelationList",
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
            )
            details = sequence_field(
                payload,
                "workFlowList",
                ds_version=self.ds_version,
                resource=WORKFLOW_RESOURCE,
            )
        return cast(
            "WorkflowLineageRecord",
            WorkflowLineageSnapshot(
                cast("tuple[WorkflowLineageRelationRecord, ...]", tuple(relations)),
                cast("tuple[WorkflowLineageDetailRecord, ...]", tuple(details)),
            ),
        )

    def _project_dependent_task(
        self, payload: OpaqueGeneratedValue
    ) -> DependentLineageTaskSnapshot:
        legacy = self.bindings.recipe.dependent_projection == "task-main-info"
        return DependentLineageTaskSnapshot(
            projectCode=_required_int_field(
                payload,
                "projectCode",
                ds_version=self.ds_version,
            ),
            workflowDefinitionCode=_required_int_field(
                payload,
                "processDefinitionCode" if legacy else "workflowDefinitionCode",
                ds_version=self.ds_version,
            ),
            workflowDefinitionName=_optional_text_field(
                payload,
                "processDefinitionName" if legacy else "workflowDefinitionName",
                ds_version=self.ds_version,
            ),
            taskDefinitionCode=_required_int_field(
                payload,
                "taskCode" if legacy else "taskDefinitionCode",
                ds_version=self.ds_version,
            ),
            taskDefinitionName=_optional_text_field(
                payload,
                "taskName" if legacy else "taskDefinitionName",
                ds_version=self.ds_version,
            ),
        )

    def _current_tenant_code(self) -> str:
        tenant = self.current_user.current().tenantCode
        if isinstance(tenant, str) and tenant.strip():
            return tenant.strip()
        message = (
            "The current DolphinScheduler user has no tenant for workflow authoring."
        )
        raise UserInputError(
            message,
            details={"ds_version": self.ds_version, "resource": WORKFLOW_RESOURCE},
            suggestion=(
                "Assign a tenant to the current user in DolphinScheduler, then retry."
            ),
        )

    def _unsupported(
        self,
        action: str,
        *,
        reason: Literal[
            "upstream_capability_absent",
            "upstream_capability_limited",
        ],
        constraint: str,
    ) -> None:
        message = f"{action} is not available in DolphinScheduler {self.ds_version}."
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "action": action,
                "ds_version": self.ds_version,
                "reason": reason,
                "constraint": constraint,
            },
            suggestion=(
                "Use `dsctl capabilities workflow` to inspect exact-version "
                "workflow support."
            ),
        )


def bind_workflow_definition_wire(
    client: DolphinSchedulerClient,
    *,
    bindings: _WorkflowBindings | None = None,
) -> WorkflowDefinitionWire:
    """Bind source-compiled definition mutations to the selected client."""
    selected = bindings or _load_bindings(client.profile.ds_version, version_slug=None)
    return WorkflowDefinitionWire(
        ds_version=client.profile.ds_version,
        _recipe=selected.recipe,
        _programs=WORKFLOW_PROGRAMS.bind(
            selected.compiled, client.profile, http_client=client
        ),
    )


def bind_workflow_execution_wire(
    client: DolphinSchedulerClient,
    *,
    bindings: _WorkflowBindings | None = None,
) -> WorkflowExecutionWire:
    """Bind source-compiled execution without changing its result policy."""
    selected = bindings or _load_bindings(client.profile.ds_version, version_slug=None)
    return WorkflowExecutionWire(
        ds_version=client.profile.ds_version,
        result_kind=selected.recipe.execution_result,
        _recipe=selected.recipe,
        _programs=WORKFLOW_PROGRAMS.bind(
            selected.compiled, client.profile, http_client=client
        ),
    )


def _load_bindings(ds_version: str, *, version_slug: str | None) -> _WorkflowBindings:
    recipe = _RECIPE_BY_VERSION.get(ds_version)
    if recipe is None:
        msg = f"DS {ds_version} has no reviewed workflow recipe"
        raise WireContractError(msg)
    expected_slug = f"ds_{ds_version.replace('.', '_')}"
    if version_slug is not None and version_slug != expected_slug:
        msg = "Compiled workflows require the exact version slug"
        raise WireContractError(msg)
    return _WorkflowBindings(
        compiled=WORKFLOW_PROGRAMS.profile(ds_version), recipe=recipe
    )


def _native_code(native: NativeIdentity, *, ds_version: str) -> int:
    if isinstance(native, NativeCode):
        return native.value
    message = f"DS {ds_version} workflow operation requires a code-native identity"
    raise WireContractError(message)


def _native_id(native: NativeIdentity, *, ds_version: str) -> int:
    if isinstance(native, NativeId):
        return native.value
    message = f"DS {ds_version} workflow operation requires an id-native identity"
    raise WireContractError(message)


def _project_name(project: ProjectRef, *, ds_version: str) -> str:
    if isinstance(project.native, NativeId) and isinstance(project.name, str):
        name = project.name.strip()
        if name:
            return name
    message = f"DS {ds_version} legacy workflow route requires a resolved project name"
    raise WireContractError(message)


def _execution_action(
    *,
    command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
    start_node_list: Sequence[int] | Sequence[str] | None,
) -> str:
    if command_type == "COMPLEMENT_DATA":
        return "workflow.backfill"
    if start_node_list is not None:
        return "workflow.run-task"
    return "workflow.run"


def _execution_form_values(
    recipe: _WorkflowRecipe,
    *,
    scope: WorkflowScope,
    schedule_time: str,
    command_type: Literal["START_PROCESS", "COMPLEMENT_DATA"],
    worker_group: str,
    tenant_code: str,
    start_node_list: Sequence[int] | Sequence[str] | None,
    task_scope: str | None,
    failure_strategy: str,
    warning_type: str,
    workflow_instance_priority: str,
    warning_group_id: int | None,
    environment_code: int | None,
    start_params: str | None,
    execution_dry_run: bool,
    run_mode: str | None,
    expected_parallelism_number: int | None,
    complement_dependent_mode: str | None,
    all_level_dependent: bool | None,
    execution_order: str | None,
) -> JsonObject:
    """Compile stable executor inputs into the exact generated parameter bag."""
    identity_field = (
        "processDefinitionId"
        if recipe.native_identity == "id"
        else (
            "processDefinitionCode"
            if recipe.family == "process"
            else "workflowDefinitionCode"
        )
    )
    priority_field = (
        "workflowInstancePriority"
        if recipe.family == "workflow"
        else "processInstancePriority"
    )
    values: JsonObject = {
        identity_field: scope.workflow.native.value,
        "scheduleTime": schedule_time,
        "failureStrategy": failure_strategy,
        "startNodeList": _start_node_list(start_node_list),
        "taskDependType": task_depend_type(task_scope),
        "execType": command_type,
        "warningType": warning_type,
        "warningGroupId": (
            0
            if warning_group_id is None and recipe.warning_group_omission == "zero"
            else warning_group_id
        ),
        "runMode": run_mode,
        priority_field: workflow_instance_priority,
        "workerGroup": worker_group,
    }
    optional_values = (
        (recipe.execution_tenant_code, "tenantCode", tenant_code),
        (recipe.environment_code, "environmentCode", environment_code),
        # Executor controllers own the positive MAX_TASK_TIMEOUT fallback when
        # this optional field is omitted.  Definition timeout zero is unrelated
        # and is rejected by the executor service when sent here.
        (recipe.timeout, "timeout", None),
        (recipe.start_params, "startParams", start_params),
        (
            recipe.expected_parallelism_number,
            "expectedParallelismNumber",
            expected_parallelism_number,
        ),
        (
            recipe.execution_dry_run,
            "dryRun",
            1 if execution_dry_run else 0,
        ),
        (recipe.test_flag, "testFlag", 0),
        (
            recipe.complement_dependent_mode,
            "complementDependentMode",
            complement_dependent_mode,
        ),
        (recipe.definition_version, "version", scope.workflow.version),
        (recipe.all_level_dependent, "allLevelDependent", all_level_dependent),
        (recipe.execution_order, "executionOrder", execution_order),
    )
    values.update(
        {field: value for enabled, field, value in optional_values if enabled}
    )
    return values


def _start_node_list(
    values: Sequence[int] | Sequence[str] | None,
) -> str | None:
    if values is None:
        return None
    normalized = [str(value) for value in values]
    return ",".join(normalized) if normalized else None


def _normalize_start_nodes(
    values: Sequence[int] | Sequence[str] | None,
    *,
    identity: Literal["name", "code"],
    ds_version: str,
) -> tuple[int, ...] | tuple[str, ...] | None:
    if values is None:
        return None
    if identity == "name":
        if all(
            isinstance(value, str) and value.strip() and "," not in value
            for value in values
        ):
            return tuple(cast("str", value).strip() for value in values)
        expected = "non-empty comma-free task names"
    elif all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in values
    ):
        return tuple(cast("int", value) for value in values)
    else:
        expected = "positive task codes"
    message = f"DS {ds_version} startNodeList requires {expected}"
    raise WireContractError(message)


def task_depend_type(scope: str | None) -> str:
    """Translate the CLI task scope to DS's TaskDependType wire enum."""
    if scope is None:
        return "TASK_POST"
    normalized = scope.strip().lower()
    values = {"self": "TASK_ONLY", "pre": "TASK_PRE", "post": "TASK_POST"}
    try:
        return values[normalized]
    except KeyError as exc:
        message = f"Unsupported task execution scope: {scope!r}"
        raise ValueError(message) from exc


def _execution_receipt(
    payload: OpaqueGeneratedValue,
    *,
    result_kind: _ExecutionResult,
    ds_version: str,
) -> WorkflowExecutionReceipt:
    if result_kind == "none":
        if payload is None:
            return WorkflowExecutionReceipt()
    elif result_kind == "trigger-code":
        if isinstance(payload, int) and not isinstance(payload, bool) and payload > 0:
            return WorkflowExecutionReceipt(
                instance_resolution="pending", trigger_code=payload
            )
    elif isinstance(payload, list) and all(
        isinstance(item, int) and not isinstance(item, bool) and item > 0
        for item in payload
    ):
        return WorkflowExecutionReceipt(
            tuple(payload), "resolved" if payload else "unavailable"
        )
    raise projection_error(
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
        field="workflowInstanceIds",
        reason=f"executor result does not match exact {result_kind} wire",
    )


def _required_legacy_json_string(
    payload: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
) -> str:
    value = response_field(
        payload,
        name,
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
    )
    if isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
        field=name,
        reason="legacy workflow detail field is not a JSON string",
    )


def _optional_workflow_text_field(
    payload: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
) -> str | None:
    value = response_field(
        payload,
        name,
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
    )
    if value is None or isinstance(value, str):
        return None if value is None else str(value)
    raise projection_error(
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
        field=name,
        reason="legacy workflow detail field is not text or null",
    )


def _required_int_field(
    payload: OpaqueGeneratedValue, name: str, *, ds_version: str
) -> int:
    value = response_field(
        payload,
        name,
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
    )
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
        field=name,
        reason="workflow response field is not an integer",
    )


def _optional_text_field(
    payload: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
) -> str | None:
    value = response_field(
        payload,
        name,
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
    )
    if value is None or isinstance(value, str):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=WORKFLOW_RESOURCE,
        field=name,
        reason="dependent-task field is not text or null",
    )


__all__ = [
    "WORKFLOW_DOMAIN",
    "DependentLineageTaskSnapshot",
    "ExecutionScheduleTimeShape",
    "LegacyWorkflowDefinitionSnapshot",
    "PreparedLegacyWorkflowCreate",
    "PreparedLegacyWorkflowRelease",
    "PreparedLegacyWorkflowUpdate",
    "PreparedWorkflowCreate",
    "PreparedWorkflowExecution",
    "PreparedWorkflowRelease",
    "PreparedWorkflowUpdate",
    "WorkflowAdapter",
    "WorkflowDagSnapshot",
    "WorkflowDeleteLineageError",
    "WorkflowDomain",
    "WorkflowExecutionWire",
    "WorkflowLineageSnapshot",
    "WorkflowOperations",
    "bind_workflow_definition_wire",
    "bind_workflow_execution_wire",
    "workflow_execution_schedule_time_shape",
]
