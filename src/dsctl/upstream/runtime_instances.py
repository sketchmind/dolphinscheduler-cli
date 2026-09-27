from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from math import ceil
from typing import TYPE_CHECKING, TypeVar, cast

from dsctl.cli_surface import TASK_INSTANCE_RESOURCE, WORKFLOW_INSTANCE_RESOURCE
from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    DsctlError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.generated.runtime_instance_profiles import (
    RUNTIME_INSTANCE_PROFILES,
    RuntimeInstanceProfile,
)
from dsctl.upstream._compiled_workflow_runtime import WORKFLOW_PROGRAMS
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    NativeIdentity,
    ProjectRef,
    WorkflowRef,
)
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from dsctl.upstream.instance_time_filters import instance_time_filter_contract
from dsctl.upstream.mutation_outcomes import mutation_call
from dsctl.upstream.pagination import MAX_AUTO_EXHAUST_PAGES, observation_time
from dsctl.upstream.read_models import ReadPage, WorkflowScalarMetadata
from dsctl.upstream.resources import bind_task_resource_resolver
from dsctl.upstream.response_projection import projection_error
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_definition_wire import (
    WorkflowDagProjectionError,
    project_exact_workflow_dag,
)
from dsctl.upstream.task_logs import (
    TaskLogChunk,
    TaskLogTail,
)
from dsctl.upstream.task_logs import (
    tail_task_log as collect_task_log_tail,
)
from dsctl.upstream.task_logs import (
    window_task_log as collect_task_log_window,
)
from dsctl.upstream.wire import (
    WireContractError,
    WireRequest,
)
from dsctl.upstream.workflows import (
    WorkflowAdapter,
    WorkflowOperations,
    task_depend_type,
)

if TYPE_CHECKING:
    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._compiled_workflow_runtime import WorkflowPrimitive
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.definition_reads import DefinitionReads
    from dsctl.upstream.protocol import (
        DataSourceOperations,
        StringEnumValue,
        TaskResourceResolver,
        WorkflowDagRecord,
    )
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire
    from dsctl.upstream.wire import PreparedCompiledWireCall


_TASK_SCAN_PAGE_SIZE = 100
_TASK_CODE_BATCH_SIZE = 100
_SIGNED_LONG_MAX = 2**63 - 1
_TASK_INSTANCE_NOT_EXIST = 10008
_ItemT = TypeVar("_ItemT")


@dataclass(frozen=True)
class WorkflowInstanceSnapshot:
    """Stable instance projection that preserves the profile's native ids."""

    ds_version: str
    id: int
    project: ProjectRef
    workflow_native: NativeIdentity | None
    workflowDefinitionVersion: int  # noqa: N815
    state: StringEnumValue | str | None
    recovery: StringEnumValue | str | None
    startTime: str | None  # noqa: N815
    endTime: str | None  # noqa: N815
    runTimes: int  # noqa: N815
    name: str | None
    host: str | None
    commandType: StringEnumValue | str | None  # noqa: N815
    taskDependType: StringEnumValue | str | None  # noqa: N815
    failureStrategy: StringEnumValue | str | None  # noqa: N815
    warningType: StringEnumValue | str | None  # noqa: N815
    scheduleTime: str | None  # noqa: N815
    executorId: int  # noqa: N815
    executorName: str | None  # noqa: N815
    tenantCode: str | None  # noqa: N815
    queue: str | None
    duration: str | None
    workflowInstancePriority: StringEnumValue | str | None  # noqa: N815
    workerGroup: str | None  # noqa: N815
    environmentCode: int | None  # noqa: N815
    timeout: int
    dryRun: int  # noqa: N815
    restartTime: str | None  # noqa: N815
    dagData: WorkflowDagRecord | None  # noqa: N815
    processInstanceJson: str | None = None  # noqa: N815
    locations: str | None = None
    connects: str | None = None
    summary: bool = False
    tenant_id: int | None = None

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Return a real project code, never an id masquerading as a code."""
        return (
            self.project.native.value
            if isinstance(self.project.native, NativeCode)
            else None
        )

    @property
    def projectId(self) -> int | None:  # noqa: N802
        """Return the native project id on the 1.3 profile only."""
        return (
            self.project.native.value
            if isinstance(self.project.native, NativeId)
            else None
        )

    @property
    def workflowDefinitionCode(self) -> int | None:  # noqa: N802
        """Return a real definition code on code-native profiles."""
        native = self.workflow_native
        return native.value if isinstance(native, NativeCode) else None

    @property
    def workflowDefinitionId(self) -> int | None:  # noqa: N802
        """Return the native definition id on the 1.3 profile only."""
        native = self.workflow_native
        return native.value if isinstance(native, NativeId) else None

    def to_data(self) -> JsonObject:
        """Render one exact-identity CLI object without fabricated aliases."""
        data: JsonObject = {
            "id": self.id,
            "workflowDefinitionVersion": self.workflowDefinitionVersion,
            "projectName": self.project.name,
            "state": enum_value(self.state),
            "recovery": enum_value(self.recovery),
            "startTime": self.startTime,
            "endTime": self.endTime,
            "runTimes": self.runTimes,
            "name": self.name,
            "host": self.host,
            "commandType": enum_value(self.commandType),
            "taskDependType": enum_value(self.taskDependType),
            "failureStrategy": enum_value(self.failureStrategy),
            "warningType": enum_value(self.warningType),
            "scheduleTime": self.scheduleTime,
            "executorId": self.executorId,
            "executorName": self.executorName,
            "tenantCode": self.tenantCode,
            "queue": self.queue,
            "duration": self.duration,
            "workflowInstancePriority": enum_value(self.workflowInstancePriority),
            "workerGroup": self.workerGroup,
            "environmentCode": self.environmentCode,
            "timeout": self.timeout,
            "dryRun": self.dryRun,
            "restartTime": self.restartTime,
        }
        if self.summary:
            data.pop("queue")
        project_field = (
            "projectId" if isinstance(self.project.native, NativeId) else "projectCode"
        )
        data[project_field] = self.project.native.value
        if RUNTIME_INSTANCE_PROFILES[self.ds_version].definition_identity == "id":
            data["workflowDefinitionId"] = self.workflowDefinitionId
        else:
            data["workflowDefinitionCode"] = self.workflowDefinitionCode
        return data


@dataclass(frozen=True)
class TaskInstanceSnapshot:
    """Stable task-instance projection above id/code and map/entity dialects."""

    ds_version: str
    id: int
    project: ProjectRef
    name: str | None
    taskType: str | None  # noqa: N815
    workflowInstanceId: int  # noqa: N815
    workflowInstanceName: str | None  # noqa: N815
    taskCode: int | None  # noqa: N815
    taskDefinitionVersion: int | None  # noqa: N815
    workflowDefinitionName: str | None  # noqa: N815
    state: StringEnumValue | str | None
    firstSubmitTime: str | None  # noqa: N815
    submitTime: str | None  # noqa: N815
    startTime: str | None  # noqa: N815
    endTime: str | None  # noqa: N815
    host: str | None
    logPath: str | None  # noqa: N815
    retryTimes: int  # noqa: N815
    duration: str | None
    executorName: str | None  # noqa: N815
    workerGroup: str | None  # noqa: N815
    environmentCode: int | None  # noqa: N815
    delayTime: int  # noqa: N815
    taskParams: str | None  # noqa: N815
    dryRun: int  # noqa: N815
    taskGroupId: int  # noqa: N815
    taskExecuteType: StringEnumValue | str | None  # noqa: N815

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        """Return a real project code on code-native profiles."""
        return (
            self.project.native.value
            if isinstance(self.project.native, NativeCode)
            else None
        )

    @property
    def projectId(self) -> int | None:  # noqa: N802
        """Return the native project id on the 1.3 profile only."""
        return (
            self.project.native.value
            if isinstance(self.project.native, NativeId)
            else None
        )

    @property
    def processDefinitionName(self) -> str | None:  # noqa: N802
        """Retain the historical protocol alias during service migration."""
        return self.workflowDefinitionName

    def to_data(self) -> JsonObject:
        """Render one exact-identity CLI object without fabricated task codes."""
        data: JsonObject = {
            "id": self.id,
            "name": self.name,
            "taskType": self.taskType,
            "workflowInstanceId": self.workflowInstanceId,
            "workflowInstanceName": self.workflowInstanceName,
            "projectName": self.project.name,
            "taskCode": self.taskCode,
            "taskDefinitionVersion": self.taskDefinitionVersion,
            "workflowDefinitionName": self.workflowDefinitionName,
            "state": enum_value(self.state),
            "firstSubmitTime": self.firstSubmitTime,
            "submitTime": self.submitTime,
            "startTime": self.startTime,
            "endTime": self.endTime,
            "host": self.host,
            "logPath": self.logPath,
            "retryTimes": self.retryTimes,
            "duration": self.duration,
            "executorName": self.executorName,
            "workerGroup": self.workerGroup,
            "environmentCode": self.environmentCode,
            "delayTime": self.delayTime,
            "taskParams": self.taskParams,
            "dryRun": self.dryRun,
            "taskGroupId": self.taskGroupId,
            "taskExecuteType": enum_value(self.taskExecuteType),
        }
        project_field = (
            "projectId" if isinstance(self.project.native, NativeId) else "projectCode"
        )
        data[project_field] = self.project.native.value
        return data


@dataclass(frozen=True)
class LocatedWorkflowInstance:
    """One instance read directly inside its selected project scope."""

    project: ProjectRef
    instance: WorkflowInstanceSnapshot


@dataclass(frozen=True)
class LocatedTaskInstance:
    """One task selected inside a project and optional workflow scope."""

    project: ProjectRef
    workflow_instance: WorkflowInstanceSnapshot | None
    task: TaskInstanceSnapshot


@dataclass(frozen=True)
class WorkflowInstanceListing:
    """Resolved project/optional definition scope plus one instance page."""

    project: ProjectRef
    workflow: WorkflowRef | None
    page: ReadPage[WorkflowInstanceSnapshot]


@dataclass(frozen=True)
class TaskInstanceListing:
    """Resolved project scope plus one task-instance page."""

    project: ProjectRef
    workflow_instance: WorkflowInstanceSnapshot | None
    page: ReadPage[TaskInstanceSnapshot]


@dataclass(frozen=True)
class _TaskPageRead:
    """One task page plus pagination metadata exactly present on the wire."""

    page: ReadPage[TaskInstanceSnapshot]
    reported_page: int | None
    reported_page_size: int | None
    reported_total: int | None
    reported_total_pages: int | None


@dataclass
class _TaskScanBranch:
    """Bounded progress for one task execute-type partition."""

    task_execute_type: str | None
    next_page: int = 1
    complete: bool = False
    pages: list[JsonObject] = field(default_factory=list)


@dataclass(frozen=True)
class WorkflowMutationSnapshot:
    """Definition identity returned after one workflow-instance edit."""

    code: int
    name: str | None
    version: int


@dataclass(frozen=True)
class PreparedLegacyWorkflowInstanceUpdate:
    """One exact DS 1.3.9 instance-update request captured before mutation."""

    request: WireRequest
    _wire_call: PreparedCompiledWireCall


@dataclass(frozen=True)
class PreparedWorkflowInstanceUpdate:
    """One exact modern instance update prepared without sending a request."""

    _wire_call: PreparedCompiledWireCall

    @property
    def request(self) -> WireRequest:
        """Return a detached preview of the request that will be applied."""
        return self._wire_call.request


@dataclass(frozen=True)
class RuntimeInstanceDomain:
    """The complete caller-oriented workflow/task runtime instance module."""

    instances: RuntimeInstanceOperations
    legacy_workflows: WorkflowOperations | None = None
    workflows: WorkflowOperations | None = None
    task_resource_resolver: TaskResourceResolver | None = None
    task_datasources: DataSourceOperations | None = None
    task_definitions: TaskDefinitionWire | None = None


@dataclass(frozen=True)
class RuntimeInstanceContractFeatures:
    """Selected-version option facts shared by runtime and CLI schema."""

    task_workflow_instance_name: bool
    task_code: bool
    task_execute_type: bool
    instance_dag_edit_requires_sync: bool


_RECIPE_BY_VERSION = RUNTIME_INSTANCE_PROFILES


def runtime_instance_contract_features(
    ds_version: str,
) -> RuntimeInstanceContractFeatures:
    """Return exact task-instance list option availability for one profile."""
    try:
        recipe = _RECIPE_BY_VERSION[ds_version]
    except KeyError as exc:
        message = f"DS {ds_version} has no reviewed runtime-instance adapter"
        raise UnsupportedFeatureError(message) from exc
    return RuntimeInstanceContractFeatures(
        task_workflow_instance_name=recipe.has_task_workflow_name,
        task_code=recipe.has_task_code_filter,
        task_execute_type=recipe.has_task_execute_type,
        instance_dag_edit_requires_sync=recipe.instance_dag_edit_requires_sync,
    )


def workflow_instance_stop_result_may_be_unknown(ds_version: str) -> bool:
    """Return whether a generic stop failure can follow an applied control."""
    try:
        recipe = _RECIPE_BY_VERSION[ds_version]
    except KeyError as exc:
        message = f"DS {ds_version} has no reviewed runtime-instance adapter"
        raise UnsupportedFeatureError(message) from exc
    return recipe.stop_result_may_be_unknown


class RuntimeInstanceAdapter:
    """Compiled runtime-instance adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Select the existing instance recipe and exact compiled profile."""
        try:
            self._recipe = _RECIPE_BY_VERSION[ds_version]
        except KeyError as exc:
            message = f"DS {ds_version} has no reviewed runtime-instance adapter"
            raise UnsupportedFeatureError(message) from exc
        self._profile = WORKFLOW_PROGRAMS.profile(ds_version)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> RuntimeInstanceAdapter:
        """Return the adapter for one explicitly reviewed exact version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> RuntimeInstanceDomain:
        """Bind the instance wire and exact definition-resolution dependency."""
        read_adapter = (
            IdNativeReadAdapter()
            if self.ds_version == "1.3.9"
            else CodeNativeReadAdapter.for_version(self.ds_version)
        )
        definitions = read_adapter.bind_read(
            profile,
            http_client=http_client,
        ).definitions
        workflow_domain = WorkflowAdapter.for_version(self.ds_version).bind(
            profile,
            http_client=http_client,
        )
        return RuntimeInstanceDomain(
            instances=RuntimeInstanceOperations(
                programs=WORKFLOW_PROGRAMS.bind(
                    self._profile, profile, http_client=http_client
                ),
                recipe=self._recipe,
                definitions=definitions,
            ),
            legacy_workflows=(
                workflow_domain.workflows if self.ds_version == "1.3.9" else None
            ),
            workflows=workflow_domain.workflows,
            task_resource_resolver=(
                workflow_domain.task_resource_resolver
                or bind_task_resource_resolver(
                    self.ds_version,
                    profile,
                    http_client=http_client,
                )
            ),
            task_datasources=workflow_domain.task_datasources,
            task_definitions=workflow_domain.task_definitions,
        )


RUNTIME_INSTANCE_DOMAIN = BoundDomain[RuntimeInstanceDomain](
    name="runtime-instance",
    adapter_for_version=RuntimeInstanceAdapter.for_version,
)


@dataclass
class RuntimeInstanceOperations:
    """Deep project-scoped runtime module consumed by both instance services."""

    programs: BoundCompiledPrograms[WorkflowPrimitive]
    recipe: RuntimeInstanceProfile
    definitions: DefinitionReads

    @property
    def ds_version(self) -> str:
        """Return the exact selected profile version."""
        return self.programs.ds_version

    def list_workflow_instances(
        self,
        *,
        project_selector: str,
        workflow_selector: str | None,
        page_no: int,
        page_size: int,
        search: str | None,
        executor: str | None,
        host: str | None,
        start_time: str | None,
        end_time: str | None,
        state: str | None,
    ) -> WorkflowInstanceListing:
        """Return one direct project-scoped runtime page."""
        instance_time_filter_contract(
            self.ds_version, "workflow-instance.list"
        ).validate(start=start_time, end=end_time)
        project = self.definitions.resolve_project(project_selector)
        workflow = (
            None
            if workflow_selector is None
            else self.definitions.resolve_workflow(
                project_selector,
                workflow_selector,
            ).workflow
        )
        return WorkflowInstanceListing(
            project,
            workflow,
            self._workflow_page(
                project,
                workflow=workflow,
                page_no=page_no,
                page_size=page_size,
                search=search,
                executor=executor,
                host=host,
                start_time=start_time,
                end_time=end_time,
                state=state,
            ),
        )

    def get_workflow_instance(
        self,
        *,
        project_selector: str,
        workflow_instance_id: int,
    ) -> LocatedWorkflowInstance:
        """Read one instance directly through its selected project route."""
        project = self.definitions.resolve_project(project_selector)
        return LocatedWorkflowInstance(
            project,
            self._workflow_detail(project, workflow_instance_id),
        )

    def list_task_instances(
        self,
        *,
        project_selector: str,
        workflow_instance_id: int | None,
        workflow_instance_name: str | None,
        page_no: int,
        page_size: int,
        search: str | None,
        task_name: str | None,
        task_code: int | None,
        executor: str | None,
        state: str | None,
        host: str | None,
        start_time: str | None,
        end_time: str | None,
        task_execute_type: str | None,
    ) -> TaskInstanceListing:
        """Return one project-scoped task page with exact option projection."""
        self._require_task_list_facets(
            workflow_instance_name=workflow_instance_name,
            task_code=task_code,
            task_execute_type=task_execute_type,
        )
        instance_time_filter_contract(self.ds_version, "task-instance.list").validate(
            start=start_time, end=end_time
        )
        project = self.definitions.resolve_project(project_selector)
        located = (
            None
            if workflow_instance_id is None
            else LocatedWorkflowInstance(
                project,
                self._workflow_detail(project, workflow_instance_id),
            )
        )
        page = self._task_page(
            project,
            workflow_instance_id=workflow_instance_id,
            workflow_instance_name=workflow_instance_name,
            workflow_definition_name=None,
            page_no=page_no,
            page_size=page_size,
            search=search,
            task_name=task_name,
            task_code=task_code,
            executor=executor,
            state=state,
            host=host,
            start_time=start_time,
            end_time=end_time,
            task_execute_type=task_execute_type,
        ).page
        return TaskInstanceListing(
            project,
            None if located is None else located.instance,
            page,
        )

    def get_task_instance(
        self,
        *,
        project_selector: str,
        task_instance_id: int,
        workflow_instance_id: int | None,
    ) -> LocatedTaskInstance:
        """Resolve a task id inside a workflow or directly inside one project."""
        located_workflow = (
            None
            if workflow_instance_id is None
            else self.get_workflow_instance(
                project_selector=project_selector,
                workflow_instance_id=workflow_instance_id,
            )
        )
        project = (
            self.definitions.resolve_project(project_selector)
            if located_workflow is None
            else located_workflow.project
        )
        execute_types: tuple[str | None, ...] = (
            ("BATCH", "STREAM")
            if workflow_instance_id is None and self.recipe.has_task_execute_type
            else (None,)
        )
        branches = [_TaskScanBranch(value) for value in execute_types]
        started_at = observation_time()
        while any(not branch.complete for branch in branches):
            for branch in branches:
                if branch.complete:
                    continue
                if branch.next_page > MAX_AUTO_EXHAUST_PAGES:
                    raise self._task_scan_incomplete(
                        task_instance_id=task_instance_id,
                        workflow_instance_id=workflow_instance_id,
                        project_selector=project_selector,
                        branch=branch,
                        branches=branches,
                        started_at=started_at,
                        reason="page_safety_limit",
                    )
                requested_page = branch.next_page
                try:
                    read = self._task_page(
                        project,
                        workflow_instance_id=workflow_instance_id,
                        workflow_instance_name=None,
                        workflow_definition_name=None,
                        page_no=requested_page,
                        page_size=_TASK_SCAN_PAGE_SIZE,
                        search=None,
                        task_name=None,
                        task_code=None,
                        executor=None,
                        state=None,
                        host=None,
                        start_time=None,
                        end_time=None,
                        task_execute_type=branch.task_execute_type,
                    )
                except DsctlError as error:
                    error.details.setdefault(
                        "coverage",
                        self._task_scan_coverage(
                            branches,
                            started_at=started_at,
                            complete=False,
                        ),
                    )
                    raise
                items = list(read.page.totalList or ())
                branch.pages.append(
                    {
                        "requested_page": requested_page,
                        "reported_page": read.reported_page,
                        "reported_page_size": read.reported_page_size,
                        "rows": len(items),
                        "total": read.reported_total,
                        "total_pages": read.reported_total_pages,
                    }
                )
                matches = [item for item in items if item.id == task_instance_id]
                if len(matches) == 1:
                    return LocatedTaskInstance(
                        project,
                        None if located_workflow is None else located_workflow.instance,
                        matches[0],
                    )
                if len(matches) > 1:
                    message = (
                        "Task instance id matched more than once inside one page scope"
                    )
                    raise ApiTransportError(
                        message,
                        details={
                            "task_instance_id": task_instance_id,
                            "workflow_instance_id": workflow_instance_id,
                            "task_execute_type": branch.task_execute_type,
                            "match_count": len(matches),
                            "coverage": self._task_scan_coverage(
                                branches,
                                started_at=started_at,
                                complete=False,
                            ),
                        },
                    )
                if (
                    read.reported_total_pages is None
                    or read.reported_page is None
                    or read.reported_page != requested_page
                    or (
                        read.reported_total_pages == 0
                        and (requested_page != 1 or bool(items))
                    )
                    or (
                        read.reported_total_pages > 0
                        and read.reported_total_pages < requested_page
                    )
                ):
                    raise self._task_scan_incomplete(
                        task_instance_id=task_instance_id,
                        workflow_instance_id=workflow_instance_id,
                        project_selector=project_selector,
                        branch=branch,
                        branches=branches,
                        started_at=started_at,
                        reason="pagination_metadata_incomplete",
                    )
                branch.complete = (
                    read.reported_total_pages == 0
                    or requested_page >= read.reported_total_pages
                )
                branch.next_page += 1
        raise ApiResultError(
            result_code=_TASK_INSTANCE_NOT_EXIST,
            result_message=f"task instance {task_instance_id} does not exist",
            details={
                "coverage": self._task_scan_coverage(
                    branches,
                    started_at=started_at,
                    complete=True,
                )
            },
        )

    @staticmethod
    def _task_scan_coverage(
        branches: Sequence[_TaskScanBranch],
        *,
        started_at: str,
        complete: bool,
    ) -> JsonObject:
        """Describe exactly how far one bounded task-id lookup progressed."""
        branch_data: list[JsonObject] = [
            {
                "task_execute_type": branch.task_execute_type,
                "pages_read": len(branch.pages),
                "rows_read": sum(cast("int", page["rows"]) for page in branch.pages),
                "scope_complete": branch.complete,
                "pages": list(branch.pages),
            }
            for branch in branches
        ]
        return {
            "scope": "task_instance_id_lookup",
            "requested_page_size": _TASK_SCAN_PAGE_SIZE,
            "max_pages_per_execute_type": MAX_AUTO_EXHAUST_PAGES,
            "pages_read": sum(len(branch.pages) for branch in branches),
            "rows_read": sum(
                sum(cast("int", page["rows"]) for page in branch.pages)
                for branch in branches
            ),
            "branches": branch_data,
            "scope_complete": complete,
            "atomic_snapshot": False,
            "observation_started_at": started_at,
            "observation_finished_at": observation_time(),
        }

    def _task_scan_incomplete(
        self,
        *,
        task_instance_id: int,
        workflow_instance_id: int | None,
        project_selector: str,
        branch: _TaskScanBranch,
        branches: Sequence[_TaskScanBranch],
        started_at: str,
        reason: str,
    ) -> UserInputError:
        """Return an actionable error without claiming an incomplete miss."""
        details: JsonObject = {
            "task_instance_id": task_instance_id,
            "project": project_selector,
            "task_execute_type": branch.task_execute_type,
            "reason": reason,
            "max_pages": MAX_AUTO_EXHAUST_PAGES,
            "coverage": self._task_scan_coverage(
                branches,
                started_at=started_at,
                complete=False,
            ),
        }
        if workflow_instance_id is not None:
            details["workflow_instance_id"] = workflow_instance_id
        return UserInputError(
            "Task instance lookup stopped before the project scope was fully scanned",
            details=details,
            suggestion=(
                "If this task belongs to a workflow instance, retry with its known "
                "`--workflow-instance WORKFLOW_INSTANCE` selector. Otherwise inspect "
                "bounded pages with `dsctl task-instance list --project PROJECT` "
                "and `--execute-type STREAM` where supported."
            ),
        )

    def task_page_for_workflow(
        self,
        located: LocatedWorkflowInstance,
        *,
        page_no: int,
        page_size: int,
    ) -> ReadPage[TaskInstanceSnapshot]:
        """Return one task page without repeating workflow ownership discovery."""
        return self._task_page(
            located.project,
            workflow_instance_id=located.instance.id,
            workflow_instance_name=None,
            workflow_definition_name=None,
            page_no=page_no,
            page_size=page_size,
            search=None,
            task_name=None,
            task_code=None,
            executor=None,
            state=None,
            host=None,
            start_time=None,
            end_time=None,
            task_execute_type=None,
        ).page

    def parent_workflow_instance_id(
        self,
        located: LocatedWorkflowInstance,
    ) -> int | None:
        """Return one normalized parent id from view or map response shapes."""
        payload = self.programs.call(
            "instance_parent",
            {**self._project_args(located.project), "subId": located.instance.id},
        )
        return _optional_int(
            payload,
            ("parentWorkflowInstance",),
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
        )

    def sub_workflow_instance_id(self, located: LocatedTaskInstance) -> int | None:
        """Return one normalized child id from view or map response shapes."""
        payload = self.programs.call(
            "instance_sub",
            {**self._project_args(located.project), "taskId": located.task.id},
        )
        return _optional_int(
            payload,
            (self.recipe.workflow_sub_result_key,),
            ds_version=self.ds_version,
            resource=TASK_INSTANCE_RESOURCE,
        )

    def _require_task_list_facets(
        self,
        *,
        workflow_instance_name: str | None,
        task_code: int | None,
        task_execute_type: str | None,
    ) -> None:
        """Reject unavailable exact-version filters before selector I/O."""
        recipe = self.recipe
        if workflow_instance_name is not None and not recipe.has_task_workflow_name:
            raise _unsupported_option(
                self.ds_version,
                "task-instance.list",
                "--workflow-instance-name",
                introduced_in="2.0.0",
            )
        if task_code is not None and not recipe.has_task_code_filter:
            raise _unsupported_option(
                self.ds_version,
                "task-instance.list",
                "--task-code",
                introduced_in="3.2.0",
            )
        if task_execute_type is not None and not recipe.has_task_execute_type:
            raise _unsupported_option(
                self.ds_version,
                "task-instance.list",
                "--execute-type",
                introduced_in="3.1.0",
            )

    def tail_task_log(
        self,
        *,
        task_instance_id: int,
        max_lines: int,
    ) -> TaskLogTail:
        """Return a canonical tail while hiding exact logger pagination."""
        return collect_task_log_tail(
            task_instance_id=task_instance_id,
            max_lines=max_lines,
            read_chunk=self._read_log_chunk,
        )

    def window_task_log(
        self,
        *,
        task_instance_id: int,
        start_line: int,
        limit: int,
    ) -> TaskLogTail:
        """Read a source-line window using the exact logger cursor."""
        return collect_task_log_window(
            task_instance_id=task_instance_id,
            start_line=start_line,
            limit=limit,
            read_chunk=self._read_log_chunk,
        )

    def _read_log_chunk(
        self,
        *,
        task_instance_id: int,
        skip_line_num: int,
        limit: int,
    ) -> TaskLogChunk:
        """Normalize legacy string and modern ResponseTaskLog payloads."""
        epoch = self.recipe.log_epoch
        if epoch in {"query-log-string", "log-detail-string"}:
            string_response = True
        elif epoch in {"log-detail-record", "query-log-record"}:
            string_response = False
        else:
            raise _projection_error(
                self.ds_version,
                TASK_INSTANCE_RESOURCE,
                "log",
                f"unsupported logger epoch {epoch!r}",
            )
        payload = self.programs.call(
            "task_log",
            {
                "taskInstanceId": task_instance_id,
                "skipLineNum": skip_line_num,
                "limit": limit,
            },
        )
        if string_response:
            if not isinstance(payload, str):
                raise _projection_error(
                    self.ds_version,
                    TASK_INSTANCE_RESOURCE,
                    "log",
                    "legacy logger payload is not text",
                )
            return self._normalize_log_chunk(
                payload,
                first_page=skip_line_num == 0,
            )
        _required_int(
            payload,
            ("lineNum",),
            ds_version=self.ds_version,
            resource=TASK_INSTANCE_RESOURCE,
        )
        message = _optional_text(
            payload,
            ("message",),
            ds_version=self.ds_version,
            resource=TASK_INSTANCE_RESOURCE,
        )
        return self._normalize_log_chunk(
            message,
            first_page=skip_line_num == 0,
        )

    def _normalize_log_chunk(
        self,
        message: str | None,
        *,
        first_page: bool,
    ) -> TaskLogChunk:
        body = message or ""
        header_lines = 0
        if first_page and self.recipe.log_first_page_header:
            if not body.startswith("[LOG-PATH]: ") or "\n" not in body:
                raise _projection_error(
                    self.ds_version,
                    TASK_INSTANCE_RESOURCE,
                    "log",
                    "logger first-page path header is missing or malformed",
                )
            _, body = body.split("\n", maxsplit=1)
            header_lines = 1
        if not body:
            return TaskLogChunk(
                message=message, cursor_advance=0, eof=True, header_lines=header_lines
            )
        cursor_advance = body.count("\r\n")
        if cursor_advance <= 0:
            raise _projection_error(
                self.ds_version,
                TASK_INSTANCE_RESOURCE,
                "log",
                "nonempty logger body has no complete source lines",
            )
        return TaskLogChunk(
            message=message,
            cursor_advance=cursor_advance,
            eof=False,
            header_lines=header_lines,
        )

    def control_workflow_instance(
        self,
        located: LocatedWorkflowInstance,
        *,
        execute_type: str,
    ) -> None:
        """Dispatch one exact main-controller workflow action without retries."""
        field = (
            "workflowInstanceId"
            if self.recipe.family == "workflow"
            else "processInstanceId"
        )
        prepared = self.programs.prepare(
            "instance_control",
            {
                **self._project_args(located.project),
                field: located.instance.id,
                "executeType": execute_type,
            },
        )

        def dispatch() -> OpaqueGeneratedValue:
            return self.programs.execute("instance_control", prepared).payload

        _dispatch_mutation(
            dispatch,
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            operation=execute_type.casefold(),
        )

    def execute_task(
        self,
        located: LocatedWorkflowInstance,
        *,
        task_code: int,
        scope: str,
    ) -> None:
        """Execute one task only on profiles with the 3.2+ main route."""
        if not self.recipe.execute_task:
            raise _unsupported(
                self.ds_version,
                "workflow-instance.execute-task",
                introduced_in="3.2.0",
            )
        field = (
            "workflowInstanceId"
            if self.recipe.family == "workflow"
            else "processInstanceId"
        )
        prepared = self.programs.prepare(
            "instance_execute_task",
            {
                **self._project_args(located.project),
                field: located.instance.id,
                "startNodeList": str(task_code),
                "taskDependType": task_depend_type(scope),
            },
        )
        _dispatch_mutation(
            lambda: self.programs.execute("instance_execute_task", prepared).payload,
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            operation="execute-task",
        )

    def task_action(self, located: LocatedTaskInstance, *, action: str) -> None:
        """Dispatch one exact task mutation and reject terminal absences locally."""
        supported = {
            "force-success": self.recipe.force_success,
            "savepoint": self.recipe.savepoint,
            "stop": self.recipe.stop_task,
        }
        introduced = {
            "force-success": "2.0.0",
            "savepoint": "3.1.0",
            "stop": "3.1.0",
        }
        primitives: dict[str, WorkflowPrimitive] = {
            "force-success": "task_instance_force_success",
            "savepoint": "task_instance_savepoint",
            "stop": "task_instance_stop",
        }
        if action not in supported:
            message = f"Unknown task-instance action {action!r}"
            raise ValueError(message)
        if not supported[action]:
            raise _unsupported(
                self.ds_version,
                f"task-instance.{action}",
                introduced_in=introduced[action],
            )
        _dispatch_mutation(
            lambda: self.programs.call(
                primitives[action],
                {**self._project_args(located.project), "id": located.task.id},
            ),
            ds_version=self.ds_version,
            resource=TASK_INSTANCE_RESOURCE,
            operation=action,
        )

    def generate_task_codes(self, project: ProjectRef, *, count: int) -> list[int]:
        """Allocate validated task codes in bounded exact-endpoint batches."""
        if self.recipe.update_shape == "legacy":
            raise _unsupported(
                self.ds_version,
                "workflow-instance.edit",
                introduced_in="2.0.0",
                limited=True,
            )
        codes: list[int] = []
        while len(codes) < count:
            requested = min(_TASK_CODE_BATCH_SIZE, count - len(codes))
            payload = self.programs.call(
                "task_code_allocate",
                {**self._project_args(project), "genNum": requested},
            )
            if not isinstance(payload, list) or len(payload) != requested:
                raise _projection_error(
                    self.ds_version,
                    WORKFLOW_INSTANCE_RESOURCE,
                    "taskCodes",
                    "task-code allocation returned an unexpected item count",
                )
            if any(
                not isinstance(code, int)
                or isinstance(code, bool)
                or not 0 < code <= _SIGNED_LONG_MAX
                for code in payload
            ):
                raise _projection_error(
                    self.ds_version,
                    WORKFLOW_INSTANCE_RESOURCE,
                    "taskCodes",
                    "task-code allocation returned a non-positive signed long",
                )
            if len(set(payload)) != len(payload) or set(codes).intersection(payload):
                raise _projection_error(
                    self.ds_version,
                    WORKFLOW_INSTANCE_RESOURCE,
                    "taskCodes",
                    "task-code allocation returned duplicate codes",
                )
            codes.extend(payload)
        return codes

    def update_workflow_instance(
        self,
        located: LocatedWorkflowInstance,
        *,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None,
        locations: str | None,
        timeout: int | None,
        schedule_time: str | None = None,
    ) -> WorkflowMutationSnapshot | None:
        """Apply the exact modern edit form, preserving legacy tenant state."""
        return self.apply_workflow_instance_update(
            self.prepare_workflow_instance_update(
                located,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                sync_define=sync_define,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                schedule_time=schedule_time,
            )
        )

    def prepare_workflow_instance_update(
        self,
        located: LocatedWorkflowInstance,
        *,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None,
        locations: str | None,
        timeout: int | None,
        schedule_time: str | None = None,
    ) -> PreparedWorkflowInstanceUpdate:
        """Prepare the exact modern form, including required tenant preservation."""
        if self.recipe.update_shape == "legacy":
            raise _unsupported(
                self.ds_version,
                "workflow-instance.edit",
                introduced_in="2.0.0",
                limited=True,
            )
        values: JsonObject = {
            "taskRelationJson": task_relation_json,
            "taskDefinitionJson": task_definition_json,
            "scheduleTime": schedule_time,
            "syncDefine": sync_define,
            "globalParams": global_params,
            "locations": locations,
            "timeout": timeout,
        }
        if self.recipe.update_shape == "tenant":
            values["tenantCode"] = self._tenant_code_for_update(located)
        prepared = self.programs.prepare(
            "instance_update",
            {
                **self._project_args(located.project),
                "id": located.instance.id,
                **values,
            },
        )
        return PreparedWorkflowInstanceUpdate(_wire_call=prepared)

    def _tenant_code_for_update(
        self,
        located: LocatedWorkflowInstance,
    ) -> str:
        direct = located.instance.tenantCode
        if direct is not None and direct.strip():
            return direct

        tenant_id = located.instance.tenant_id
        if tenant_id == -1:
            # ProcessInstanceService maps this native sentinel back from the
            # required tenantCode value without resolving a tenant record.
            return "default"
        if tenant_id is None or tenant_id <= 0:
            raise _projection_error(
                self.ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "tenantId",
                (
                    "instance detail omitted the tenant identity required to "
                    "recover tenantCode"
                ),
            )
        workflow_native = located.instance.workflow_native
        if not isinstance(workflow_native, NativeCode):
            raise _projection_error(
                self.ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "workflowDefinitionCode",
                "tenant-bearing instance edit requires a workflow definition code",
            )
        workflow = self.definitions.resolve_workflow_by_code(
            located.project,
            workflow_native.value,
        )
        if workflow.view.tenant_id != tenant_id:
            raise _projection_error(
                self.ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "tenantCode",
                (
                    "instance tenantId does not match the workflow definition "
                    "tenantId; refusing to substitute a tenant code"
                ),
            )
        tenant_code = workflow.view.tenant_code
        if tenant_code is None or not tenant_code.strip():
            raise _projection_error(
                self.ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "tenantCode",
                "matching workflow definition omitted the required tenant code",
            )
        return tenant_code

    def apply_workflow_instance_update(
        self,
        prepared: PreparedWorkflowInstanceUpdate,
    ) -> WorkflowMutationSnapshot | None:
        """Apply one prepared modern instance update and project its definition."""
        payload = _dispatch_mutation(
            lambda: (
                self.programs.execute("instance_update", prepared._wire_call).payload
            ),
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            operation="edit",
        )
        request_form = prepared.request.form
        if (
            payload is None
            and self.recipe.instance_dag_edit_requires_sync
            and request_form is not None
            and request_form.get("syncDefine") is False
        ):
            # Native scalar-only update has no definition DATA_LIST. The caller
            # must resolve identity from its mandatory post-update instance read.
            return None
        return WorkflowMutationSnapshot(
            code=_required_int(
                payload,
                ("code",),
                ds_version=self.ds_version,
                resource=WORKFLOW_INSTANCE_RESOURCE,
            ),
            name=_optional_text(
                payload,
                ("name",),
                ds_version=self.ds_version,
                resource=WORKFLOW_INSTANCE_RESOURCE,
            ),
            version=_required_int(
                payload,
                ("version",),
                ds_version=self.ds_version,
                resource=WORKFLOW_INSTANCE_RESOURCE,
            ),
        )

    def prepare_legacy_workflow_instance_update(
        self,
        located: LocatedWorkflowInstance,
        *,
        process_instance_json: str,
        locations: str,
        connects: str,
        sync_define: bool,
    ) -> PreparedLegacyWorkflowInstanceUpdate:
        """Capture the exact DS 1.3.9 string-native instance update form."""
        if self.recipe.update_shape != "legacy":
            raise _unsupported(
                self.ds_version,
                "workflow-instance.edit",
                introduced_in="1.3.9",
                limited=True,
            )
        project_route = _route_project_identity(located.project)
        if not isinstance(project_route, str):
            message = "Legacy workflow-instance update requires a project-name route"
            raise WireContractError(message)
        call = self.programs.prepare(
            "instance_update_legacy",
            {
                "projectName": project_route,
                "processInstanceId": located.instance.id,
                "processInstanceJson": process_instance_json,
                "locations": locations,
                "connects": connects,
                "syncDefine": sync_define,
            },
        )
        return PreparedLegacyWorkflowInstanceUpdate(
            request=call.request,
            _wire_call=call,
        )

    def apply_legacy_workflow_instance_update(
        self,
        prepared: PreparedLegacyWorkflowInstanceUpdate,
    ) -> None:
        """Execute one previously captured DS 1.3.9 instance update exactly once."""
        if self.recipe.update_shape != "legacy":
            message = "Legacy workflow-instance update wire is not bound"
            raise WireContractError(message)
        _dispatch_mutation(
            lambda: (
                self.programs.execute(
                    "instance_update_legacy", prepared._wire_call
                ).payload
            ),
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
            operation="edit",
        )

    def _workflow_page(
        self,
        project: ProjectRef,
        *,
        workflow: WorkflowRef | None,
        page_no: int,
        page_size: int,
        search: str | None,
        executor: str | None,
        host: str | None,
        start_time: str | None,
        end_time: str | None,
        state: str | None,
    ) -> ReadPage[WorkflowInstanceSnapshot]:
        recipe = self.recipe
        definition_field = (
            "processDefinitionId"
            if recipe.definition_identity == "id"
            else "workflowDefinitionCode"
            if recipe.family == "workflow"
            else "processDefineCode"
        )
        values: JsonObject = {
            definition_field: None if workflow is None else workflow.native.value,
            "searchVal": search,
            "executorName": executor,
            "stateType": state,
            "host": host,
            "startDate": start_time,
            "endDate": end_time,
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if recipe.has_other_workflow_params:
            values["otherParamsJson"] = None
        page = self.programs.call(
            "instance_page", {**self._project_args(project), **values}
        )
        rows = _sequence(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
        )
        return _read_page(
            page,
            tuple(
                self._workflow_snapshot(
                    row, project, summary=self.recipe.workflow_summary
                )
                for row in rows
            ),
            page_no=page_no,
            page_size=page_size,
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
        )

    def _workflow_detail(
        self,
        project: ProjectRef,
        workflow_instance_id: int,
    ) -> WorkflowInstanceSnapshot:
        recipe = self.recipe
        field = "id" if recipe.workflow_get_params is None else "processInstanceId"
        payload = self.programs.call(
            "instance_get",
            {**self._project_args(project), field: workflow_instance_id},
        )
        return self._workflow_snapshot(payload, project)

    def _workflow_snapshot(
        self,
        item: OpaqueGeneratedValue,
        project: ProjectRef,
        *,
        summary: bool = False,
    ) -> WorkflowInstanceSnapshot:
        recipe = self.recipe
        definition_field = (
            "processDefinitionId"
            if recipe.definition_identity == "id"
            else "workflowDefinitionCode"
            if recipe.family == "workflow"
            else "processDefinitionCode"
        )
        definition_value = _optional_int(
            item,
            (definition_field,),
            ds_version=self.ds_version,
            resource=WORKFLOW_INSTANCE_RESOURCE,
        )
        native: NativeIdentity | None = None
        if definition_value is not None:
            native = (
                NativeId(definition_value)
                if recipe.definition_identity == "id"
                else NativeCode(definition_value)
            )
        dag_raw = _optional_value(item, ("dagData",))
        dag = cast("WorkflowDagRecord | None", dag_raw)
        if dag_raw is not None and isinstance(project.native, NativeCode):
            if not isinstance(native, NativeCode):
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=WORKFLOW_INSTANCE_RESOURCE,
                    field="dagData",
                    reason=(
                        "generated workflow DAG omitted its code-native "
                        "workflow identity"
                    ),
                )
            try:
                dag = project_exact_workflow_dag(
                    dag_raw,
                    ds_version=self.ds_version,
                    expected_project_code=project.native.value,
                    expected_workflow_code=native.value,
                    scalar_metadata=_instance_scalar_metadata(
                        item, ds_version=self.ds_version
                    ),
                )
            except WorkflowDagProjectionError as exc:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=WORKFLOW_INSTANCE_RESOURCE,
                    field=exc.field,
                    reason=exc.reason,
                ) from exc
            except WireContractError as exc:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=WORKFLOW_INSTANCE_RESOURCE,
                    field="dagData",
                    reason="generated workflow DAG violated its exact task contract",
                ) from exc
        return WorkflowInstanceSnapshot(
            ds_version=self.ds_version,
            id=_required_int(
                item,
                ("id",),
                ds_version=self.ds_version,
                resource=WORKFLOW_INSTANCE_RESOURCE,
            ),
            project=project,
            workflow_native=native,
            workflowDefinitionVersion=_int_or_default(
                item,
                ("workflowDefinitionVersion", "processDefinitionVersion"),
                default=0,
                ds_version=self.ds_version,
                resource=WORKFLOW_INSTANCE_RESOURCE,
            ),
            state=_optional_enum_value(item, ("state",)),
            recovery=_optional_enum_value(item, ("recovery",)),
            startTime=_optional_text_value(item, ("startTime",)),
            endTime=_optional_text_value(item, ("endTime",)),
            runTimes=_int_value_or_default(item, ("runTimes",), 0),
            name=_optional_text_value(item, ("name",)),
            host=_optional_text_value(item, ("host",)),
            commandType=_optional_enum_value(item, ("commandType",)),
            taskDependType=_optional_enum_value(item, ("taskDependType",)),
            failureStrategy=_optional_enum_value(item, ("failureStrategy",)),
            warningType=_optional_enum_value(item, ("warningType",)),
            scheduleTime=_optional_text_value(item, ("scheduleTime",)),
            executorId=_int_value_or_default(item, ("executorId",), 0),
            executorName=_optional_text_value(item, ("executorName",)),
            tenantCode=_optional_text_value(item, ("tenantCode",)),
            queue=_optional_text_value(item, ("queue",)),
            duration=_optional_text_value(item, ("duration",)),
            workflowInstancePriority=_optional_enum_value(
                item,
                ("workflowInstancePriority", "processInstancePriority"),
            ),
            workerGroup=_optional_text_value(item, ("workerGroup",)),
            environmentCode=_optional_int_value(item, ("environmentCode",)),
            timeout=_int_value_or_default(item, ("timeout",), 0),
            dryRun=_int_value_or_default(item, ("dryRun",), 0),
            restartTime=_optional_text_value(item, ("restartTime",)),
            dagData=dag,
            processInstanceJson=_optional_text_value(item, ("processInstanceJson",)),
            locations=_optional_text_value(item, ("locations",)),
            connects=_optional_text_value(item, ("connects",)),
            summary=summary,
            tenant_id=_optional_int_value(item, ("tenantId",)),
        )

    def _task_page(
        self,
        project: ProjectRef,
        *,
        workflow_instance_id: int | None,
        workflow_instance_name: str | None,
        workflow_definition_name: str | None,
        page_no: int,
        page_size: int,
        search: str | None,
        task_name: str | None,
        task_code: int | None,
        executor: str | None,
        state: str | None,
        host: str | None,
        start_time: str | None,
        end_time: str | None,
        task_execute_type: str | None,
    ) -> _TaskPageRead:
        recipe = self.recipe
        workflow_id_field = (
            "workflowInstanceId" if recipe.family == "workflow" else "processInstanceId"
        )
        values: JsonObject = {
            workflow_id_field: workflow_instance_id,
            "searchVal": search,
            "taskName": task_name,
            "executorName": executor,
            "stateType": state,
            "host": host,
            "startDate": start_time,
            "endDate": end_time,
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if recipe.has_task_workflow_name:
            field = (
                "workflowInstanceName"
                if recipe.family == "workflow"
                else "processInstanceName"
            )
            values[field] = workflow_instance_name
        if recipe.has_task_definition_name:
            field = (
                "workflowDefinitionName"
                if recipe.family == "workflow"
                else "processDefinitionName"
            )
            values[field] = workflow_definition_name
        if recipe.has_task_code_filter:
            values["taskCode"] = task_code
        if recipe.has_task_execute_type:
            values["taskExecuteType"] = task_execute_type
        page = self.programs.call(
            "task_instance_page", {**self._project_args(project), **values}
        )
        rows = _sequence(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=TASK_INSTANCE_RESOURCE,
        )
        return _TaskPageRead(
            page=_read_page(
                page,
                tuple(self._task_snapshot(row, project) for row in rows),
                page_no=page_no,
                page_size=page_size,
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            reported_page=_optional_int(
                page,
                ("currentPage", "pageNo"),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            reported_page_size=_optional_int(
                page,
                ("pageSize",),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            reported_total=_optional_int(
                page,
                ("total",),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            reported_total_pages=_optional_int(
                page,
                ("totalPage",),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
        )

    def _task_snapshot(
        self,
        item: OpaqueGeneratedValue,
        project: ProjectRef,
    ) -> TaskInstanceSnapshot:
        return TaskInstanceSnapshot(
            ds_version=self.ds_version,
            id=_required_int(
                item,
                ("id",),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            project=project,
            name=_optional_text_value(item, ("name",)),
            taskType=_optional_text_value(item, ("taskType",)),
            workflowInstanceId=_required_int(
                item,
                ("workflowInstanceId", "processInstanceId"),
                ds_version=self.ds_version,
                resource=TASK_INSTANCE_RESOURCE,
            ),
            workflowInstanceName=_optional_text_value(
                item,
                ("workflowInstanceName", "processInstanceName"),
            ),
            taskCode=_optional_int_value(item, ("taskCode",)),
            taskDefinitionVersion=_optional_int_value(
                item,
                ("taskDefinitionVersion", "taskDefinitionVersion"),
            ),
            workflowDefinitionName=_optional_text_value(
                item,
                ("workflowDefinitionName", "processDefinitionName"),
            ),
            state=_optional_enum_value(item, ("state",)),
            firstSubmitTime=_optional_text_value(item, ("firstSubmitTime",)),
            submitTime=_optional_text_value(item, ("submitTime",)),
            startTime=_optional_text_value(item, ("startTime",)),
            endTime=_optional_text_value(item, ("endTime",)),
            host=_optional_text_value(item, ("host",)),
            logPath=_optional_text_value(item, ("logPath",)),
            retryTimes=_int_value_or_default(item, ("retryTimes",), 0),
            duration=_optional_text_value(item, ("duration",)),
            executorName=_optional_text_value(item, ("executorName",)),
            workerGroup=_optional_text_value(item, ("workerGroup",)),
            environmentCode=_optional_int_value(item, ("environmentCode",)),
            delayTime=_int_value_or_default(item, ("delayTime",), 0),
            taskParams=_optional_text_value(item, ("taskParams",)),
            dryRun=_int_value_or_default(item, ("dryRun",), 0),
            taskGroupId=_int_value_or_default(item, ("taskGroupId",), 0),
            taskExecuteType=_optional_enum_value(item, ("taskExecuteType",)),
        )

    def _project_args(self, project: ProjectRef) -> JsonObject:
        field = (
            "projectName" if self.recipe.project_identity == "name" else "projectCode"
        )
        return {field: _route_project_identity(project)}


def _route_project_identity(project: ProjectRef) -> int | str:
    native = project.native
    if isinstance(native, NativeId):
        if project.name is None:
            message = "Legacy project route requires the project name"
            raise ApiTransportError(
                message,
                details={"project_id": native.value},
            )
        return project.name
    return native.value


def _dispatch_mutation(
    call: Callable[[], OpaqueGeneratedValue],
    *,
    ds_version: str,
    resource: str,
    operation: str,
) -> OpaqueGeneratedValue:
    return mutation_call(
        call,
        ds_version=ds_version,
        resource=resource,
        operation=operation,
    )


def _field(
    item: OpaqueGeneratedValue, names: Sequence[str]
) -> tuple[bool, OpaqueGeneratedValue]:
    for name in names:
        if isinstance(item, Mapping) and name in item:
            return True, item[name]
        try:
            return True, getattr(item, name)
        except AttributeError:
            continue
    return False, None


def _instance_scalar_metadata(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> WorkflowScalarMetadata:
    found, raw = _field(item, ("globalParams",))
    if not found or (raw is not None and not isinstance(raw, str)):
        raise _projection_error(
            ds_version,
            WORKFLOW_INSTANCE_RESOURCE,
            "globalParams",
            "instance globalParams is unavailable or is not nullable JSON text",
        )
    text = "[]" if raw is None or not raw.strip() else raw
    try:
        values: JsonValue = json.loads(text)
    except json.JSONDecodeError as exc:
        raise _projection_error(
            ds_version,
            WORKFLOW_INSTANCE_RESOURCE,
            "globalParams",
            "instance globalParams is not valid JSON",
        ) from exc
    if not isinstance(values, list):
        raise _projection_error(
            ds_version,
            WORKFLOW_INSTANCE_RESOURCE,
            "globalParams",
            "instance globalParams must be a JSON property array",
        )
    params: dict[str, str | None] = {}
    for parameter in values:
        if not isinstance(parameter, dict):
            raise _projection_error(
                ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "globalParams",
                "instance globalParams contains a non-object property",
            )
        name, value = parameter.get("prop"), parameter.get("value")
        if (
            not isinstance(name, str)
            or not name.strip()
            or (value is not None and not isinstance(value, str))
            or name in params
        ):
            raise _projection_error(
                ds_version,
                WORKFLOW_INSTANCE_RESOURCE,
                "globalParams",
                "instance globalParams requires unique nonempty names "
                "and nullable string values",
            )
        params[name] = value
    timeout = _required_int(
        item, ("timeout",), ds_version=ds_version, resource=WORKFLOW_INSTANCE_RESOURCE
    )
    return WorkflowScalarMetadata(
        global_params=text, global_param_map=params, timeout=timeout
    )


def _optional_value(
    item: OpaqueGeneratedValue, names: Sequence[str]
) -> OpaqueGeneratedValue:
    return _field(item, names)[1]


def _optional_enum_value(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
) -> StringEnumValue | str | None:
    value = _optional_value(item, names)
    return cast("StringEnumValue | str | None", value)


def _optional_text_value(
    item: OpaqueGeneratedValue, names: Sequence[str]
) -> str | None:
    found, value = _field(item, names)
    if not found or value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _optional_int_value(item: OpaqueGeneratedValue, names: Sequence[str]) -> int | None:
    found, value = _field(item, names)
    if not found or value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _int_value_or_default(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
    default: int,
) -> int:
    value = _optional_int_value(item, names)
    return default if value is None else value


def _optional_text(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
    *,
    ds_version: str,
    resource: str,
) -> str | None:
    found, value = _field(item, names)
    if not found or value is None:
        return None
    if isinstance(value, str):
        return value
    raise _projection_error(
        ds_version,
        resource,
        names[0],
        "payload field is not text or null",
    )


def _optional_int(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
    *,
    ds_version: str,
    resource: str,
) -> int | None:
    found, value = _field(item, names)
    if not found or value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise _projection_error(
        ds_version,
        resource,
        names[0],
        "payload field is not an integer or null",
    )


def _required_int(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
    *,
    ds_version: str,
    resource: str,
) -> int:
    value = _optional_int(
        item,
        names,
        ds_version=ds_version,
        resource=resource,
    )
    if value is None:
        raise _projection_error(
            ds_version,
            resource,
            names[0],
            "payload omitted a required integer",
        )
    return value


def _int_or_default(
    item: OpaqueGeneratedValue,
    names: Sequence[str],
    *,
    default: int,
    ds_version: str,
    resource: str,
) -> int:
    value = _optional_int(
        item,
        names,
        ds_version=ds_version,
        resource=resource,
    )
    return default if value is None else value


def _sequence(
    item: OpaqueGeneratedValue,
    name: str,
    *,
    ds_version: str,
    resource: str,
) -> list[OpaqueGeneratedValue]:
    found, value = _field(item, (name,))
    if not found or value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    raise _projection_error(
        ds_version,
        resource,
        name,
        "page items are not a sequence",
    )


def _read_page(
    page: OpaqueGeneratedValue,
    items: Sequence[_ItemT],
    *,
    page_no: int,
    page_size: int,
    ds_version: str,
    resource: str,
) -> ReadPage[_ItemT]:
    total = _optional_int(
        page,
        ("total",),
        ds_version=ds_version,
        resource=resource,
    )
    total = len(items) if total is None else total
    total_pages = _optional_int(
        page,
        ("totalPage",),
        ds_version=ds_version,
        resource=resource,
    )
    total_pages = (
        ceil(total / page_size) if total_pages is None and total else total_pages or 0
    )
    current = _optional_int(
        page,
        ("currentPage", "pageNo"),
        ds_version=ds_version,
        resource=resource,
    )
    current = page_no if current is None else current
    return ReadPage(
        totalList=items,
        total=total,
        totalPage=total_pages,
        pageSize=page_size,
        currentPage=current,
        pageNo=current,
    )


def _projection_error(
    ds_version: str,
    resource: str,
    field: str,
    reason: str,
) -> ApiTransportError:
    return ApiTransportError(
        "DolphinScheduler returned an incompatible runtime-instance payload",
        details={
            "ds_version": ds_version,
            "resource": resource,
            "field": field,
            "reason": reason,
        },
        suggestion=(
            "Verify DS_VERSION matches the server and report this exact-version "
            "projection mismatch."
        ),
    )


def _unsupported(
    ds_version: str,
    action: str,
    *,
    introduced_in: str,
    limited: bool = False,
) -> UnsupportedFeatureError:
    reason = "upstream_capability_limited" if limited else "upstream_capability_absent"
    return UnsupportedFeatureError(
        f"{action} is not available on DolphinScheduler {ds_version}.",
        details={
            "action": action,
            "selected_version": ds_version,
            "reason": reason,
            "introduced_in": introduced_in,
        },
        suggestion=(
            f"Use DolphinScheduler {introduced_in} or newer for this stable action."
        ),
    )


def _unsupported_option(
    ds_version: str,
    action: str,
    flag: str,
    *,
    introduced_in: str,
) -> UnsupportedFeatureError:
    return UnsupportedFeatureError(
        f"{flag} is not available for {action} on DolphinScheduler {ds_version}.",
        details={
            "action": action,
            "flag": flag,
            "selected_version": ds_version,
            "reason": "upstream_capability_absent",
            "introduced_in": introduced_in,
        },
        suggestion=f"Omit {flag} or use DolphinScheduler {introduced_in} or newer.",
    )


__all__ = [
    "RUNTIME_INSTANCE_DOMAIN",
    "LocatedTaskInstance",
    "LocatedWorkflowInstance",
    "PreparedLegacyWorkflowInstanceUpdate",
    "PreparedWorkflowInstanceUpdate",
    "RuntimeInstanceAdapter",
    "RuntimeInstanceContractFeatures",
    "RuntimeInstanceDomain",
    "RuntimeInstanceOperations",
    "TaskInstanceListing",
    "TaskInstanceSnapshot",
    "TaskLogTail",
    "WorkflowInstanceListing",
    "WorkflowInstanceSnapshot",
    "WorkflowMutationSnapshot",
    "runtime_instance_contract_features",
    "workflow_instance_stop_result_may_be_unknown",
]
