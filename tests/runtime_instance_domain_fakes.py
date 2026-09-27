from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar, cast

from dsctl.errors import ApiResultError
from dsctl.generated.runtime_instance_profiles import RUNTIME_INSTANCE_PROFILES
from dsctl.services import task_instance as task_instance_service
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.services.version_resolution import RuntimeSelection, resolve_version
from dsctl.services.workflow_instance import actions, edit, reads, watch
from dsctl.upstream.definition_models import (
    NativeCode,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.runtime_instances import (
    LocatedTaskInstance,
    LocatedWorkflowInstance,
    RuntimeInstanceDomain,
    RuntimeInstanceOperations,
    TaskInstanceListing,
    TaskInstanceSnapshot,
    TaskLogTail,
    WorkflowInstanceListing,
    WorkflowInstanceSnapshot,
    WorkflowMutationSnapshot,
)
from dsctl.upstream.task_logs import TaskLogChunk, window_task_log
from dsctl.upstream.wire import WireRequest
from tests.fakes import (
    FakeHttpClient,
    FakeProject,
    FakeProjectAdapter,
    FakeResourceAdapter,
    FakeTaskAdapter,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    empty_task_adapter,
    empty_task_instance_adapter,
    empty_workflow_adapter,
    empty_workflow_instance_adapter,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest

    from dsctl.config import ClusterProfile
    from dsctl.upstream.pagination import PageRecord
    from dsctl.upstream.protocol import StringEnumValue, WorkflowDagRecord
    from dsctl.upstream.protocols.governance import TaskResourceResolver
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire
    from dsctl.upstream.workflows import WorkflowOperations


RecordT = TypeVar("RecordT")
DEFAULT_RUNTIME_INSTANCE_CONTEXT = ResourceDefaults(project="etl-prod")


def install_runtime_instance_domain_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter | None = None,
    workflow_adapter: FakeWorkflowAdapter | None = None,
    task_adapter: FakeTaskAdapter | None = None,
    task_instance_adapter: FakeTaskInstanceAdapter | None = None,
    context: ResourceDefaults = DEFAULT_RUNTIME_INSTANCE_CONTEXT,
    profile: ClusterProfile | None = None,
    resource_adapter: FakeResourceAdapter | None = None,
    workflow_update_returns_none: bool = False,
    task_definition_wire: TaskDefinitionWire | None = None,
) -> None:
    """Bind workflow/task-instance tests directly to the deep domain seam."""
    selected_profile = profile or make_profile()
    operations = _FakeRuntimeInstanceOperations(
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter or empty_workflow_adapter(),
        workflow_instance_adapter=(
            workflow_instance_adapter or empty_workflow_instance_adapter()
        ),
        task_adapter=task_adapter or empty_task_adapter(),
        task_instance_adapter=task_instance_adapter or empty_task_instance_adapter(),
        ds_version=selected_profile.ds_version,
        workflow_update_returns_none=workflow_update_returns_none,
    )
    runtime = BoundDomainServiceRuntime(
        profile=selected_profile,
        context=context,
        http_client=FakeHttpClient(),
        domain=RuntimeInstanceDomain(
            instances=cast("RuntimeInstanceOperations", operations),
            workflows=cast("WorkflowOperations", operations),
            task_resource_resolver=cast(
                "TaskResourceResolver | None",
                resource_adapter,
            ),
            task_definitions=task_definition_wire,
        ),
    )

    def run(
        env_file: str | None,
        domain: object,
        operation: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        del env_file, domain
        return operation(runtime, *args, **kwargs)

    monkeypatch.setattr(
        task_instance_service,
        "run_with_bound_domain_service_runtime",
        run,
    )
    for service in (actions, reads, watch):
        monkeypatch.setattr(service, "run_with_bound_domain_service_runtime", run)
    for service in (edit,):
        monkeypatch.setattr(service, "run_with_bound_domain_selection", run)
        monkeypatch.setattr(
            service,
            "resolve_runtime_selection",
            lambda env_file=None: RuntimeSelection(
                selected_profile
                if env_file is None
                else make_profile(
                    ds_version=resolve_version(env_file, mode="local").version,
                ),
                project=runtime.context.project,
            ),
        )


@dataclass
class _FakePreparedWorkflowInstanceUpdate:
    request: WireRequest
    apply: Callable[[], WorkflowMutationSnapshot]


@dataclass
class _FakeRuntimeInstanceOperations:
    project_adapter: FakeProjectAdapter
    workflow_adapter: FakeWorkflowAdapter
    workflow_instance_adapter: FakeWorkflowInstanceAdapter
    task_adapter: FakeTaskAdapter
    task_instance_adapter: FakeTaskInstanceAdapter
    ds_version: str
    workflow_update_returns_none: bool = False

    def visible_workflow_refs(
        self,
        project: ProjectRef,
    ) -> tuple[WorkflowRef, ...]:
        return tuple(
            _workflow_ref(workflow)
            for workflow in self.workflow_adapter.workflows
            if workflow.projectCode == project.native.value
        )

    def resolve_workflow_by_name(
        self,
        project: ProjectRef,
        workflow_name: str,
    ) -> WorkflowScope:
        matches = [
            workflow
            for workflow in self.workflow_adapter.workflows
            if workflow.projectCode == project.native.value
            and workflow.name == workflow_name
        ]
        if len(matches) != 1:
            raise ApiResultError(
                result_code=50003,
                result_message=f"workflow name {workflow_name!r} not found",
            )
        return self._workflow_scope(project, matches[0])

    def resolve_workflow_by_code(
        self,
        project: ProjectRef,
        workflow_code: int,
    ) -> WorkflowScope:
        workflow = self.workflow_adapter.get(
            project_code=project.native.value,
            code=workflow_code,
        )
        return self._workflow_scope(project, workflow)

    def dag(self, scope: WorkflowScope, *, action: str) -> WorkflowDagRecord:
        del action
        return cast(
            "WorkflowDagRecord",
            self.workflow_adapter.describe(
                project_code=scope.project.native.value,
                code=scope.workflow.native.value,
            ),
        )

    @staticmethod
    def _workflow_scope(
        project: ProjectRef,
        workflow: FakeWorkflow,
    ) -> WorkflowScope:
        return WorkflowScope(
            project=project,
            workflow=_workflow_ref(workflow),
            view=WorkflowView(
                ref=_workflow_ref(workflow),
                project_native=project.native,
                id=workflow.id,
                description=workflow.description,
                global_params=workflow.globalParams,
                global_param_map=workflow.globalParamMap,
                create_time=workflow.createTime,
                update_time=workflow.updateTime,
                user_id=workflow.userId,
                user_name=workflow.userName,
                project_name=workflow.projectName,
                timeout=workflow.timeout,
                release_state=None
                if workflow.releaseState is None
                else workflow.releaseState.value,
                execution_type=None
                if workflow.executionType is None
                else workflow.executionType.value,
                include_execution_type=workflow.executionType is not None,
            ),
        )

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
        project = self._project(project_selector)
        workflow = (
            None
            if workflow_selector is None
            else self._workflow(project, workflow_selector)
        )
        page = self.workflow_instance_adapter.list(
            page_no=page_no,
            page_size=page_size,
            project_code=project.native.value,
            workflow_code=None if workflow is None else workflow.native.value,
            workflow_name=(None),
            search=search,
            executor=executor,
            host=host,
            start_time=start_time,
            end_time=end_time,
            state=state,
        )
        items = tuple(
            self._workflow_snapshot(
                item,
                project,
            )
            for item in (page.totalList or ())
        )
        return WorkflowInstanceListing(
            project=project,
            workflow=workflow,
            page=_read_page(page, items),
        )

    def get_workflow_instance(
        self,
        *,
        project_selector: str,
        workflow_instance_id: int,
    ) -> LocatedWorkflowInstance:
        item = self.workflow_instance_adapter.get(
            workflow_instance_id=workflow_instance_id
        )
        if item is None:
            raise ApiResultError(
                result_code=50001,
                result_message=(
                    f"workflow instance id {workflow_instance_id} not found"
                ),
            )
        project = self._project(project_selector)
        return LocatedWorkflowInstance(
            project=project,
            instance=self._workflow_snapshot(item, project),
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
        project = self._project(project_selector)
        located_workflow = (
            None
            if workflow_instance_id is None
            else self.get_workflow_instance(
                project_selector=project_selector,
                workflow_instance_id=workflow_instance_id,
            )
        )
        page = self.task_instance_adapter.list(
            project_code=project.native.value,
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
        )
        items = tuple(
            self._task_snapshot(item, project) for item in (page.totalList or ())
        )
        return TaskInstanceListing(
            project=project,
            workflow_instance=(
                None if located_workflow is None else located_workflow.instance
            ),
            page=_read_page(page, items),
        )

    def get_task_instance(
        self,
        *,
        project_selector: str,
        task_instance_id: int,
        workflow_instance_id: int | None,
    ) -> LocatedTaskInstance:
        project = self._project(project_selector)
        workflow = (
            None
            if workflow_instance_id is None
            else self.get_workflow_instance(
                project_selector=project_selector,
                workflow_instance_id=workflow_instance_id,
            )
        )
        item = self.task_instance_adapter.get(
            project_code=project.native.value,
            task_instance_id=task_instance_id,
        )
        if item is None or (
            workflow_instance_id is not None
            and item.workflowInstanceId != workflow_instance_id
        ):
            raise ApiResultError(
                result_code=10008,
                result_message=f"task instance id {task_instance_id} not found",
            )
        return LocatedTaskInstance(
            project=project,
            workflow_instance=None if workflow is None else workflow.instance,
            task=self._task_snapshot(item, project),
        )

    def task_page_for_workflow(
        self,
        located: LocatedWorkflowInstance,
        *,
        page_no: int,
        page_size: int,
    ) -> ReadPage[TaskInstanceSnapshot]:
        page = self.task_instance_adapter.list(
            project_code=located.project.native.value,
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
        )
        return _read_page(
            page,
            tuple(
                self._task_snapshot(item, located.project)
                for item in (page.totalList or ())
            ),
        )

    def parent_workflow_instance_id(
        self,
        located: LocatedWorkflowInstance,
    ) -> int | None:
        relation = self.workflow_instance_adapter.parent_instance_by_sub_workflow(
            project_code=located.project.native.value,
            sub_workflow_instance_id=located.instance.id,
        )
        return relation.parentWorkflowInstance

    def sub_workflow_instance_id(self, located: LocatedTaskInstance) -> int | None:
        relation = self.workflow_instance_adapter.sub_workflow_instance_by_task(
            project_code=located.project.native.value,
            task_instance_id=located.task.id,
        )
        return relation.subWorkflowInstanceId

    def tail_task_log(
        self,
        *,
        task_instance_id: int,
        max_lines: int,
    ) -> TaskLogTail:
        lines = self.task_instance_adapter.task_log_lines(
            task_instance_id=task_instance_id
        )
        tail = lines[-max_lines:]
        return TaskLogTail(text="\n".join(tail), line_count=len(tail))

    def window_task_log(
        self,
        *,
        task_instance_id: int,
        start_line: int,
        limit: int,
    ) -> TaskLogTail:
        def read_chunk(
            *, task_instance_id: int, skip_line_num: int, limit: int
        ) -> TaskLogChunk:
            lines = self.task_instance_adapter.task_log_lines(
                task_instance_id=task_instance_id
            )
            selected = lines[skip_line_num : skip_line_num + limit]
            return TaskLogChunk(
                "".join(line + "\r\n" for line in selected), len(selected), not selected
            )

        return window_task_log(
            task_instance_id=task_instance_id,
            start_line=start_line,
            limit=limit,
            read_chunk=read_chunk,
        )

    def control_workflow_instance(
        self,
        located: LocatedWorkflowInstance,
        *,
        execute_type: str,
    ) -> None:
        if execute_type == "STOP":
            self.workflow_instance_adapter.stop(
                workflow_instance_id=located.instance.id
            )
            return
        if execute_type == "REPEAT_RUNNING":
            self.workflow_instance_adapter.rerun(
                workflow_instance_id=located.instance.id
            )
            return
        if execute_type == "START_FAILURE_TASK_PROCESS":
            self.workflow_instance_adapter.recover_failed(
                workflow_instance_id=located.instance.id
            )
            return
        message = f"Unknown fake workflow execute type {execute_type!r}"
        raise AssertionError(message)

    def execute_task(
        self,
        located: LocatedWorkflowInstance,
        *,
        task_code: int,
        scope: str,
    ) -> None:
        self.workflow_instance_adapter.execute_task(
            project_code=located.project.native.value,
            workflow_instance_id=located.instance.id,
            task_code=task_code,
            scope=scope,
        )

    def task_action(self, located: LocatedTaskInstance, *, action: str) -> None:
        methods = {
            "force-success": self.task_instance_adapter.force_success,
            "savepoint": self.task_instance_adapter.savepoint,
            "stop": self.task_instance_adapter.stop,
        }
        try:
            method = methods[action]
        except KeyError as exc:
            message = f"Unknown fake task-instance action {action!r}"
            raise AssertionError(message) from exc
        method(
            project_code=located.project.native.value,
            task_instance_id=located.task.id,
        )

    def generate_task_codes(
        self,
        project: ProjectRef,
        *,
        count: int,
    ) -> list[int]:
        return self.task_adapter.generate_codes(
            project_code=project.native.value,
            count=count,
        )

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
    ) -> _FakePreparedWorkflowInstanceUpdate:
        recipe = RUNTIME_INSTANCE_PROFILES[self.ds_version]
        values: dict[str, str | int | bool | None] = {
            "taskRelationJson": task_relation_json,
            "taskDefinitionJson": task_definition_json,
            "scheduleTime": schedule_time,
            "syncDefine": sync_define,
            "globalParams": global_params,
            "locations": locations,
            "timeout": timeout,
        }
        if recipe.update_shape == "tenant":
            values["tenantCode"] = located.instance.tenantCode

        def apply() -> WorkflowMutationSnapshot:
            saved = self.workflow_instance_adapter.update(
                project_code=located.project.native.value,
                workflow_instance_id=located.instance.id,
                task_relation_json=task_relation_json,
                task_definition_json=task_definition_json,
                sync_define=sync_define,
                global_params=global_params,
                locations=locations,
                timeout=timeout,
                schedule_time=schedule_time,
            )
            return WorkflowMutationSnapshot(
                code=saved.code,
                name=saved.name,
                version=saved.version or 0,
            )

        return _FakePreparedWorkflowInstanceUpdate(
            request=WireRequest(
                method="PUT",
                path=(
                    f"/projects/{located.project.native.value}/"
                    f"{recipe.family}-instances/{located.instance.id}"
                ),
                query=None,
                form={key: value for key, value in values.items() if value is not None},
                json=None,
                content=None,
            ),
            apply=apply,
        )

    def apply_workflow_instance_update(
        self,
        prepared: _FakePreparedWorkflowInstanceUpdate,
    ) -> WorkflowMutationSnapshot | None:
        saved = prepared.apply()
        return None if self.workflow_update_returns_none else saved

    def _project(self, selector: str) -> ProjectRef:
        numeric = int(selector) if selector.isdigit() else None
        matches = [
            project
            for project in self.project_adapter.projects
            if project.code == numeric or project.name == selector
        ]
        if len(matches) != 1:
            raise ApiResultError(
                result_code=10018,
                result_message=f"project {selector!r} not found",
            )
        return _project_ref(matches[0])

    def _project_for_code(self, code: int | None) -> ProjectRef:
        if code is None:
            raise ApiResultError(
                result_code=10018,
                result_message="workflow instance has no project code",
            )
        return self._project(str(code))

    def _workflow(self, project: ProjectRef, selector: str) -> WorkflowRef:
        numeric = int(selector) if selector.isdigit() else None
        matches = [
            workflow
            for workflow in self.workflow_adapter.workflows
            if workflow.projectCode == project.native.value
            and (workflow.code == numeric or workflow.name == selector)
        ]
        if len(matches) != 1:
            raise ApiResultError(
                result_code=10018,
                result_message=f"workflow {selector!r} not found",
            )
        return _workflow_ref(matches[0])

    def _workflow_snapshot(
        self,
        item: FakeWorkflowInstance,
        project: ProjectRef,
    ) -> WorkflowInstanceSnapshot:
        workflow_native = (
            None
            if item.workflowDefinitionCode is None
            else NativeCode(item.workflowDefinitionCode)
        )
        return WorkflowInstanceSnapshot(
            ds_version=self.ds_version,
            id=item.id,
            project=project,
            workflow_native=workflow_native,
            workflowDefinitionVersion=item.workflowDefinitionVersion,
            state=cast("StringEnumValue | str | None", item.state),
            recovery=cast("StringEnumValue | str | None", item.recovery),
            startTime=item.startTime,
            endTime=item.endTime,
            runTimes=item.runTimes,
            name=item.name,
            host=item.host,
            commandType=cast("StringEnumValue | str | None", item.commandType),
            taskDependType=cast(
                "StringEnumValue | str | None",
                item.taskDependType,
            ),
            failureStrategy=cast(
                "StringEnumValue | str | None",
                item.failureStrategy,
            ),
            warningType=cast("StringEnumValue | str | None", item.warningType),
            scheduleTime=item.scheduleTime,
            executorId=item.executorId,
            executorName=item.executorName,
            tenantCode=item.tenantCode,
            queue=item.queue,
            duration=item.duration,
            workflowInstancePriority=cast(
                "StringEnumValue | str | None",
                item.workflowInstancePriority,
            ),
            workerGroup=item.workerGroup,
            environmentCode=item.environmentCode,
            timeout=item.timeout,
            dryRun=item.dryRun,
            restartTime=item.restartTime,
            dagData=cast("WorkflowDagRecord | None", item.dagData),
        )

    def _task_snapshot(
        self,
        item: FakeTaskInstance,
        project: ProjectRef,
    ) -> TaskInstanceSnapshot:
        return TaskInstanceSnapshot(
            ds_version=self.ds_version,
            id=item.id,
            project=project,
            name=item.name,
            taskType=item.taskType,
            workflowInstanceId=item.workflowInstanceId,
            workflowInstanceName=item.workflowInstanceName,
            taskCode=item.taskCode,
            taskDefinitionVersion=item.taskDefinitionVersion,
            workflowDefinitionName=item.processDefinitionName,
            state=cast("StringEnumValue | str | None", item.state),
            firstSubmitTime=item.firstSubmitTime,
            submitTime=item.submitTime,
            startTime=item.startTime,
            endTime=item.endTime,
            host=item.host,
            logPath=item.logPath,
            retryTimes=item.retryTimes,
            duration=item.duration,
            executorName=item.executorName,
            workerGroup=item.workerGroup,
            environmentCode=item.environmentCode,
            delayTime=item.delayTime,
            taskParams=item.taskParams,
            dryRun=item.dryRun,
            taskGroupId=item.taskGroupId,
            taskExecuteType=cast(
                "StringEnumValue | str | None",
                item.taskExecuteType,
            ),
        )


def _project_ref(project: FakeProject) -> ProjectRef:
    return ProjectRef(
        native=NativeCode(project.code),
        name=project.name,
        description=project.description,
    )


def _workflow_ref(workflow: FakeWorkflow) -> WorkflowRef:
    return WorkflowRef(
        native=NativeCode(workflow.code),
        name=workflow.name,
        version=workflow.version,
    )


def _read_page(
    page: PageRecord[object],
    items: tuple[RecordT, ...],
) -> ReadPage[RecordT]:
    return ReadPage(
        totalList=items,
        total=page.total,
        totalPage=page.totalPage,
        pageSize=page.pageSize,
        currentPage=page.currentPage,
        pageNo=page.pageNo,
    )


__all__ = ["install_runtime_instance_domain_runtime"]
