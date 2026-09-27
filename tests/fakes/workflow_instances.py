"""In-memory workflow instances collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)
from tests.fakes.definitions import (
    FakeDag,
    FakeWorkflow,
    _updated_fake_workflow_tasks_and_relations,
)
from tests.fakes.values import (
    _global_param_map,
)


@dataclass(frozen=True)
class FakeWorkflowInstance:
    id: int
    workflow_definition_code_value: int | None = None
    workflow_definition_version_value: int = 0
    project_code_value: int | None = None
    dag_data_value: FakeDag | None = None
    state_value: FakeEnumValue | None = None
    recovery_value: FakeEnumValue | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    run_times_value: int = 0
    name: str | None = None
    host: str | None = None
    command_type_value: FakeEnumValue | None = None
    task_depend_type_value: FakeEnumValue | None = None
    failure_strategy_value: FakeEnumValue | None = None
    warning_type_value: FakeEnumValue | None = None
    schedule_time_value: str | None = None
    executor_id_value: int = 0
    executor_name_value: str | None = None
    tenant_code_value: str | None = None
    queue_value: str | None = None
    duration_value: str | None = None
    workflow_instance_priority_value: FakeEnumValue | None = None
    worker_group_value: str | None = None
    environment_code_value: int | None = None
    timeout: int = 0
    dry_run_value: int = 0
    restart_time_value: str | None = None

    @property
    def workflowDefinitionCode(self) -> int | None:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionVersion(self) -> int:  # noqa: N802
        return self.workflow_definition_version_value

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        return self.project_code_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def recovery(self) -> FakeEnumValue | None:
        return self.recovery_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def runTimes(self) -> int:  # noqa: N802
        return self.run_times_value

    @property
    def commandType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.command_type_value

    @property
    def taskDependType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_depend_type_value

    @property
    def failureStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.failure_strategy_value

    @property
    def warningType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.warning_type_value

    @property
    def scheduleTime(self) -> str | None:  # noqa: N802
        return self.schedule_time_value

    @property
    def executorId(self) -> int:  # noqa: N802
        return self.executor_id_value

    @property
    def executorName(self) -> str | None:  # noqa: N802
        return self.executor_name_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def queue(self) -> str | None:
        return self.queue_value

    @property
    def duration(self) -> str | None:
        return self.duration_value

    @property
    def workflowInstancePriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.workflow_instance_priority_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def dryRun(self) -> int:  # noqa: N802
        return self.dry_run_value

    @property
    def restartTime(self) -> str | None:  # noqa: N802
        return self.restart_time_value

    @property
    def dagData(self) -> FakeDag | None:  # noqa: N802
        return self.dag_data_value


@dataclass(frozen=True)
class FakeWorkflowInstanceSubWorkflow:
    sub_workflow_instance_id_value: int | None

    @property
    def subWorkflowInstanceId(self) -> int | None:  # noqa: N802
        return self.sub_workflow_instance_id_value


@dataclass(frozen=True)
class FakeWorkflowInstanceParent:
    parent_workflow_instance_value: int | None

    @property
    def parentWorkflowInstance(self) -> int | None:  # noqa: N802
        return self.parent_workflow_instance_value


@dataclass(frozen=True)
class FakeWorkflowInstancePage(_FakePage[FakeWorkflowInstance]):
    pass


@dataclass
class FakeWorkflowInstanceAdapter:
    workflow_instances: list[FakeWorkflowInstance]
    workflow_instance_sequences_by_id: dict[int, list[FakeWorkflowInstance]] | None = (
        None
    )
    update_errors_by_id: dict[int, ApiResultError] | None = None
    sub_workflow_instance_ids_by_task_id: dict[int, int] = field(default_factory=dict)
    parent_workflow_instance_ids_by_sub_id: dict[int, int] = field(default_factory=dict)
    update_calls: list[dict[str, object]] = field(default_factory=list)
    stopped_ids: list[int] = field(default_factory=list)
    rerun_ids: list[int] = field(default_factory=list)
    recovered_failed_ids: list[int] = field(default_factory=list)
    executed_tasks: list[tuple[int, int, str]] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        project_code: int | None = None,
        workflow_code: int | None = None,
        project_name: str | None = None,
        workflow_name: str | None = None,
        search: str | None = None,
        executor: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        state: str | None = None,
    ) -> FakeWorkflowInstancePage:
        del project_name
        filtered = list(self.workflow_instances)
        if project_code is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.projectCode == project_code
            ]
        if workflow_code is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.workflowDefinitionCode == workflow_code
            ]
        if workflow_name is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.name is not None
                and workflow_name.lower() in workflow_instance.name.lower()
            ]
        if search is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.name is not None
                and search.lower() in workflow_instance.name.lower()
            ]
        if executor is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.executorName == executor
            ]
        if host is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.host is not None
                and host.lower() in workflow_instance.host.lower()
            ]
        if start_time is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.startTime is not None
                and workflow_instance.startTime >= start_time
            ]
        if end_time is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.startTime is not None
                and workflow_instance.startTime <= end_time
            ]
        if state is not None:
            filtered = [
                workflow_instance
                for workflow_instance in filtered
                if workflow_instance.state is not None
                and workflow_instance.state.value == state
            ]
        return FakeWorkflowInstancePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(self, *, workflow_instance_id: int) -> FakeWorkflowInstance:
        sequence = (self.workflow_instance_sequences_by_id or {}).get(
            workflow_instance_id
        )
        if sequence:
            if len(sequence) > 1:
                return sequence.pop(0)
            return sequence[0]
        for workflow_instance in self.workflow_instances:
            if workflow_instance.id == workflow_instance_id:
                return workflow_instance
        raise ApiResultError(
            result_code=50001,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )

    def update(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_relation_json: str,
        task_definition_json: str,
        sync_define: bool,
        global_params: str | None = None,
        locations: str | None = None,
        timeout: int | None = None,
        schedule_time: str | None = None,
    ) -> FakeWorkflow:
        self.update_calls.append(
            {
                "project_code": project_code,
                "workflow_instance_id": workflow_instance_id,
                "task_relation_json": task_relation_json,
                "task_definition_json": task_definition_json,
                "sync_define": sync_define,
                "global_params": global_params,
                "locations": locations,
                "timeout": timeout,
                "schedule_time": schedule_time,
            }
        )
        if (
            self.update_errors_by_id is not None
            and workflow_instance_id in self.update_errors_by_id
        ):
            raise self.update_errors_by_id[workflow_instance_id]
        for index, workflow_instance in enumerate(self.workflow_instances):
            if workflow_instance.id != workflow_instance_id:
                continue
            if workflow_instance.projectCode != project_code:
                break
            current_dag = workflow_instance.dagData
            current_workflow = (
                None if current_dag is None else current_dag.workflowDefinition
            )
            workflow_code = workflow_instance.workflowDefinitionCode
            if current_dag is None or current_workflow is None or workflow_code is None:
                message = "workflow instance fake update requires dagData"
                raise TypeError(message)
            updated_tasks, updated_relations = (
                _updated_fake_workflow_tasks_and_relations(
                    dags={workflow_code: current_dag},
                    workflow_code=workflow_code,
                    project_code=project_code,
                    task_definition_json=task_definition_json,
                    task_relation_json=task_relation_json,
                )
            )
            current_version = (
                current_workflow.version
                or workflow_instance.workflowDefinitionVersion
                or 1
            )
            next_version = current_version + 1
            updated_workflow = replace(
                current_workflow,
                version=next_version,
                global_params_value=global_params,
                global_param_map_value=_global_param_map(global_params),
                timeout=current_workflow.timeout if timeout is None else timeout,
                update_time_value="2026-04-13 12:00:00",
            )
            updated_dag = replace(
                current_dag,
                workflow_definition_value=updated_workflow,
                task_definition_list_value=updated_tasks,
                workflow_task_relation_list_value=updated_relations,
            )
            updated_instance = replace(
                workflow_instance,
                workflow_definition_version_value=next_version,
                dag_data_value=updated_dag,
                timeout=updated_workflow.timeout,
                schedule_time_value=(
                    workflow_instance.scheduleTime
                    if schedule_time is None
                    else schedule_time
                ),
            )
            self.workflow_instances[index] = updated_instance
            sequence = (self.workflow_instance_sequences_by_id or {}).get(
                workflow_instance_id
            )
            if sequence:
                sequence[-1] = updated_instance
            return updated_workflow
        raise ApiResultError(
            result_code=10211,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )

    def parent_instance_by_sub_workflow(
        self,
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> FakeWorkflowInstanceParent:
        del project_code
        parent_workflow_instance_id = self.parent_workflow_instance_ids_by_sub_id.get(
            sub_workflow_instance_id
        )
        if parent_workflow_instance_id is None:
            raise ApiResultError(
                result_code=50007,
                result_message="sub workflow instance not found",
            )
        return FakeWorkflowInstanceParent(
            parent_workflow_instance_value=parent_workflow_instance_id
        )

    def sub_workflow_instance_by_task(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> FakeWorkflowInstanceSubWorkflow:
        del project_code
        sub_workflow_instance_id = self.sub_workflow_instance_ids_by_task_id.get(
            task_instance_id
        )
        if sub_workflow_instance_id is None:
            raise ApiResultError(
                result_code=50007,
                result_message="sub workflow instance not found",
            )
        return FakeWorkflowInstanceSubWorkflow(
            sub_workflow_instance_id_value=sub_workflow_instance_id
        )

    def stop(self, *, workflow_instance_id: int) -> None:
        self.stopped_ids.append(workflow_instance_id)
        self._set_state(workflow_instance_id=workflow_instance_id, state="READY_STOP")

    def rerun(self, *, workflow_instance_id: int) -> None:
        self.rerun_ids.append(workflow_instance_id)
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def recover_failed(self, *, workflow_instance_id: int) -> None:
        self.recovered_failed_ids.append(workflow_instance_id)
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def execute_task(
        self,
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code
        self.executed_tasks.append((workflow_instance_id, task_code, scope))
        self._set_state(
            workflow_instance_id=workflow_instance_id,
            state="RUNNING_EXECUTION",
        )

    def _set_state(self, *, workflow_instance_id: int, state: str) -> None:
        for index, workflow_instance in enumerate(self.workflow_instances):
            if workflow_instance.id == workflow_instance_id:
                self.workflow_instances[index] = replace(
                    workflow_instance,
                    state_value=FakeEnumValue(state),
                )
                return
        raise ApiResultError(
            result_code=10211,
            result_message=f"workflow instance id {workflow_instance_id} not found",
        )


def empty_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
    return FakeWorkflowInstanceAdapter(workflow_instances=[])
