"""In-memory task instances collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)


@dataclass(frozen=True)
class FakeTaskInstance:
    id: int
    name: str | None = None
    task_type_value: str | None = None
    workflow_instance_id_value: int = 0
    workflow_instance_name_value: str | None = None
    project_code_value: int | None = None
    task_code_value: int = 0
    task_definition_version_value: int = 0
    process_definition_name_value: str | None = None
    state_value: FakeEnumValue | None = None
    first_submit_time_value: str | None = None
    submit_time_value: str | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    host: str | None = None
    log_path_value: str | None = None
    retry_times_value: int = 0
    duration_value: str | None = None
    executor_name_value: str | None = None
    worker_group_value: str | None = None
    environment_code_value: int | None = None
    delay_time_value: int = 0
    task_params_value: str | None = None
    dry_run_value: int = 0
    task_group_id_value: int = 0
    task_execute_type_value: FakeEnumValue | None = None

    @property
    def taskType(self) -> str | None:  # noqa: N802
        return self.task_type_value

    @property
    def workflowInstanceId(self) -> int:  # noqa: N802
        return self.workflow_instance_id_value

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        return self.workflow_instance_name_value

    @property
    def projectCode(self) -> int | None:  # noqa: N802
        return self.project_code_value

    @property
    def taskCode(self) -> int:  # noqa: N802
        return self.task_code_value

    @property
    def taskDefinitionVersion(self) -> int:  # noqa: N802
        return self.task_definition_version_value

    @property
    def processDefinitionName(self) -> str | None:  # noqa: N802
        return self.process_definition_name_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def firstSubmitTime(self) -> str | None:  # noqa: N802
        return self.first_submit_time_value

    @property
    def submitTime(self) -> str | None:  # noqa: N802
        return self.submit_time_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def logPath(self) -> str | None:  # noqa: N802
        return self.log_path_value

    @property
    def retryTimes(self) -> int:  # noqa: N802
        return self.retry_times_value

    @property
    def duration(self) -> str | None:
        return self.duration_value

    @property
    def executorName(self) -> str | None:  # noqa: N802
        return self.executor_name_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def delayTime(self) -> int:  # noqa: N802
        return self.delay_time_value

    @property
    def taskParams(self) -> str | None:  # noqa: N802
        return self.task_params_value

    @property
    def dryRun(self) -> int:  # noqa: N802
        return self.dry_run_value

    @property
    def taskGroupId(self) -> int:  # noqa: N802
        return self.task_group_id_value

    @property
    def taskExecuteType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.task_execute_type_value


@dataclass(frozen=True)
class FakeTaskInstancePage(_FakePage[FakeTaskInstance]):
    pass


@dataclass
class FakeTaskInstanceAdapter:
    task_instances: list[FakeTaskInstance]
    task_instance_sequences_by_id: dict[int, list[FakeTaskInstance]] | None = None
    log_messages_by_task_instance_id: dict[int, list[str]] | None = None
    force_success_ids: list[int] = field(default_factory=list)
    savepoint_ids: list[int] = field(default_factory=list)
    stopped_ids: list[int] = field(default_factory=list)

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_instance_id: int | None = None,
        workflow_instance_name: str | None = None,
        workflow_definition_name: str | None = None,
        search: str | None = None,
        task_name: str | None = None,
        task_code: int | None = None,
        executor: str | None = None,
        state: str | None = None,
        host: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        task_execute_type: str | None = None,
    ) -> FakeTaskInstancePage:
        filtered = [
            task_instance
            for task_instance in self.task_instances
            if _matches_task_instance_query(
                task_instance,
                project_code=project_code,
                workflow_instance_id=workflow_instance_id,
                workflow_instance_name=workflow_instance_name,
                workflow_definition_name=workflow_definition_name,
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
        ]
        return FakeTaskInstancePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def get(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> FakeTaskInstance | None:
        sequence = (self.task_instance_sequences_by_id or {}).get(task_instance_id)
        if sequence:
            if len(sequence) > 1:
                return sequence.pop(0)
            return sequence[0]
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                return task_instance
        return None

    def task_log_lines(
        self,
        *,
        task_instance_id: int,
    ) -> tuple[str, ...]:
        lines = list(
            (self.log_messages_by_task_instance_id or {}).get(task_instance_id, [])
        )
        if not lines:
            lines = [
                f"task instance {task_instance_id} log line {index}"
                for index in range(1, 6)
            ]
        return tuple(lines)

    def force_success(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.force_success_ids.append(task_instance_id)
                object.__setattr__(
                    task_instance,
                    "state_value",
                    FakeEnumValue("FORCED_SUCCESS"),
                )
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )

    def savepoint(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.savepoint_ids.append(task_instance_id)
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )

    def stop(
        self,
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        for task_instance in self.task_instances:
            if (
                task_instance.id == task_instance_id
                and task_instance.projectCode == project_code
            ):
                self.stopped_ids.append(task_instance_id)
                return
        raise ApiResultError(
            result_code=10008,
            result_message=f"task instance id {task_instance_id} not found",
        )


def _matches_task_instance_query(
    task_instance: FakeTaskInstance,
    *,
    project_code: int,
    workflow_instance_id: int | None,
    workflow_instance_name: str | None,
    workflow_definition_name: str | None,
    search: str | None,
    task_name: str | None,
    task_code: int | None,
    executor: str | None,
    state: str | None,
    host: str | None,
    start_time: str | None,
    end_time: str | None,
    task_execute_type: str | None,
) -> bool:
    start_value = task_instance.startTime
    state_value = None if task_instance.state is None else task_instance.state.value
    execute_type_value = (
        None
        if task_instance.taskExecuteType is None
        else task_instance.taskExecuteType.value
    )
    return all(
        (
            task_instance.projectCode == project_code,
            workflow_instance_id is None
            or task_instance.workflowInstanceId == workflow_instance_id,
            workflow_instance_name is None
            or task_instance.workflowInstanceName == workflow_instance_name,
            workflow_definition_name is None
            or task_instance.processDefinitionName == workflow_definition_name,
            search is None
            or (
                task_instance.name is not None
                and search.lower() in task_instance.name.lower()
            ),
            task_name is None or task_instance.name == task_name,
            task_code is None or task_instance.taskCode == task_code,
            executor is None or task_instance.executorName == executor,
            state is None or state_value == state,
            host is None
            or (
                task_instance.host is not None
                and host.lower() in task_instance.host.lower()
            ),
            start_time is None
            or (start_value is not None and start_value >= start_time),
            end_time is None or (start_value is not None and start_value <= end_time),
            task_execute_type is None or execute_type_value == task_execute_type,
        )
    )


def empty_task_instance_adapter() -> FakeTaskInstanceAdapter:
    return FakeTaskInstanceAdapter(task_instances=[])
