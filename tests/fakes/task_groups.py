"""In-memory task groups collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeTaskGroup:
    id: int | None
    name: str | None
    project_code_value: int
    description: str | None = None
    group_size_value: int = 1
    use_size_value: int = 0
    user_id_value: int = 1
    status_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def projectCode(self) -> int:  # noqa: N802
        return self.project_code_value

    @property
    def groupSize(self) -> int:  # noqa: N802
        return self.group_size_value

    @property
    def useSize(self) -> int:  # noqa: N802
        return self.use_size_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def status(self) -> FakeEnumValue | None:
        return self.status_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeTaskGroupPage(_FakePage[FakeTaskGroup]):
    pass


@dataclass(frozen=True)
class FakeTaskGroupQueue:
    id: int | None
    task_id_value: int | None
    task_name_value: str | None
    project_name_value: str | None
    project_code_value: str | None
    workflow_instance_name_value: str | None
    group_id_value: int
    workflow_instance_id_value: int | None = None
    priority: int = 0
    force_start_value: int = 0
    in_queue_value: int = 1
    status_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def taskId(self) -> int | None:  # noqa: N802
        return self.task_id_value

    @property
    def taskName(self) -> str | None:  # noqa: N802
        return self.task_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def projectCode(self) -> str | None:  # noqa: N802
        return self.project_code_value

    @property
    def workflowInstanceName(self) -> str | None:  # noqa: N802
        return self.workflow_instance_name_value

    @property
    def groupId(self) -> int:  # noqa: N802
        return self.group_id_value

    @property
    def workflowInstanceId(self) -> int | None:  # noqa: N802
        return self.workflow_instance_id_value

    @property
    def forceStart(self) -> int:  # noqa: N802
        return self.force_start_value

    @property
    def inQueue(self) -> int:  # noqa: N802
        return self.in_queue_value

    @property
    def status(self) -> FakeEnumValue | None:
        return self.status_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeTaskGroupQueuePage(_FakePage[FakeTaskGroupQueue]):
    pass


@dataclass
class FakeTaskGroupAdapter:
    task_groups: list[FakeTaskGroup]
    task_group_queues: list[FakeTaskGroupQueue] = field(default_factory=list)
    get_calls: list[int] = field(default_factory=list)

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
        status: int | None = None,
    ) -> FakeTaskGroupPage:
        filtered = list(self.task_groups)
        if search is not None:
            filtered = [
                task_group
                for task_group in filtered
                if task_group.name is not None
                and search.lower() in task_group.name.lower()
            ]
        if status is not None:
            filtered = [
                task_group
                for task_group in filtered
                if _task_group_status_code(task_group.status) == status
            ]
        return _task_group_page(filtered, page_no=page_no, page_size=page_size)

    def list_by_project(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
    ) -> FakeTaskGroupPage:
        filtered = [
            task_group
            for task_group in self.task_groups
            if task_group.projectCode == project_code
        ]
        return _task_group_page(filtered, page_no=page_no, page_size=page_size)

    def list_all(self) -> Sequence[FakeTaskGroup]:
        return list(self.task_groups)

    def get(self, *, task_group_id: int) -> FakeTaskGroup:
        self.get_calls.append(task_group_id)
        for task_group in self.task_groups:
            if task_group.id == task_group_id:
                return task_group
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def create(
        self,
        *,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> FakeTaskGroup:
        if group_size < 1:
            raise ApiResultError(
                result_code=130002,
                result_message="task group size error",
            )
        for existing in self.task_groups:
            if existing.name == name:
                raise ApiResultError(
                    result_code=130001,
                    result_message=f"task group {name} already exists",
                )
        next_id = max((item.id or 0 for item in self.task_groups), default=0) + 1
        created = FakeTaskGroup(
            id=next_id,
            name=name,
            project_code_value=project_code,
            description=description,
            group_size_value=group_size,
            status_value=FakeEnumValue("YES"),
        )
        self.task_groups.append(created)
        return created

    def update(
        self,
        *,
        task_group_id: int,
        project_code: int,
        name: str,
        description: str,
        group_size: int,
    ) -> FakeTaskGroup:
        if group_size < 1:
            raise ApiResultError(
                result_code=130002,
                result_message="task group size error",
            )
        for existing in self.task_groups:
            if existing.id == task_group_id:
                continue
            if existing.name == name:
                raise ApiResultError(
                    result_code=130001,
                    result_message=f"task group {name} already exists",
                )
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.projectCode != project_code:
                    continue
                if existing.status is not None and existing.status.value != "YES":
                    raise ApiResultError(
                        result_code=130003,
                        result_message="task group status error",
                    )
                updated = replace(
                    existing,
                    name=name,
                    description=description,
                    group_size_value=group_size,
                )
                self.task_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def close(self, *, task_group_id: int) -> FakeTaskGroup:
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.status is not None and existing.status.value == "NO":
                    raise ApiResultError(
                        result_code=130018,
                        result_message="task group already closed",
                    )
                updated = replace(
                    existing,
                    status_value=FakeEnumValue("NO"),
                )
                self.task_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def start(self, *, task_group_id: int) -> FakeTaskGroup:
        for index, existing in enumerate(self.task_groups):
            if existing.id == task_group_id:
                if existing.status is not None and existing.status.value == "YES":
                    raise ApiResultError(
                        result_code=130019,
                        result_message="task group already opened",
                    )
                updated = replace(
                    existing,
                    status_value=FakeEnumValue("YES"),
                )
                self.task_groups[index] = updated
                return updated
        raise ApiResultError(
            result_code=130010,
            result_message=f"task group {task_group_id} not found",
        )

    def list_queues(
        self,
        *,
        group_id: int,
        page_no: int,
        page_size: int,
        task_instance_name: str | None = None,
        workflow_instance_name: str | None = None,
        status: int | None = None,
    ) -> FakeTaskGroupQueuePage:
        filtered = [
            queue for queue in self.task_group_queues if queue.groupId == group_id
        ]
        if task_instance_name is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.taskName is not None
                and task_instance_name.lower() in queue.taskName.lower()
            ]
        if workflow_instance_name is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.workflowInstanceName is not None
                and workflow_instance_name.lower() in queue.workflowInstanceName.lower()
            ]
        if status is not None:
            filtered = [
                queue
                for queue in filtered
                if _task_group_queue_status_code(queue.status) == status
            ]
        return _task_group_queue_page(filtered, page_no=page_no, page_size=page_size)

    def force_start(self, *, queue_id: int) -> None:
        for index, queue in enumerate(self.task_group_queues):
            if queue.id == queue_id:
                if queue.forceStart == 1:
                    raise ApiResultError(
                        result_code=130017,
                        result_message="task group queue already start",
                    )
                self.task_group_queues[index] = replace(queue, force_start_value=1)
                return
        raise ApiResultError(
            result_code=130013,
            result_message=f"task group queue {queue_id} not found",
        )

    def set_queue_priority(self, *, queue_id: int, priority: int) -> None:
        for index, queue in enumerate(self.task_group_queues):
            if queue.id == queue_id:
                self.task_group_queues[index] = replace(queue, priority=priority)
                return
        raise ApiResultError(
            result_code=130013,
            result_message=f"task group queue {queue_id} not found",
        )


def _task_group_page(
    task_groups: list[FakeTaskGroup],
    *,
    page_no: int,
    page_size: int,
) -> FakeTaskGroupPage:
    return FakeTaskGroupPage.from_items(
        task_groups,
        page_no=page_no,
        page_size=page_size,
    )


def _task_group_queue_page(
    queues: list[FakeTaskGroupQueue],
    *,
    page_no: int,
    page_size: int,
) -> FakeTaskGroupQueuePage:
    return FakeTaskGroupQueuePage.from_items(
        queues,
        page_no=page_no,
        page_size=page_size,
    )


def _task_group_status_code(status: FakeEnumValue | None) -> int | None:
    if status is None:
        return None
    if status.value == "YES":
        return 1
    if status.value == "NO":
        return 0
    return None


def _task_group_queue_status_code(status: FakeEnumValue | None) -> int | None:
    if status is None:
        return None
    if status.value == "WAIT_QUEUE":
        return -1
    if status.value == "ACQUIRE_SUCCESS":
        return 1
    if status.value == "RELEASE":
        return 2
    return None


def empty_task_group_adapter() -> FakeTaskGroupAdapter:
    return FakeTaskGroupAdapter(task_groups=[])
