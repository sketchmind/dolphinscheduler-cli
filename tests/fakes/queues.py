"""In-memory queues collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class FakeQueue:
    id: int
    queue_name_value: str | None
    queue: str | None
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def queueName(self) -> str | None:  # noqa: N802
        return self.queue_name_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeQueuePage(_FakePage[FakeQueue]):
    pass


@dataclass
class FakeQueueAdapter:
    queues: list[FakeQueue]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeQueuePage:
        filtered = list(self.queues)
        if search is not None:
            filtered = [
                queue
                for queue in filtered
                if queue.queueName is not None
                and search.lower() in queue.queueName.lower()
            ]
        return FakeQueuePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def list_all(self) -> Sequence[FakeQueue]:
        return list(self.queues)

    def get(self, *, queue_id: int) -> FakeQueue:
        for queue in self.queues:
            if queue.id == queue_id:
                return queue
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )

    def create(self, *, queue: str, queue_name: str) -> FakeQueue:
        for existing in self.queues:
            if existing.queue == queue:
                raise ApiResultError(
                    result_code=10129,
                    result_message=f"queue value {queue} already exists",
                )
            if existing.queueName == queue_name:
                raise ApiResultError(
                    result_code=10130,
                    result_message=f"queue name {queue_name} already exists",
                )
        next_id = max((item.id for item in self.queues), default=0) + 1
        created = FakeQueue(
            id=next_id,
            queue_name_value=queue_name,
            queue=queue,
        )
        self.queues.append(created)
        return created

    def update(self, *, queue_id: int, queue: str, queue_name: str) -> FakeQueue:
        for existing in self.queues:
            if existing.id == queue_id:
                continue
            if existing.queue == queue:
                raise ApiResultError(
                    result_code=10129,
                    result_message=f"queue value {queue} already exists",
                )
            if existing.queueName == queue_name:
                raise ApiResultError(
                    result_code=10130,
                    result_message=f"queue name {queue_name} already exists",
                )
        for index, existing in enumerate(self.queues):
            if existing.id == queue_id:
                updated = replace(
                    existing,
                    queue_name_value=queue_name,
                    queue=queue,
                )
                self.queues[index] = updated
                return updated
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )

    def delete(self, *, queue_id: int) -> bool:
        for index, queue in enumerate(self.queues):
            if queue.id == queue_id:
                self.queues.pop(index)
                return True
        raise ApiResultError(
            result_code=10128,
            result_message=f"queue {queue_id} not exists",
        )


def empty_queue_adapter() -> FakeQueueAdapter:
    return FakeQueueAdapter(queues=[])
