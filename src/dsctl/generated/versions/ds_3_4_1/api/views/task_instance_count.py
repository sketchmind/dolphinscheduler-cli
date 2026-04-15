from __future__ import annotations

from pydantic import Field
from ..._models import BaseViewModel

from ...dao.model.task_instance_status_count_dto import TaskInstanceStatusCountDto

class TaskInstanceCountVO(BaseViewModel):
    totalCount: int = Field(default=0)
    taskInstanceStatusCounts: list[TaskInstanceStatusCountDto] | None = Field(default=None)

__all__ = ["TaskInstanceCountVO"]
