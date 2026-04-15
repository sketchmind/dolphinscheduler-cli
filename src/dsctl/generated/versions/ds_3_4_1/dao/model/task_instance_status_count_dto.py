from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from ...plugin.task_api.enums.task_execution_status import TaskExecutionStatus

class TaskInstanceStatusCountDto(BaseContractModel):
    state: TaskExecutionStatus | None = Field(default=None)
    count: int = Field(default=0)

__all__ = ["TaskInstanceStatusCountDto"]
