from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from .task_state_count import TaskStateCount

class TaskCountDto(BaseContractModel):
    totalCount: int = Field(default=0, description='total count')
    taskCountDtos: list[TaskStateCount] | None = Field(default=None, description='task state count list')

__all__ = ["TaskCountDto"]
