from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

from .task import Task

class GanttDto(BaseContractModel):
    height: int = Field(default=0, description='height')
    tasks: list[Task] = Field(default_factory=list, description='tasks list')
    taskNames: list[int] | None = Field(default=None, description='task code list')
    taskStatus: dict[str, str] | None = Field(default=None, description='task status map')

__all__ = ["GanttDto"]
