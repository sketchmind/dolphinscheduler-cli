from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class Task(BaseContractModel):
    """Task"""
    taskName: str | None = Field(default=None, description='task name')
    startDate: list[int] = Field(default_factory=list, description='task start date')
    endDate: list[int] = Field(default_factory=list, description='task end date')
    executionDate: str | None = Field(default=None, description='task execution date')
    isoStart: str | None = Field(default=None, description='task iso start')
    isoEnd: str | None = Field(default=None, description='task iso end')
    status: str | None = Field(default=None, description='task status')
    duration: str | None = Field(default=None, description='task duration')

__all__ = ["Task"]
