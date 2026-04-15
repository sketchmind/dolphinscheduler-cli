from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class StatisticsStateRequest(BaseContractModel):
    isAll: bool = Field(default=False, examples=[True])
    projectName: str | None = Field(default=None)
    projectCode: int | None = Field(default=None, examples=[1234567890])
    workflowName: str | None = Field(default=None)
    workflowCode: int | None = Field(default=None, examples=[1234567890])
    taskName: str | None = Field(default=None)
    taskCode: int | None = Field(default=None, examples=[1234567890])
    startTime: str | None = Field(default=None, alias='startDate', examples=['2022-01-01 10:01:02'])
    endTime: str | None = Field(default=None, alias='endDate', examples=['2022-01-02 10:01:02'])

__all__ = ["StatisticsStateRequest"]
