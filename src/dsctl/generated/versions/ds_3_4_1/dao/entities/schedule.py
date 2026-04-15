from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...common.enums.failure_strategy import FailureStrategy
from ...common.enums.priority import Priority
from ...common.enums.release_state import ReleaseState
from ...common.enums.warning_type import WarningType

class Schedule(BaseEntityModel):
    id: int | None = Field(default=None)
    workflowDefinitionCode: int = Field(default=0)
    workflowDefinitionName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    definitionDescription: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    timezoneId: str | None = Field(default=None, description='timezoneId <p>see {@link java.util.TimeZone#getTimeZone(String)}')
    crontab: str | None = Field(default=None)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    releaseState: ReleaseState | None = Field(default=None)
    warningGroupId: int = Field(default=0)
    workflowInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    environmentName: str | None = Field(default=None)

__all__ = ["Schedule"]
