from __future__ import annotations

from pydantic import Field
from ...._models import BaseContractModel

class ScheduleCreateRequest(BaseContractModel):
    workflowDefinitionCode: int = Field(examples=[1234567890123])
    crontab: str = Field(examples=['schedule timezone'])
    startTime: str = Field(examples=['2021-01-01 10:00:00'])
    endTime: str = Field(examples=['2022-01-01 12:00:00'])
    timezoneId: str = Field(examples=['Asia/Shanghai'])
    failureStrategy: str | None = Field(default=None, description='default CONTINUE if value not provide.', json_schema_extra={'allowable_values': ['CONTINUE', 'END']})
    releaseState: str | None = Field(default=None, description='default OFFLINE if value not provide.', json_schema_extra={'allowable_values': ['ONLINE', 'OFFLINE']})
    warningType: str | None = Field(default=None, description='default NONE if value not provide.', json_schema_extra={'allowable_values': ['NONE', 'SUCCESS', 'FAILURE', 'ALL']})
    warningGroupId: int = Field(default=0, description='default 0 if value not provide.', examples=[2])
    workflowInstancePriority: str | None = Field(default=None, description='default MEDIUM if value not provide.', json_schema_extra={'allowable_values': ['HIGHEST', 'HIGH', 'MEDIUM', 'LOW', 'LOWEST']})
    workerGroup: str | None = Field(default=None, examples=['worker-group-name'])
    tenantCode: str | None = Field(default=None, examples=['tenant-code'])
    environmentCode: int = Field(default=0, examples=['environment-code'])

__all__ = ["ScheduleCreateRequest"]
