from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

class Schedule(BaseEntityModel):
    id: int | None = Field(default=None)
    processDefinitionCode: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    definitionDescription: str | None = Field(default=None)
    startTime: str | None = Field(default=None)
    endTime: str | None = Field(default=None)
    timezoneId: str | None = Field(default=None)
    crontab: str | None = Field(default=None)
    failureStrategy: FailureStrategy | None = Field(default=None)
    warningType: WarningType | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    releaseState: ReleaseState | None = Field(default=None)
    warningGroupId: int = Field(default=0)
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default=None)
    tenantCode: str | None = Field(default=None)
    environmentCode: int | None = Field(default=None)
    environmentName: str | None = Field(default=None)

__all__ = ["FailureStrategy", "Priority", "ReleaseState", "WarningType", "Schedule"]

Schedule.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:1b3ad3eca4638f440bf1841604a3242fd55185ea8f59794372adfe2f792442ff'

RESPONSE_TYPE = Schedule
EXECUTABLE_SCHEMA_DIGEST = 'sha256:524db5725278d213c0fb58479961f48d7220f1b886de200c960e0d84c86b8b22'
