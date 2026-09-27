from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_9019775daf3bdd352213be60744947257cd9af6c87b3e1d245a0c55fdfc59924 import CommandType as CommandType

from ..._schemas._enums.enum_d4adabe99491f9a2581f85077eaf70c501856a8cfeee3cca6699117d4b1c6240 import ComplementDependentMode as ComplementDependentMode

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_888f9a41ef6e77cb9a37ee7387caffe2afe1f434f4b95e7b367505bbf07645d0 import RunMode as RunMode

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

__all__ = ["CommandType", "ComplementDependentMode", "FailureStrategy", "Priority", "RunMode", "TaskDependType", "WarningType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class WorkflowExecuteParams(BaseParamsModel):
    projectCode: int
    processDefinitionCode: int
    scheduleTime: str
    failureStrategy: FailureStrategy
    startNodeList: str | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    execType: CommandType | None = Field(default=None)
    warningType: WarningType
    warningGroupId: int | None = Field(default=None)
    runMode: RunMode | None = Field(default=None)
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default='default')
    environmentCode: int | None = Field(default=-1)
    timeout: int | None = Field(default=None)
    startParams: str | None = Field(default=None)
    expectedParallelismNumber: int | None = Field(default=None)
    dryRun: int | None = Field(default=0)
    complementDependentMode: ComplementDependentMode | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:68c8912c0708d1dd1c2c429e22e009e89eb0d94ea6597af395927ce8152fc3e2'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:72817782295e5df8e9bc96c1fac201b1b16ebd1a48c658c03e8cdc13eba5269e'
