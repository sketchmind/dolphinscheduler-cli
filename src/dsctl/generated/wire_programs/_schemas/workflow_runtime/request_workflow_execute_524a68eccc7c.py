from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_b9e7938dda00be79843bbefb00b71c40c79660692145625fb8b978f8760401cf import CommandType as CommandType

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_888f9a41ef6e77cb9a37ee7387caffe2afe1f434f4b95e7b367505bbf07645d0 import RunMode as RunMode

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

__all__ = ["CommandType", "FailureStrategy", "Priority", "RunMode", "TaskDependType", "WarningType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class WorkflowExecuteParams(BaseParamsModel):
    projectCode: int
    processDefinitionCode: int
    scheduleTime: str | None = Field(default=None)
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

SOURCE_CLOSURE_DIGEST = 'sha256:e68c9c8e60649851d7312eff4abca0008a7802e9125d7a7fb1c6036b3ec8bff0'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:524a68eccc7cb89010f4f1318f90280ae59c8cac0bcd73563c0ab4cf0f58cc3c'
