from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_bc4cd4180cacc5f42d4176ec2d34db870eee919298a123ffedbc690854336877 import CommandType as CommandType

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_888f9a41ef6e77cb9a37ee7387caffe2afe1f434f4b95e7b367505bbf07645d0 import RunMode as RunMode

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_ed0180176a382ad27a1c0709aa04b1040af0b0f2b1ead67fe29fe7f849923c64 import WarningType as WarningType

__all__ = ["CommandType", "FailureStrategy", "Priority", "RunMode", "TaskDependType", "WarningType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class WorkflowExecuteParams(BaseParamsModel):
    projectName: str
    processDefinitionId: int
    scheduleTime: str | None = Field(default=None)
    failureStrategy: FailureStrategy
    startNodeList: str | None = Field(default=None)
    taskDependType: TaskDependType | None = Field(default=None)
    execType: CommandType | None = Field(default=None)
    warningType: WarningType
    warningGroupId: int | None = Field(default=None)
    receivers: str | None = Field(default=None)
    receiversCc: str | None = Field(default=None)
    runMode: RunMode | None = Field(default=None)
    processInstancePriority: Priority | None = Field(default=None)
    workerGroup: str | None = Field(default='default')
    timeout: int | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:676b0c142e40a653685ba20f2af79cc1e2a3bcc38a5e3ac3f358250234260689'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:129c7043ffb255ea3163876ee50f2b1e6da17540503401bd258d75a77165564b'
