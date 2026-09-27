from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_c11d3edddb73e44c6b6bd8e6c14388d4bfbebf321256ace15cce422c39e3e2d4 import CommandType as CommandType

from ..._schemas._enums.enum_d4adabe99491f9a2581f85077eaf70c501856a8cfeee3cca6699117d4b1c6240 import ComplementDependentMode as ComplementDependentMode

from ..._schemas._enums.enum_3bb59dfb27471d5a89a98e4e57132d96ac3a47213921826ae8f1d9eebbc75565 import ExecutionOrder as ExecutionOrder

from ..._schemas._enums.enum_340913ac6c37a9d9d3e68b1d50565a70b364df06a173e4ed945588d246ecf607 import FailureStrategy as FailureStrategy

from ..._schemas._enums.enum_1bfd725ef403a1fdc215a0df82a4cf6f6f57b576f222c5a3efb4b33006134f72 import Priority as Priority

from ..._schemas._enums.enum_888f9a41ef6e77cb9a37ee7387caffe2afe1f434f4b95e7b367505bbf07645d0 import RunMode as RunMode

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

from ..._schemas._enums.enum_878d4d11e37dab4c1fa1553fc63169e22641320fdd5253b5ae993e948f231c32 import WarningType as WarningType

__all__ = ["CommandType", "ComplementDependentMode", "ExecutionOrder", "FailureStrategy", "Priority", "RunMode", "TaskDependType", "WarningType"]

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
    tenantCode: str | None = Field(default='default')
    environmentCode: int | None = Field(default=-1)
    timeout: int | None = Field(default=None)
    startParams: str | None = Field(default=None)
    expectedParallelismNumber: int | None = Field(default=None)
    dryRun: int | None = Field(default=0)
    testFlag: int | None = Field(default=0)
    complementDependentMode: ComplementDependentMode | None = Field(default=None)
    version: int | None = Field(default=None)
    allLevelDependent: bool | None = Field(default=False)
    executionOrder: ExecutionOrder | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:575121a1e95bcbb83fe1dcdfa945bd9c45aecd89a42e6878df3a07661109c30c'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:633a780aec3540d0c8658b3ae412f8454ec06291b824d53f5ef3467969fe6d29'
