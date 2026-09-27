from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

__all__ = ["TaskDependType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceExecuteTaskParams(BaseParamsModel):
    projectCode: int
    workflowInstanceId: int
    startNodeList: str
    taskDependType: TaskDependType

SOURCE_CLOSURE_DIGEST = 'sha256:7772fb68f09689abf4554f5cdf168f2dc23a7cb8d3a7856b1816e1e7a2d35b4f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8500931317ed483415b786460a15818a3562ed37ed63a7150c76f23629ee8bd7'
