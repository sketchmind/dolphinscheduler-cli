from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_4002f7add060509f8a0c64b5fa9f8c548f969f746f1dd3fd35b232f58bfc3301 import ExecuteType as ExecuteType

__all__ = ["ExecuteType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceControlParams(BaseParamsModel):
    projectCode: int
    workflowInstanceId: int
    executeType: ExecuteType

SOURCE_CLOSURE_DIGEST = 'sha256:a6952bf33433a6a7ac62f4f2493cccdc2b71cdfe89bfcc22b288139ada6ae951'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d5a0cbcc3f42c0401ed1ef69133e1027c9a4475c66a0b6374eb12b4d58d94358'
