from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_4002f7add060509f8a0c64b5fa9f8c548f969f746f1dd3fd35b232f58bfc3301 import ExecuteType as ExecuteType

__all__ = ["ExecuteType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceControlParams(BaseParamsModel):
    projectCode: int
    processInstanceId: int
    executeType: ExecuteType

SOURCE_CLOSURE_DIGEST = 'sha256:c8da0b5e7d1be9ccd363404070786f3ceb8349fda27ba3bc490135b600efe239'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ccd1d860efdecdb59e490566330461454ad7ea87c7a06942851906e2d68e155d'
