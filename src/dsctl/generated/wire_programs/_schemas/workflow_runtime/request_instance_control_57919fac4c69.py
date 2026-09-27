from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_b134db701311e34c218630703b55d6fc7d6aaf3728df2c0f22f9d2f682687964 import ExecuteType as ExecuteType

__all__ = ["ExecuteType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceControlParams(BaseParamsModel):
    projectCode: int
    processInstanceId: int
    executeType: ExecuteType

SOURCE_CLOSURE_DIGEST = 'sha256:2e5a1a67a1139ff5e69ce9d76c7c038ebfd175fc4de59e3f28479385a88c6255'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:57919fac4c69f550f7fb5f032f9e852219cd7c9aba71e3e3dbc43140e510f59c'
