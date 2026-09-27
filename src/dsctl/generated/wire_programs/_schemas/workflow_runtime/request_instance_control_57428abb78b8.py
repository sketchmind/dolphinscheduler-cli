from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_b134db701311e34c218630703b55d6fc7d6aaf3728df2c0f22f9d2f682687964 import ExecuteType as ExecuteType

__all__ = ["ExecuteType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceControlParams(BaseParamsModel):
    projectName: str
    processInstanceId: int
    executeType: ExecuteType

SOURCE_CLOSURE_DIGEST = 'sha256:8976a05db2a8fd325940ae2a41a77a2d5ef6eeb96b5d980091e32bc786401d5f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:57428abb78b839969fd3ffda77b3b6c593d8466fe43a4cfce66d3b0d9ae65999'
