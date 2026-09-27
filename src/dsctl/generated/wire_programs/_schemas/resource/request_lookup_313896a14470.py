from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class LookupParams(BaseParamsModel):
    fullName: str | None = Field(default=None)
    id: int | None = Field(default=None)
    type: ResourceType

SOURCE_CLOSURE_DIGEST = 'sha256:68cb903e4870e4574adbf5028f65b3a2fdfb2ab3db992e8de569b273ea02400f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:313896a1447070a6fdc907f2699ec50336e221a6f89f8c6be0d73a3845665b61'
