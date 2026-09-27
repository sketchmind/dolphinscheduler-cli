from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class MkdirParams(BaseParamsModel):
    type: ResourceType
    name: str
    description: str | None = Field(default=None)
    pid: int
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:caba8e5887b56cb84b9e83087696b760b4cd68c33e4a799897dc29a518964a34'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ef200b1860d5a13f5e5961c65d76fd8731505b2c007f29d15ecc5488b4c46d68'
