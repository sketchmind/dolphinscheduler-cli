from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class UploadParams(BaseParamsModel):
    type: ResourceType
    name: str
    description: str | None = Field(default=None)
    pid: int
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:c12e3079a744e257bff6f3d13017877205fe992b845dceb4cd660748aaff1b3a'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:bc44db877d7e43f7344b3129ca6cd16e2f8b8087c7a73cf2e29ae04f6f5bf185'
