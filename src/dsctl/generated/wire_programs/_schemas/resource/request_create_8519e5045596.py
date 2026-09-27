from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class CreateParams(BaseParamsModel):
    type: ResourceType
    fileName: str
    suffix: str
    description: str | None = Field(default=None)
    content: str
    pid: int
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:81698c50db047f3dfb27ffc11aabd4591f0221f3ae15f48b1f2833083b243524'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8519e5045596a7659e690bcf42403fcc57c8d784ffebe960e3d8eb8b9436d0a8'
