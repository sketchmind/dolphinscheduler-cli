from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class CreateParams(BaseParamsModel):
    type: ResourceType
    fileName: str
    suffix: str
    content: str
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:1dfca98d359158f48b9572286e22ed216a152a89570643d9403969ec37afb417'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8d236105753033da1cf8f88a4c7b6f45a27855a1f9877f6150d14f51a33320cd'
