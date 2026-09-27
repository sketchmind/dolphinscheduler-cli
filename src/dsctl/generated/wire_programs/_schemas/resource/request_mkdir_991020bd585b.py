from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class MkdirParams(BaseParamsModel):
    type: ResourceType
    name: str
    pid: int
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:4951ad9dfec394535dccccd642b50563d6720b8b84a630a97eafc1e90709c762'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:991020bd585ba006f0cfe7bc0d1e86fd4854c1fe7f588a0f67408aabc7e759ae'
