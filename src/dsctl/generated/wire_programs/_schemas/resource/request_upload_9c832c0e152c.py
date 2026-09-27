from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class UploadParams(BaseParamsModel):
    type: ResourceType
    name: str
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:19241899eba3a90908c2e0cb412d9bc3cd88925e563357963f59b9b7caeeb99d'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9c832c0e152c5a60a9845d57ac6a55e2f5f4fcc204656cdca38a18f0c1f9a099'
