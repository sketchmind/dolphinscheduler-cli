from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class MkdirParams(BaseParamsModel):
    type: ResourceType
    name: str
    currentDir: str

SOURCE_CLOSURE_DIGEST = 'sha256:2d56795543058b5eb71b20e746dee080eac107f30e4eb5e6ad95337d473f6c89'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:42d837d5fd1cf20a11f0e7a5ac7f1d99898df39004a3f2462761755109ce5011'
