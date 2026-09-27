from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

__all__ = ["Flag"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceUpdateLegacyParams(BaseParamsModel):
    projectName: str
    processInstanceJson: str | None = Field(default=None)
    processInstanceId: int
    scheduleTime: str | None = Field(default=None)
    syncDefine: bool
    locations: str | None = Field(default=None)
    connects: str | None = Field(default=None)
    flag: Flag | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:01953550f0e4c84a2e515673bbe6e98dc0409d8fbf301d4229e5fe773bafe10a'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1f822c582040e0522f1970a708b10da48a82d886b3c636e019813e15d744dfd0'
