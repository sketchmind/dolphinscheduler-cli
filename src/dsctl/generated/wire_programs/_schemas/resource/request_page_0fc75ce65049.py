from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_042208af0f3d7ce654cafe8b2a2ecbf82779bf76ad54923af79663905b70d26d import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class PageParams(BaseParamsModel):
    fullName: str
    tenantCode: str
    type: ResourceType
    pageNo: int
    searchVal: str | None = Field(default=None)
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:8a61a0454939e9e2b78bfe92b95c163babc07901884ea6fd59ff0d4821da119f'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:0fc75ce6504944fdef504937e75959890079fad11236d912747ef2273e86235a'
