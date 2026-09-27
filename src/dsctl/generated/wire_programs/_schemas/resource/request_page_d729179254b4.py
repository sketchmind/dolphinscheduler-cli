from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class PageParams(BaseParamsModel):
    fullName: str
    type: ResourceType
    searchVal: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:3c66723d583eed23c3c3230343ba1167d74332715d7e569700e5035c4aeca2d5'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d729179254b4da8bdbeb8086608313c738ba7c0a46905a8f36d4d598e2b44087'
