from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

__all__ = ["ResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class PageParams(BaseParamsModel):
    type: ResourceType
    id: int
    pageNo: int
    searchVal: str | None = Field(default=None)
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:26342d8293ac589a7db8427ffcd5567de2e101fae165ba95f8a5e7267c7910ae'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:568a94e3f34622eef5d4f420c5c6f89ff91a46a4d8efdf13f003bd4a0f61a1cd'
