from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

class Resource(BaseEntityModel):
    id: int = Field(default=0)
    pid: int = Field(default=0)
    alias: str | None = Field(default=None)
    fullName: str | None = Field(default=None)
    isDirectory: bool = Field(default=False, alias='directory')
    description: str | None = Field(default=None)
    fileName: str | None = Field(default=None)
    userId: int = Field(default=0)
    type: ResourceType | None = Field(default=None)
    size: int = Field(default=0)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    userName: str | None = Field(default=None)

__all__ = ["ResourceType", "Resource"]

Resource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:d75778ed2600a24355e301676fef2d1cf83132548cb3cc694bf8cdae053da787'

RESPONSE_TYPE = Resource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:a64ce492c0f78a2cc3a665fe8dc2139f1e1efbe8583f5a61e9273bccb94daffd'
