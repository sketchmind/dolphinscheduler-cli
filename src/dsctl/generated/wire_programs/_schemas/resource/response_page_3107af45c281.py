from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

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

class PageInfoResource(PageInfo[Resource]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Resource>."""

__all__ = ["ResourceType", "PageInfo", "Resource", "PageInfoResource"]

PageInfo.model_rebuild(_types_namespace=globals())
Resource.model_rebuild(_types_namespace=globals())
PageInfo[Resource].model_rebuild(_types_namespace=globals())
PageInfoResource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:bfc4d61f7e0245100dc8b7386ee8ffa3f41587e21cbf777f12405ab6c319a60d'

RESPONSE_TYPE = PageInfoResource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:3107af45c2812d3c892c0955a261a5a697931e24e30ca28937259fc94f928e73'
