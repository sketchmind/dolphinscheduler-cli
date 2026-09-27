from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

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
    """Specialized view for PageInfo<org.apache.dolphinscheduler.dao.entity.Resource>."""

__all__ = ["ResourceType", "PageInfo", "Resource", "PageInfoResource"]

PageInfo.model_rebuild(_types_namespace=globals())
Resource.model_rebuild(_types_namespace=globals())
PageInfo[Resource].model_rebuild(_types_namespace=globals())
PageInfoResource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:2f21e26a133ec5231f4e275e8bc08304beb1d28960a30ed9f99c125bcb91a91c'

RESPONSE_TYPE = PageInfoResource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e63bd3ffe1885124a2a82e5983cc4dedabf84221255aeae379211893fb3b5267'
