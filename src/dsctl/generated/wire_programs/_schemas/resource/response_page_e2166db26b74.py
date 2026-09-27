from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_748959ead874e66f97819a45ee9db5491b525a1779092c4b6016b975bbd79d55 import ResourceType as ResourceType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class Resource(BaseEntityModel):
    id: int | None = Field(default=None)
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

class PageInfoResource(PageInfo[Resource]):
    """Specialized view for PageInfo<org.apache.dolphinscheduler.dao.entity.Resource>."""

__all__ = ["ResourceType", "PageInfo", "Resource", "PageInfoResource"]

PageInfo.model_rebuild(_types_namespace=globals())
Resource.model_rebuild(_types_namespace=globals())
PageInfo[Resource].model_rebuild(_types_namespace=globals())
PageInfoResource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:ef55197e1ff55f8c010ff0fd472c58632675e217b0bd1347f6145afcdf105b30'

RESPONSE_TYPE = PageInfoResource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e2166db26b74f6a617a368a51aa6b17078d92d26b90aeb4466558b88178fbe76'
