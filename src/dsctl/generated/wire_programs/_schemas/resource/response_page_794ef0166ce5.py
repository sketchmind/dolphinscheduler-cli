from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseViewModel

T = TypeVar("T")

from ..._schemas._enums.enum_399430dcb8566f02b5fb650e3a5e67aa92ec10588807f1b093a92bb17ff7edec import ResourceType as ResourceType

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class ResourceItemVO(BaseViewModel):
    alias: str | None = Field(default=None)
    userName: str | None = Field(default=None)
    fileName: str | None = Field(default=None)
    fullName: str | None = Field(default=None)
    isDirectory: bool = Field(default=False, alias='directory')
    type: ResourceType | None = Field(default=None)
    size: int = Field(default=0)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoResourceItemVO(PageInfo[ResourceItemVO]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.vo.ResourceItemVO>."""

__all__ = ["ResourceType", "PageInfo", "ResourceItemVO", "PageInfoResourceItemVO"]

PageInfo.model_rebuild(_types_namespace=globals())
ResourceItemVO.model_rebuild(_types_namespace=globals())
PageInfo[ResourceItemVO].model_rebuild(_types_namespace=globals())
PageInfoResourceItemVO.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:6622cfb08fc0d5d0c3f19dc8bfcafd081f5645f3d62e28a1f571a014f0b11036'

RESPONSE_TYPE = PageInfoResourceItemVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:794ef0166ce59037c92e564161f1ca132e67b7b2ffd904e2472b5ccee07909ea'
