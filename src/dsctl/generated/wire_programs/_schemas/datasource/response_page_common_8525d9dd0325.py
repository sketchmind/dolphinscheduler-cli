from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_ee75e34c201ea2cf08b93e7553403796e37f254fc82b2ca9a9d73ba2f5755553 import DbType as DbType

class DataSource(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    name: str | None = Field(default=None)
    note: str | None = Field(default=None)
    type: DbType | None = Field(default=None)
    connectionParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoDataSource(PageInfo[DataSource]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.DataSource>."""

__all__ = ["DbType", "DataSource", "PageInfo", "PageInfoDataSource"]

DataSource.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[DataSource].model_rebuild(_types_namespace=globals())
PageInfoDataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:3fb5f565db393d0939cb6b2d632973357e8148601a6278e6d3716f5767994573'

RESPONSE_TYPE = PageInfoDataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8525d9dd032550f92ada044753d84e816a10eed379e3e2d9133658ea88b6eee1'
