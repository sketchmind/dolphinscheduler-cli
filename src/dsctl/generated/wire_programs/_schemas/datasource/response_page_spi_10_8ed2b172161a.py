from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_4aec52e74ae3a9c3579966c298e6afbb81d0be67897fba3aa60d6fc18bce10c1 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:6865a6041390d45ea4b4b16bec038ad5e3d4c7bbe23edc27f68267ef79e386da'

RESPONSE_TYPE = PageInfoDataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8ed2b172161ac27d4c2cde0b11faa6a81705cd22069d00654081850115a18874'
