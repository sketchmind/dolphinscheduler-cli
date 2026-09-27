from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_af09a232aa00e57cc077eeded778ccd92a92db99071af42f4858a2550ec94484 import DbType as DbType

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
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class PageInfoDataSource(PageInfo[DataSource]):
    """Specialized view for PageInfo<DataSource>."""

__all__ = ["DbType", "DataSource", "PageInfo", "PageInfoDataSource"]

DataSource.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[DataSource].model_rebuild(_types_namespace=globals())
PageInfoDataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:8d18dbc816024fae5f44da96a65b08ada4a41d65be1336f8ce5532845d67aeea'

RESPONSE_TYPE = PageInfoDataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7cfe88880f52862a12d8a1460a60672916d9744a442d3e02fcd9e42d9da1db52'
