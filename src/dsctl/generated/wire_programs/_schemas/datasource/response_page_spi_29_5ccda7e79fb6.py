from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_71e71b7c3b3544206b54a7f7a4bc9aeaab72aad8ecd41a4cadae93013a7408fc import DbType as DbType

class DataSource(BaseEntityModel):
    id: int | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    name: str | None = Field(default=None)
    note: str | None = Field(default=None)
    type: DbType | None = Field(default=None)
    connectionParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoDataSource(PageInfo[DataSource]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.DataSource>."""

__all__ = ["DbType", "DataSource", "PageInfo", "PageInfoDataSource"]

DataSource.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[DataSource].model_rebuild(_types_namespace=globals())
PageInfoDataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:b61b0ee0f4b72df2deeac310fab038730b62671233174851252c95fb5dabb13b'

RESPONSE_TYPE = PageInfoDataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:5ccda7e79fb60a7546defdcf97851e90eba03a1a7300ed347a1d453859b57118'
