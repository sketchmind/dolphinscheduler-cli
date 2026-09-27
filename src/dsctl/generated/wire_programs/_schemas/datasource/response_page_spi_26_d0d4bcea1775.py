from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_f5bd40c5fc8687c62dacbfda904be11adadf9dcccf553dafbbf52172141f4934 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:07e412b2f9473378192e3ef6650c2f0d9997362772835d402da629dae54b078c'

RESPONSE_TYPE = PageInfoDataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d0d4bcea17756b0e3cddcc9a89119332467c95772aa12bc7dbe98a49611fadb4'
