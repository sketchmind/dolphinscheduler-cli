from __future__ import annotations

from pydantic import Field
from ..._models import BaseEntityModel

from ...spi.enums.db_type import DbType

class DataSource(BaseEntityModel):
    id: int | None = Field(default=None, description='id')
    userId: int = Field(default=0, description='user id')
    userName: str | None = Field(default=None, description='user name')
    name: str | None = Field(default=None, description='data source name')
    note: str | None = Field(default=None, description='note')
    type: DbType | None = Field(default=None, description='data source type')
    connectionParams: str | None = Field(default=None, description='connection parameters')
    createTime: str | None = Field(default=None, description='create time')
    updateTime: str | None = Field(default=None, description='update time')

__all__ = ["DataSource"]
