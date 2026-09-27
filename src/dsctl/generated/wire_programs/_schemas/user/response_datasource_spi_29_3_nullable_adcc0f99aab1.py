from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

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

__all__ = ["DbType", "DataSource"]

DataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:6589f709d6e9453d1a54932b8c9983b1a51bbe5dce9fe1dcf63593492e452aa2'

RESPONSE_TYPE = list[DataSource]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:adcc0f99aab15ba930c219d3e56cd963e08fbadef15c237ffd07067e5e7e9ab9'
