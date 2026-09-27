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

SOURCE_CLOSURE_DIGEST = 'sha256:8072c6ddb3356af423eac142cbdc11f0d5cf9def034c3d518a01e1c75e8390d7'

RESPONSE_TYPE = DataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4318f0f71c3ff40be8e2afa190cb7e47a3b269e7dc9908030e7e8b97cf90ccfc'
