from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_5eae97d0be49766a3d02fc6172c51d7e5ad97512c323e4243e35e06381d60332 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:cee712809537cd821556f625a080700af82c701ecfb058c853d067635ab0761f'

RESPONSE_TYPE = list[DataSource]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c5f61d2ec4649559c752c1dde9d12e362a3478fab031fd6cc8d22164d07b604b'
