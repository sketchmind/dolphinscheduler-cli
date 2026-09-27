from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

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

__all__ = ["DbType", "DataSource"]

DataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:3348ebf0af19d80689d20a5c90c183516d637de220efd58f5300ab0fa4d79180'

RESPONSE_TYPE = list[DataSource]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:87a4150fdfadc6bc7431bed13ef909bb10859623d72e0da6ba81cd96f5d09768'
