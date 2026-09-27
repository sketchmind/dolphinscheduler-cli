from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_7d5407323243938975d9a2555727912955d4d9da0bf0bc3ad99674350d454147 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:fb083d29c4660dbc1145952d6c744fb296210bd22701cb1e2adb12bc37af9937'

RESPONSE_TYPE = list[DataSource]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b504f5fa6b706c4e531fecb3bc3e8fe8064daaa437ae2e9678b9104b7b9d2574'
