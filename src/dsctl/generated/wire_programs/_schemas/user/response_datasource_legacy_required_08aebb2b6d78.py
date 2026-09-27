from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

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

__all__ = ["DbType", "DataSource"]

DataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:7ecbcd1454ef2e0cbb97c095991f8d74ae77627f02837d2f8fd39723aeae30b0'

RESPONSE_TYPE = list[DataSource]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:08aebb2b6d78dc68c40d456bca0cdf47ee6e352c27a77ba59fd63fa9dfa9ef61'
