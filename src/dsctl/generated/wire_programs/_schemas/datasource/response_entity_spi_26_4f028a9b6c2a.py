from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

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

__all__ = ["DbType", "DataSource"]

DataSource.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:bc88938929d269c987e625dc607a29f148a558030975a7c33b9e0c03bd5f9a27'

RESPONSE_TYPE = DataSource
EXECUTABLE_SCHEMA_DIGEST = 'sha256:4f028a9b6c2a66b1193950bcc61ad82d6a70eb6d9094655441af6cd90003fcc3'
