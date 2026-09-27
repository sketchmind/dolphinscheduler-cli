from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import ConfigDict, Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_64a31f78df280707aebf10647477999145daf0b04dab41d7dd996d76cd7f443a import DbType as DbType

class BaseDataSourceParamDTO(BaseContractModel):
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        extra="allow",
    )
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    note: str | None = Field(default=None)
    host: str | None = Field(default=None)
    port: int | None = Field(default=None)
    database: str | None = Field(default=None)
    userName: str | None = Field(default=None)
    password: str | None = Field(default=None)
    other: dict[str, str] | None = Field(default=None)
    type: DbType | None = Field(default=None)

__all__ = ["DbType", "BaseDataSourceParamDTO"]

BaseDataSourceParamDTO.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:05c3fc1ad69a32639c3dafc2f6e1034735eb99dbad896cb75e009c1b423e2e9d'

RESPONSE_TYPE = BaseDataSourceParamDTO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d8172c61cbc7549847a286331219771c7d1f2f7534bfeab22c459e2a463a2428'
