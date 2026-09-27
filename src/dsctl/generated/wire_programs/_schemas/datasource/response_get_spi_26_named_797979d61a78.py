from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import ConfigDict, Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_5eae97d0be49766a3d02fc6172c51d7e5ad97512c323e4243e35e06381d60332 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:ccc5a24f6fb6c1c645946f24904afc54da7d63ff723ef93fb6ddeaf26aa0bc05'

RESPONSE_TYPE = BaseDataSourceParamDTO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:797979d61a78c7841c10760f21a2f5e282b1cd417b39de06df80a44b0226d4da'
