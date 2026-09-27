from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import ConfigDict, Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_867adbc57bc7fc423bd5487d46e7a24a517219e4f4d80444a54b8fdc66fe45ee import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:3a2c73b425202395a8b82e3a85b517e50f0ae5442ab953d1fb154ae16772ed2c'

RESPONSE_TYPE = BaseDataSourceParamDTO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:933ad9266acf8e453a95d1e5e49bac04cd5daacb086f4fd72918d7b75a95feda'
