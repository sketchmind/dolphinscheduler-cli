from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_ee75e34c201ea2cf08b93e7553403796e37f254fc82b2ca9a9d73ba2f5755553 import DbType as DbType

class BaseDataSourceParamDTO(BaseContractModel):
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

SOURCE_CLOSURE_DIGEST = 'sha256:6b036171a23ccd502b893eef158ff14694ceea581e5e5b5237f6c98b366f2a68'

RESPONSE_TYPE = BaseDataSourceParamDTO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:035bd3b96bca8c64ed64a187d50eaa7c0b422404b0b7ed75b4d6f0d674d99bb9'
