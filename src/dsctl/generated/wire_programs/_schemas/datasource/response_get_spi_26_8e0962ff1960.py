from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import ConfigDict, Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_f5bd40c5fc8687c62dacbfda904be11adadf9dcccf553dafbbf52172141f4934 import DbType as DbType

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

SOURCE_CLOSURE_DIGEST = 'sha256:7dd814f34f968068a68a165635ed4e9d7ca148d49833d45b61847fd2ebd7f0df'

RESPONSE_TYPE = BaseDataSourceParamDTO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8e0962ff1960b1f86f9904b598da959f6f9f37dd046ab57898f672db2ab4104e'
