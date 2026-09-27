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

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DataSourceUpdateBodyParams(BaseParamsModel):
    id: int
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:2a5120d2d314e44d237792853223cc76a92b4a3969eacb2bdbcb8b90e058bb97'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1a00d9e812ff83ed8914e39653cba0e3b414f537ac512baefd3404df9c7b308b'
