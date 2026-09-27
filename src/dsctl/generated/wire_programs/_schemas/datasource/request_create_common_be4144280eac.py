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

class DataSourceCreateBodyParams(BaseParamsModel):
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:5a9a9ce9ff7349483cc75e10d5a8412e3b9d813c1b1b8ad94283c18834794497'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:be4144280eac9a99e2e5b9ad5b633a307164f3f7a256b7967c1df1cd260e0e96'
