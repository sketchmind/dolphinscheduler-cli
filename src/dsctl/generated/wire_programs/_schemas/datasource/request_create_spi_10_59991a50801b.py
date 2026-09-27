from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import ConfigDict, Field
from ....wire_runtime._models import BaseContractModel

from ..._schemas._enums.enum_4aec52e74ae3a9c3579966c298e6afbb81d0be67897fba3aa60d6fc18bce10c1 import DbType as DbType

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

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DataSourceCreateBodyParams(BaseParamsModel):
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:42c36195e9c6c72a9624c167d872529d0435e36c6421a7974a1449bda20ee4e9'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:59991a50801b30fea6ba6b17780721c88343c367fa4ef5f257e9e031fcd46a36'
