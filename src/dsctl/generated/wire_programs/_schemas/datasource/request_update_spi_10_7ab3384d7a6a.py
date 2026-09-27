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

class DataSourceUpdateBodyParams(BaseParamsModel):
    id: int
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:d39889ad97400e876a5fed1907e7f69a2bed33b3b7fce0d5e6a3f3ddb02b9d33'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:7ab3384d7a6ac10e77d5c95b48bb22e4c74945e6e6aa07db799b9d2b84f2d542'
