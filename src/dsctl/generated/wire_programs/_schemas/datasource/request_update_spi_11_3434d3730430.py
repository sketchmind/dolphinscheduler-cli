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

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DataSourceUpdateBodyParams(BaseParamsModel):
    id: int
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:a31d161e9c4f73dbfe02e2623bb296ca821f89314c179e98a3d5d730df6786ab'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:3434d3730430c15101c5bfffd3018d728b0ca9fec398a299b49982659d1a8afc'
