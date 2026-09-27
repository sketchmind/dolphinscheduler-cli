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

class DataSourceCreateBodyParams(BaseParamsModel):
    dataSourceParam: BaseDataSourceParamDTO

SOURCE_CLOSURE_DIGEST = 'sha256:5b7968c3e19ec6b4c7555d0670b86bd3d7d4e6d50d0ab8000c7e5829762182d3'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b2216bc63b7a51138d06e1ff370f063bf3c046f3e4e4b743f7eff9f698c89c01'
