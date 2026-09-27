from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseViewModel

from ..._schemas._enums.enum_c203d130ba407abef39441f76199290d0be22719505cf28180d5dba89d88d94e import DbConnectType as DbConnectType

class DataSourceServiceQueryDataSourceMap(BaseViewModel):
    """AST-inferred view from generated.view.DataSourceService_queryDataSource_map."""
    name: str | None = Field(default=None)
    note: str | None = Field(default=None)
    type: str | None = Field(default=None)
    connectType: DbConnectType | None = Field(default=None)
    host: str | None = Field(default=None)
    port: str | None = Field(default=None)
    principal: str | None = Field(default=None)
    database: str | None = Field(default=None)
    userName: str | None = Field(default=None)
    password: str | None = Field(default=None)
    other: dict[str, str] | None = Field(default=None)

__all__ = ["DbConnectType", "DataSourceServiceQueryDataSourceMap"]

DataSourceServiceQueryDataSourceMap.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:4f3ace6ac2b253f4c83f8db94f8016e23db2c4793f5e3e32f4a4ed4960a70d2a'

RESPONSE_TYPE = DataSourceServiceQueryDataSourceMap
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fb2a916630e5c54ad4978ccbe2dc230d0f3cd90f0cbafcad5d7a04587d667dcf'
