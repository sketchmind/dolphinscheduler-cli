from __future__ import annotations

from pydantic import ConfigDict, Field
from ...._models import BaseContractModel

from ....spi.enums.db_type import DbType

class BaseDataSourceParamDTO(BaseContractModel):
    """Basic datasource params submitted to api, each datasource plugin should have implementation."""
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
    type: DbType | None = Field(default=None, description='datasource type')

__all__ = ["BaseDataSourceParamDTO"]
