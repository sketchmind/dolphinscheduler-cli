from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class AuditDto(BaseContractModel):
    userName: str | None = Field(default=None)
    resource: str | None = Field(default=None)
    operation: str | None = Field(default=None)
    time: str | None = Field(default=None)
    resourceName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoAuditDto(PageInfo[AuditDto]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.dto.AuditDto>."""

__all__ = ["AuditDto", "PageInfo", "PageInfoAuditDto"]

AuditDto.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AuditDto].model_rebuild(_types_namespace=globals())
PageInfoAuditDto.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAuditDto
EXECUTABLE_SCHEMA_DIGEST = 'sha256:aacbfa16526a21cd4941d0e942e1bf886c3e958121519733fb4b94957b917d1d'
