from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class Tenant(BaseEntityModel):
    id: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    description: str | None = Field(default=None)
    queueId: int = Field(default=0)
    queueName: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoTenant(PageInfo[Tenant]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Tenant>."""

__all__ = ["PageInfo", "Tenant", "PageInfoTenant"]

PageInfo.model_rebuild(_types_namespace=globals())
Tenant.model_rebuild(_types_namespace=globals())
PageInfo[Tenant].model_rebuild(_types_namespace=globals())
PageInfoTenant.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoTenant
EXECUTABLE_SCHEMA_DIGEST = 'sha256:eb17e6b60e3e8e9617ff9b1ff8830869665267e076eee4a530294bfc2d88aa29'
