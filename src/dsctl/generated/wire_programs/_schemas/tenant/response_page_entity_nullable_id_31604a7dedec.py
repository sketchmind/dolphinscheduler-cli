from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class Tenant(BaseEntityModel):
    id: int | None = Field(default=None)
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
EXECUTABLE_SCHEMA_DIGEST = 'sha256:31604a7dedec9cb56cc2d64470fa247daa409135a885482effff40f579cec101'
