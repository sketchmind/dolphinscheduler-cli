from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class AlertGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    groupName: str | None = Field(default=None)
    alertInstanceIds: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUserId: int = Field(default=0)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoAlertGroup(PageInfo[AlertGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AlertGroup>."""

__all__ = ["AlertGroup", "PageInfo", "PageInfoAlertGroup"]

AlertGroup.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertGroup].model_rebuild(_types_namespace=globals())
PageInfoAlertGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAlertGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:9c0b239585dece0a7ab9986bf70a7c3d01cff89c7bf7d6a5efb31fe8ab8afa7d'
