from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class AlertGroup(BaseEntityModel):
    id: int = Field(default=0)
    groupName: str | None = Field(default=None)
    alertInstanceIds: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    createUserId: int = Field(default=0)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoAlertGroup(PageInfo[AlertGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AlertGroup>."""

__all__ = ["AlertGroup", "PageInfo", "PageInfoAlertGroup"]

AlertGroup.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertGroup].model_rebuild(_types_namespace=globals())
PageInfoAlertGroup.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAlertGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:75c237208ab4aa857f4737bd45837ac1f363ff5daf9107b1ba47fa2e4d2fb96d'
