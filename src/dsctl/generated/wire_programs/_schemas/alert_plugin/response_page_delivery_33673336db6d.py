from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseViewModel

T = TypeVar("T")

class AlertPluginInstanceVO(BaseViewModel):
    id: int = Field(default=0)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    instanceType: str | None = Field(default=None)
    warningType: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    alertPluginName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class PageInfoAlertPluginInstanceVO(PageInfo[AlertPluginInstanceVO]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>."""

__all__ = ["AlertPluginInstanceVO", "PageInfo", "PageInfoAlertPluginInstanceVO"]

AlertPluginInstanceVO.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertPluginInstanceVO].model_rebuild(_types_namespace=globals())
PageInfoAlertPluginInstanceVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAlertPluginInstanceVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:33673336db6df862a8172808f5db547dd0891467103ac239f1cf76fb51181ca5'
