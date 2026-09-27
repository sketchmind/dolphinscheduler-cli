from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseViewModel

T = TypeVar("T")

class AlertPluginInstanceVO(BaseViewModel):
    id: int = Field(default=0)
    pluginDefineId: int = Field(default=0)
    instanceName: str | None = Field(default=None)
    pluginInstanceParams: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    alertPluginName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoAlertPluginInstanceVO(PageInfo[AlertPluginInstanceVO]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>."""

__all__ = ["AlertPluginInstanceVO", "PageInfo", "PageInfoAlertPluginInstanceVO"]

AlertPluginInstanceVO.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertPluginInstanceVO].model_rebuild(_types_namespace=globals())
PageInfoAlertPluginInstanceVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAlertPluginInstanceVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:772d150485bec463f892a386d65159b8d1d2d04516814b5ba3dfac5bacbea55d'
