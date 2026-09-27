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
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class AlertPluginInstanceControllerListPagingPageTotalListNullableRequired(BaseViewModel, Generic[T]):
    """AST-inferred view from generated.view.AlertPluginInstanceController_listPaging_page_total_list_nullable_required."""
    totalList: list[T] | None
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class AlertPluginInstanceControllerListPagingPageTotalListNullableRequiredAlertPluginInstanceVO(AlertPluginInstanceControllerListPagingPageTotalListNullableRequired[AlertPluginInstanceVO]):
    """Specialized view for AlertPluginInstanceController_listPaging_page_total_list_nullable_required<org.apache.dolphinscheduler.api.vo.AlertPluginInstanceVO>."""

__all__ = ["AlertPluginInstanceVO", "PageInfo", "AlertPluginInstanceControllerListPagingPageTotalListNullableRequired", "AlertPluginInstanceControllerListPagingPageTotalListNullableRequiredAlertPluginInstanceVO"]

AlertPluginInstanceVO.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
AlertPluginInstanceControllerListPagingPageTotalListNullableRequired.model_rebuild(_types_namespace=globals())
AlertPluginInstanceControllerListPagingPageTotalListNullableRequired[AlertPluginInstanceVO].model_rebuild(_types_namespace=globals())
AlertPluginInstanceControllerListPagingPageTotalListNullableRequiredAlertPluginInstanceVO.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = AlertPluginInstanceControllerListPagingPageTotalListNullableRequiredAlertPluginInstanceVO
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8d6d3568ffd1f175cb9479792a5bbf07706c56d99404090a574fed8bb8e13f81'
