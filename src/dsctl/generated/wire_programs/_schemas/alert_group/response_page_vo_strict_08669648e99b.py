from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel

T = TypeVar("T")

class AlertGroupVo(BaseContractModel):
    id: int = Field(default=0)
    groupName: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoAlertGroupVo(PageInfo[AlertGroupVo]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.vo.AlertGroupVo>."""

__all__ = ["AlertGroupVo", "PageInfo", "PageInfoAlertGroupVo"]

AlertGroupVo.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertGroupVo].model_rebuild(_types_namespace=globals())
PageInfoAlertGroupVo.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoAlertGroupVo
EXECUTABLE_SCHEMA_DIGEST = 'sha256:08669648e99b7bdebd29f15d9c91e5ce777904bb1258135e064fd4bbbc439b0a'
