from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field, StrictInt
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class K8sNamespace(BaseEntityModel):
    id: int | None = Field(default=None)
    code: int | None = Field(default=None)
    namespace: str | None = Field(default=None)
    limitsCpu: float | None = Field(default=None)
    limitsMemory: int | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    podRequestCpu: float = Field(default=0.0)
    podRequestMemory: int = Field(default=0)
    podReplicas: int = Field(default=0)
    clusterCode: int | None = Field(default=None)
    clusterName: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: StrictInt = Field(default=0)
    totalPage: StrictInt | None = Field(default=None)
    pageSize: StrictInt = Field(default=20)
    currentPage: StrictInt | None = Field(default=0)
    pageNo: StrictInt | None = Field(default=None)

class PageInfoK8sNamespace(PageInfo[K8sNamespace]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.K8sNamespace>."""

__all__ = ["K8sNamespace", "PageInfo", "PageInfoK8sNamespace"]

K8sNamespace.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[K8sNamespace].model_rebuild(_types_namespace=globals())
PageInfoK8sNamespace.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoK8sNamespace
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fb9ad9249241e157eeab3422fc09c8e3cf337cb450ad99cee1bbb96dd443d396'
