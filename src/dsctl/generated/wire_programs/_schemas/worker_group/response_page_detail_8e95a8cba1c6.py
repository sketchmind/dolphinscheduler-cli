from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_411e2a7b984c822f3011d591eaf4dd8facb1aefcde33489dd8a9e759682669f2 import WorkerGroupSource as WorkerGroupSource

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class WorkerGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    addrList: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    description: str | None = Field(default=None)
    systemDefault: bool = Field(default=False)

class WorkerGroupPageDetail(WorkerGroup):
    source: WorkerGroupSource | None = Field(default=None)

class PageInfoWorkerGroupPageDetail(PageInfo[WorkerGroupPageDetail]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail>."""

__all__ = ["WorkerGroupSource", "PageInfo", "WorkerGroup", "WorkerGroupPageDetail", "PageInfoWorkerGroupPageDetail"]

PageInfo.model_rebuild(_types_namespace=globals())
WorkerGroup.model_rebuild(_types_namespace=globals())
WorkerGroupPageDetail.model_rebuild(_types_namespace=globals())
PageInfo[WorkerGroupPageDetail].model_rebuild(_types_namespace=globals())
PageInfoWorkerGroupPageDetail.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:4fbb90ebd09dbd75a858024d966892e523697ac58b3a247a819e4c2b112a34da'

RESPONSE_TYPE = PageInfoWorkerGroupPageDetail
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8e95a8cba1c631fedaf2726311977cbdf1dacdb580ac593e5d17625a9a97ae86'
