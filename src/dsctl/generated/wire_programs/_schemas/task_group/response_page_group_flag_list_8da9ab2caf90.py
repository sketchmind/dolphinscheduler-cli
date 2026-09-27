from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] = Field(default_factory=list)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    pageSize: int = Field(default=20)
    currentPage: int | None = Field(default=0)
    pageNo: int | None = Field(default=None)

class TaskGroup(BaseEntityModel):
    id: int | None = Field(default=None)
    name: str | None = Field(default=None)
    projectCode: int = Field(default=0)
    description: str | None = Field(default=None)
    groupSize: int = Field(default=0)
    useSize: int = Field(default=0)
    userId: int = Field(default=0)
    status: Flag | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfoTaskGroup(PageInfo[TaskGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.TaskGroup>."""

__all__ = ["Flag", "PageInfo", "TaskGroup", "PageInfoTaskGroup"]

PageInfo.model_rebuild(_types_namespace=globals())
TaskGroup.model_rebuild(_types_namespace=globals())
PageInfo[TaskGroup].model_rebuild(_types_namespace=globals())
PageInfoTaskGroup.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:1b311e6f01c5f53f0bf13fcb19d08edbdc7630e3951e62e0852b23623d2cdb8c'

RESPONSE_TYPE = PageInfoTaskGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8da9ab2caf905c18ded3ae7d7879470643eb6deb6bca9a529f649cfdd9d04893'
