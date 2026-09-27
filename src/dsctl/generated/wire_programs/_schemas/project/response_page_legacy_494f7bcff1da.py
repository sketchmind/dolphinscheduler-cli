from __future__ import annotations

from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class Project(BaseEntityModel):
    id: int = Field(default=0)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    perm: int = Field(default=0)
    defCount: int = Field(default=0)
    instRunningCount: int = Field(default=0)

class PageInfoProject(PageInfo[Project]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.Project>."""

__all__ = ["PageInfo", "Project", "PageInfoProject"]

PageInfo.model_rebuild(_types_namespace=globals())
Project.model_rebuild(_types_namespace=globals())
PageInfo[Project].model_rebuild(_types_namespace=globals())
PageInfoProject.model_rebuild(_types_namespace=globals())

RESPONSE_TYPE = PageInfoProject
EXECUTABLE_SCHEMA_DIGEST = 'sha256:494f7bcff1daa0a860c57ab21f5084500b4b54c79d2615bf52a2345d91cc58de'
