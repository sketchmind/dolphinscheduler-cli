from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from typing import Generic, TypeVar
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

T = TypeVar("T")

from ..._schemas._enums.enum_47a4a3e3a65c8347aba75f954c8bf048bc9168aef084c2c3bc7d0110cca7aac3 import AlertType as AlertType

class AlertGroup(BaseEntityModel):
    id: int = Field(default=0)
    groupName: str | None = Field(default=None)
    groupType: AlertType | None = Field(default=None)
    description: str | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)

class PageInfo(BaseContractModel, Generic[T]):
    totalList: list[T] | None = Field(default=None)
    total: int = Field(default=0)
    totalPage: int | None = Field(default=None)
    currentPage: int | None = Field(default=0)

class PageInfoAlertGroup(PageInfo[AlertGroup]):
    """Specialized view for org.apache.dolphinscheduler.api.utils.PageInfo<org.apache.dolphinscheduler.dao.entity.AlertGroup>."""

__all__ = ["AlertType", "AlertGroup", "PageInfo", "PageInfoAlertGroup"]

AlertGroup.model_rebuild(_types_namespace=globals())
PageInfo.model_rebuild(_types_namespace=globals())
PageInfo[AlertGroup].model_rebuild(_types_namespace=globals())
PageInfoAlertGroup.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:354559eecd13b68ed45d6c979c82bc09fbfd3e105e70ff9ecb8a9557ff5315bb'

RESPONSE_TYPE = PageInfoAlertGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:b70a35080843f61071876b426562acce8c5dad723561b33bfb94276ddb36ee45'
