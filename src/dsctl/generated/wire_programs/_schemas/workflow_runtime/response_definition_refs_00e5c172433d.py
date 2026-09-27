from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

from ..._schemas._enums.enum_78e381705bdbe0474b22ab02487219cbbdb989107b1553b39dbdde5254627fbc import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

class ProcessDefinition(BaseEntityModel):
    id: int = Field(default=0)
    name: str | None = Field(default=None)
    version: int = Field(default=0)
    releaseState: ReleaseState | None = Field(default=None)
    projectId: int = Field(default=0)
    processDefinitionJson: str | None = Field(default=None)
    description: str | None = Field(default=None)
    globalParams: str | None = Field(default=None)
    globalParamList: list[Property] | None = Field(default=None)
    globalParamMap: dict[str, str] | None = Field(default=None)
    createTime: str | None = Field(default=None)
    updateTime: str | None = Field(default=None)
    flag: Flag | None = Field(default=None)
    userId: int = Field(default=0)
    userName: str | None = Field(default=None)
    projectName: str | None = Field(default=None)
    locations: str | None = Field(default=None)
    connects: str | None = Field(default=None)
    receivers: str | None = Field(default=None)
    receiversCc: str | None = Field(default=None)
    scheduleReleaseState: ReleaseState | None = Field(default=None)
    timeout: int = Field(default=0)
    tenantId: int = Field(default=0)
    modifyBy: str | None = Field(default=None)
    resourceIds: str | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

__all__ = ["DataType", "Direct", "Flag", "ReleaseState", "ProcessDefinition", "Property"]

ProcessDefinition.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:38666e2506f2b762caf8fe262f6370e06869abec3f1c38a1aa5e9a677da2de36'

RESPONSE_TYPE = list[ProcessDefinition]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:00e5c172433dff819f8753cbee6de5c2195ab57ef01e6da34d182c42d3969d28'
