from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

from ..._schemas._enums.enum_20d207118e9e893582e7f5cc3b5726390f0180559edac951673652b3125d5d9f import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

class ProcessDefinition(BaseEntityModel):
    id: int = Field(default=0)
    code: int = Field(default=0)
    name: str | None = Field(default=None)
    version: int = Field(default=0)
    releaseState: ReleaseState | None = Field(default=None)
    projectCode: int = Field(default=0)
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
    scheduleReleaseState: ReleaseState | None = Field(default=None)
    timeout: int = Field(default=0)
    tenantId: int = Field(default=0)
    tenantCode: str | None = Field(default=None)
    modifyBy: str | None = Field(default=None)
    warningGroupId: int = Field(default=0)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

__all__ = ["DataType", "Direct", "Flag", "ReleaseState", "ProcessDefinition", "Property"]

ProcessDefinition.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:247de7ec689acb917ad175e62f84211e8e8875ca54cdb6a3211c0c82fc0d377c'

RESPONSE_TYPE = ProcessDefinition
EXECUTABLE_SCHEMA_DIGEST = 'sha256:1da403820e1194beb6e260aa61d365d2466548abd5e7e1af95327d810a8885ad'
