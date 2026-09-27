from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseContractModel, BaseEntityModel

from ..._schemas._enums.enum_4d4785692b8670b354c4d35d14faf3c271fe5144ba0b9d261ee02e66907631a1 import DataType as DataType

from ..._schemas._enums.enum_8d471a35722c2ff88418836345e29fb1aae45407f2106711c1d8a9028772810b import Direct as Direct

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

from ..._schemas._enums.enum_c7ef46fe4ab574cc54a5e52a08773a1bb7b934f4fe11274c94f950c6425066b1 import ProcessExecutionTypeEnum as ProcessExecutionTypeEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

class ProcessDefinition(BaseEntityModel):
    id: int | None = Field(default=None)
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
    modifyBy: str | None = Field(default=None)
    warningGroupId: int | None = Field(default=None)
    executionType: ProcessExecutionTypeEnum | None = Field(default=None)

class Property(BaseContractModel):
    prop: str | None = Field(default=None)
    direct: Direct | None = Field(default=None)
    type: DataType | None = Field(default=None)
    value: str | None = Field(default=None)

__all__ = ["DataType", "Direct", "Flag", "ProcessExecutionTypeEnum", "ReleaseState", "ProcessDefinition", "Property"]

ProcessDefinition.model_rebuild(_types_namespace=globals())
Property.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:8f96c97fb0f5e03b024468dd07f0f4a0e559c49f265d45feadc6b083a58988a8'

RESPONSE_TYPE = ProcessDefinition
EXECUTABLE_SCHEMA_DIGEST = 'sha256:ad92b9869457081a9c482d43a513ea6e767d7ad7d8c9fa62459418bfbf989065'
