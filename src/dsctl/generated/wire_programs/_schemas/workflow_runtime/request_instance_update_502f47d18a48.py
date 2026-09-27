from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

__all__ = ["Flag"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceUpdateParams(BaseParamsModel):
    projectCode: int
    taskRelationJson: str
    taskDefinitionJson: str
    id: int
    scheduleTime: str | None = Field(default=None)
    syncDefine: bool
    globalParams: str | None = Field(default='[]')
    locations: str | None = Field(default=None)
    timeout: int | None = Field(default=0)
    tenantCode: str
    flag: Flag | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:09a205d0e3607a299c6de4bfbf558013a7adabe3ae9f0e40ed4c02e19ba8064b'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:502f47d18a4822cb472d9de45047b24060ca5e9da57f9516c7f41d93636ead27'
