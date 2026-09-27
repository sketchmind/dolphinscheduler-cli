from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_d57270264d12abc7ab255eea64c0275d6d014000b7b6aca56aeb03f2227b6a83 import AuditOperationType as AuditOperationType

from ..._schemas._enums.enum_b0d48f9fecb576112e4820f2e834ac13f14204a0851f1106f6dbb6a6ea92e036 import AuditResourceType as AuditResourceType

__all__ = ["AuditOperationType", "AuditResourceType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class AuditSingularPageParams(BaseParamsModel):
    pageNo: int
    pageSize: int
    resourceType: AuditResourceType | None = Field(default=None)
    operationType: AuditOperationType | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    userName: str | None = Field(default=None)

SOURCE_CLOSURE_DIGEST = 'sha256:7e986fd87fe7a925e0f8ed4bc79c09b72a190ce24993f4c80673c7aa4dcf3a35'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:8951e07dcce7cf8a3ff67cef49dd01a7cfe61ad257d5f6b3f8fb3d5236da56a1'
