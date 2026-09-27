from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_3f648bf0967bf9b023cb4b874fe7000847e14d34036ab1c73d2526b8435d0e3c import Flag as Flag

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

__all__ = ["Flag", "TaskGroup"]

TaskGroup.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:e2ab82c1e13afd5b4a0645b6a12ffe38ebfa1dba7e2bfac8b827603aa337566a'

RESPONSE_TYPE = TaskGroup
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c0804713c41acaee52d2edf78c0f8dba0e4dc4aa602ee7c88d0ab4738a5f1095'
