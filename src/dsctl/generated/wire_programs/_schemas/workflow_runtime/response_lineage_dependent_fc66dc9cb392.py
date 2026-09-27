from __future__ import annotations

from enum import Enum, IntEnum, StrEnum
from pydantic import Field
from ....wire_runtime._models import BaseEntityModel

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

class TaskMainInfo(BaseEntityModel):
    id: int = Field(default=0)
    taskName: str | None = Field(default=None)
    taskCode: int = Field(default=0)
    taskVersion: int = Field(default=0)
    taskType: str | None = Field(default=None)
    taskCreateTime: str | None = Field(default=None)
    taskUpdateTime: str | None = Field(default=None)
    projectCode: int = Field(default=0)
    processDefinitionCode: int = Field(default=0)
    processDefinitionVersion: int = Field(default=0)
    processDefinitionName: str | None = Field(default=None)
    processReleaseState: ReleaseState | None = Field(default=None)
    upstreamTaskMap: dict[int, str | None] | None = Field(default=None)
    upstreamTaskCode: int = Field(default=0)
    upstreamTaskName: str | None = Field(default=None)

__all__ = ["ReleaseState", "TaskMainInfo"]

TaskMainInfo.model_rebuild(_types_namespace=globals())

SOURCE_CLOSURE_DIGEST = 'sha256:a72030ad10c17d36741477e00ed70025d458c1942143a2b5da7aba9fa7045a09'

RESPONSE_TYPE = list[TaskMainInfo]
EXECUTABLE_SCHEMA_DIGEST = 'sha256:fc66dc9cb3925ff3cbe42745ecd6a58332d52a582d9733e87f943929f1dfa34e'
