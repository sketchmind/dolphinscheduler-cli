from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_65a98f45fef9d2238bc307a44e09cf80e2e00807dffbb6786119a150441997c8 import WorkflowExecutionStatus as WorkflowExecutionStatus

__all__ = ["WorkflowExecutionStatus"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstancePageParams(BaseParamsModel):
    projectCode: int
    workflowDefinitionCode: int | None = Field(default=0)
    searchVal: str | None = Field(default=None)
    executorName: str | None = Field(default=None)
    stateType: WorkflowExecutionStatus | None = Field(default=None)
    host: str | None = Field(default=None)
    startDate: str | None = Field(default=None)
    endDate: str | None = Field(default=None)
    otherParamsJson: str | None = Field(default=None)
    pageNo: int
    pageSize: int

SOURCE_CLOSURE_DIGEST = 'sha256:19a749118d341127a5f2dd65bd207697642350cd814dfc2eb6e11cdba90a4ce6'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:85444e234488b720eb6a0829e1d3adb81409ba283b863a2223aa3d23b7f655d3'
