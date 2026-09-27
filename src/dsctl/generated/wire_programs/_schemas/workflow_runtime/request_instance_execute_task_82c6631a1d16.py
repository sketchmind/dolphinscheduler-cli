from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_218e01f94e85b13f51221bb663dd1fce2e8cd502caee65aedb9b8fe9509eda6c import TaskDependType as TaskDependType

__all__ = ["TaskDependType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class InstanceExecuteTaskParams(BaseParamsModel):
    projectCode: int
    processInstanceId: int
    startNodeList: str
    taskDependType: TaskDependType

SOURCE_CLOSURE_DIGEST = 'sha256:f37aaa5f4f1e73f9c1ef157296dd37e0e2d9beb800496a653f88e94aadf76b2d'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:82c6631a1d16f9317779babafb0a5e39eb738d9e5fb2e2eaf8cab66dff07367c'
