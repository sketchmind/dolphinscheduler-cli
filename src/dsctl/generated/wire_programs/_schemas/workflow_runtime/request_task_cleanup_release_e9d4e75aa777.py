from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

__all__ = ["ReleaseState"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class TaskCleanupReleaseParams(BaseParamsModel):
    projectCode: int
    code: int
    releaseState: ReleaseState

SOURCE_CLOSURE_DIGEST = 'sha256:51aa265d849bb3972ccc83546763c79e5dec1e0e09f3ac1c2708eea2aedc7e88'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:e9d4e75aa77739746045256828d6357c56ab29007f4e630676c4b19d4efacc9a'
