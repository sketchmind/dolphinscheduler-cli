from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_22d45c0dad770da81d8b43eb6910f221bc4177bf3bb277d5073b5d81d38dbeb2 import ReleaseState as ReleaseState

__all__ = ["ReleaseState"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class DefinitionReleaseParams(BaseParamsModel):
    projectCode: int
    code: int
    releaseState: ReleaseState

SOURCE_CLOSURE_DIGEST = 'sha256:1f4a97f8b019e6af95043d07df916e986a304bcbe28f062420547f0a8dcc22c5'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:d237b5fb600cfb960c8783b2a6ae5ddec5bddf3765e7817011980601e0caf99e'
