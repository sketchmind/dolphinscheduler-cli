from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

from ..._schemas._enums.enum_b2fc892b7c6e5b8194502514e414218ccf75c6c3eedd69ec970a13684efd031f import RegistryNodeType as RegistryNodeType

__all__ = ["RegistryNodeType"]

from pydantic import Field
from ....wire_runtime.api.operations._base import BaseParamsModel

class MonitorNodeTypeLegacyParams(BaseParamsModel):
    nodeType: RegistryNodeType

SOURCE_CLOSURE_DIGEST = 'sha256:b481c075410813899afd7ddf2bc7ca917e3baa8da991be96cf554acc4a9f5459'
EXECUTABLE_SCHEMA_DIGEST = 'sha256:c6940e726fc43864640f22918e67892efb2d0727810cd3e85a4e56a581b3ea87'
