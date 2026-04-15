from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

from ...common.enums.command_type import CommandType

class CommandStateCount(BaseContractModel):
    """Command state count"""
    errorCount: int = Field(default=0)
    normalCount: int = Field(default=0)
    commandState: CommandType | None = Field(default=None)

__all__ = ["CommandStateCount"]
