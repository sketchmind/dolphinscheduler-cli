from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ProgramType(StrEnum):
    """Support program types"""
    JAVA = 'JAVA'
    SCALA = 'SCALA'
    PYTHON = 'PYTHON'

__all__ = ["ProgramType"]
