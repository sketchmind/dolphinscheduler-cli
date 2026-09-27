from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class Direct(StrEnum):
    """Parameter of stored procedure"""
    IN = 'IN'
    OUT = 'OUT'

__all__ = ["Direct"]
