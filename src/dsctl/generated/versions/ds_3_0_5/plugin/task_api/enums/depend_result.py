from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DependResult(StrEnum):
    """Depend result"""
    SUCCESS = 'SUCCESS'
    WAITING = 'WAITING'
    FAILED = 'FAILED'
    NON_EXEC = 'NON_EXEC'

__all__ = ["DependResult"]
