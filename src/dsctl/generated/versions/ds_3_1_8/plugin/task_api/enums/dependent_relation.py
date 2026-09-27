from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DependentRelation(StrEnum):
    """Dependent relation: and or"""
    AND = 'AND'
    OR = 'OR'

__all__ = ["DependentRelation"]
