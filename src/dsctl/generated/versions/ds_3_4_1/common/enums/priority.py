from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class Priority(StrEnum):
    """Define process and task priority"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> Priority:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 highest priority 1 higher priority 2 medium priority 3 lower priority 4 lowest
    # priority
    HIGHEST = ('HIGHEST', 0, 'highest')
    HIGH = ('HIGH', 1, 'high')
    MEDIUM = ('MEDIUM', 2, 'medium')
    LOW = ('LOW', 3, 'low')
    LOWEST = ('LOWEST', 4, 'lowest')

    @classmethod
    def from_code(cls, code: int) -> "Priority":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown Priority code: {code}")

__all__ = ["Priority"]
