from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TaskTimeoutStrategy(StrEnum):
    """Task timeout strategy"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> TaskTimeoutStrategy:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 warn 1 failed 2 warn+failed
    WARN = ('WARN', 0, 'warn')
    FAILED = ('FAILED', 1, 'failed')
    WARNFAILED = ('WARNFAILED', 2, 'warnfailed')

    @classmethod
    def from_code(cls, code: int) -> "TaskTimeoutStrategy":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown TaskTimeoutStrategy code: {code}")

__all__ = ["TaskTimeoutStrategy"]
