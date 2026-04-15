from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ExecutionOrder(StrEnum):
    """Complement data in some kind of order"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> ExecutionOrder:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # 0 complement data in descending order 1 complement data in ascending order
    DESC_ORDER = ('DESC_ORDER', 0, 'descending order')
    ASC_ORDER = ('ASC_ORDER', 1, 'ascending order')

    @classmethod
    def from_code(cls, code: int) -> "ExecutionOrder":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ExecutionOrder code: {code}")

__all__ = ["ExecutionOrder"]
