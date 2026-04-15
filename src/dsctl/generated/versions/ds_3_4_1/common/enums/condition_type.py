from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ConditionType(StrEnum):
    """Condition type"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> ConditionType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # 0 none 1 judge 2 delay
    NONE = ('NONE', 0, 'none')
    JUDGE = ('JUDGE', 1, 'judge')
    DELAY = ('DELAY', 2, 'delay')

    @classmethod
    def from_code(cls, code: int) -> "ConditionType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ConditionType code: {code}")

__all__ = ["ConditionType"]
