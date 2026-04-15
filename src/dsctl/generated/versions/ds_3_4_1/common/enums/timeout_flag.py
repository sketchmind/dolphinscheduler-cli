from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TimeoutFlag(StrEnum):
    """Timeout flag"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> TimeoutFlag:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # 0 close 1 open
    CLOSE = ('CLOSE', 0, 'close')
    OPEN = ('OPEN', 1, 'open')

    @classmethod
    def from_code(cls, code: int) -> "TimeoutFlag":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown TimeoutFlag code: {code}")

__all__ = ["TimeoutFlag"]
