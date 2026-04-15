from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ComplementDependentMode(StrEnum):
    """Task node depend type"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> ComplementDependentMode:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # 0 off mode 1 run complement data with all dependent process
    OFF_MODE = ('OFF_MODE', 0, 'off mode')
    ALL_DEPENDENT = ('ALL_DEPENDENT', 1, 'all dependent')

    @classmethod
    def from_code(cls, code: int) -> "ComplementDependentMode":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ComplementDependentMode code: {code}")

__all__ = ["ComplementDependentMode"]
