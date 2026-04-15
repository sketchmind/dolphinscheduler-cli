from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class Flag(StrEnum):
    """Have_script have_file can_retry have_arr_variables have_map_variables have_alert"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> Flag:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 no 1 yes
    NO = ('NO', 0, 'no')
    YES = ('YES', 1, 'yes')

    @classmethod
    def from_code(cls, code: int) -> "Flag":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown Flag code: {code}")

__all__ = ["Flag"]
