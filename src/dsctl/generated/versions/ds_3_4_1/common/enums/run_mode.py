from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class RunMode(StrEnum):
    """Complement data run mode"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> RunMode:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 serial run 1 parallel run
    RUN_MODE_SERIAL = ('RUN_MODE_SERIAL', 0, 'serial run')
    RUN_MODE_PARALLEL = ('RUN_MODE_PARALLEL', 1, 'parallel run')

    @classmethod
    def from_code(cls, code: int) -> "RunMode":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown RunMode code: {code}")

__all__ = ["RunMode"]
