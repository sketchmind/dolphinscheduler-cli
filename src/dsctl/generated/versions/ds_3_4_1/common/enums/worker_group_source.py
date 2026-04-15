from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class WorkerGroupSource(StrEnum):
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> WorkerGroupSource:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    CONFIG = ('CONFIG', 1, 'config')
    UI = ('UI', 2, 'ui')

    @classmethod
    def from_code(cls, code: int) -> "WorkerGroupSource":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown WorkerGroupSource code: {code}")

__all__ = ["WorkerGroupSource"]
