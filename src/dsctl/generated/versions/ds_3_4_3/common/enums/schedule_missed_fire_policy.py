from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ScheduleMissedFirePolicy(StrEnum):
    code: int

    def __new__(cls, wire_value: str, code: int) -> ScheduleMissedFirePolicy:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        return obj
    SKIP_MISSED = ('SKIP_MISSED', 0)
    FIRE_ONCE_NOW = ('FIRE_ONCE_NOW', 1)
    FIRE_ALL_MISSED = ('FIRE_ALL_MISSED', 2)

    @classmethod
    def from_code(cls, code: int) -> "ScheduleMissedFirePolicy":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ScheduleMissedFirePolicy code: {code}")

__all__ = ["ScheduleMissedFirePolicy"]
