from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class FailureStrategy(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> FailureStrategy:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    END = ('END', 0, 'end')
    CONTINUE = ('CONTINUE', 1, 'continue')

    @classmethod
    def from_code(cls, code: int) -> 'FailureStrategy':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown FailureStrategy code: {code}')

__all__ = ['FailureStrategy']
