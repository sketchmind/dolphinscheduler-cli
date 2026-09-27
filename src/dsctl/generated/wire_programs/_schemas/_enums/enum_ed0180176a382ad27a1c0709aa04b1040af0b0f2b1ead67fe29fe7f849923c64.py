from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class WarningType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> WarningType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    NONE = ('NONE', 0, 'none')
    SUCCESS = ('SUCCESS', 1, 'success')
    FAILURE = ('FAILURE', 2, 'failure')
    ALL = ('ALL', 3, 'all')

    @classmethod
    def from_code(cls, code: int) -> 'WarningType':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown WarningType code: {code}')

__all__ = ['WarningType']
