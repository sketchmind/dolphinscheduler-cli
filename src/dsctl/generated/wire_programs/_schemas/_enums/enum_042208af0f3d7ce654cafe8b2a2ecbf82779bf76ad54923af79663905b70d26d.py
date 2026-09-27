from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ResourceType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> ResourceType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    FILE = ('FILE', 0, 'file')
    UDF = ('UDF', 1, 'udf')
    ALL = ('ALL', 2, 'all')

    @classmethod
    def from_code(cls, code: int) -> 'ResourceType':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown ResourceType code: {code}')

__all__ = ['ResourceType']
