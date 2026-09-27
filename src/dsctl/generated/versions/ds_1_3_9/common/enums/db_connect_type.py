from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DbConnectType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> DbConnectType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    ORACLE_SERVICE_NAME = ('ORACLE_SERVICE_NAME', 0, 'Oracle Service Name')
    ORACLE_SID = ('ORACLE_SID', 1, 'Oracle SID')

    @classmethod
    def from_code(cls, code: int) -> "DbConnectType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown DbConnectType code: {code}")

__all__ = ["DbConnectType"]
