from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DbType(StrEnum):
    code: int

    def __new__(cls, wire_value: str, code: int) -> DbType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        return obj
    MYSQL = ('MYSQL', 0)
    POSTGRESQL = ('POSTGRESQL', 1)
    HIVE = ('HIVE', 2)
    SPARK = ('SPARK', 3)
    CLICKHOUSE = ('CLICKHOUSE', 4)
    ORACLE = ('ORACLE', 5)
    SQLSERVER = ('SQLSERVER', 6)
    DB2 = ('DB2', 7)
    PRESTO = ('PRESTO', 8)
    H2 = ('H2', 9)

    @classmethod
    def from_code(cls, code: int) -> "DbType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown DbType code: {code}")

__all__ = ["DbType"]
