from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class DbType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> DbType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    MYSQL = ('MYSQL', 0, 'mysql')
    POSTGRESQL = ('POSTGRESQL', 1, 'postgresql')
    HIVE = ('HIVE', 2, 'hive')
    SPARK = ('SPARK', 3, 'spark')
    CLICKHOUSE = ('CLICKHOUSE', 4, 'clickhouse')
    ORACLE = ('ORACLE', 5, 'oracle')
    SQLSERVER = ('SQLSERVER', 6, 'sqlserver')
    DB2 = ('DB2', 7, 'db2')
    PRESTO = ('PRESTO', 8, 'presto')
    H2 = ('H2', 9, 'h2')
    REDSHIFT = ('REDSHIFT', 10, 'redshift')

    @classmethod
    def from_code(cls, code: int) -> "DbType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown DbType code: {code}")

__all__ = ["DbType"]
