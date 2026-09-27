from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class UdfType(StrEnum):
    """UDF type"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> UdfType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 hive; 1 spark
    HIVE = ('HIVE', 0, 'hive')
    SPARK = ('SPARK', 1, 'spark')

    @classmethod
    def from_code(cls, code: int) -> "UdfType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown UdfType code: {code}")

__all__ = ["UdfType"]
