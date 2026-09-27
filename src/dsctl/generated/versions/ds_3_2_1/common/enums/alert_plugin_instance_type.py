from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class AlertPluginInstanceType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> AlertPluginInstanceType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    NORMAL = ('NORMAL', 0, 'NORMAL')
    GLOBAL = ('GLOBAL', 1, 'GLOBAL')

    @classmethod
    def from_code(cls, code: int) -> "AlertPluginInstanceType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown AlertPluginInstanceType code: {code}")

__all__ = ["AlertPluginInstanceType"]
