from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class AlertType(StrEnum):
    """Warning message notification method"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> AlertType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 email; 1 SMS
    EMAIL = ('EMAIL', 0, 'email')
    SMS = ('SMS', 1, 'SMS')

    @classmethod
    def from_code(cls, code: int) -> "AlertType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown AlertType code: {code}")

__all__ = ["AlertType"]
