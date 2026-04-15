from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class UserType(StrEnum):
    """User type"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> UserType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # 0 admin user; 1 general user
    ADMIN_USER = ('ADMIN_USER', 0, 'admin user')
    GENERAL_USER = ('GENERAL_USER', 1, 'general user')

    @classmethod
    def from_code(cls, code: int) -> "UserType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown UserType code: {code}")

__all__ = ["UserType"]
