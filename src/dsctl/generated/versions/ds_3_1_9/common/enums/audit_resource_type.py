from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class AuditResourceType(StrEnum):
    """Audit Module type"""
    code: int
    enMsg: str

    def __new__(cls, wire_value: str, code: int, enMsg: str) -> AuditResourceType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.enMsg = enMsg
        return obj
    USER_MODULE = ('USER_MODULE', 0, 'USER')
    PROJECT_MODULE = ('PROJECT_MODULE', 1, 'PROJECT')

    @classmethod
    def from_code(cls, code: int) -> "AuditResourceType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown AuditResourceType code: {code}")

__all__ = ["AuditResourceType"]
