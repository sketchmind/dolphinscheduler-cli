from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class AuditOperationType(StrEnum):
    code: int
    enMsg: str

    def __new__(cls, wire_value: str, code: int, enMsg: str) -> AuditOperationType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.enMsg = enMsg
        return obj
    CREATE = ('CREATE', 0, 'CREATE')
    READ = ('READ', 1, 'READ')
    UPDATE = ('UPDATE', 2, 'UPDATE')
    DELETE = ('DELETE', 3, 'DELETE')

    @classmethod
    def from_code(cls, code: int) -> 'AuditOperationType':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown AuditOperationType code: {code}')

__all__ = ['AuditOperationType']
