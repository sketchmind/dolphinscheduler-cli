from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TaskExecuteType(StrEnum):
    """Task execute type"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> TaskExecuteType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # 0 batch 1 stream
    BATCH = ('BATCH', 0, 'batch')
    STREAM = ('STREAM', 1, 'stream')

    @classmethod
    def from_code(cls, code: int) -> "TaskExecuteType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown TaskExecuteType code: {code}")

__all__ = ["TaskExecuteType"]
