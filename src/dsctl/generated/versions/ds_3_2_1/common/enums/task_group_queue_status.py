from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TaskGroupQueueStatus(StrEnum):
    """Running status for task group queue"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> TaskGroupQueueStatus:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    WAIT_QUEUE = ('WAIT_QUEUE', -1, 'wait queue')
    ACQUIRE_SUCCESS = ('ACQUIRE_SUCCESS', 1, 'acquire success')
    RELEASE = ('RELEASE', 2, 'release')

    @classmethod
    def from_code(cls, code: int) -> "TaskGroupQueueStatus":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown TaskGroupQueueStatus code: {code}")

__all__ = ["TaskGroupQueueStatus"]
