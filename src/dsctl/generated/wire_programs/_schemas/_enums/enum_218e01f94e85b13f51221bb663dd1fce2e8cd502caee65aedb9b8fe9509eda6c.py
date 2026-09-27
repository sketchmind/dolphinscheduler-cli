from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TaskDependType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> TaskDependType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    TASK_ONLY = ('TASK_ONLY', 0, 'task only')
    TASK_PRE = ('TASK_PRE', 1, 'task pre')
    TASK_POST = ('TASK_POST', 2, 'task post')

    @classmethod
    def from_code(cls, code: int) -> 'TaskDependType':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown TaskDependType code: {code}')

__all__ = ['TaskDependType']
