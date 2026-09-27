from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class TaskExecutionStatus(StrEnum):
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> TaskExecutionStatus:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    SUBMITTED_SUCCESS = ('SUBMITTED_SUCCESS', 0, 'submit success')
    RUNNING_EXECUTION = ('RUNNING_EXECUTION', 1, 'running')
    PAUSE = ('PAUSE', 3, 'pause')
    FAILURE = ('FAILURE', 6, 'failure')
    SUCCESS = ('SUCCESS', 7, 'success')
    NEED_FAULT_TOLERANCE = ('NEED_FAULT_TOLERANCE', 8, 'need fault tolerance')
    KILL = ('KILL', 9, 'kill')
    DELAY_EXECUTION = ('DELAY_EXECUTION', 12, 'delay execution')
    FORCED_SUCCESS = ('FORCED_SUCCESS', 13, 'forced success')
    DISPATCH = ('DISPATCH', 17, 'dispatch')

    @classmethod
    def from_code(cls, code: int) -> "TaskExecutionStatus":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown TaskExecutionStatus code: {code}")

__all__ = ["TaskExecutionStatus"]
