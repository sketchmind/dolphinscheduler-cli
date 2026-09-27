from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ExecuteType(StrEnum):
    """Execute type"""
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> ExecuteType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    # operation type 1 repeat running 2 resume pause 3 resume failure 4 stop 5 pause
    NONE = ('NONE', 0, 'NONE')
    REPEAT_RUNNING = ('REPEAT_RUNNING', 1, 'REPEAT_RUNNING')
    RECOVER_SUSPENDED_PROCESS = ('RECOVER_SUSPENDED_PROCESS', 2, 'RECOVER_SUSPENDED_PROCESS')
    START_FAILURE_TASK_PROCESS = ('START_FAILURE_TASK_PROCESS', 3, 'START_FAILURE_TASK_PROCESS')
    STOP = ('STOP', 4, 'STOP')
    PAUSE = ('PAUSE', 5, 'PAUSE')
    EXECUTE_TASK = ('EXECUTE_TASK', 6, 'EXECUTE_TASK')

    @classmethod
    def from_code(cls, code: int) -> "ExecuteType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ExecuteType code: {code}")

__all__ = ["ExecuteType"]
