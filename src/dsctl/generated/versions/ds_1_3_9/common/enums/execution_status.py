from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ExecutionStatus(StrEnum):
    """Running status for workflow and task nodes"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> ExecutionStatus:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # status： 0 submit success 1 running 2 ready pause 3 pause 4 ready stop 5 stop 6
    # failure 7 success 8 need fault tolerance 9 kill 10 waiting thread 11 waiting
    # depend node complete
    SUBMITTED_SUCCESS = ('SUBMITTED_SUCCESS', 0, 'submit success')
    RUNNING_EXECUTION = ('RUNNING_EXECUTION', 1, 'running')
    READY_PAUSE = ('READY_PAUSE', 2, 'ready pause')
    PAUSE = ('PAUSE', 3, 'pause')
    READY_STOP = ('READY_STOP', 4, 'ready stop')
    STOP = ('STOP', 5, 'stop')
    FAILURE = ('FAILURE', 6, 'failure')
    SUCCESS = ('SUCCESS', 7, 'success')
    NEED_FAULT_TOLERANCE = ('NEED_FAULT_TOLERANCE', 8, 'need fault tolerance')
    KILL = ('KILL', 9, 'kill')
    WAITTING_THREAD = ('WAITTING_THREAD', 10, 'waiting thread')
    WAITTING_DEPEND = ('WAITTING_DEPEND', 11, 'waiting depend node complete')

    @classmethod
    def from_code(cls, code: int) -> "ExecutionStatus":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown ExecutionStatus code: {code}")

__all__ = ["ExecutionStatus"]
