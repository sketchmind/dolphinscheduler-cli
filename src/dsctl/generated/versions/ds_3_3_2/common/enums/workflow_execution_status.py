from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class WorkflowExecutionStatus(StrEnum):
    code: int
    canStop: bool
    canDirectStopInDB: bool
    canPause: bool
    canDirectPauseInDB: bool
    finalState: bool
    needFailover: bool

    def __new__(cls, wire_value: str, code: int, canStop: bool, canDirectStopInDB: bool, canPause: bool, canDirectPauseInDB: bool, finalState: bool, needFailover: bool) -> WorkflowExecutionStatus:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.canStop = canStop
        obj.canDirectStopInDB = canDirectStopInDB
        obj.canPause = canPause
        obj.canDirectPauseInDB = canDirectPauseInDB
        obj.finalState = finalState
        obj.needFailover = needFailover
        return obj
    SUBMITTED_SUCCESS = ('SUBMITTED_SUCCESS', 0, False, False, False, False, False, False)
    RUNNING_EXECUTION = ('RUNNING_EXECUTION', 1, True, False, True, False, False, True)
    READY_PAUSE = ('READY_PAUSE', 2, True, False, True, False, False, True)
    PAUSE = ('PAUSE', 3, False, False, False, False, True, False)
    READY_STOP = ('READY_STOP', 4, True, False, False, False, False, True)
    STOP = ('STOP', 5, False, False, False, False, True, False)
    FAILURE = ('FAILURE', 6, False, False, False, False, True, False)
    SUCCESS = ('SUCCESS', 7, False, False, False, False, True, False)
    SERIAL_WAIT = ('SERIAL_WAIT', 14, True, True, True, True, False, False)
    FAILOVER = ('FAILOVER', 18, False, False, False, False, False, False)

    @classmethod
    def from_code(cls, code: int) -> "WorkflowExecutionStatus":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown WorkflowExecutionStatus code: {code}")

__all__ = ["WorkflowExecutionStatus"]
