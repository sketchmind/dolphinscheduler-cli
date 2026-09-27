from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class CommandType(StrEnum):
    """Command types"""
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> CommandType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # command types 0 start a new process 1 start a new process from current nodes 2
    # recover tolerance fault process 3 recover suspended process 4 start process from
    # failure task nodes 5 complement data 6 start a new process from scheduler 7 repeat
    # running a process 8 pause a process 9 stop a process 10 recover waiting thread
    START_PROCESS = ('START_PROCESS', 0, 'start a new process')
    START_CURRENT_TASK_PROCESS = ('START_CURRENT_TASK_PROCESS', 1, 'start a new process from current nodes')
    RECOVER_TOLERANCE_FAULT_PROCESS = ('RECOVER_TOLERANCE_FAULT_PROCESS', 2, 'recover tolerance fault process')
    RECOVER_SUSPENDED_PROCESS = ('RECOVER_SUSPENDED_PROCESS', 3, 'recover suspended process')
    START_FAILURE_TASK_PROCESS = ('START_FAILURE_TASK_PROCESS', 4, 'start process from failure task nodes')
    COMPLEMENT_DATA = ('COMPLEMENT_DATA', 5, 'complement data')
    SCHEDULER = ('SCHEDULER', 6, 'start a new process from scheduler')
    REPEAT_RUNNING = ('REPEAT_RUNNING', 7, 'repeat running a process')
    PAUSE = ('PAUSE', 8, 'pause a process')
    STOP = ('STOP', 9, 'stop a process')
    RECOVER_WAITING_THREAD = ('RECOVER_WAITING_THREAD', 10, 'recover waiting thread')
    RECOVER_SERIAL_WAIT = ('RECOVER_SERIAL_WAIT', 11, 'recover serial wait')

    @classmethod
    def from_code(cls, code: int) -> "CommandType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown CommandType code: {code}")

__all__ = ["CommandType"]
