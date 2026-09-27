from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class WorkflowExecutionStatus(StrEnum):
    code: int
    desc: str

    def __new__(cls, wire_value: str, code: int, desc: str) -> WorkflowExecutionStatus:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.desc = desc
        return obj
    SUBMITTED_SUCCESS = ('SUBMITTED_SUCCESS', 0, 'submit success')
    RUNNING_EXECUTION = ('RUNNING_EXECUTION', 1, 'running')
    READY_PAUSE = ('READY_PAUSE', 2, 'ready pause')
    PAUSE = ('PAUSE', 3, 'pause')
    READY_STOP = ('READY_STOP', 4, 'ready stop')
    STOP = ('STOP', 5, 'stop')
    FAILURE = ('FAILURE', 6, 'failure')
    SUCCESS = ('SUCCESS', 7, 'success')
    DELAY_EXECUTION = ('DELAY_EXECUTION', 12, 'delay execution')
    SERIAL_WAIT = ('SERIAL_WAIT', 14, 'serial wait')
    READY_BLOCK = ('READY_BLOCK', 15, 'ready block')
    BLOCK = ('BLOCK', 16, 'block')

    @classmethod
    def from_code(cls, code: int) -> 'WorkflowExecutionStatus':
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f'Unknown WorkflowExecutionStatus code: {code}')

__all__ = ['WorkflowExecutionStatus']
