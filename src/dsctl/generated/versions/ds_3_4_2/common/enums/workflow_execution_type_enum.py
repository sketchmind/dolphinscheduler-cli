from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class WorkflowExecutionTypeEnum(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> WorkflowExecutionTypeEnum:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    PARALLEL = ('PARALLEL', 0, 'parallel')
    SERIAL_WAIT = ('SERIAL_WAIT', 1, 'serial wait')
    SERIAL_DISCARD = ('SERIAL_DISCARD', 2, 'serial discard')
    SERIAL_PRIORITY = ('SERIAL_PRIORITY', 3, 'serial priority')

    @classmethod
    def from_code(cls, code: int) -> "WorkflowExecutionTypeEnum":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown WorkflowExecutionTypeEnum code: {code}")

__all__ = ["WorkflowExecutionTypeEnum"]
