from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class ExecuteType(StrEnum):
    """Execute type"""
    NONE = 'NONE'
    REPEAT_RUNNING = 'REPEAT_RUNNING'
    RECOVER_SUSPENDED_PROCESS = 'RECOVER_SUSPENDED_PROCESS'
    START_FAILURE_TASK_PROCESS = 'START_FAILURE_TASK_PROCESS'
    STOP = 'STOP'
    PAUSE = 'PAUSE'

__all__ = ["ExecuteType"]
