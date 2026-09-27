from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class CommandType(StrEnum):
    code: int
    descp: str

    def __new__(cls, wire_value: str, code: int, descp: str) -> CommandType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.code = code
        obj.descp = descp
        return obj
    # Start the workflow definition, will generate a new workflow instance and start
    # from the StartNodeList, if StartNodeList is empty will start from the beginning
    # tasks.
    START_PROCESS = ('START_PROCESS', 0, 'start a new workflow')
    # todo: remove this command, this command doesn't used?
    START_CURRENT_TASK_PROCESS = ('START_CURRENT_TASK_PROCESS', 1, 'start a new workflow from current nodes')
    # Recover the workflow instance from tolerance fault, these may happened when the
    # master is crashed. Will recover the workflow instance from the last running task
    # node.
    RECOVER_TOLERANCE_FAULT_PROCESS = ('RECOVER_TOLERANCE_FAULT_PROCESS', 2, 'recover fault tolerance workflow instance')
    # Recover the workflow instance from pause status, will start from the paused and
    # unTriggered task instance.
    RECOVER_SUSPENDED_PROCESS = ('RECOVER_SUSPENDED_PROCESS', 3, 'Recover suspended workflow instance')
    # Recover the workflow instance from failure task nodes, will start from the failed
    # task nodes. In fact this command has the same logic with
    # RECOVER_SUSPENDED_WORKFLOW.
    START_FAILURE_TASK_PROCESS = ('START_FAILURE_TASK_PROCESS', 4, 'Recover workflow instance from failure tasks')
    # Backfill the workflow, will use complementScheduleDateList to generate the
    # workflow instance.
    COMPLEMENT_DATA = ('COMPLEMENT_DATA', 5, 'complement data')
    # Start workflow from scheduler, will generate a new workflow instance and start
    # from the beginning tasks. This command is same with START_PROCESS but with
    # different trigger source.
    SCHEDULER = ('SCHEDULER', 6, 'start a new workflow from scheduler')
    # Repeat running a workflow instance, will mark the history task instances' flag to
    # no and start from the beginning tasks.
    REPEAT_RUNNING = ('REPEAT_RUNNING', 7, 'repeat running a workflow')
    # Pause a workflow instance, will pause the running tasks, but not all tasks will be
    # paused.
    PAUSE = ('PAUSE', 8, 'pause a workflow')
    # Stop a workflow instance, will kill the running tasks.
    STOP = ('STOP', 9, 'stop a workflow')
    # Recover from the serial-wait state.
    RECOVER_SERIAL_WAIT = ('RECOVER_SERIAL_WAIT', 11, 'recover serial wait')
    # Trigger the workflow instance from the given StartNodeList, will mark the task
    # instance which is behind the given StartNodeList flag to no and retrigger the task
    # instances.
    EXECUTE_TASK = ('EXECUTE_TASK', 12, 'start a task node in a workflow instance')
    # Used in dynamic logic task instance.
    DYNAMIC_GENERATION = ('DYNAMIC_GENERATION', 13, 'dynamic generation')

    @classmethod
    def from_code(cls, code: int) -> "CommandType":
        for member in cls:
            if member.code == code:
                return member
        raise ValueError(f"Unknown CommandType code: {code}")

__all__ = ["CommandType"]
