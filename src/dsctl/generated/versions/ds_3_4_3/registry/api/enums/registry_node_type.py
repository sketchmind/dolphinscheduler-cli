from __future__ import annotations

from enum import Enum, IntEnum, StrEnum

class RegistryNodeType(StrEnum):
    name_field: str
    registryPath: str

    def __new__(cls, wire_value: str, name_arg: str, registryPath: str) -> RegistryNodeType:
        obj = str.__new__(cls, wire_value)
        obj._value_ = wire_value
        obj.name_field = name_arg
        obj.registryPath = registryPath
        return obj
    FAILOVER_FINISH_NODES = ('FAILOVER_FINISH_NODES', 'FailoverFinishNodes', '/nodes/failover-finish-nodes')
    GLOBAL_MASTER_FAILOVER_LOCK = ('GLOBAL_MASTER_FAILOVER_LOCK', 'GlobalMasterFailoverLock', '/lock/global-master-failover')
    MASTER = ('MASTER', 'Master', '/nodes/master')
    MASTER_FAILOVER_LOCK = ('MASTER_FAILOVER_LOCK', 'MasterFailoverLock', '/lock/master-failover')
    MASTER_COORDINATOR = ('MASTER_COORDINATOR', 'MasterCoordinator', '/nodes/master-coordinator')
    MASTER_TASK_GROUP_COORDINATOR_LOCK = ('MASTER_TASK_GROUP_COORDINATOR_LOCK', 'TaskGroupCoordinatorLock', '/lock/master-task-group-coordinator')
    MASTER_SERIAL_COORDINATOR_LOCK = ('MASTER_SERIAL_COORDINATOR_LOCK', 'SerialWorkflowCoordinator', '/lock/master-serial-workflow-coordinator')
    WORKER = ('WORKER', 'Worker', '/nodes/worker')
    ALERT_SERVER = ('ALERT_SERVER', 'AlertServer', '/nodes/alert-server')
    ALERT_HA_LEADER = ('ALERT_HA_LEADER', 'AlertHALeader', '/nodes/alert-server-ha-leader')

__all__ = ["RegistryNodeType"]
