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
    ALL_SERVERS = ('ALL_SERVERS', 'nodes', '/nodes')
    MASTER = ('MASTER', 'Master', '/nodes/master')
    MASTER_NODE_LOCK = ('MASTER_NODE_LOCK', 'MasterNodeLock', '/lock/master-node')
    MASTER_FAILOVER_LOCK = ('MASTER_FAILOVER_LOCK', 'MasterFailoverLock', '/lock/master-failover')
    MASTER_TASK_GROUP_COORDINATOR_LOCK = ('MASTER_TASK_GROUP_COORDINATOR_LOCK', 'TaskGroupCoordinatorLock', '/lock/master-task-group-coordinator')
    WORKER = ('WORKER', 'Worker', '/nodes/worker')
    ALERT_SERVER = ('ALERT_SERVER', 'AlertServer', '/nodes/alert-server')
    ALERT_LOCK = ('ALERT_LOCK', 'AlertNodeLock', '/lock/alert')

__all__ = ['RegistryNodeType']
