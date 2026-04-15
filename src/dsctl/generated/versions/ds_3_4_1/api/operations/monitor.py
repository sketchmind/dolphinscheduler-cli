from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...common.model.server import Server
from ...dao.plugin_api.monitor.database_metrics import DatabaseMetrics
from ...registry.api.enums.registry_node_type import RegistryNodeType

class MonitorOperations(BaseRequestsClient):
    def query_database_state(
        self
    ) -> list[DatabaseMetrics]:
        """
        Query Database State
        
        Query database state
        
        DS operation: MonitorController.queryDatabaseState | GET /monitor/databases
        
        Returns:
            database state
        """
        payload = self._request("GET", "monitor/databases")
        return self._validate_payload(payload, TypeAdapter(list[DatabaseMetrics]))

    def list_server(
        self,
        node_type: RegistryNodeType
    ) -> list[Server]:
        """
        List Server
        
        Server list
        
        DS operation: MonitorController.listServer | GET /monitor/{nodeType}
        
        Returns:
            server list
        """
        path = f"monitor/{node_type}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[Server]))

__all__ = ["MonitorOperations"]
