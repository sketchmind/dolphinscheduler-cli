"""In-memory monitoring collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from tests.fakes.common import (
        FakeEnumValue,
    )


@dataclass(frozen=True)
class FakeMonitorServer:
    id: int
    host: str | None
    port: int
    server_directories_value: tuple[str, ...] | None = None
    server_directory_value: str | None = None
    heart_beat_info_value: str | None = None
    create_time_value: str | None = None
    last_heartbeat_time_value: str | None = None

    @property
    def serverDirectory(self) -> str | None:  # noqa: N802
        return self.server_directory_value

    @property
    def serverDirectories(self) -> Sequence[str]:  # noqa: N802
        if self.server_directories_value is not None:
            return self.server_directories_value
        if self.server_directory_value is None:
            return ()
        return (self.server_directory_value,)

    @property
    def heartBeatInfo(self) -> str | None:  # noqa: N802
        return self.heart_beat_info_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def lastHeartbeatTime(self) -> str | None:  # noqa: N802
        return self.last_heartbeat_time_value


@dataclass(frozen=True)
class FakeMonitorDatabase:
    db_type_value: FakeEnumValue | None = None
    state_value: FakeEnumValue | None = None
    max_connections_value: int = 0
    max_used_connections_value: int = 0
    threads_connections_value: int = 0
    threads_running_connections_value: int = 0
    date_value: str | None = None

    @property
    def dbType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.db_type_value

    @property
    def state(self) -> FakeEnumValue | None:
        return self.state_value

    @property
    def maxConnections(self) -> int:  # noqa: N802
        return self.max_connections_value

    @property
    def maxUsedConnections(self) -> int:  # noqa: N802
        return self.max_used_connections_value

    @property
    def threadsConnections(self) -> int:  # noqa: N802
        return self.threads_connections_value

    @property
    def threadsRunningConnections(self) -> int:  # noqa: N802
        return self.threads_running_connections_value

    @property
    def date(self) -> str | None:
        return self.date_value


@dataclass
class FakeMonitorAdapter:
    servers_by_node_type: dict[str, list[FakeMonitorServer]]
    databases: list[FakeMonitorDatabase] = field(default_factory=list)

    def list_servers(self, *, node_type: str) -> Sequence[FakeMonitorServer]:
        return list(self.servers_by_node_type.get(node_type, []))

    def list_databases(self) -> Sequence[FakeMonitorDatabase]:
        return list(self.databases)


def empty_monitor_adapter() -> FakeMonitorAdapter:
    return FakeMonitorAdapter(servers_by_node_type={})
