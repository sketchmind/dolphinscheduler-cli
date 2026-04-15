from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeHttpClient,
    FakeMonitorAdapter,
    FakeMonitorDatabase,
    FakeMonitorServer,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import UserInputError
from dsctl.services import monitor as monitor_service
from dsctl.services import runtime as runtime_service


def _install_monitor_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    http_client: FakeHttpClient | None = None,
    monitor_adapter: FakeMonitorAdapter | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            http_client=http_client or FakeHttpClient(),
            monitor_adapter=monitor_adapter or FakeMonitorAdapter({}),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_get_health_result_returns_api_health_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_monitor_service_fakes(
        monkeypatch,
        http_client=FakeHttpClient(
            health_payload={
                "status": "UP",
                "components": {"db": {"status": "UP"}},
            }
        ),
    )

    result = monitor_service.get_health_result()
    data = _mapping(result.data)

    assert result.resolved == {
        "endpoint": "http://example.test/dolphinscheduler/actuator/health",
        "scope": "api_server",
    }
    assert data["status"] == "UP"
    assert _mapping(data["components"])["db"] == {"status": "UP"}


def test_list_servers_result_returns_server_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_monitor_service_fakes(
        monkeypatch,
        monitor_adapter=FakeMonitorAdapter(
            {
                "MASTER": [
                    FakeMonitorServer(
                        id=1,
                        host="master-1",
                        port=5678,
                        server_directory_value="/opt/ds/master",
                        heart_beat_info_value="healthy",
                        create_time_value="2026-04-11 10:00:00",
                        last_heartbeat_time_value="2026-04-11 10:05:00",
                    )
                ]
            }
        ),
    )

    result = monitor_service.list_servers_result("master")
    items = _sequence(result.data)
    first = _mapping(items[0])

    assert result.resolved == {"node_type": "MASTER"}
    assert first == {
        "id": 1,
        "host": "master-1",
        "port": 5678,
        "serverDirectory": "/opt/ds/master",
        "heartBeatInfo": "healthy",
        "createTime": "2026-04-11 10:00:00",
        "lastHeartbeatTime": "2026-04-11 10:05:00",
    }


def test_get_database_result_returns_metrics_and_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_monitor_service_fakes(
        monkeypatch,
        monitor_adapter=FakeMonitorAdapter(
            {},
            databases=[
                FakeMonitorDatabase(
                    db_type_value=FakeEnumValue("MYSQL"),
                    state_value=FakeEnumValue("NO"),
                    max_connections_value=50,
                    max_used_connections_value=42,
                    threads_connections_value=12,
                    threads_running_connections_value=3,
                    date_value="2026-04-11 10:10:00",
                )
            ],
        ),
    )

    result = monitor_service.get_database_result()
    items = _sequence(result.data)
    first = _mapping(items[0])

    assert result.resolved == {
        "endpoint": "http://example.test/dolphinscheduler/monitor/databases",
    }
    assert result.warnings == ["database health degraded: MYSQL state=NO"]
    assert result.warning_details == [
        {
            "code": "monitor_database_degraded",
            "message": "database health degraded: MYSQL state=NO",
            "db_type": "MYSQL",
            "state": "NO",
        }
    ]
    assert first == {
        "dbType": "MYSQL",
        "state": "NO",
        "maxConnections": 50,
        "maxUsedConnections": 42,
        "threadsConnections": 12,
        "threadsRunningConnections": 3,
        "date": "2026-04-11 10:10:00",
    }


def test_list_servers_result_rejects_unknown_node_type() -> None:
    with pytest.raises(
        UserInputError,
        match="Unsupported monitor server type",
    ) as exc_info:
        monitor_service.list_servers_result("master-coordinator")
    assert exc_info.value.suggestion == (
        "Retry with one of: master, worker, alert-server."
    )
