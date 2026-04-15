import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
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

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_monitor_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            http_client=FakeHttpClient(
                health_payload={"status": "UP", "components": {"db": {"status": "UP"}}}
            ),
            monitor_adapter=FakeMonitorAdapter(
                {
                    "MASTER": [
                        FakeMonitorServer(
                            id=1,
                            host="master-1",
                            port=5678,
                        )
                    ]
                },
                databases=[
                    FakeMonitorDatabase(
                        db_type_value=FakeEnumValue("MYSQL"),
                        state_value=FakeEnumValue("YES"),
                        max_connections_value=50,
                        max_used_connections_value=10,
                        threads_connections_value=4,
                        threads_running_connections_value=1,
                        date_value="2026-04-11 10:10:00",
                    )
                ],
            ),
        ),
    )


def test_monitor_health_command_returns_health_payload() -> None:
    result = runner.invoke(app, ["monitor", "health"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "monitor.health"
    assert payload["data"]["status"] == "UP"
    assert payload["resolved"]["scope"] == "api_server"


def test_monitor_server_command_returns_server_list() -> None:
    result = runner.invoke(app, ["monitor", "server", "master"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "monitor.server"
    assert payload["resolved"]["node_type"] == "MASTER"
    assert payload["data"][0]["host"] == "master-1"


def test_monitor_database_command_returns_database_metrics() -> None:
    result = runner.invoke(app, ["monitor", "database"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "monitor.database"
    assert payload["resolved"] == {
        "endpoint": "http://example.test/dolphinscheduler/monitor/databases"
    }
    assert payload["data"][0]["dbType"] == "MYSQL"
    assert payload["warnings"] == []
    assert payload["warning_details"] == []


def test_monitor_server_command_rejects_unknown_node_type() -> None:
    result = runner.invoke(app, ["monitor", "server", "master-coordinator"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "monitor.server"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Retry with one of: master, worker, alert-server."
    )
