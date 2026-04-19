import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeAudit,
    FakeAuditAdapter,
    FakeAuditModelType,
    FakeAuditOperationType,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_audit_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            audit_adapter=FakeAuditAdapter(
                audit_logs=[
                    FakeAudit(
                        user_name_value="alice",
                        model_type_value="Workflow",
                        model_name_value="daily-etl",
                        operation_value="Create",
                        create_time_value="2026-04-11 10:00:00",
                    )
                ],
                model_types=[
                    FakeAuditModelType(
                        name="Project",
                        child=[FakeAuditModelType(name="Workflow")],
                    )
                ],
                operation_types=[FakeAuditOperationType(name="Create")],
            ),
            profile=make_profile(),
        ),
    )


def test_audit_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["audit", "list", "--model-type", "Workflow"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "audit.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["modelName"] == "daily-etl"


def test_audit_list_help_points_to_filter_discovery_commands() -> None:
    result = runner.invoke(app, ["audit", "list", "--help"])

    assert result.exit_code == 0
    assert "audit model-types" in result.stdout
    assert "audit operation-types" in result.stdout


def test_audit_model_types_command_returns_tree() -> None:
    result = runner.invoke(app, ["audit", "model-types"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "audit.model-types"
    assert payload["data"][0]["name"] == "Project"
    assert payload["data"][0]["child"][0]["name"] == "Workflow"


def test_audit_operation_types_command_returns_list() -> None:
    result = runner.invoke(app, ["audit", "operation-types"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "audit.operation-types"
    assert payload["data"] == [{"name": "Create"}]


def test_audit_list_command_rejects_invalid_datetime() -> None:
    result = runner.invoke(app, ["audit", "list", "--start", "2026-04-11T10:00:00"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "audit.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass --start in '%Y-%m-%d %H:%M:%S' format, for example '2026-04-11 10:00:00'."
    )


def test_audit_list_command_rejects_inverted_range() -> None:
    result = runner.invoke(
        app,
        [
            "audit",
            "list",
            "--start",
            "2026-04-11 11:00:00",
            "--end",
            "2026-04-11 10:00:00",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "audit.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass an --end value that is later than or equal to --start."
    )
