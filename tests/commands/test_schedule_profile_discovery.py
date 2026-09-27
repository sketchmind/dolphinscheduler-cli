from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app


@pytest.mark.parametrize(
    "version", ["1.3.9", "2.0.0", "3.1.0", "3.2.1", "3.2.2", "3.4.3"]
)
def test_environment_capability_and_schema_explain_runtime_support(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    runner = CliRunner()
    capability = runner.invoke(app, ["capabilities", "--section", "schedule"])
    assert capability.exit_code == 0, capability.output
    supports = version in {"3.2.2", "3.4.3"}
    data = json.loads(capability.stdout)["data"]["schedule"]
    assert data["environment_inheritance"] is supports
    assert ("environment_limitation" in data) is (not supports)
    for action in ("schedule.create", "schedule.update", "schedule.explain"):
        result = runner.invoke(app, ["schema", "--command", action])
        assert result.exit_code == 0, result.output
        options = json.loads(result.stdout)["data"]["command"]["options"]
        environment = next(
            (item for item in options if item["name"] == "environment-code"), None
        )
        if version == "1.3.9":
            assert environment is None
        else:
            assert environment is not None
            assert environment["runtime_inheritance"] is supports
            if not supports:
                assert "Positive selections are rejected" in environment["description"]
                assert "manual runs" in environment["description"]


@pytest.mark.parametrize(
    "action", ["workflow.run", "workflow.run-task", "workflow.backfill"]
)
@pytest.mark.parametrize("version", ["1.3.9", "3.2.1", "3.2.2"])
def test_manual_runtime_environment_schema_matches_task_inheritance(
    monkeypatch: pytest.MonkeyPatch, version: str, action: str
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    result = CliRunner().invoke(app, ["schema", "--command", action])
    assert result.exit_code == 0, result.output
    command = json.loads(result.stdout)["data"]["command"]
    options = command["options"]
    if version == "1.3.9":
        assert all(item["name"] != "environment-code" for item in options)
        assert any(
            item["flag"] == "--environment-code"
            and item["availability"] == "upstream_absent"
            for item in command["unavailable_options"]
        )
        return
    environment = next(item for item in options if item["name"] == "environment-code")
    assert environment["runtime_inheritance"] is (version == "3.2.2")
    if version == "3.2.1":
        assert "Positive selections are rejected" in environment["description"]


@pytest.mark.parametrize("action", ["create", "update", "explain"])
@pytest.mark.parametrize("version", ["3.4.2", "3.4.3"])
def test_schedule_policy_discovery_matches_selected_native_contract(
    monkeypatch: pytest.MonkeyPatch, action: str, version: str
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    result = CliRunner().invoke(app, ["schema", "--command", f"schedule.{action}"])
    assert result.exit_code == 0, result.output
    command = json.loads(result.stdout)["data"]["command"]
    options = {item["name"]: item for item in command["options"]}
    if version == "3.4.2":
        assert "missed-fire-policy" not in options
        assert any(
            item["flag"] == "--missed-fire-policy"
            and item["availability"] == "upstream_absent"
            for item in command["unavailable_options"]
        )
    else:
        policy = options["missed-fire-policy"]
        assert policy["choices"] == ["SKIP_MISSED", "FIRE_ONCE_NOW", "FIRE_ALL_MISSED"]
        assert policy["upstream_default"] == "FIRE_ALL_MISSED"
        assert (
            policy["discovery_command"] == "dsctl enum list schedule-missed-fire-policy"
        )


@pytest.mark.parametrize("version", ["3.4.2", "3.4.3"])
def test_attached_schedule_template_explains_only_native_policy(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    monkeypatch.setenv("DS_VERSION", version)
    result = CliRunner().invoke(
        app, ["template", "workflow", "--with-schedule", "--raw"]
    )
    assert result.exit_code == 0, result.output
    assert ("missed_fire_policy" in result.stdout) is (version == "3.4.3")
