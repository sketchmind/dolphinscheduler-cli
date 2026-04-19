import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_project_preference_adapter() -> FakeProjectPreferenceAdapter:
    return FakeProjectPreferenceAdapter(
        project_preferences=[
            FakeProjectPreference(
                id=11,
                code=101,
                project_code_value=7,
                preferences_value='{"taskPriority":"MEDIUM"}',
                state=1,
            )
        ]
    )


@pytest.fixture(autouse=True)
def patch_project_preference_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_preference_adapter: FakeProjectPreferenceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            project_preference_adapter=fake_project_preference_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod"),
        ),
    )


def test_project_preference_get_command_returns_payload() -> None:
    result = runner.invoke(app, ["project-preference", "get"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-preference.get"
    assert payload["data"]["projectCode"] == 7
    assert payload["data"]["state"] == 1
    assert payload["resolved"]["project"]["source"] == "context"


def test_project_preference_get_help_points_to_project_list() -> None:
    result = runner.invoke(app, ["project-preference", "get", "--help"])

    assert result.exit_code == 0
    assert "project list" in result.stdout


def test_project_preference_update_command_accepts_inline_json() -> None:
    result = runner.invoke(
        app,
        [
            "project-preference",
            "update",
            "--preferences-json",
            '{"taskPriority":"HIGH"}',
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-preference.update"
    assert payload["data"]["preferences"] == '{"taskPriority":"HIGH"}'
    assert payload["data"]["state"] == 1


def test_project_preference_disable_command_returns_updated_state() -> None:
    result = runner.invoke(app, ["project-preference", "disable"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-preference.disable"
    assert payload["data"]["state"] == 0


def test_project_preference_update_command_requires_input() -> None:
    result = runner.invoke(app, ["project-preference", "update"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-preference.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass exactly one of --preferences-json or --file."
    )
