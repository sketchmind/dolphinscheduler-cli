import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.project_preferences import (
    PROJECT_PREFERENCE_DOMAIN,
    ProjectPreferenceDomain,
)
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    fake_bound_domain_service_runtime,
    fake_project_definitions,
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
    context = ResourceDefaults(project="etl-prod")

    def bound_runtime_factory(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        assert domain is PROJECT_PREFERENCE_DOMAIN
        return fake_bound_domain_service_runtime(
            ProjectPreferenceDomain(
                definitions=fake_project_definitions(fake_project_adapter),
                preferences=fake_project_preference_adapter,
            ),
            profile=make_profile(),
            context=context,
        )

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        bound_runtime_factory,
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
    payload = json.loads(result.stderr)
    assert payload["action"] == "project-preference.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass exactly one of --preferences-json or --file."
    )
