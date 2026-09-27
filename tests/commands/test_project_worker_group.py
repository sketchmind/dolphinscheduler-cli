import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.commands import project_worker_group as project_worker_group_commands
from dsctl.services import runtime as runtime_service
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.project_worker_groups import (
    PROJECT_WORKER_GROUP_DOMAIN,
    ProjectWorkerGroupDomain,
)
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectWorkerGroup,
    FakeProjectWorkerGroupAdapter,
    fake_bound_domain_service_runtime,
    fake_project_definitions,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_project_worker_group_adapter() -> FakeProjectWorkerGroupAdapter:
    return FakeProjectWorkerGroupAdapter(
        project_worker_groups=[
            FakeProjectWorkerGroup(
                id=11,
                project_code_value=7,
                worker_group_value="default",
            )
        ]
    )


@pytest.fixture(autouse=True)
def patch_project_worker_group_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_project_worker_group_adapter: FakeProjectWorkerGroupAdapter,
) -> None:
    context = ResourceDefaults(project="etl-prod")

    def bound_runtime_factory(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        assert domain is PROJECT_WORKER_GROUP_DOMAIN
        return fake_bound_domain_service_runtime(
            ProjectWorkerGroupDomain(
                definitions=fake_project_definitions(fake_project_adapter),
                worker_groups=fake_project_worker_group_adapter,
            ),
            profile=make_profile(),
            context=context,
        )

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        bound_runtime_factory,
    )


def test_project_worker_group_list_command_returns_current_assignments() -> None:
    result = runner.invoke(app, ["project-worker-group", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-worker-group.list"
    assert payload["data"] == [
        {
            "id": 11,
            "projectCode": 7,
            "workerGroup": "default",
            "createTime": None,
            "updateTime": None,
        }
    ]
    assert payload["resolved"]["project"]["code"] == 7
    assert payload["resolved"]["project"]["source"] == "context"


def test_project_worker_group_set_command_accepts_repeated_worker_group_flags() -> None:
    result = runner.invoke(
        app,
        [
            "project-worker-group",
            "set",
            "--worker-group",
            "default",
            "--worker-group",
            "gpu",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-worker-group.set"
    assert payload["resolved"]["requested_worker_groups"] == ["default", "gpu"]
    assert [item["workerGroup"] for item in payload["data"]] == ["default", "gpu"]


def test_project_worker_group_set_help_points_to_worker_group_list() -> None:
    result = runner.invoke(app, ["project-worker-group", "set", "--help"])

    assert result.exit_code == 0
    assert "worker-group list" in result.stdout


def test_project_worker_group_clear_command_requires_force() -> None:
    result = runner.invoke(app, ["project-worker-group", "clear"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "project-worker-group.clear"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_322_clear_is_rejected_by_cli_preflight_before_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.2.2")
    service_called = False

    def fail_if_called(**kwargs: object) -> None:
        del kwargs
        nonlocal service_called
        service_called = True
        message = "limited action service must not run"
        raise AssertionError(message)

    monkeypatch.setattr(
        project_worker_group_commands,
        "clear_project_worker_groups_result",
        fail_if_called,
    )

    result = runner.invoke(app, ["project-worker-group", "clear", "--force"])

    assert result.exit_code == 1
    assert service_called is False
    payload = json.loads(result.stderr)
    assert payload["action"] == "project-worker-group.clear"
    error = payload["error"]
    assert error["type"] == "unsupported_feature"
    assert error["details"]["selected_version"] == "3.2.2"
    assert error["details"]["availability"] == "limited"
    assert "1402003" in error["details"]["constraint"]
