import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProject,
    FakeProjectAdapter,
    FakeProjectWorkerGroup,
    FakeProjectWorkerGroupAdapter,
    fake_service_runtime,
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
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            project_worker_group_adapter=fake_project_worker_group_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod"),
        ),
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


def test_project_worker_group_clear_command_requires_force() -> None:
    result = runner.invoke(app, ["project-worker-group", "clear"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "project-worker-group.clear"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."
