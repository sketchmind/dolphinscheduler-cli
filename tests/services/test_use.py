from pathlib import Path

import pytest

from dsctl.context import (
    ContextScope,
    load_context,
    project_context_path,
    read_context_layer,
    update_context,
    user_context_path,
)
from dsctl.errors import UserInputError
from dsctl.services import use as use_service


def _mapping(value: object) -> dict[str, str | None]:
    assert isinstance(value, dict)
    return value


def test_set_context_value_result_sets_project_and_clears_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    update_context(
        project="etl-prod",
        workflow="daily-sync",
        scope="project",
        cwd=tmp_path,
    )

    result = use_service.set_context_value_result(
        "project",
        "streaming",
        cwd=tmp_path,
    )
    data = _mapping(result.data)

    assert data["project"] == "streaming"
    assert data["workflow"] is None
    assert load_context(cwd=tmp_path).project == "streaming"
    assert load_context(cwd=tmp_path).workflow is None


def test_clear_context_result_clears_only_selected_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    update_context(
        project="etl-prod",
        workflow="daily-sync",
        scope="project",
        cwd=tmp_path,
    )

    result = use_service.clear_context_result(
        target="workflow",
        cwd=tmp_path,
    )
    data = _mapping(result.data)

    assert data["project"] == "etl-prod"
    assert data["workflow"] is None


def test_set_workflow_context_persists_effective_project_in_selected_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    update_context(project="etl-prod", scope="user", cwd=tmp_path)

    use_service.set_context_value_result(
        "workflow",
        "daily-sync",
        scope="project",
        cwd=tmp_path,
    )

    project_layer = read_context_layer(scope="project", cwd=tmp_path)
    assert project_layer.project == "etl-prod"
    assert project_layer.workflow == "daily-sync"


@pytest.mark.parametrize("scope", ["project", "user"])
def test_set_workflow_context_requires_effective_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: ContextScope,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))

    with pytest.raises(
        UserInputError,
        match="Project context is required",
    ) as exc_info:
        use_service.set_context_value_result(
            "workflow",
            "daily-sync",
            scope=scope,
            cwd=tmp_path,
        )

    assert exc_info.value.suggestion == (
        f"Run `dsctl use project NAME --scope {scope}` before setting workflow context."
    )


@pytest.mark.parametrize("scope", ["project", "user"])
def test_set_project_context_repairs_legacy_workflow_only_layer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: ContextScope,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    path = (
        project_context_path(cwd=tmp_path)
        if scope == "project"
        else user_context_path()
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("workflow: orphan-workflow\n", encoding="utf-8")

    result = use_service.set_context_value_result(
        "project",
        "etl-prod",
        scope=scope,
        cwd=tmp_path,
    )

    layer = read_context_layer(scope=scope, cwd=tmp_path)
    assert layer.project == "etl-prod"
    assert layer.workflow is None
    assert _mapping(result.data)["project"] == "etl-prod"


@pytest.mark.parametrize("scope", ["project", "user"])
def test_clear_project_context_repairs_legacy_workflow_only_layer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: ContextScope,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    path = (
        project_context_path(cwd=tmp_path)
        if scope == "project"
        else user_context_path()
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("workflow: orphan-workflow\n", encoding="utf-8")

    result = use_service.clear_context_result(
        target="project",
        scope=scope,
        cwd=tmp_path,
    )

    layer = read_context_layer(scope=scope, cwd=tmp_path)
    assert layer.project is None
    assert layer.workflow is None
    assert _mapping(result.data)["workflow"] is None


@pytest.mark.parametrize("scope", ["project", "user"])
def test_clear_workflow_context_repairs_legacy_workflow_only_layer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: ContextScope,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    path = (
        project_context_path(cwd=tmp_path)
        if scope == "project"
        else user_context_path()
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("workflow: orphan-workflow\n", encoding="utf-8")

    use_service.clear_context_result(
        target="workflow",
        scope=scope,
        cwd=tmp_path,
    )

    layer = read_context_layer(scope=scope, cwd=tmp_path)
    assert layer.workflow is None


def test_repairing_project_then_user_succeeds_when_both_layers_are_legacy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    project_path = project_context_path(cwd=tmp_path)
    user_path = user_context_path()
    user_path.parent.mkdir(parents=True, exist_ok=True)
    project_path.write_text("workflow: project-orphan\n", encoding="utf-8")
    user_path.write_text("workflow: user-orphan\n", encoding="utf-8")

    project_result = use_service.set_context_value_result(
        "project",
        "repo-project",
        scope="project",
        cwd=tmp_path,
    )
    user_result = use_service.set_context_value_result(
        "project",
        "user-project",
        scope="user",
        cwd=tmp_path,
    )

    assert _mapping(project_result.data)["project"] == "repo-project"
    assert _mapping(user_result.data)["project"] == "repo-project"
    assert read_context_layer(scope="project", cwd=tmp_path).project == "repo-project"
    assert read_context_layer(scope="user", cwd=tmp_path).project == "user-project"


def test_clear_reports_success_when_another_legacy_layer_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    project_path = project_context_path(cwd=tmp_path)
    user_path = user_context_path()
    user_path.parent.mkdir(parents=True, exist_ok=True)
    project_path.write_text("workflow: project-orphan\n", encoding="utf-8")
    user_path.write_text("workflow: user-orphan\n", encoding="utf-8")

    result = use_service.clear_context_result(scope="project", cwd=tmp_path)

    assert not project_path.exists()
    assert _mapping(result.data) == {
        "project": None,
        "workflow": None,
        "set_at": None,
    }
    assert result.warnings == [
        "Context layer updated, but another persisted context layer is invalid."
    ]
    detail = result.warning_details[0]
    assert detail["code"] == "context_layer_invalid_after_update"
    assert detail["scope"] == "project"
    assert detail["suggestion"] == (
        "Run `dsctl use project NAME --scope user` to bind a project and discard "
        "the unbound workflow, or run `dsctl use --clear --scope user` to clear "
        "that context layer."
    )
