from pathlib import Path

import pytest

from dsctl.context import load_context, update_context
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
