from pathlib import Path

import pytest

from dsctl.context import (
    SessionContext,
    clear_context,
    load_context,
    project_context_path,
    read_context_layer,
    update_context,
    user_context_path,
    write_context,
)
from dsctl.errors import ConfigError


def test_load_context_merges_user_and_project_layers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(project="global-project"),
        scope="user",
    )
    write_context(
        SessionContext(project="repo-project", workflow="daily-etl"),
        scope="project",
        cwd=tmp_path,
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded.project == "repo-project"
    assert loaded.workflow == "daily-etl"


def test_update_and_clear_context_layer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))

    updated = update_context(project="etl-prod", workflow="daily-etl", cwd=tmp_path)

    assert updated.project == "etl-prod"
    assert updated.workflow == "daily-etl"
    assert updated.set_at is not None
    assert read_context_layer(cwd=tmp_path).project == "etl-prod"

    clear_context(cwd=tmp_path)

    assert not project_context_path(cwd=tmp_path).exists()
    assert user_context_path() == tmp_path / "xdg" / "dsctl" / "context.yaml"


def test_update_context_can_clear_one_field_without_resetting_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(
            project="etl-prod",
            workflow="daily-etl",
        ),
        cwd=tmp_path,
    )

    updated = update_context(workflow=None, cwd=tmp_path)

    assert updated.project == "etl-prod"
    assert updated.workflow is None
    assert read_context_layer(cwd=tmp_path) == updated


def test_read_context_layer_rejects_invalid_yaml(tmp_path: Path) -> None:
    project_context_path(cwd=tmp_path).write_text(
        "project: [unterminated\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert "Invalid YAML" in exc_info.value.message


def test_read_context_layer_rejects_unsupported_keys(tmp_path: Path) -> None:
    project_context_path(cwd=tmp_path).write_text(
        "project: etl-prod\nextra: nope\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details["keys"] == ["extra"]


def test_read_context_layer_rejects_legacy_tenant_key(tmp_path: Path) -> None:
    project_context_path(cwd=tmp_path).write_text(
        "project: etl-prod\ntenant: legacy\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details["keys"] == ["tenant"]
