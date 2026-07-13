from pathlib import Path

import pytest

from dsctl.context import (
    ContextScope,
    EffectiveContext,
    SessionContext,
    clear_context,
    load_context,
    project_context_path,
    read_context_layer,
    resolve_context,
    update_context,
    user_context_path,
    write_context,
)
from dsctl.errors import ConfigError


def test_session_context_rejects_workflow_without_project() -> None:
    with pytest.raises(ValueError, match="workflow context requires project context"):
        SessionContext(workflow="daily-etl")


@pytest.mark.parametrize(
    ("project", "workflow"),
    [
        ("   ", None),
        ("etl-prod", "   "),
    ],
)
def test_session_context_rejects_blank_selection_value(
    project: str,
    workflow: str | None,
) -> None:
    with pytest.raises(ValueError, match="must not be blank"):
        SessionContext(project=project, workflow=workflow)


def test_update_context_rejects_workflow_without_same_layer_project(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="workflow context requires project context"):
        update_context(workflow="daily-etl", cwd=tmp_path)

    assert not project_context_path(cwd=tmp_path).exists()


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


def test_resolve_context_reports_project_source(
    tmp_path: Path,
) -> None:
    session = SessionContext(project="repo-project", workflow="daily-etl")
    write_context(session, scope="project", cwd=tmp_path)

    resolved = resolve_context(cwd=tmp_path)

    assert resolved == EffectiveContext(session=session, scope="project")


def test_resolve_context_reports_user_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    session = SessionContext(project="user-project", workflow="daily-etl")
    write_context(session, scope="user")

    resolved = resolve_context(cwd=tmp_path)

    assert resolved == EffectiveContext(session=session, scope="user")


def test_resolve_context_reports_no_source_when_context_is_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))

    resolved = resolve_context(cwd=tmp_path)

    assert resolved == EffectiveContext(session=SessionContext(), scope=None)


def test_load_context_does_not_inherit_workflow_across_project_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(project="user-project", workflow="user-workflow"),
        scope="user",
    )
    write_context(
        SessionContext(project="repo-project"),
        scope="project",
        cwd=tmp_path,
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded.project == "repo-project"
    assert loaded.workflow is None


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


def test_write_context_replace_failure_preserves_existing_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = write_context(SessionContext(project="original"), cwd=tmp_path)
    original_document = path.read_text(encoding="utf-8")

    def fail_replace(_source: object, _destination: object) -> None:
        message = "replace failed"
        raise OSError(message)

    monkeypatch.setattr("dsctl.context.os.replace", fail_replace)

    with pytest.raises(ConfigError) as exc_info:
        write_context(SessionContext(project="replacement"), cwd=tmp_path)

    assert path.read_text(encoding="utf-8") == original_document
    assert list(tmp_path.iterdir()) == [path]
    assert exc_info.value.details == {
        "operation": "write",
        "path": str(path),
    }
    assert exc_info.value.suggestion == (
        f"Make sure {tmp_path} is a writable directory, then retry the command."
    )


def test_write_context_atomically_updates_symlink_target_without_replacing_link(
    tmp_path: Path,
) -> None:
    shared_path = tmp_path / "shared-context.yaml"
    shared_path.write_text("project: original\n", encoding="utf-8")
    context_path = project_context_path(cwd=tmp_path)
    context_path.symlink_to(shared_path.name)

    write_context(SessionContext(project="replacement"), cwd=tmp_path)

    assert context_path.is_symlink()
    assert read_context_layer(cwd=tmp_path).project == "replacement"
    assert "project: replacement" in shared_path.read_text(encoding="utf-8")


def test_write_context_translates_unwritable_parent_path(tmp_path: Path) -> None:
    blocked_parent = tmp_path / "blocked"
    blocked_parent.write_text("not a directory", encoding="utf-8")
    path = project_context_path(cwd=blocked_parent)

    with pytest.raises(ConfigError) as exc_info:
        write_context(SessionContext(project="etl-prod"), cwd=blocked_parent)

    assert exc_info.value.details == {
        "operation": "write",
        "path": str(path),
    }
    assert exc_info.value.suggestion == (
        f"Make sure {blocked_parent} is a writable directory, then retry the command."
    )


def test_clear_context_translates_directory_removal_error(tmp_path: Path) -> None:
    path = project_context_path(cwd=tmp_path)
    path.mkdir()

    with pytest.raises(ConfigError) as exc_info:
        clear_context(cwd=tmp_path)

    assert exc_info.value.details == {
        "operation": "clear",
        "path": str(path),
    }
    assert exc_info.value.suggestion == (
        f"Make sure {path} is a removable regular file, then retry the command."
    )


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


def test_update_context_project_change_clears_stored_workflow(
    tmp_path: Path,
) -> None:
    update_context(project="etl-prod", workflow="daily-etl", cwd=tmp_path)

    updated = update_context(project="streaming", cwd=tmp_path)

    assert updated.project == "streaming"
    assert updated.workflow is None


def test_clearing_project_layer_workflow_does_not_reveal_another_project_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(project="user-project", workflow="user-workflow"),
        scope="user",
    )
    write_context(
        SessionContext(project="repo-project", workflow="repo-workflow"),
        scope="project",
        cwd=tmp_path,
    )

    update_context(workflow=None, scope="project", cwd=tmp_path)

    loaded = load_context(cwd=tmp_path)
    assert loaded.project == "repo-project"
    assert loaded.workflow is None


def test_clearing_project_layer_values_reveals_user_layer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(project="user-project", workflow="user-workflow"),
        scope="user",
    )
    write_context(
        SessionContext(project="repo-project", workflow="repo-workflow"),
        scope="project",
        cwd=tmp_path,
    )

    updated = update_context(
        project=None,
        workflow=None,
        scope="project",
        cwd=tmp_path,
    )

    loaded = load_context(cwd=tmp_path)
    assert updated == SessionContext()
    assert not project_context_path(cwd=tmp_path).exists()
    assert loaded.project == "user-project"
    assert loaded.workflow == "user-workflow"


def test_set_at_only_layer_does_not_override_contributing_selection_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(
            project="user-project",
            workflow="user-workflow",
            set_at="2026-07-10T00:00:00+00:00",
        ),
        scope="user",
    )
    write_context(
        SessionContext(set_at="2026-07-11T00:00:00+00:00"),
        scope="project",
        cwd=tmp_path,
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded.set_at == "2026-07-10T00:00:00+00:00"


def test_selected_layer_without_set_at_does_not_inherit_lower_layer_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(
            project="user-project",
            set_at="2026-07-10T00:00:00+00:00",
        ),
        scope="user",
    )
    write_context(
        SessionContext(project="repo-project"),
        scope="project",
        cwd=tmp_path,
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded.project == "repo-project"
    assert loaded.set_at is None


def test_project_null_layer_falls_back_to_user_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(
            project="user-project",
            workflow="user-workflow",
            set_at="2026-07-10T00:00:00+00:00",
        ),
        scope="user",
    )
    project_context_path(cwd=tmp_path).write_text(
        "project: null\nset_at: '2026-07-11T00:00:00+00:00'\n",
        encoding="utf-8",
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded == SessionContext(
        project="user-project",
        workflow="user-workflow",
        set_at="2026-07-10T00:00:00+00:00",
    )


def test_workflow_null_is_absent_from_selected_project_tuple(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    write_context(
        SessionContext(project="user-project", workflow="user-workflow"),
        scope="user",
    )
    project_context_path(cwd=tmp_path).write_text(
        (
            "project: repo-project\n"
            "workflow: null\n"
            "set_at: '2026-07-11T00:00:00+00:00'\n"
        ),
        encoding="utf-8",
    )

    loaded = load_context(cwd=tmp_path)

    assert loaded == SessionContext(
        project="repo-project",
        set_at="2026-07-11T00:00:00+00:00",
    )


def test_read_context_layer_rejects_invalid_yaml(tmp_path: Path) -> None:
    project_context_path(cwd=tmp_path).write_text(
        "project: [unterminated\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert "Invalid YAML" in exc_info.value.message


def test_read_context_layer_translates_directory_read_error(tmp_path: Path) -> None:
    path = project_context_path(cwd=tmp_path)
    path.mkdir()

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details == {
        "operation": "read",
        "path": str(path),
    }
    assert exc_info.value.suggestion == (
        f"Make sure {path} is a readable regular file, then retry the command."
    )


def test_read_context_layer_rejects_non_utf8_document(tmp_path: Path) -> None:
    path = project_context_path(cwd=tmp_path)
    path.write_bytes(b"\xff")

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details == {
        "operation": "read",
        "path": str(path),
        "encoding": "utf-8",
    }
    assert exc_info.value.suggestion == (
        f"Rewrite {path} as UTF-8 YAML, then retry the command."
    )


@pytest.mark.parametrize(
    ("document", "actual_type"),
    [
        ("project:\n  - etl-prod\n", "list"),
        ("project: true\n", "bool"),
        ("project:\n  name: etl-prod\n", "dict"),
    ],
)
def test_read_context_layer_rejects_non_string_value(
    tmp_path: Path,
    document: str,
    actual_type: str,
) -> None:
    project_context_path(cwd=tmp_path).write_text(
        document,
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details["key"] == "project"
    assert exc_info.value.details["actual_type"] == actual_type


@pytest.mark.parametrize(
    ("document", "key"),
    [
        ("project: '   '\n", "project"),
        ("project: etl-prod\nworkflow: '   '\n", "workflow"),
    ],
)
def test_read_context_layer_rejects_blank_selection_value(
    tmp_path: Path,
    document: str,
    key: str,
) -> None:
    project_context_path(cwd=tmp_path).write_text(document, encoding="utf-8")

    with pytest.raises(ConfigError, match="must not be blank") as exc_info:
        read_context_layer(cwd=tmp_path)

    assert exc_info.value.details["key"] == key


@pytest.mark.parametrize("scope", ["project", "user"])
def test_read_context_layer_rejects_workflow_without_same_layer_project(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: ContextScope,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    path = (
        project_context_path(cwd=tmp_path)
        if scope == "project"
        else user_context_path()
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "workflow: daily-etl\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="workflow requires project") as exc_info:
        read_context_layer(scope=scope, cwd=tmp_path)

    assert exc_info.value.details["key"] == "workflow"
    assert exc_info.value.details["required_key"] == "project"
    assert exc_info.value.suggestion == (
        f"Run `dsctl use project NAME --scope {scope}` to bind a project and "
        "discard the unbound workflow, or run `dsctl use --clear --scope "
        f"{scope}` to clear that context layer."
    )


def test_load_context_rejects_legacy_workflow_only_layer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    project_context_path(cwd=tmp_path).write_text(
        "workflow: orphan-workflow\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="workflow requires project") as exc_info:
        load_context(cwd=tmp_path)

    assert "dsctl use project NAME --scope project" in str(exc_info.value.suggestion)


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
