from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    DsctlCommandResult,
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


TASK_GROUP_PROJECT_CASCADE_VERSIONS = frozenset(
    {
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)


def require_task_group_project_cleanup_version(
    repo_root: Path,
    env_file: Path,
    ds_version: str | None,
    *,
    execute: Callable[..., DsctlCommandResult] = run_dsctl,
) -> str:
    if ds_version is None or ds_version.lower() == "auto":
        payload = require_ok_payload(
            execute(repo_root, ["version"], env_file=env_file),
            expected_action="version",
            label="task-group cleanup version preflight",
        )
        data = require_mapping(
            payload["data"],
            label="task-group cleanup version data",
        )
        if data.get("version_source") in {None, "offline_default"}:
            message = (
                "Task-group live mutation requires an exact cluster version; "
                "the CLI version preflight did not prove one"
            )
            raise AssertionError(message)
        selected = data.get("selected_ds_version")
        if not isinstance(selected, str) or selected == "":
            message = "Task-group cleanup version preflight returned no exact version"
            raise AssertionError(message)
        ds_version = selected

    if ds_version not in TASK_GROUP_PROJECT_CASCADE_VERSIONS:
        pytest.skip(
            f"Task-group mutation is disabled for DS {ds_version}: this exact "
            "version has no task-group delete REST operation and project deletion "
            "has no reviewed task-group cascade"
        )
    return ds_version


def require_task_group_get_absent(
    repo_root: Path,
    creator_env_file: Path,
    *,
    task_group: str,
    execute: Callable[..., DsctlCommandResult] = run_dsctl,
) -> None:
    error = require_error_payload(
        execute(
            repo_root,
            ["task-group", "get", task_group],
            env_file=creator_env_file,
        ),
        expected_action="task-group.get",
        expected_type="not_found",
        label="task-group get after project cleanup",
    )
    assert error["type"] == "not_found"


def require_task_group_list_absent(
    repo_root: Path,
    creator_env_file: Path,
    *,
    task_group: str,
    execute: Callable[..., DsctlCommandResult] = run_dsctl,
) -> None:
    payload = require_ok_payload(
        execute(
            repo_root,
            ["task-group", "list", "--search", task_group, "--all"],
            env_file=creator_env_file,
        ),
        expected_action="task-group.list",
        label="task-group complete list after project cleanup",
    )
    data = require_mapping(payload["data"], label="task-group cleanup list data")
    rows = require_list(data["totalList"], label="task-group cleanup list rows")
    coverage = require_mapping(
        data["coverage"],
        label="task-group cleanup list coverage",
    )
    assert coverage["scope_complete"] is True
    assert coverage["totals_changed"] is False
    assert data["total"] == len(rows)
    residues = [
        row
        for value in rows
        for row in [require_mapping(value, label="task-group cleanup list row")]
        if row.get("name") == task_group
    ]
    assert not residues, f"Task-group cleanup left remote residue: {task_group}"
