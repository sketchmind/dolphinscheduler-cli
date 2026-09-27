"""User journeys through fresh CLI processes and real isolated config files."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from tests.value_shape_assertions import assert_mapping as _mapping

if TYPE_CHECKING:
    from collections.abc import Mapping

_SECRETS = ("journey-secret-a", "journey-secret-b", "journey-rotated-secret")


def _cli(
    root: Path,
    *args: str,
    environment: dict[str, str] | None = None,
    succeeds: bool = True,
) -> Mapping[str, object]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DS_", "DSCTL_"))
    }
    env.update(
        HOME=str(root),
        XDG_CONFIG_HOME=str(root / "config"),
        XDG_CACHE_HOME=str(root / "cache"),
        PYTHONPATH=str(Path(__file__).resolve().parents[2] / "src"),
    )
    env.update(environment or {})
    result = subprocess.run(  # noqa: S603 - fixed interpreter, public CLI arguments
        [sys.executable, "-m", "dsctl", *args],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    output = result.stdout + result.stderr
    assert all(secret not in output for secret in _SECRETS)
    assert (result.returncode == 0) is succeeds, output
    payload = _mapping(json.loads(result.stdout if succeeds else result.stderr))
    assert payload["ok"] is succeeds
    return payload


def _file(root: Path, name: str, url: str, token: str) -> Path:
    path = root / f"{name}.env"
    path.write_text(f"DS_API_URL={url}\nDS_API_TOKEN={token}\nDS_VERSION=3.4.1\n")
    return path


def _register(root: Path, name: str, project: str, token: str) -> Path:
    path = _file(root, name, f"https://{name}.example/dolphinscheduler", token)
    _cli(root, "context", "create", name, "--file", str(path), "--project", project)
    return path


def test_fresh_shell_and_two_tasks_share_directory_without_sharing_selection(
    tmp_path: Path,
) -> None:
    _register(tmp_path, "alpha", "project-a", _SECRETS[0])
    _register(tmp_path, "beta", "project-b", _SECRETS[1])
    _cli(tmp_path, "config", "set", "default-context", "alpha")

    # Old directory and user defaults cannot influence either task.
    legacy = tmp_path / ".dsctl-context.yaml"
    legacy.write_text("project: stale-project\nworkflow: stale-workflow\n")
    old_user = tmp_path / "config" / "dsctl" / "context.yaml"
    old_user.write_text("project: stale-user-project\n")
    for name, project in (("alpha", "project-a"), ("beta", "project-b")):
        current = _cli(tmp_path, "context", environment={"DSCTL_CONTEXT": name})
        assert _mapping(current["data"])["context"] == name
        assert (
            _mapping(current["data"])["api_url"]
            == f"https://{name}.example/dolphinscheduler"
        )
        assert _mapping(current["data"])["project"] == project
        assert "workflow" not in _mapping(current["data"])
        assert _mapping(current["resolved"])["remote_validation"] == "not_performed"

    fresh = _cli(tmp_path, "context")
    assert _mapping(fresh["data"])["context"] == "alpha"
    assert _mapping(fresh["data"])["project"] == "project-a"
    assert (
        _mapping(_cli(tmp_path, "config", "get", "default-context")["data"])["value"]
        == "alpha"
    )
    assert "stale-workflow" in legacy.read_text()
    assert "stale-user-project" in old_user.read_text()
    registry = (tmp_path / "config" / "dsctl" / "config.yaml").read_text()
    assert all(secret not in registry for secret in _SECRETS)


def test_saved_default_and_process_overrides_remain_distinct(tmp_path: Path) -> None:
    path = _register(tmp_path, "alpha", "project-a", _SECRETS[0])
    direct = {
        "DS_API_URL": "https://direct.example",
        "DS_API_TOKEN": _SECRETS[1],
    }
    saved = _cli(
        tmp_path, "config", "set", "default-context", "alpha", environment=direct
    )
    assert _mapping(saved["data"])["value"] == "alpha"
    assert _mapping(saved["resolved"])["saved"] is True
    assert _mapping(_mapping(saved["resolved"])["effective"])["context"] is None
    current = _cli(tmp_path, "context", environment=direct)
    assert _mapping(current["data"])["api_url"] == "https://direct.example"
    assert _mapping(current["data"])["project"] is None

    # One explicit selector supersedes even conflicting inherited selectors.
    explicit = _cli(
        tmp_path,
        "--context",
        "alpha",
        "context",
        environment={**direct, "DSCTL_CONTEXT": "missing", "DSCTL_ENV_FILE": "missing"},
    )
    assert _mapping(explicit["data"])["project"] == "project-a"
    temporary = _cli(
        tmp_path,
        "--env-file",
        str(path),
        "context",
        environment={"DSCTL_CONTEXT": "alpha", "DS_VERSION": "3.4.2"},
    )
    assert _mapping(temporary["data"])["context"] is None
    assert _mapping(temporary["data"])["project"] is None
    assert _mapping(temporary["data"])["ds_version"] == "3.4.1"

    for incomplete in ({"DS_VERSION": "3.4.1"}, {"DS_API_TOKEN": ""}):
        offline = _cli(tmp_path, "context", environment=incomplete)
        assert _mapping(offline["data"])["context"] is None
        assert _mapping(offline["data"])["project"] is None
        assert not _mapping(offline["data"])["api_url"]
        failure = _cli(
            tmp_path, "project", "list", environment=incomplete, succeeds=False
        )
        assert _mapping(failure["error"])["type"] == "config_error"
    assert _mapping(_cli(tmp_path, "context")["data"])["context"] == "alpha"


def test_external_endpoint_change_requires_explicit_rebind(tmp_path: Path) -> None:
    path = _register(tmp_path, "alpha", "project-a", _SECRETS[0])
    _cli(tmp_path, "config", "set", "default-context", "alpha")
    _file(tmp_path, "alpha", "https://alpha.example/dolphinscheduler", _SECRETS[2])
    assert _mapping(_cli(tmp_path, "context")["data"])["project"] == "project-a"
    _file(tmp_path, "alpha", "https://replacement.example", _SECRETS[2])
    failed = _cli(tmp_path, "context", succeeds=False)
    assert _mapping(failed["error"])["type"] == "config_error"
    assert _mapping(failed["error"])["suggestion"]
    assert _mapping(_cli(tmp_path, "context", "get", "alpha")["data"])["api_url"] == (
        "https://alpha.example/dolphinscheduler"
    )
    _cli(tmp_path, "context", "update", "alpha", "--file", str(path))
    rebound = _cli(tmp_path, "context")
    assert _mapping(rebound["data"])["api_url"] == "https://replacement.example"
    assert _mapping(rebound["data"])["project"] is None
    _cli(tmp_path, "context", "delete", "alpha", succeeds=False)
    _cli(tmp_path, "config", "unset", "default-context")
    _cli(tmp_path, "context", "delete", "alpha")
    assert _cli(tmp_path, "context", "list")["data"] == []
