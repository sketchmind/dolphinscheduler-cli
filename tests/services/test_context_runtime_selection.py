from __future__ import annotations

import json
import os
import shlex
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from dsctl.action_policy import preflight_selected_action
from dsctl.app import app
from dsctl.context import create_context, set_default_context, update_context
from dsctl.errors import ConfigError
from dsctl.output import CommandResult, require_json_object, result_payload
from dsctl.services import runtime as runtime_service
from dsctl.services import version_resolution as resolution
from dsctl.services._workflow.authoring_schema import workflow_authoring_schema_data
from dsctl.services.version_resolution import (
    annotate_target_result,
    invocation_scope,
    resolve_runtime_selection,
    resolve_settings,
)
from dsctl.services.workflow_instance._commands import (
    _workflow_instance_edit_retry_command,
)
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.version_discovery import DiscoveredVersion

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile, ConnectionSettings
    from dsctl.context import NamedContext
    from dsctl.output import JsonObject


def _object(value: object) -> JsonObject:
    return require_json_object(value, label="test response object")


def _command_tokens(value: object) -> list[str]:
    assert isinstance(value, str)
    return shlex.split(value)


@pytest.fixture
def named_target(tmp_path: Path) -> NamedContext:
    path = tmp_path / "connection $(opaque).env"
    path.write_text(
        "DS_VERSION=3.4.1\nDS_API_URL=https://original.test/dolphinscheduler\n"
        "DS_API_TOKEN=fixture-secret-token\n",
        encoding="utf-8",
    )
    return create_context(
        "production's cluster",
        env_file=path,
        api_url="https://original.test/dolphinscheduler",
        project="original-project",
    )


@pytest.mark.parametrize(
    "source", ["flag", "default", "environment_context", "file", "environment_file"]
)
@pytest.mark.parametrize(
    "command",
    [["workflow"], ["task", "SQL"], ["workflow-patch"], ["workflow-instance-patch"]],
)
def test_template_artifacts_and_hints_preserve_selected_target(
    named_target: NamedContext,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    command: list[str],
) -> None:
    prefix = []
    if source == "flag":
        prefix = ["--context", named_target.name]
    elif source == "default":
        set_default_context(named_target.name)
    elif source == "environment_context":
        monkeypatch.setenv("DSCTL_CONTEXT", named_target.name)
    elif source == "file":
        prefix = ["--env-file", str(named_target.env_file)]
    else:
        monkeypatch.setenv("DSCTL_ENV_FILE", str(named_target.env_file))
    expected = (
        ["dsctl", "--env-file", str(named_target.env_file)]
        if source in {"file", "environment_file"}
        else ["dsctl", "--context", named_target.name]
    )
    before = dict(os.environ)
    result = CliRunner().invoke(app, [*prefix, "template", *command])
    assert result.exit_code == 0, result.stderr
    payload = _object(json.loads(result.stdout))
    assert "fixture-secret-token" not in result.stdout
    assert dict(os.environ) == before
    data = _object(payload["data"])
    assert _command_tokens(_object(data["artifact"])["raw_command"])[:3] == expected
    related_commands = data.get("related_commands", [])
    assert isinstance(related_commands, list)
    for related in related_commands:
        assert _command_tokens(related)[:3] == expected
    template = _object(data.get("template", {}))
    for key in ("schema_command", "summary_command", "index_command"):
        if key in template:
            assert _command_tokens(template[key])[:3] == expected
    yaml_text = data["yaml"]
    assert isinstance(yaml_text, str)
    command_comments = [
        line.partition("dsctl ")[2].rstrip("`")
        for line in yaml_text.splitlines()
        if line.startswith("#") and "dsctl " in line
    ]
    assert command_comments
    for comment in command_comments:
        assert shlex.split(f"dsctl {comment}")[:3] == expected


class _EchoAdapter:
    def bind(
        self, profile: ClusterProfile, *, http_client: DolphinSchedulerClient
    ) -> ClusterProfile:
        return profile


def test_preflight_runtime_and_navigation_share_named_target_after_external_update(
    named_target: NamedContext,
) -> None:
    domain = BoundDomain(
        name="snapshot", adapter_for_version=lambda version: _EchoAdapter()
    )
    with invocation_scope("workflow.run", context_name=named_target.name):
        preflight_selected_action("workflow.run", None)
        original = resolve_settings()
        named_target.env_file.write_text(
            "DS_VERSION=1.3.9\nDS_API_URL=https://changed.test\nDS_API_TOKEN=changed-token\n",
            encoding="utf-8",
        )
        update_context(named_target.name, project="changed-project")
        selection = resolve_runtime_selection()
        with runtime_service.open_bound_domain_service_runtime(
            domain, selection=selection
        ) as runtime:
            assert runtime.profile.api_url == original.api_url
            assert runtime.profile.api_token == original.api_token
            assert runtime.profile.ds_version == "3.4.1"
            assert runtime.context.project == "original-project"
        assert resolve_settings() is original
        result = annotate_target_result(
            CommandResult(
                data={"workflowInstanceIds": [901]},
                resolved={"project": {"name": original.project}},
            ),
            None,
        )
        payload = result_payload("workflow.run", result)
        assert (
            _object(_object(payload["resolved"])["selection"])["context"]
            == named_target.name
        )
        assert "fixture-secret-token" not in json.dumps(payload)
        next_actions = payload["next_actions"]
        assert isinstance(next_actions, list)
        assert _command_tokens(_object(next_actions[0])["command"])[:3] == [
            "dsctl",
            "--context",
            named_target.name,
        ]
    with (
        invocation_scope(context_name=named_target.name),
        pytest.raises(ConfigError, match="different API URL"),
    ):
        resolve_settings()


@pytest.mark.parametrize("source", ["context", "file"])
def test_authoring_schema_and_instance_retry_hints_keep_invocation_target(
    named_target: NamedContext, tmp_path: Path, source: str
) -> None:
    context_name = named_target.name if source == "context" else None
    env_file = named_target.env_file if source == "file" else None
    expected = [
        "dsctl",
        "--context" if source == "context" else "--env-file",
        named_target.name if source == "context" else str(named_target.env_file),
    ]
    with invocation_scope(context_name=context_name, env_file=env_file):
        schema = workflow_authoring_schema_data("3.4.1")
        assert _command_tokens(schema["template_command"])[:3] == expected
        assert (
            _command_tokens(
                _object(schema["task_authoring"])["schema_command_pattern"]
            )[:3]
            == expected
        )
        command = _workflow_instance_edit_retry_command(
            "patch",
            workflow_instance_id=901,
            project_selector="project's name",
            input_path=tmp_path / "patch $(opaque).yaml",
        )
        assert shlex.split(command)[:3] == expected


def test_explicit_and_ambient_file_consumers_share_one_discovery(
    named_target: NamedContext, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    named_target.env_file.write_text(
        "DS_API_URL=https://original.test\nDS_API_TOKEN=original-token\n",
        encoding="utf-8",
    )
    calls: list[ConnectionSettings] = []

    def discover(connection: ConnectionSettings) -> DiscoveredVersion:
        calls.append(connection)
        return DiscoveredVersion(version="3.4.1", source="product_info")

    monkeypatch.setattr(resolution, "discover_target", discover)
    with invocation_scope("workflow.list", env_file=named_target.env_file):
        preflight_selected_action("workflow.list", named_target.env_file)
        initial = resolve_settings(named_target.env_file)
        assert resolve_settings() is initial
        assert resolve_runtime_selection().execution_profile.api_url == initial.api_url
        assert resolution.selected_target_globals() == {
            "env-file": str(named_target.env_file)
        }
        assert (
            resolution.resolve_profile(named_target.env_file).api_token
            == initial.api_token
        )
    assert len(calls) == 1
