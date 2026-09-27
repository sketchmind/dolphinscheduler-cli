from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.command_references import project_command_references
from dsctl.context import create_context
from dsctl.services.capabilities import get_capabilities_result
from dsctl.services.enums import list_enum_names_result
from dsctl.services.schema import get_schema_result

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def target_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    path = tmp_path / "connection team's.env"
    path.write_text(
        "DS_API_URL=https://review.example/dolphinscheduler\n"
        "DS_API_TOKEN=private-review-token\nDS_VERSION=3.3.2\n",
        encoding="utf-8",
    )
    create_context(
        "review team's target",
        env_file=path,
        api_url="https://review.example/dolphinscheduler",
    )
    return path


@pytest.mark.parametrize("selector", ["context", "env-file"])
@pytest.mark.parametrize(
    "args",
    [
        ["capabilities", "--action", "template.task"],
        ["schema"],
        ["schema", "--group", "template"],
        ["schema", "--command", "template.task"],
        ["schema", "--list-groups"],
        ["schema", "--list-commands"],
        ["enum", "names"],
    ],
)
def test_concrete_discovery_commands_retain_selected_target(
    target_file: Path, selector: str, args: list[str]
) -> None:
    selected = "review team's target" if selector == "context" else str(target_file)
    result = CliRunner().invoke(app, [f"--{selector}", selected, *args])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    commands = _concrete_commands(payload["data"])
    assert commands
    for command in commands:
        tokens = shlex.split(command)
        assert tokens[:3] == ["dsctl", f"--{selector}", selected]
        assert tokens.count(f"--{selector}") == 1
    assert "private-review-token" not in result.stdout


@pytest.mark.parametrize("selector", ["context", "env-file"])
def test_capability_schema_link_preserves_exact_task_membership(
    target_file: Path, selector: str
) -> None:
    selected = "review team's target" if selector == "context" else str(target_file)
    runner = CliRunner()
    first = runner.invoke(
        app,
        [f"--{selector}", selected, "capabilities", "--action", "template.task"],
    )
    assert first.exit_code == 0, first.output
    links = json.loads(first.stdout)["data"]["links"]
    command = next(link["command"] for link in links if link["rel"] == "schema")
    followed = runner.invoke(app, shlex.split(command)[1:])
    assert followed.exit_code == 0, followed.output
    data = json.loads(followed.stdout)["data"]
    assert data["ds"]["selected_version"] == "3.3.2"
    assert "PYTORCH" in data["command"]["arguments"][0]["choices"]
    assert "GRPC" not in data["command"]["arguments"][0]["choices"]


def test_service_discovery_commands_retain_direct_env_file(target_file: Path) -> None:
    results = (
        get_capabilities_result(action="template.task", env_file=str(target_file)),
        get_schema_result(env_file=str(target_file)),
        get_schema_result(list_groups=True, env_file=str(target_file)),
        get_schema_result(command_action="template.task", env_file=str(target_file)),
        list_enum_names_result(env_file=str(target_file)),
    )
    for result in results:
        commands = _concrete_commands(result.data)
        assert commands
        for command in commands:
            assert shlex.split(command)[:3] == ["dsctl", "--env-file", str(target_file)]


@pytest.mark.parametrize("exact", [True, False])
def test_schema_input_discovery_retains_target_without_network(
    target_file: Path, monkeypatch: pytest.MonkeyPatch, *, exact: bool
) -> None:
    if not exact:
        target_file.write_text(
            target_file.read_text(encoding="utf-8").replace("3.3.2", "auto"),
            encoding="utf-8",
        )

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Local discovery must not access the network")

    monkeypatch.setattr("dsctl.services.version_resolution.discover_target", forbidden)
    result = get_schema_result(
        command_action="template.task", env_file=str(target_file)
    )
    assert isinstance(result.data, dict)
    command = result.data["command"]
    assert isinstance(command, dict)
    arguments = command["arguments"]
    assert isinstance(arguments, list)
    task_type = arguments[0]
    assert isinstance(task_type, dict)
    assert shlex.split(str(task_type["discovery_command"])) == [
        "dsctl",
        "--env-file",
        str(target_file),
        "template",
        "task",
    ]
    ds = result.data["ds"]
    assert isinstance(ds, dict)
    assert ds["selected_version"] == ("3.3.2" if exact else None)


def test_full_schema_template_metadata_retains_target(target_file: Path) -> None:
    result = get_schema_result(full=True, env_file=str(target_file))
    assert isinstance(result.data, dict)
    capabilities = result.data["capabilities"]
    assert isinstance(capabilities, dict)
    templates = capabilities["templates"]
    assert isinstance(templates, dict)
    commands = []
    for name, key in (
        ("workflow", "raw_template_command"),
        ("workflow_patch", "raw_template_command"),
        ("cluster", "command"),
        ("environment", "command"),
        ("task", "index_command"),
        ("datasource", "template_command"),
        ("datasource", "type_discovery_command"),
    ):
        template = templates[name]
        assert isinstance(template, dict)
        commands.append(str(template[key]))
    parameters = templates["parameters"]
    assert isinstance(parameters, dict)
    topics = parameters["topics"]
    assert isinstance(topics, list)
    for topic in topics:
        assert isinstance(topic, dict)
        commands.append(str(topic["command"]))
    for command in commands:
        assert shlex.split(command)[:3] == ["dsctl", "--env-file", str(target_file)]
    workflow = templates["workflow"]
    assert isinstance(workflow, dict)
    assert workflow["export_command_pattern"] == "dsctl workflow export WORKFLOW"


@pytest.mark.parametrize(
    "args",
    [
        ["capabilities", "--action", "template.tasks"],
        ["schema", "--command", "template.tasks"],
        ["schema", "--group", "templates"],
    ],
)
def test_discovery_error_candidates_retain_selected_file(
    target_file: Path, args: list[str]
) -> None:
    result = CliRunner().invoke(app, ["--env-file", str(target_file), *args])
    assert result.exit_code != 0
    payload = json.loads(result.stderr)
    candidates = payload["error"]["details"]["candidates"]
    assert candidates
    for command in _concrete_commands(candidates):
        assert shlex.split(command)[:3] == ["dsctl", "--env-file", str(target_file)]


def test_catalog_concrete_discovery_hints_have_valid_parser_syntax(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Offline discovery syntax validation must not access HTTP")

    monkeypatch.setattr("httpx.Client.request", forbidden)
    references = {
        item.discovery_command
        for command in COMMAND_CATALOG.commands
        for item in (*command.arguments, *command.options)
        if item.discovery_command is not None
    }
    runner = CliRunner()
    for reference in sorted(references):
        projected = project_command_references({"discovery_command": reference})
        assert isinstance(projected, dict)
        if "discovery_command_pattern" in projected:
            continue
        result = runner.invoke(
            app, shlex.split(reference)[1:], env={"DS_VERSION": "3.4.1"}
        )
        assert result.exit_code != 2, f"{reference}: {result.output}"


@pytest.mark.parametrize(
    ("action", "input_name"),
    [
        ("task.get", "task"),
        ("task.update", "task"),
        ("workflow.run-task", "task"),
        ("task-instance.list", "task-code"),
    ],
)
def test_task_discovery_requires_explicit_workflow_pattern(
    action: str, input_name: str
) -> None:
    result = get_schema_result(command_action=action)
    assert isinstance(result.data, dict)
    command = result.data["command"]
    assert isinstance(command, dict)
    inputs = []
    for key in ("arguments", "options"):
        collection = command[key]
        assert isinstance(collection, list)
        inputs.extend(collection)
    task = next(
        item for item in inputs if isinstance(item, dict) and item["name"] == input_name
    )
    assert isinstance(task, dict)
    assert "discovery_command" not in task
    assert task["discovery_command_pattern"] == (
        "dsctl task list --project PROJECT --workflow WORKFLOW"
    )


def _concrete_commands(value: object) -> list[str]:
    commands: list[str] = []
    if isinstance(value, list):
        for item in value:
            commands.extend(_concrete_commands(item))
    elif isinstance(value, dict):
        for key, item in value.items():
            if (
                key in {"schema_command", "list_command", "capabilities_command"}
                or (key == "command" and value.get("rel") in {"schema", "group_schema"})
            ) and isinstance(item, str):
                commands.append(item)
            else:
                commands.extend(_concrete_commands(item))
    return commands
