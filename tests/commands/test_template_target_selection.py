from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import create_context
from dsctl.services.template import _targeted_template_hint, _template_result_data
from dsctl.services.version_resolution import invocation_scope
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def selected_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "settings"))
    monkeypatch.delenv("DSCTL_CONTEXT", raising=False)
    monkeypatch.delenv("DSCTL_ENV_FILE", raising=False)
    file = tmp_path / "production connection.env"
    file.write_text(
        "DS_API_URL=https://production.example\nDS_API_TOKEN=private-token\nDS_VERSION=3.4.1\n"
    )
    create_context(
        "production team", env_file=file, api_url="https://production.example"
    )
    return file


@pytest.mark.parametrize("selector", ["context", "env-file"])
@pytest.mark.parametrize(
    "template", ["environment", "cluster", "datasource", "datasource-mysql", "params"]
)
def test_structured_template_commands_retain_selected_target(
    selected_file: Path,
    selector: str,
    template: str,
) -> None:
    selected = "production team" if selector == "context" else str(selected_file)
    template_args = (
        ["datasource", "--type", "MYSQL"]
        if template == "datasource-mysql"
        else [template]
    )
    result = CliRunner().invoke(
        app, [f"--{selector}", selected, "template", *template_args]
    )
    assert result.exit_code == 0, result.output
    data = _mapping(json.loads(result.stdout)["data"])
    commands: list[str] = []
    if template == "params":
        commands = [
            str(_mapping(topic)["command"]) for topic in _sequence(data["topics"])
        ]
    else:
        assert "target_commands" not in data
        commands.extend(
            str(command) for command in _sequence(data["target_command_patterns"])
        )
        if template == "datasource":
            commands.extend(
                str(data[key])
                for key in (
                    "template_command",
                    "template_command_pattern",
                    "type_discovery_command",
                )
            )
            commands.extend(
                str(_mapping(row)["template_command"])
                for row in _sequence(data["rows"])
            )
    assert commands
    for command in commands:
        tokens = shlex.split(command)
        assert tokens[:3] == ["dsctl", f"--{selector}", selected]
        assert tokens.count(f"--{selector}") == 1
    assert "private-token" not in result.stdout


@pytest.mark.parametrize("selector", ["context", "env-file"])
def test_datasource_discovery_preserves_explicit_version_option(
    selected_file: Path,
    selector: str,
) -> None:
    selected = "production team" if selector == "context" else str(selected_file)
    result = CliRunner().invoke(
        app,
        [f"--{selector}", selected, "template", "datasource", "--ds-version", "1.3.9"],
    )
    assert result.exit_code == 0, result.output
    data = _mapping(json.loads(result.stdout)["data"])
    tokens = shlex.split(str(data["template_command"]))
    assert tokens[:3] == ["dsctl", f"--{selector}", selected]
    assert tokens[tokens.index("--ds-version") + 1] == "1.3.9"


@pytest.mark.parametrize(
    "explicit",
    [
        "--context other",
        "--env-file '/other connection.env'",
        "--context=other",
        "--env-file=/other.env",
    ],
)
def test_template_hint_preserves_existing_selector_and_version(explicit: str) -> None:
    command = f"dsctl template datasource {explicit} --ds-version 1.3.9 --type MYSQL"
    with invocation_scope(context_name="missing"):
        assert _targeted_template_hint(command) == command


def test_target_binding_does_not_rewrite_template_artifacts_or_prose(
    selected_file: Path,
) -> None:
    source = {
        "target_command_patterns": [
            "dsctl environment create --name NAME --config-file FILE"
        ],
        "yaml": "command: dsctl version\n",
        "config": "dsctl version\n",
        "rules": ["Run dsctl version before applying."],
        "description": "dsctl version prints metadata.",
    }
    with invocation_scope(env_file=selected_file):
        data = _template_result_data(source, label="template fixture")
    for key in ("yaml", "config", "rules", "description"):
        assert data[key] == source[key]
    assert shlex.split(str(_sequence(data["target_command_patterns"])[0]))[:3] == [
        "dsctl",
        "--env-file",
        str(selected_file),
    ]
