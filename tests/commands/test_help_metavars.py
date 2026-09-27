from __future__ import annotations

import re

import pytest
from typer.core import TyperCommand, TyperGroup, TyperOption
from typer.main import get_command
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.command_contract import COMMAND_CATALOG
from dsctl.services.schema import get_schema_result
from tests.support import normalize_cli_help

_SEMANTIC_METAVAR = re.compile(r"^[A-Z][A-Z0-9_]*(?::\{[^}]+\})?$")
_GENERIC_RENDERED_TYPE = re.compile(r"\b(?:TEXT|INTEGER)\b")


def test_every_cli_value_uses_a_semantic_metavar() -> None:
    missing: list[str] = []
    invalid: list[str] = []
    root = get_command(app)
    assert isinstance(root, TyperGroup)

    for path, command in _iter_commands(root):
        for parameter in command.params:
            if isinstance(parameter, TyperOption) and (
                parameter.is_flag or parameter.count or parameter.type.name == "choice"
            ):
                continue
            metavar = parameter.metavar
            label = f"{' '.join(path)}:{parameter.name}"
            if metavar is None:
                missing.append(label)
            elif _SEMANTIC_METAVAR.fullmatch(metavar) is None:
                invalid.append(f"{label}={metavar}")

    assert missing == []
    assert invalid == []


def test_semantic_metavars_preserve_range_and_service_choice_metadata() -> None:
    runner = CliRunner()

    workflow_help = normalize_cli_help(
        runner.invoke(app, ["workflow", "list", "--help"]).stdout
    )
    run_task_help = normalize_cli_help(
        runner.invoke(app, ["workflow", "run-task", "--help"]).stdout
    )

    assert "--project PROJECT" in workflow_help
    assert "--page-no PAGE_NO [x>=1]" in workflow_help
    assert "--scope SCOPE" in run_task_help
    assert "self, pre, or post" in run_task_help
    assert "default: self" in run_task_help

    root = get_command(app)
    assert isinstance(root, TyperGroup)
    workflow = root.commands["workflow"]
    assert isinstance(workflow, TyperGroup)
    run_task = workflow.commands["run-task"]
    scope = next(
        parameter for parameter in run_task.params if parameter.name == "scope"
    )
    assert isinstance(scope, TyperOption)
    assert scope.default == "self"
    contract = COMMAND_CATALOG.command("workflow.run-task").input("scope")
    assert contract.choices == ("self", "pre", "post")
    assert contract.parser_choices == ()
    assert tuple(getattr(scope.type, "choices", ())) == ()
    for value in ("self", "pre", "post", "invalid"):
        assert scope.type.convert(value, scope, None) == value


def test_rendered_help_never_reintroduces_generic_argument_types() -> None:
    runner = CliRunner()
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    violations: list[str] = []

    for path, command in _iter_commands(root):
        if command is root:
            continue
        result = runner.invoke(app, [*path[1:], "--help"])
        assert result.exit_code == 0, " ".join(path)
        generic_types = sorted(set(_GENERIC_RENDERED_TYPE.findall(result.stdout)))
        if generic_types:
            violations.append(f"{' '.join(path)}: {', '.join(generic_types)}")

    assert violations == []


def test_leaf_help_has_usable_globals_without_repeating_output_tutorial() -> None:
    result = CliRunner().invoke(app, ["task", "get", "--help"])
    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "Global options" in help_text
    for flag in ("--context NAME", "--env-file PATH", "--columns FIELDS"):
        assert flag in help_text
    assert "Result format: json, json-compact, table or tsv." in help_text
    assert "[default: json]" in help_text
    assert "mutually exclusive with --env-file" in help_text
    assert "id,name,state" in help_text
    assert "Schema: dsctl schema --command task.get" in help_text
    assert "before or after" not in help_text
    assert "JSON retains types" not in help_text
    assert "JSON preserves types" not in help_text
    assert "Fields, defaults and related commands" not in help_text


@pytest.mark.parametrize("group", ["workflow", "workflow-instance"])
def test_raw_export_help_and_schema_describe_the_actual_output(group: str) -> None:
    result = CliRunner().invoke(app, [group, "export", "--help"])
    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "raw YAML" in help_text
    assert "display options do not alter it" in help_text
    assert "--context NAME" in help_text
    assert "--env-file PATH" in help_text
    for flag in ("--format", "--columns"):
        assert flag not in help_text
    schema = get_schema_result(command_action=f"{group}.export").data
    assert isinstance(schema, dict)
    command = schema["command"]
    assert isinstance(command, dict)
    payload = command["payload"]
    assert isinstance(payload, dict)
    assert payload["format"] == "yaml"
    assert payload["output"] == "raw_document"


@pytest.mark.parametrize(
    "route",
    [
        ["template", "workflow"],
        ["template", "workflow-patch"],
        ["template", "workflow-instance-patch"],
        ["template", "task"],
        ["task-instance", "log"],
    ],
)
def test_optional_raw_output_keeps_standard_controls_and_explains_exception(
    route: list[str],
) -> None:
    result = CliRunner().invoke(app, [*route, "--help"])
    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "--format FORMAT" in help_text
    assert "--columns FIELDS" in help_text
    assert "With --raw, display options do not alter the artifact output." in help_text


def test_rendering_inherited_options_never_changes_parser_parameters(
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    command = root.commands["version"]
    original_parameters = tuple(command.params)
    with (
        root.make_context("dsctl", [], resilient_parsing=True) as root_context,
        command.make_context("version", [], parent=root_context) as context,
    ):
        command.get_help(context)
        command.get_help(context)
    assert "Global options" in capsys.readouterr().out
    assert tuple(command.params) == original_parameters
    assert all(parameter.name != "columns" for parameter in command.params)
    assert all(
        getattr(parameter, "rich_help_panel", None) != "Global options"
        for parameter in root.params
    )


def _iter_commands(
    command: TyperCommand | TyperGroup,
    path: tuple[str, ...] = ("dsctl",),
) -> list[tuple[tuple[str, ...], TyperCommand | TyperGroup]]:
    commands = [(path, command)]
    if isinstance(command, TyperGroup):
        for name, child in command.commands.items():
            assert isinstance(child, TyperCommand | TyperGroup)
            commands.extend(_iter_commands(child, (*path, name)))
    return commands
