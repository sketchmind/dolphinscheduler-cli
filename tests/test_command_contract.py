from __future__ import annotations

import shlex
from pathlib import Path

import pytest
from typer import BadParameter
from typer.core import TyperArgument, TyperCommand, TyperGroup, TyperOption
from typer.main import get_command
from typer.models import TyperPath
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.cli_surface import stable_leaf_actions
from dsctl.command_contract import (
    COMMAND_CATALOG,
    MISSING_DEFAULT,
    REMOTE_READ,
    REMOTE_READ_LOCAL_FILE_WRITE,
    REMOTE_WRITE,
    CommandBindingError,
    CommandCatalog,
    CommandContract,
    CommandContractError,
    InputContract,
    ValueResolution,
)
from dsctl.services._schema_primitives import option_from_contract
from dsctl.services.schema import get_schema_result
from tests.support import normalize_cli_help

runner = CliRunner()


def test_every_stable_command_has_explicit_reviewed_effects() -> None:
    commands = {command.action: command for command in COMMAND_CATALOG.commands}

    assert set(commands) == stable_leaf_actions()
    assert {
        action
        for action, command in commands.items()
        if command.effects.dry_run is not None
    } == {
        "workflow.create",
        "workflow.edit",
        "workflow.run",
        "workflow.run-task",
        "workflow.backfill",
        "workflow-instance.edit",
        "task.update",
    }
    assert commands["resource.download"].effects == (REMOTE_READ_LOCAL_FILE_WRITE)
    assert commands["access-token.generate"].effects == REMOTE_READ
    assert commands["datasource.test"].effects == REMOTE_READ
    assert commands["alert-plugin.test"].effects == REMOTE_WRITE


def test_compile_dry_run_effects_still_declare_remote_reads() -> None:
    effects = COMMAND_CATALOG.command("workflow.create").effects

    assert effects.remote == "write"
    assert effects.local == "none"
    assert effects.dry_run is not None
    assert effects.dry_run.remote == "read"
    assert effects.dry_run.local == "none"


def test_command_catalog_rejects_missing_compile_dry_run_effects() -> None:
    with pytest.raises(
        CommandContractError,
        match="compile dry-run effects must exactly match",
    ):
        CommandCatalog(
            global_options=(),
            commands=(
                CommandContract(
                    action="workflow.run",
                    effects=REMOTE_READ,
                    route=("workflow", "run"),
                    summary="Run one workflow.",
                    options=(
                        InputContract(
                            name="dry-run",
                            kind="option",
                            value_type="boolean",
                            description="Prepare without mutation.",
                        ),
                    ),
                ),
            ),
        )


_REQUIRED_WORKFLOW_ARGUMENT_ACTIONS = (
    "workflow.get",
    "workflow.export",
    "workflow.describe",
    "workflow.digest",
    "workflow.edit",
    "workflow.online",
    "workflow.offline",
    "workflow.run",
    "workflow.run-task",
    "workflow.backfill",
    "workflow.delete",
    "workflow.lineage.get",
    "workflow.lineage.dependent-tasks",
)
_REQUIRED_WORKFLOW_OPTION_ACTIONS = (
    "task.list",
    "task.get",
    "task.update",
    "schedule.create",
)


def test_workflow_create_contract_renders_opaque_values_in_cli_order() -> None:
    rendered = COMMAND_CATALOG.render(
        "workflow.create",
        global_values={
            "env-file": " /profiles/$(cluster).env ",
            "format": "json-compact",
            "columns": "code,name,releaseState",
        },
        values={
            "file": " /workflows/it's $(unsafe).yaml ",
            "project": "7",
            "confirm-risk": " risk\n'$(token)' ",
        },
    )

    assert shlex.split(rendered) == [
        "dsctl",
        "--env-file",
        " /profiles/$(cluster).env ",
        "--format",
        "json-compact",
        "--columns",
        "code,name,releaseState",
        "workflow",
        "create",
        "--file",
        " /workflows/it's $(unsafe).yaml ",
        "--project",
        "7",
        "--confirm-risk",
        " risk\n'$(token)' ",
    ]


def test_workflow_create_help_and_schema_project_the_canonical_inputs() -> None:
    contract = COMMAND_CATALOG.command("workflow.create")
    help_result = runner.invoke(app, [*contract.route, "--help"])
    help_text = normalize_cli_help(help_result.stdout)
    schema_result = get_schema_result(command_action=contract.action)
    schema_data = schema_result.data

    assert help_result.exit_code == 0
    assert isinstance(schema_data, dict)
    schema_command = schema_data["command"]
    assert isinstance(schema_command, dict)
    schema_options = schema_command["options"]
    assert isinstance(schema_options, list)
    schema_links = schema_data["links"]
    assert isinstance(schema_links, list)
    assert schema_command["invocation"] == f"{contract.command_path} [OPTIONS]"
    assert {
        "rel": "help",
        "command": f"{contract.command_path} --help",
    } in schema_links
    assert [item["name"] for item in schema_options if isinstance(item, dict)] == [
        item.name for item in contract.options
    ]

    for input_contract in contract.options:
        normalized_description = normalize_cli_help(input_contract.description)
        assert normalized_description in help_text
        schema_option = next(
            item
            for item in schema_options
            if isinstance(item, dict) and item.get("name") == input_contract.name
        )
        assert schema_option["description"] == input_contract.description
        assert schema_option["required"] is input_contract.required
        assert schema_option["type"] == input_contract.value_type
        if input_contract.path_rules is not None:
            rules = input_contract.path_rules
            assert schema_option["input_policy"] == {
                "exists": rules.exists,
                "file_okay": rules.file_okay,
                "dir_okay": rules.dir_okay,
                "readable": rules.readable,
                "resolve_path": rules.resolve_path,
            }


def test_workflow_create_click_parser_matches_the_canonical_inputs() -> None:
    contract = COMMAND_CATALOG.command("workflow.create")
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    workflow = root.commands["workflow"]
    assert isinstance(workflow, TyperGroup)
    click_command = workflow.commands["create"]
    assert [item.name for item in click_command.params] == [
        item.name.replace("-", "_") for item in contract.options
    ]
    click_options = {item.name: item for item in click_command.params}
    assert click_command.help == contract.summary
    for input_contract in contract.options:
        click_option = click_options[input_contract.name.replace("-", "_")]
        assert isinstance(click_option, TyperOption)
        assert click_option.opts == [input_contract.flag]
        assert click_option.required is input_contract.required
        assert click_option.multiple is input_contract.multiple
        _assert_canonical_input_conversion(click_option, input_contract)
        assert click_option.help == input_contract.description
        if input_contract.parse_default is not MISSING_DEFAULT:
            assert click_option.default == input_contract.parse_default
        if input_contract.path_rules is not None:
            rules = input_contract.path_rules
            assert isinstance(click_option.type, TyperPath)
            assert click_option.type.exists is rules.exists
            assert click_option.type.file_okay is rules.file_okay
            assert click_option.type.dir_okay is rules.dir_okay
            assert click_option.type.readable is rules.readable
            assert click_option.type.resolve_path is rules.resolve_path


@pytest.mark.parametrize(
    ("action", "kind"),
    [
        *((action, "argument") for action in _REQUIRED_WORKFLOW_ARGUMENT_ACTIONS),
        *((action, "option") for action in _REQUIRED_WORKFLOW_OPTION_ACTIONS),
    ],
)
def test_workflow_selectors_are_required_across_catalog_parser_help_and_schema(
    action: str,
    kind: str,
) -> None:
    contract = COMMAND_CATALOG.command(action)
    workflow = contract.input("workflow")
    assert workflow.kind == kind
    assert workflow.required is True
    assert workflow.parse_default is MISSING_DEFAULT

    root = get_command(app)
    assert isinstance(root, TyperGroup)
    command: TyperCommand | TyperGroup = root
    for segment in contract.route:
        assert isinstance(command, TyperGroup)
        child = command.commands[segment]
        assert isinstance(child, (TyperCommand, TyperGroup))
        command = child
    parameter = next(item for item in command.params if item.name == "workflow")
    expected_type = TyperArgument if kind == "argument" else TyperOption
    assert isinstance(parameter, expected_type)
    assert parameter.required is True

    help_result = runner.invoke(app, [*contract.route, "--help"])
    assert help_result.exit_code == 0
    help_text = normalize_cli_help(help_result.stdout)
    assert "[required]" in help_text
    assert "[WORKFLOW]" not in help_text

    schema_data = get_schema_result(command_action=action).data
    assert isinstance(schema_data, dict)
    schema_command = schema_data["command"]
    assert isinstance(schema_command, dict)
    rows = schema_command[f"{kind}s"]
    assert isinstance(rows, list)
    schema_workflow = next(
        row for row in rows if isinstance(row, dict) and row.get("name") == "workflow"
    )
    assert schema_workflow["required"] is True


@pytest.mark.parametrize(
    "action",
    [
        "schedule.explain",
        "schedule.list",
        "workflow-instance.list",
        "task-instance.list",
    ],
)
def test_mode_specific_and_filter_workflow_options_remain_parser_optional(
    action: str,
) -> None:
    workflow = COMMAND_CATALOG.command(action).input("workflow")
    assert workflow.kind == "option"
    assert workflow.required is False
    assert workflow.parse_default is None

    if action == "schedule.explain":
        schema_data = get_schema_result(command_action=action).data
        assert isinstance(schema_data, dict)
        schema_command = schema_data["command"]
        assert isinstance(schema_command, dict)
        constraints = schema_command["constraints"]
        assert isinstance(constraints, list)
        assert any(
            isinstance(constraint, dict)
            and constraint.get("kind") == "requires"
            and constraint.get("if_absent") == "SCHEDULE_ID"
            and "--workflow" in constraint.get("fields", [])
            for constraint in constraints
        )


def test_root_help_and_full_schema_project_the_canonical_global_options() -> None:
    help_result = runner.invoke(app, ["--help"])
    help_text = normalize_cli_help(help_result.stdout)
    schema_data = get_schema_result(full=True).data

    assert help_result.exit_code == 0
    assert isinstance(schema_data, dict)
    schema_options = schema_data["global_options"]
    assert isinstance(schema_options, list)
    assert [item["name"] for item in schema_options if isinstance(item, dict)] == [
        item.name
        for item in sorted(
            COMMAND_CATALOG.global_options,
            key=lambda item: item.schema_order,
        )
    ]

    for global_contract in COMMAND_CATALOG.global_options:
        input_contract = global_contract.input
        schema_option = next(
            item
            for item in schema_options
            if isinstance(item, dict) and item.get("name") == input_contract.name
        )
        assert schema_option["description"] == input_contract.description
        assert normalize_cli_help(input_contract.description) in help_text
        if input_contract.path_rules is not None:
            rules = input_contract.path_rules
            assert schema_option["input_policy"] == {
                "exists": rules.exists,
                "file_okay": rules.file_okay,
                "dir_okay": rules.dir_okay,
                "readable": rules.readable,
                "resolve_path": rules.resolve_path,
            }


def test_root_click_parser_matches_all_canonical_global_options() -> None:
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    contracts = sorted(
        COMMAND_CATALOG.global_options,
        key=lambda item: item.schema_order,
    )
    framework_names = {"version", "install_completion", "show_completion"}
    framework_options = [item for item in root.params if item.name in framework_names]
    assert {item.name for item in framework_options} == framework_names
    for option in framework_options:
        assert isinstance(option, TyperOption)
        assert not option.required
        assert option.is_eager
    version_option = next(item for item in framework_options if item.name == "version")
    assert isinstance(version_option, TyperOption)
    assert version_option.is_flag
    completion_options = [item for item in framework_options if item.name != "version"]
    for option in completion_options:
        assert isinstance(option, TyperOption)
        assert option.nargs == 1
        assert hasattr(option.type, "choices")
        assert tuple(option.type.choices) == (
            "bash",
            "zsh",
            "fish",
            "powershell",
            "pwsh",
        )
    catalog_options = [item for item in root.params if item.name not in framework_names]
    assert [item.name for item in catalog_options] == [
        (contract.input.parameter_name or contract.name.replace("-", "_"))
        for contract in contracts
    ]
    for global_contract, click_parameter in zip(
        contracts, catalog_options, strict=True
    ):
        input_contract = global_contract.input
        assert isinstance(click_parameter, TyperOption)
        assert click_parameter.opts == [global_contract.flag]
        _assert_canonical_input_conversion(click_parameter, input_contract)
        assert click_parameter.required is input_contract.required
        assert click_parameter.default == input_contract.parse_default
        assert click_parameter.multiple is input_contract.multiple
        assert click_parameter.help == input_contract.description
        assert click_parameter.metavar == input_contract.value_name
        assert click_parameter.is_flag is (input_contract.value_type == "boolean")
        click_arity = 0 if click_parameter.is_flag else click_parameter.nargs
        assert global_contract.arity == click_arity
        if input_contract.path_rules is not None:
            rules = input_contract.path_rules
            assert isinstance(click_parameter.type, TyperPath)
            assert click_parameter.type.exists is rules.exists
            assert click_parameter.type.file_okay is rules.file_okay
            assert click_parameter.type.dir_okay is rules.dir_okay
            assert click_parameter.type.readable is rules.readable
            assert click_parameter.type.resolve_path is rules.resolve_path


def _assert_canonical_input_conversion(
    parameter: TyperOption,
    contract: InputContract,
) -> None:
    """Exercise parameter conversion without invoking command or option callbacks."""
    if contract.value_type == "string":
        literal = "007 literal value $(token)"
        converted = parameter.type.convert(literal, parameter, None)
        assert type(converted) is str
        assert converted == literal
    elif contract.value_type == "boolean":
        assert parameter.type.convert("true", parameter, None) is True
        assert parameter.type.convert("false", parameter, None) is False
        with pytest.raises(BadParameter):
            parameter.type.convert("not-a-boolean", parameter, None)
    elif contract.value_type == "path":
        path = Path(__file__).resolve()
        converted = parameter.type.convert(str(path), parameter, None)
        assert type(converted) is str
        assert converted == str(path)
        with pytest.raises(BadParameter):
            parameter.type.convert(str(path.parent), parameter, None)
    else:
        pytest.fail(f"Add a conversion example for {contract.value_type}")


def test_command_catalog_rejects_removed_compact_option() -> None:
    with pytest.raises(CommandBindingError, match="compact"):
        COMMAND_CATALOG.render(
            "workflow.create",
            global_values={"compact": True},
            values={"file": "/workflows/demo.yaml"},
        )


def test_command_catalog_rejects_unknown_output_format_choice() -> None:
    with pytest.raises(
        CommandBindingError,
        match="--format expects one of: json, json-compact, table, tsv",
    ):
        COMMAND_CATALOG.render(
            "workflow.create",
            global_values={"format": "yaml"},
            values={"file": "/workflows/demo.yaml"},
        )


def test_command_catalog_normalizes_output_format() -> None:
    rendered = COMMAND_CATALOG.render(
        "workflow.create",
        global_values={"format": "JSON-COMPACT"},
        values={"file": "/workflows/demo.yaml"},
    )

    assert shlex.split(rendered)[:3] == ["dsctl", "--format", "json-compact"]


def test_command_catalog_rejects_an_empty_required_binding() -> None:
    with pytest.raises(
        CommandBindingError,
        match=r"workflow\.create requires a non-empty value for 'file'",
    ):
        COMMAND_CATALOG.render(
            "workflow.create",
            global_values={},
            values={"file": ""},
        )


def test_command_catalog_renders_repeatable_options_in_declared_order() -> None:
    catalog = CommandCatalog(
        global_options=(),
        commands=(
            CommandContract(
                action="workflow.run",
                effects=REMOTE_READ,
                route=("workflow", "run"),
                summary="Run one workflow.",
                options=(
                    InputContract(
                        name="param",
                        kind="option",
                        value_type="string",
                        description="Repeatable workflow parameter.",
                        multiple=True,
                    ),
                ),
            ),
        ),
    )

    rendered = catalog.render(
        "workflow.run",
        global_values={},
        values={"param": ("bizdate=20260712", "region=cn")},
    )

    assert shlex.split(rendered) == [
        "dsctl",
        "workflow",
        "run",
        "--param",
        "bizdate=20260712",
        "--param",
        "region=cn",
    ]


def test_command_catalog_rejects_binding_types_not_accepted_by_the_parser() -> None:
    with pytest.raises(
        CommandBindingError,
        match="--file expects a string path binding",
    ):
        COMMAND_CATALOG.render(
            "workflow.create",
            global_values={},
            values={"file": 7},
        )


def test_command_catalog_separates_option_like_positional_values_for_click() -> None:
    catalog = CommandCatalog(
        global_options=(),
        commands=(
            CommandContract(
                action="workflow.run",
                effects=REMOTE_READ,
                route=("workflow", "run"),
                summary="Run one workflow.",
                arguments=(
                    InputContract(
                        name="workflow",
                        kind="argument",
                        value_type="string",
                        description="Workflow selector.",
                    ),
                ),
                options=(
                    InputContract(
                        name="project",
                        kind="option",
                        value_type="string",
                        description="Project selector.",
                    ),
                ),
            ),
        ),
    )

    rendered = catalog.render(
        "workflow.run",
        global_values={},
        values={"workflow": "--help", "project": "7"},
    )
    argv = shlex.split(rendered)
    root = get_command(app)
    assert isinstance(root, TyperGroup)
    workflow = root.commands["workflow"]
    assert isinstance(workflow, TyperGroup)
    run = workflow.commands["run"]

    with run.make_context("run", argv[3:]) as context:
        assert context.params["workflow"] == "--help"
        assert context.params["project"] == "7"
    assert argv == [
        "dsctl",
        "workflow",
        "run",
        "--project",
        "7",
        "--",
        "--help",
    ]


def test_command_catalog_rejects_nul_that_cannot_enter_os_argv() -> None:
    with pytest.raises(CommandBindingError, match="NUL"):
        COMMAND_CATALOG.render(
            "workflow.create",
            global_values={},
            values={
                "file": "/workflows/demo.yaml",
                "confirm-risk": "token\0suffix",
            },
        )


def test_input_contract_rejects_a_fixed_default_for_multisource_resolution() -> None:
    with pytest.raises(
        CommandContractError,
        match="fixed default cannot coexist with value resolution",
    ):
        InputContract(
            name="priority",
            kind="option",
            value_type="string",
            description="Workflow priority.",
            fixed_default="medium",
            resolution=ValueResolution(
                precedence=("flag", "project_preference", "default"),
                fallback="medium",
            ),
        )


def test_schema_projection_preserves_an_explicit_null_default() -> None:
    projected = option_from_contract(
        InputContract(
            name="optional-selector",
            kind="option",
            value_type="string",
            description="Optional selector.",
            fixed_default=None,
        )
    )

    assert "default" in projected
    assert projected["default"] is None
