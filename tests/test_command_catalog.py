from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
from types import FunctionType

import pytest
from typer.core import TyperCommand, TyperGroup, TyperOption
from typer.main import get_command
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.cli_surface import stable_leaf_actions
from dsctl.command_contract import COMMAND_CATALOG, CommandContractError
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.schema import get_schema_result
from tests.support import normalize_cli_help

_BASELINE = json.loads(
    (
        Path(__file__).parent
        / "fixtures/command_contract/pre_catalog_fingerprints.json"
    ).read_text()
)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _parameters(command: TyperCommand | TyperGroup) -> list[dict[str, object]]:
    params: list[dict[str, object]] = []
    for parameter in command.params:
        record: dict[str, object] = {
            key: getattr(parameter, key, None)
            for key in (
                "name",
                "opts",
                "secondary_opts",
                "required",
                "default",
                "nargs",
                "multiple",
                "metavar",
                "help",
                "show_default",
                "show_choices",
                "show_envvar",
                "hidden",
                "is_flag",
                "count",
            )
        }
        record["type"] = parameter.type.name
        record["choices"] = list(getattr(parameter.type, "choices", []))
        record["type_attrs"] = {
            key: getattr(parameter.type, key)
            for key in (
                "min",
                "max",
                "clamp",
                "exists",
                "file_okay",
                "dir_okay",
                "readable",
                "writable",
                "resolve_path",
                "allow_dash",
            )
            if hasattr(parameter.type, key)
        }
        record["kind"] = "option" if isinstance(parameter, TyperOption) else "argument"
        params.append(record)
    return params


@pytest.fixture(scope="module")
def command_tree() -> TyperGroup:
    command = get_command(app)
    assert isinstance(command, TyperGroup)
    return command


@pytest.mark.parametrize("action", sorted(_BASELINE))
def test_complete_catalog_preserves_independent_parser_and_help_baseline(
    action: str, command_tree: TyperGroup
) -> None:
    # Parser fingerprints were captured from 4044131 before catalog migration,
    # not from the catalog. Explicit behavior changes update only affected
    # entries; named context management adds its new parser contracts.
    # Inspect semantic help facts: supported Typer versions render the same
    # argument metadata with different decorative labels in their Rich tables.
    contract = COMMAND_CATALOG.command(action)
    command: TyperCommand | TyperGroup = command_tree
    for segment in contract.route:
        assert isinstance(command, TyperGroup)
        child = command.commands[segment]
        assert isinstance(child, (TyperCommand, TyperGroup))
        command = child
    assert _digest(_parameters(command)) == _BASELINE[action]["parser"]
    assert command.help is not None
    assert command.help == _BASELINE[action]["summary"]
    help_result = CliRunner().invoke(
        app, [*contract.route, "--help"], terminal_width=120
    )
    assert help_result.exit_code == 0
    assert normalize_cli_help(command.help) in normalize_cli_help(help_result.output)


def test_catalog_covers_every_stable_action_and_retains_typed_callbacks(
    command_tree: TyperGroup,
) -> None:
    assert {item.action for item in COMMAND_CATALOG.commands} == set(
        stable_leaf_actions()
    )
    assert len(COMMAND_CATALOG.commands) == 181
    project = command_tree.commands["project"]
    assert isinstance(project, TyperGroup)
    bound_callback = project.commands["list"].callback
    assert bound_callback is not None
    callback = inspect.unwrap(bound_callback)
    assert isinstance(callback, FunctionType)
    signature = inspect.signature(callback)
    assert signature.parameters["page_size"].default == 100
    assert callback.__kwdefaults__ is not None
    assert callback.__kwdefaults__["page_size"] == 100
    assert callback.__name__ == "list_command"


@pytest.mark.parametrize(
    ("action", "hidden"),
    [("tenant.update", "tenant-code"), ("task-instance.list", "workflow")],
)
def test_hidden_compatibility_inputs_remain_catalogued_but_undiscoverable(
    action: str, hidden: str
) -> None:
    assert COMMAND_CATALOG.command(action).input(hidden).hidden
    result = get_schema_result(command_action=action)
    assert isinstance(result.data, dict)
    command = result.data["command"]
    assert isinstance(command, dict)
    options = command["options"]
    assert isinstance(options, list)
    assert all(isinstance(option, dict) for option in options)
    assert hidden not in {option["name"] for option in options}


@pytest.mark.parametrize(
    ("action", "name", "value_name"),
    [
        ("alert-plugin.create", "param", "KEY=VALUE"),
        ("alert-plugin.update", "param", "KEY=VALUE"),
        ("alert-plugin.create", "file", "PATH"),
        ("alert-plugin.update", "file", "PATH"),
        ("datasource.create", "file", "PATH"),
        ("datasource.update", "file", "PATH"),
        ("project-preference.update", "file", "PATH"),
        ("resource.upload", "file", "PATH"),
        ("resource.download", "output", "PATH"),
    ],
)
def test_schema_v2_value_names_remain_independent_of_parser_metavars(
    action: str, name: str, value_name: str
) -> None:
    # These public schema labels predate the catalog; their parser metavars
    # differ and are checked independently by the frozen parser fingerprints.
    result = get_schema_result(command_action=action)
    assert isinstance(result.data, dict)
    command = result.data["command"]
    assert isinstance(command, dict)
    options = command["options"]
    assert isinstance(options, list)
    assert all(isinstance(item, dict) for item in options)
    option = next(item for item in options if item["name"] == name)
    assert option["value_name"] == value_name


def test_catalog_preserves_service_requiredness_and_string_path_semantics() -> None:
    timezone = COMMAND_CATALOG.command("schedule.create").input("timezone")
    assert timezone.required
    assert timezone.parse_default is None
    lint_file = COMMAND_CATALOG.command("lint.workflow").input("file")
    assert lint_file.value_type == "path"
    assert lint_file.path_as_string
    assert lint_file.path_rules is None


def test_callback_binding_rejects_type_disagreement() -> None:
    def wrong_type(project: int) -> None:
        pass

    with pytest.raises(CommandContractError, match="callback type differs"):
        bind_command("project.get")(wrong_type)


def test_callback_binding_rejects_duplicate_default_declarations() -> None:
    def duplicate_default(project: str = "default") -> None:
        pass

    with pytest.raises(CommandContractError, match="defaults belong in the catalog"):
        bind_command("project.get")(duplicate_default)


def test_context_group_and_management_action_rendering() -> None:
    assert (
        COMMAND_CATALOG.render("context", global_values={}, values={})
        == "dsctl context"
    )
    assert COMMAND_CATALOG.render(
        "context.create",
        global_values={"context": "production"},
        values={"name": "testing", "file": "/profiles/test.env", "project": "etl"},
    ) == (
        "dsctl --context production context create testing "
        "--file /profiles/test.env --project etl"
    )
    for action in ("use.clear", "use.project", "use.workflow"):
        with pytest.raises(KeyError):
            COMMAND_CATALOG.command(action)


def test_callback_binding_rejects_order_drift() -> None:
    def reordered(
        *, search: str | None, page_size: int, page_no: int, all_pages: bool
    ) -> None:
        pass

    with pytest.raises(CommandContractError, match="callback inputs differ"):
        bind_command("project.list")(reordered)


def test_queue_update_renders_distinct_argument_and_option_bindings() -> None:
    assert (
        COMMAND_CATALOG.render(
            "queue.update",
            global_values={},
            values={"queue-identifier": "existing queue", "queue": "new-yarn-queue"},
        )
        == "dsctl queue update 'existing queue' --queue new-yarn-queue"
    )
