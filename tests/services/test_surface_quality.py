from __future__ import annotations

import ast
import inspect
import json
import re
import shlex
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, cast

import pytest
from tests.support import normalize_cli_help, strip_cli_ansi
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.cli_surface import (
    NAME_FIRST_RESOURCES,
    RESOURCE_COMMAND_TREE,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)
from dsctl.services.schema import get_schema_result
from dsctl.upstream import resolver as resolver_service

if TYPE_CHECKING:
    from collections.abc import Callable


class CommandNode(TypedDict):
    """Minimal schema command node used by surface-quality tests."""

    name: str
    action: str
    options: list[dict[str, object]]


class GroupNode(TypedDict):
    """Minimal schema group node used by surface-quality tests."""

    name: str
    commands: list[dict[str, object]]


class HelpResultNode(TypedDict):
    """One immutable-on-the-wire CLI help rendering."""

    path: list[str]
    command: dict[str, object]
    exit_code: int
    stdout: str


class HelpCorpus(TypedDict):
    """JSON-backed schema and rendered help corpus shared by this module."""

    schema: object
    results: list[HelpResultNode]


RAW_RESOURCE_LITERAL_PATTERN = re.compile(r'"resource":\s*"[^"]+"|resource="[^"]+"')
LOCAL_PAGINATION_CONSTANT_PATTERN = re.compile(
    r"^(DEFAULT_PAGE_SIZE|MAX_AUTO_EXHAUST_PAGES)\s*="
)
SERVICES_DIR = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "services"
UPSTREAM_DIR = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "upstream"
REPO_ROOT = Path(__file__).resolve().parents[2]
COMMANDS_DIR = REPO_ROOT / "src" / "dsctl" / "commands"
NAME_FIRST_RESOURCE_RESOLVERS = {
    "project": "project",
    "environment": "environment",
    "cluster": "cluster",
    "datasource": "datasource",
    "namespace": "namespace",
    "queue": "queue",
    "worker-group": "worker_group",
    "task-group": "task_group",
    "alert-plugin": "alert_plugin",
    "alert-group": "alert_group",
    "tenant": "tenant",
    "user": "user",
    "project-parameter": "project_parameter",
    "workflow": "workflow",
    "task": "task",
}
RUNNER = CliRunner()
HELP_OUTPUT_BYTE_BUDGET = 10 * 1024


@pytest.fixture(scope="module")
def cli_help_corpus_payload() -> str:
    """Render each declared help path once and expose an immutable snapshot."""
    data = get_schema_result(full=True).data
    results: list[HelpResultNode] = []
    rendered: dict[tuple[str, ...], tuple[int, str]] = {}
    if isinstance(data, dict):
        commands = data.get("commands")
        if isinstance(commands, list):
            for path, command in _iter_schema_command_paths(commands):
                if path not in rendered:
                    result = RUNNER.invoke(app, [*path, "--help"])
                    rendered[path] = (result.exit_code, result.stdout)
                exit_code, stdout = rendered[path]
                results.append(
                    HelpResultNode(
                        path=list(path),
                        command=command,
                        exit_code=exit_code,
                        stdout=stdout,
                    )
                )
    return json.dumps(HelpCorpus(schema=data, results=results), sort_keys=True)


def _decode_cli_help_corpus(payload: str) -> HelpCorpus:
    corpus = json.loads(payload)
    assert isinstance(corpus, dict)
    return cast("HelpCorpus", corpus)


def test_services_do_not_inline_resource_slug_literals() -> None:
    matches: list[str] = []
    for path in SERVICES_DIR.rglob("*.py"):
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if RAW_RESOURCE_LITERAL_PATTERN.search(line):
                relative_path = path.relative_to(SERVICES_DIR.parent)
                matches.append(f"{relative_path}:{line_number}: {line.strip()}")

    assert matches == []


def test_services_do_not_redefine_shared_pagination_constants() -> None:
    matches: list[str] = []
    for path in SERVICES_DIR.rglob("*.py"):
        if path.name == "pagination.py":
            continue
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if LOCAL_PAGINATION_CONSTANT_PATTERN.search(line):
                relative_path = path.relative_to(SERVICES_DIR.parent)
                matches.append(f"{relative_path}:{line_number}: {line.strip()}")

    assert matches == []


def test_services_do_not_import_generated_directly() -> None:
    matches: list[str] = []
    for path in SERVICES_DIR.rglob("*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "dsctl.generated"
            ):
                relative_path = path.relative_to(SERVICES_DIR.parent)
                matches.append(f"{relative_path}:{node.lineno}:from {node.module}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("dsctl.generated"):
                        relative_path = path.relative_to(SERVICES_DIR.parent)
                        matches.append(
                            f"{relative_path}:{node.lineno}:import {alias.name}"
                        )

    assert matches == []


def test_paginated_schema_commands_expose_standard_all_option() -> None:
    data = get_schema_result(full=True).data
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    missing: list[str] = []
    malformed: list[str] = []
    for command in _iter_schema_commands(commands):
        option_by_name = {
            option["name"]: option
            for option in command.get("options", [])
            if isinstance(option, dict) and isinstance(option.get("name"), str)
        }
        if {"page-no", "page-size"} - option_by_name.keys():
            continue
        all_option = option_by_name.get("all")
        if all_option is None:
            missing.append(command["action"])
            continue
        if all_option != {
            "kind": "option",
            "name": "all",
            "flag": "--all",
            "type": "boolean",
            "required": False,
            "description": "Fetch all remaining pages up to the safety limit.",
            "default": False,
        }:
            malformed.append(command["action"])

    assert missing == []
    assert malformed == []


def test_literal_emit_result_actions_are_declared_in_schema() -> None:
    data = get_schema_result(full=True).data
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)
    declared_actions = _iter_schema_action_names(commands)
    emitted_actions = _literal_emit_result_actions()

    missing = [
        f"{action} ({path.relative_to(REPO_ROOT)}:{line_number})"
        for action, path, line_number in emitted_actions
        if action not in declared_actions
    ]

    assert missing == []


def test_schema_declared_commands_expose_help(
    cli_help_corpus_payload: str,
) -> None:
    corpus = _decode_cli_help_corpus(cli_help_corpus_payload)
    data = corpus["schema"]
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    failures: list[str] = []
    for help_result in corpus["results"]:
        path = help_result["path"]
        command = help_result["command"]
        exit_code = help_result["exit_code"]
        if exit_code != 0:
            action = command.get("action")
            failures.append(f"{' '.join(path)} ({action}) exited {exit_code}")

    assert failures == []


def test_leaf_help_records_discovery_size(
    cli_help_corpus_payload: str,
    record_property: Callable[[str, object], None],
) -> None:
    corpus = _decode_cli_help_corpus(cli_help_corpus_payload)
    data = corpus["schema"]
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    sizes: dict[str, int] = {}
    for help_result in corpus["results"]:
        path = help_result["path"]
        size = len(strip_cli_ansi(help_result["stdout"]).encode("utf-8"))
        sizes[" ".join(path)] = size

    assert sizes
    record_property("help_bytes", json.dumps(sizes, sort_keys=True))
    record_property("review_threshold_bytes", HELP_OUTPUT_BYTE_BUDGET)


def test_schema_selector_fields_expose_discovery_commands() -> None:
    data = get_schema_result(full=True).data
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    missing: list[str] = []
    for path, command in _iter_schema_command_paths(commands):
        for field_kind, field in _iter_schema_command_fields(command):
            if not field.get("selector"):
                continue
            if _field_has_discovery_command(field):
                continue
            missing.append(
                f"{'.'.join(path)}:{field_kind}:{field.get('name', '<unknown>')}"
            )

    assert missing == []


def test_schema_choice_fields_are_discoverable_from_help_or_schema(
    cli_help_corpus_payload: str,
) -> None:
    corpus = _decode_cli_help_corpus(cli_help_corpus_payload)
    data = corpus["schema"]
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    issues: list[str] = []
    for help_result in corpus["results"]:
        path = help_result["path"]
        command = help_result["command"]
        exit_code = help_result["exit_code"]
        if exit_code != 0:
            issues.append(f"{' '.join(path)}: help exited {exit_code}")
            continue
        help_text = normalize_cli_help(help_result["stdout"])
        for field_kind, field in _iter_schema_command_fields(command):
            choices = field.get("choices")
            if not isinstance(choices, list) or not choices:
                continue
            field_label = f"{'.'.join(path)}:{field_kind}:{field.get('name')}"
            discovery_pattern = field.get("discovery_command_pattern")
            if isinstance(discovery_pattern, str) and discovery_pattern:
                if discovery_pattern not in help_text:
                    issues.append(
                        f"{field_label} missing contextual discovery "
                        f"{discovery_pattern}"
                    )
                continue
            if len(choices) <= 10:
                missing_choices = [
                    str(choice) for choice in choices if str(choice) not in help_text
                ]
                if missing_choices:
                    issues.append(
                        f"{field_label} missing help choices {missing_choices}"
                    )
                continue
            if not _field_has_discovery_command(field):
                issues.append(
                    f"{field_label} has {len(choices)} choices without discovery"
                )

    assert issues == []


def test_schema_discovery_commands_point_to_existing_help_surfaces(
    cli_help_corpus_payload: str,
) -> None:
    corpus = _decode_cli_help_corpus(cli_help_corpus_payload)
    data = corpus["schema"]
    assert isinstance(data, dict)
    commands = data["commands"]
    assert isinstance(commands, list)

    known_paths = {path for path, _command in _iter_schema_command_paths(commands)}
    help_by_path = {
        tuple(help_result["path"]): help_result for help_result in corpus["results"]
    }
    issues: list[str] = []
    for discovery_command in sorted(_iter_discovery_commands(data)):
        tokens = shlex.split(discovery_command)
        if not tokens or tokens[0] != "dsctl":
            issues.append(f"{discovery_command}: must start with dsctl")
            continue

        command_path = _longest_known_command_path(tokens[1:], known_paths)
        if command_path is None:
            issues.append(f"{discovery_command}: no declared command path")
            continue

        help_result = help_by_path[command_path]
        exit_code = help_result["exit_code"]
        if exit_code != 0:
            issues.append(
                f"{discovery_command}: {' '.join(command_path)} --help "
                f"exited {exit_code}"
            )
            continue

        help_text = normalize_cli_help(help_result["stdout"])
        issues.extend(
            f"{discovery_command}: {flag} missing from {' '.join(command_path)} --help"
            for flag in _option_flags_after_path(tokens[1:], command_path)
            if flag not in help_text
        )

    assert issues == []


def test_name_first_resources_have_resolver_functions() -> None:
    assert set(NAME_FIRST_RESOURCE_RESOLVERS) == set(NAME_FIRST_RESOURCES)
    available = {
        name
        for name, member in inspect.getmembers(resolver_service)
        if inspect.isfunction(member)
    }
    missing = sorted(
        resolver_name
        for resolver_name in NAME_FIRST_RESOURCE_RESOLVERS.values()
        if resolver_name not in available
    )
    assert missing == []


def test_cli_contract_covers_shared_stable_command_surface() -> None:
    expected_paths = _stable_surface_leaf_paths()
    text = (REPO_ROOT / "docs/reference/cli-contract.md").read_text(encoding="utf-8")
    documented_paths = _documented_command_paths(
        _text_between(text, "Current stable commands:", "## Naming and Selection Rules")
    )
    assert sorted(expected_paths - documented_paths) == []


def test_cli_contract_command_blocks_do_not_duplicate_rules_sections() -> None:
    text = (REPO_ROOT / "docs/reference/cli-contract.md").read_text(encoding="utf-8")
    violations: list[str] = []
    for block in re.split(r"(?=^## `dsctl )", text, flags=re.MULTILINE):
        if not block.startswith("## `dsctl "):
            continue
        title = block.splitlines()[0]
        rules_count = sum(1 for line in block.splitlines() if line.strip() == "Rules:")
        if rules_count > 1:
            violations.append(f"{title}: {rules_count} Rules sections")

    assert violations == []


def _iter_schema_commands(nodes: list[object]) -> list[CommandNode]:
    commands: list[CommandNode] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        nested = node.get("commands")
        if isinstance(nested, list):
            commands.extend(_iter_schema_commands(nested))
        action = node.get("action")
        if not isinstance(action, str):
            continue
        options = node.get("options", [])
        if not isinstance(options, list):
            continue
        commands.append(
            CommandNode(
                name=str(node.get("name", "")),
                action=action,
                options=[option for option in options if isinstance(option, dict)],
            )
        )
    return commands


def _iter_schema_action_names(nodes: list[object]) -> set[str]:
    actions: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        action = node.get("action")
        if isinstance(action, str):
            actions.add(action)
        group_action = node.get("group_action")
        if isinstance(group_action, dict):
            group_action_name = group_action.get("action")
            if isinstance(group_action_name, str):
                actions.add(group_action_name)
        nested = node.get("commands")
        if isinstance(nested, list):
            actions.update(_iter_schema_action_names(nested))
    return actions


def _literal_emit_result_actions() -> list[tuple[str, Path, int]]:
    emitted_actions: list[tuple[str, Path, int]] = []
    for path in COMMANDS_DIR.glob("*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "emit_result"
                and node.args
            ):
                continue
            first_arg = node.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                emitted_actions.append((first_arg.value, path, node.lineno))
    return emitted_actions


SurfacePath = tuple[str, ...]
SchemaCommandPath = tuple[SurfacePath, dict[str, object]]


def _iter_schema_command_paths(
    nodes: list[object],
    prefix: SurfacePath = (),
) -> list[SchemaCommandPath]:
    commands: list[SchemaCommandPath] = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        name = node.get("name")
        if not isinstance(name, str):
            continue
        path = (*prefix, name)
        action = node.get("action")
        if isinstance(action, str):
            commands.append((path, node))
        group_action = node.get("group_action")
        if isinstance(group_action, dict) and isinstance(
            group_action.get("action"),
            str,
        ):
            commands.append((path, group_action))
        nested = node.get("commands")
        if isinstance(nested, list):
            commands.extend(_iter_schema_command_paths(nested, path))
    return commands


def _iter_schema_command_fields(
    command: dict[str, object],
) -> list[tuple[str, dict[str, object]]]:
    fields: list[tuple[str, dict[str, object]]] = []
    for field_kind in ("arguments", "options"):
        value = command.get(field_kind)
        if not isinstance(value, list):
            continue
        fields.extend((field_kind, field) for field in value if isinstance(field, dict))
    return fields


def _iter_discovery_commands(value: object) -> set[str]:
    commands: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {
                "discovery_command",
                "discovery_command_pattern",
            } and isinstance(item, str):
                commands.add(item)
                continue
            commands.update(_iter_discovery_commands(item))
    elif isinstance(value, list):
        for item in value:
            commands.update(_iter_discovery_commands(item))
    return commands


def _field_has_discovery_command(field: dict[str, object]) -> bool:
    """Accept bound commands and canonical parameterized command patterns."""
    return any(
        isinstance(field.get(key), str) and bool(field[key])
        for key in ("discovery_command", "discovery_command_pattern")
    )


def _longest_known_command_path(
    tokens: list[str],
    known_paths: set[SurfacePath],
) -> SurfacePath | None:
    for length in range(len(tokens), 0, -1):
        candidate = tuple(tokens[:length])
        if candidate in known_paths:
            return candidate
    return None


def _option_flags_after_path(
    tokens: list[str],
    command_path: SurfacePath,
) -> tuple[str, ...]:
    return tuple(
        token.split("=", 1)[0]
        for token in tokens[len(command_path) :]
        if token.startswith("--")
    )


def _stable_surface_leaf_paths() -> set[SurfacePath]:
    paths: set[SurfacePath] = {(command,) for command in TOP_LEVEL_COMMANDS}
    for resource, commands in RESOURCE_COMMAND_TREE.items():
        for command_node in commands:
            paths.update(_surface_leaf_paths((resource,), command_node))
    return paths


def _surface_leaf_paths(
    prefix: SurfacePath,
    command_node: SurfaceCommand,
) -> set[SurfacePath]:
    current_path = (*prefix, command_node.name)
    if not command_node.commands:
        return {current_path}
    paths: set[SurfacePath] = set()
    for child in command_node.commands:
        paths.update(_surface_leaf_paths(current_path, child))
    return paths


def _documented_command_paths(text: str) -> set[SurfacePath]:
    paths: set[SurfacePath] = set()
    for match in re.finditer(r"^- `dsctl ([^`]+)`", text, flags=re.MULTILINE):
        choices = [
            token_choices
            for token in match.group(1).split()
            if (token_choices := _documented_token_choices(token))
        ]
        for path in product(*choices):
            paths.add(tuple(path))
    return paths


def _documented_token_choices(token: str) -> tuple[str, ...]:
    choices = []
    for choice in token.split("|"):
        if _is_command_placeholder(choice):
            continue
        choices.append(choice)
    return tuple(choices)


def _is_command_placeholder(token: str) -> bool:
    return token.startswith(("[", "--")) or token.isupper()


def _text_between(text: str, start_marker: str, end_marker: str) -> str:
    start_index = text.index(start_marker)
    end_index = text.index(end_marker, start_index)
    return text[start_index:end_index]
