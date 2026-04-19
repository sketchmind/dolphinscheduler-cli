from __future__ import annotations

import ast
import inspect
import re
from itertools import product
from pathlib import Path
from typing import TypedDict

from dsctl.cli_surface import (
    NAME_FIRST_RESOURCES,
    RESOURCE_COMMAND_TREE,
    TOP_LEVEL_COMMANDS,
    SurfaceCommand,
)
from dsctl.services import resolver as resolver_service
from dsctl.services.schema import get_schema_result


class CommandNode(TypedDict):
    """Minimal schema command node used by surface-quality tests."""

    name: str
    action: str
    options: list[dict[str, object]]


class GroupNode(TypedDict):
    """Minimal schema group node used by surface-quality tests."""

    name: str
    commands: list[dict[str, object]]


RAW_RESOURCE_LITERAL_PATTERN = re.compile(r'"resource":\s*"[^"]+"|resource="[^"]+"')
LOCAL_PAGINATION_CONSTANT_PATTERN = re.compile(
    r"^(DEFAULT_PAGE_SIZE|MAX_AUTO_EXHAUST_PAGES)\s*="
)
SERVICES_DIR = Path(__file__).resolve().parents[2] / "src" / "dsctl" / "services"
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
NAME_FIRST_SERVICE_MODULES = {
    "project.py": "project",
    "env.py": "environment",
    "cluster.py": "cluster",
    "datasource.py": "datasource",
    "namespace.py": "namespace",
    "queue.py": "queue",
    "worker_group.py": "worker_group",
    "task_group.py": "task_group",
    "alert_plugin.py": "alert_plugin",
    "alert_group.py": "alert_group",
    "tenant.py": "tenant",
    "user.py": "user",
    "project_parameter.py": "project_parameter",
    "workflow.py": "workflow",
    "task.py": "task",
}
SUGGESTION_GOVERNED_SERVICE_MODULES = (
    "_resolver_kernel.py",
    "_validation.py",
    "_workflow_compile.py",
    "access_token.py",
    "alert_group.py",
    "alert_plugin.py",
    "audit.py",
    "datasource.py",
    "env.py",
    "namespace.py",
    "project_preference.py",
    "queue.py",
    "resource.py",
    "schedule.py",
    "_schedule_support.py",
    "_workflow_patch.py",
    "pagination.py",
    "task_group.py",
    "tenant.py",
    "worker_group.py",
    "workflow.py",
    "workflow_instance.py",
    "task.py",
    "task_instance.py",
    "user.py",
)
STABLE_COMMAND_DOC_BLOCKS = {
    "README.md": (
        "Stable user-facing commands today:",
        "## Documentation",
    ),
    "docs/development/architecture.md": (
        "The current stable CLI surface is:",
        "Everything else remains roadmap work.",
    ),
    "docs/reference/cli-contract.md": (
        "Current stable commands:",
        "## Naming and Selection Rules",
    ),
}


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
    data = get_schema_result().data
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
    data = get_schema_result().data
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


def test_name_first_services_import_and_call_their_resource_resolver() -> None:
    missing_imports: list[str] = []
    missing_calls: list[str] = []
    for module_name, resolver_name in NAME_FIRST_SERVICE_MODULES.items():
        module = ast.parse(
            (SERVICES_DIR / module_name).read_text(encoding="utf-8"),
        )
        alias_name = f"resolve_{resolver_name}"
        imported = any(
            isinstance(node, ast.ImportFrom)
            and node.module == "dsctl.services.resolver"
            and any(
                alias.name == resolver_name and alias.asname == alias_name
                for alias in node.names
            )
            for node in module.body
        )
        called = any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == alias_name
            for node in ast.walk(module)
        )
        if not imported:
            missing_imports.append(f"{module_name}:{alias_name}")
        if not called:
            missing_calls.append(f"{module_name}:{alias_name}")

    assert missing_imports == []
    assert missing_calls == []


def test_runtime_interaction_services_attach_suggestions_to_direct_state_errors() -> (
    None
):
    missing: list[str] = []
    for module_name in SUGGESTION_GOVERNED_SERVICE_MODULES:
        module = ast.parse(
            (SERVICES_DIR / module_name).read_text(encoding="utf-8"),
        )
        for node in ast.walk(module):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"UserInputError", "InvalidStateError"}
            ):
                continue
            if any(keyword.arg == "suggestion" for keyword in node.keywords):
                continue
            missing.append(f"{module_name}:{node.lineno}:{node.func.id}")

    assert missing == []


def test_stable_command_docs_cover_shared_cli_surface() -> None:
    expected_paths = _stable_surface_leaf_paths()
    missing_by_doc: dict[str, list[str]] = {}
    for relative_path, (start_marker, end_marker) in STABLE_COMMAND_DOC_BLOCKS.items():
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        documented_paths = _documented_command_paths(
            _text_between(text, start_marker, end_marker),
        )
        missing = sorted(expected_paths - documented_paths)
        if missing:
            missing_by_doc[relative_path] = [
                f"dsctl {' '.join(path)}" for path in missing
            ]

    assert missing_by_doc == {}


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
