from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMMANDS_DIR = REPO_ROOT / "src/dsctl/commands"
LIVE_DIR = REPO_ROOT / "tests/live"

LOCAL_ONLY_COMMANDS = {
    "capabilities",
    "context",
    "enum names",
    "enum list",
    "lint workflow",
    "schema",
    "template cluster",
    "template datasource",
    "template environment",
    "template params",
    "template task",
    "template workflow",
    "task-type get",
    "task-type schema",
    "use project",
    "use workflow",
    "version",
}

LIVE_COMMAND_HELPERS = {
    "run_dsctl",
    "wait_for_result",
}


def _constant_str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_target_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        value_name = _call_target_name(node.value)
        if value_name is None:
            return None
        return f"{value_name}.{node.attr}"
    return None


def _keyword_str(call: ast.Call, *, name: str) -> str | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            return _constant_str(keyword.value)
    return None


def _iter_register_functions(module: ast.Module) -> list[ast.FunctionDef]:
    return [
        statement
        for statement in module.body
        if isinstance(statement, ast.FunctionDef)
        and statement.name.startswith("register_")
        and statement.name.endswith("_commands")
    ]


def _is_typer_factory(call: ast.Call) -> bool:
    target = _call_target_name(call.func)
    return target in {"typer.Typer", "TyperApp"}


def _collect_typer_vars(module: ast.Module) -> set[str]:
    typer_vars: set[str] = set()
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if len(statement.targets) != 1 or not isinstance(
            statement.targets[0], ast.Name
        ):
            continue
        if not isinstance(statement.value, ast.Call) or not _is_typer_factory(
            statement.value
        ):
            continue
        typer_vars.add(statement.targets[0].id)
    return typer_vars


def _collect_nested_typer_links(
    module: ast.Module,
    typer_vars: set[str],
) -> list[tuple[str, str, str]]:
    nested_links: list[tuple[str, str, str]] = []
    for statement in module.body:
        if not isinstance(statement, ast.Expr) or not isinstance(
            statement.value, ast.Call
        ):
            continue
        call = statement.value
        if not isinstance(call.func, ast.Attribute) or call.func.attr != "add_typer":
            continue
        if not isinstance(call.func.value, ast.Name):
            continue
        if not call.args or not isinstance(call.args[0], ast.Name):
            continue
        parent = call.func.value.id
        child = call.args[0].id
        name = _keyword_str(call, name="name")
        if parent in typer_vars and child in typer_vars and name is not None:
            nested_links.append((parent, child, name))
    return nested_links


def _collect_typer_commands(
    module: ast.Module,
    typer_vars: set[str],
) -> list[tuple[str, str]]:
    typer_commands: list[tuple[str, str]] = []
    for statement in module.body:
        if not isinstance(statement, ast.FunctionDef):
            continue
        for decorator in statement.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            if not isinstance(decorator.func, ast.Attribute):
                continue
            if decorator.func.attr != "command":
                continue
            if not isinstance(decorator.func.value, ast.Name):
                continue
            typer_name = decorator.func.value.id
            command_name = _constant_str(decorator.args[0] if decorator.args else None)
            if typer_name in typer_vars and command_name is not None:
                typer_commands.append((typer_name, command_name))
    return typer_commands


def _collect_root_registrations(
    module: ast.Module,
    typer_vars: set[str],
) -> tuple[list[tuple[str, str]], set[tuple[str, ...]]]:
    root_typer_links: list[tuple[str, str]] = []
    direct_root_commands: set[tuple[str, ...]] = set()
    for register_fn in _iter_register_functions(module):
        for node in ast.walk(register_fn):
            if not isinstance(node, ast.Call) or not isinstance(
                node.func, ast.Attribute
            ):
                continue
            if not isinstance(node.func.value, ast.Name) or node.func.value.id != "app":
                continue
            if node.func.attr == "add_typer":
                if not node.args or not isinstance(node.args[0], ast.Name):
                    continue
                child = node.args[0].id
                name = _keyword_str(node, name="name")
                if child in typer_vars and name is not None:
                    root_typer_links.append((child, name))
                continue
            if node.func.attr == "command":
                command_name = _constant_str(node.args[0] if node.args else None)
                if command_name is not None:
                    direct_root_commands.add((command_name,))
    return root_typer_links, direct_root_commands


def _resolve_typer_paths(
    root_links: list[tuple[str, str]],
    nested_links: list[tuple[str, str, str]],
) -> dict[str, tuple[str, ...]]:
    typer_paths: dict[str, tuple[str, ...]] = {
        child: (segment,) for child, segment in root_links
    }
    changed = True
    while changed:
        changed = False
        for parent, child, segment in nested_links:
            parent_path = typer_paths.get(parent)
            if parent_path is None:
                continue
            child_path = (*parent_path, segment)
            if typer_paths.get(child) != child_path:
                typer_paths[child] = child_path
                changed = True
    return typer_paths


def _module_command_paths(path: Path) -> set[tuple[str, ...]]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    typer_vars = _collect_typer_vars(module)
    nested_links = _collect_nested_typer_links(module, typer_vars)
    typer_commands = _collect_typer_commands(module, typer_vars)
    root_links, direct_root_commands = _collect_root_registrations(module, typer_vars)
    typer_paths = _resolve_typer_paths(root_links, nested_links)

    command_paths = set(direct_root_commands)
    for typer_name, command_name in typer_commands:
        typer_path = typer_paths.get(typer_name)
        if typer_path is not None:
            command_paths.add((*typer_path, command_name))
    return command_paths


def _all_command_paths() -> set[str]:
    paths: set[str] = set()
    for path in sorted(COMMANDS_DIR.glob("*.py")):
        if path.name in {"__init__.py", "registry.py"}:
            continue
        for command_path in _module_command_paths(path):
            paths.add(" ".join(command_path))
    return paths


def _leading_constant_parts(list_node: ast.List) -> list[str]:
    parts: list[str] = []
    for element in list_node.elts:
        part = _constant_str(element)
        if part is None:
            break
        parts.append(part)
    return parts


def _match_command_path(parts: list[str], known_commands: set[str]) -> str | None:
    for size in range(len(parts), 0, -1):
        candidate = " ".join(parts[:size])
        if candidate in known_commands:
            return candidate
    return None


def _extract_live_command_paths(known_commands: set[str]) -> set[str]:
    seen: set[str] = set()
    for path in sorted(LIVE_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Name):
                continue
            if node.func.id not in LIVE_COMMAND_HELPERS:
                continue
            if len(node.args) < 2 or not isinstance(node.args[1], ast.List):
                continue
            command_path = _match_command_path(
                _leading_constant_parts(node.args[1]),
                known_commands,
            )
            if command_path is not None:
                seen.add(command_path)
    return seen


def test_every_cluster_interacting_command_has_live_coverage() -> None:
    all_commands = _all_command_paths()
    cluster_interacting_commands = all_commands - LOCAL_ONLY_COMMANDS
    covered_commands = _extract_live_command_paths(cluster_interacting_commands)

    missing_commands = sorted(cluster_interacting_commands - covered_commands)

    assert missing_commands == [], (
        "cluster-interacting commands missing live coverage:\n- "
        + "\n- ".join(missing_commands)
    )
