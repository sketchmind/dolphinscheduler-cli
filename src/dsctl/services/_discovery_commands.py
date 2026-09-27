from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from dsctl.command_contract import COMMAND_CATALOG, semantic_value_name
from dsctl.services.version_resolution import selected_target_globals

if TYPE_CHECKING:
    from collections.abc import Mapping


def render_discovery_command(
    action: str,
    *,
    values: Mapping[str, str | int | bool] | None = None,
    env_file: str | None = None,
) -> str:
    """Render one concrete discovery command for the selected target."""
    return COMMAND_CATALOG.render(
        action,
        values={} if values is None else values,
        global_values=selected_target_globals(env_file),
    )


def render_discovery_reference(reference: str, *, env_file: str | None = None) -> str:
    """Bind reviewed concrete choice hints; retain prose and abstract patterns."""
    if not reference.startswith("dsctl "):
        return reference
    try:
        tokens = shlex.split(reference)
    except ValueError:
        return reference
    binding = _discovery_reference_binding(tokens)
    if binding is None:
        return reference
    action, values = binding
    return render_discovery_command(action, values=values, env_file=env_file)


def _discovery_reference_binding(
    tokens: list[str],
) -> tuple[str, dict[str, str | bool]] | None:
    for contract in COMMAND_CATALOG.commands:
        if (
            contract.effects.remote != "write"
            and not any(
                item.required for item in (*contract.arguments, *contract.options)
            )
            and tokens == ["dsctl", *contract.route]
        ):
            return contract.action, {}
    if (
        tokens[:3] == ["dsctl", "enum", "list"]
        and len(tokens) == 4
        and tokens[3] != "ENUM"
    ):
        return "enum.list", {"enum": tokens[3]}
    selectors = (
        ("template.params", "topic"),
        ("schema", "group"),
        ("schema", "command"),
        ("capabilities", "action"),
    )
    for action, name in selectors:
        contract = COMMAND_CATALOG.command(action)
        prefix = ["dsctl", *contract.route, f"--{name}"]
        if tokens[:-1] != prefix:
            continue
        declared_input = contract.input(name)
        if tokens[-1] != semantic_value_name(declared_input.name) and (
            not declared_input.choices or tokens[-1] in declared_input.choices
        ):
            return action, {name: tokens[-1]}
    for name in ("list-groups", "list-commands"):
        if tokens == ["dsctl", "schema", f"--{name}"]:
            return "schema", {name: True}
    return None
