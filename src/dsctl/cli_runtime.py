from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import typer

from dsctl.errors import DsctlError
from dsctl.output import CommandResult, error_payload, print_json, success_payload


@dataclass(frozen=True)
class AppState:
    """Global CLI state shared across commands."""

    env_file: Path | None


def get_app_state(ctx: typer.Context) -> AppState:
    """Return the initialized CLI state object."""
    state = ctx.obj
    if isinstance(state, AppState):
        return state
    message = "CLI app state is not initialized"
    raise RuntimeError(message)


def emit_result(action: str, builder: Callable[[], CommandResult]) -> None:
    """Render a command result as the standard JSON envelope."""
    try:
        result = builder()
    except DsctlError as exc:
        print_json(error_payload(action, exc))
        raise typer.Exit(code=1) from None

    print_json(success_payload(action, result))
