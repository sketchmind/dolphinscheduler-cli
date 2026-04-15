from __future__ import annotations

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.doctor import get_doctor_result

TyperApp = typer.Typer
TyperContext = typer.Context


def register_doctor_commands(app: TyperApp) -> None:
    """Register the top-level `doctor` command."""
    app.command("doctor")(doctor_command)


def doctor_command(ctx: TyperContext) -> None:
    """Run local and remote diagnostics for the current DS runtime."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("doctor", lambda: get_doctor_result(env_file=env_file))


__all__ = ["register_doctor_commands"]
