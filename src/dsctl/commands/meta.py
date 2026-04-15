import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.meta import get_context_result, get_version_result


def register_meta_commands(app: typer.Typer) -> None:
    """Register top-level metadata commands on the root CLI app."""
    app.command("version")(version_command)
    app.command("context")(context_command)


def version_command(ctx: typer.Context) -> None:
    """Print CLI and supported DolphinScheduler version metadata."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("version", lambda: get_version_result(env_file=env_file))


def context_command(ctx: typer.Context) -> None:
    """Print the effective config profile and stored session context."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("context", lambda: get_context_result(env_file=env_file))
