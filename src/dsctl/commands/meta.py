import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.meta import get_version_result


def register_meta_commands(app: typer.Typer) -> None:
    """Register top-level metadata commands on the root CLI app."""
    app.command("version")(version_command)


@bind_command("version")
def version_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("version", lambda: get_version_result(env_file=env_file))
