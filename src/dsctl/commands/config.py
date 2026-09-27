import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.configuration import (
    get_config_result,
    set_config_result,
    unset_config_result,
)

config_app = typer.Typer(help="Manage saved user configuration.", no_args_is_help=True)


def register_config_commands(app: typer.Typer) -> None:
    """Register the user configuration commands."""
    app.add_typer(config_app, name="config")


@config_app.command("get")
@bind_command("config.get")
def get_command(key: str) -> None:
    emit_result("config.get", lambda: get_config_result(key))


@config_app.command("set")
@bind_command("config.set")
def set_command(ctx: typer.Context, key: str, value: str) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("config.set", lambda: set_config_result(key, value, env_file=env_file))


@config_app.command("unset")
@bind_command("config.unset")
def unset_command(ctx: typer.Context, key: str) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result("config.unset", lambda: unset_config_result(key, env_file=env_file))
