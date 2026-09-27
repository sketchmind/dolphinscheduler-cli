import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.monitor import (
    get_database_result,
    get_health_result,
    list_servers_result,
)

monitor_app = typer.Typer(
    help="Inspect DolphinScheduler platform health, server state, and DB state.",
    no_args_is_help=True,
)


def register_monitor_commands(app: typer.Typer) -> None:
    """Register the `monitor` command group."""
    app.add_typer(monitor_app, name="monitor")


@monitor_app.command("health")
@bind_command("monitor.health")
def health_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.health",
        lambda: get_health_result(env_file=env_file),
    )


@monitor_app.command("server")
@bind_command("monitor.server")
def server_command(
    ctx: typer.Context,
    node_type: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.server",
        lambda: list_servers_result(node_type, env_file=env_file),
    )


@monitor_app.command("database")
@bind_command("monitor.database")
def database_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.database",
        lambda: get_database_result(env_file=env_file),
    )
