from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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
def health_command(ctx: typer.Context) -> None:
    """Get the API server actuator health payload."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.health",
        lambda: get_health_result(env_file=env_file),
    )


@monitor_app.command("server")
def server_command(
    ctx: typer.Context,
    node_type: Annotated[
        str,
        typer.Argument(
            help="Server node type: master, worker, or alert-server.",
            metavar="TYPE",
        ),
    ],
) -> None:
    """List registry-backed servers for one node type."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.server",
        lambda: list_servers_result(node_type, env_file=env_file),
    )


@monitor_app.command("database")
def database_command(ctx: typer.Context) -> None:
    """List database health metrics reported by the monitor API."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "monitor.database",
        lambda: get_database_result(env_file=env_file),
    )
