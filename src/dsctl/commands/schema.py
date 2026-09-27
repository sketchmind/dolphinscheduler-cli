import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.schema import get_schema_result


def register_schema_commands(app: typer.Typer) -> None:
    """Register the top-level `schema` command."""
    app.command("schema")(schema_command)


@bind_command("schema")
def schema_command(
    ctx: typer.Context,
    *,
    group: str | None,
    command: str | None,
    list_groups: bool,
    list_commands: bool,
    full: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "schema",
        lambda: get_schema_result(
            env_file=env_file,
            group=group,
            command_action=command,
            list_groups=list_groups,
            list_commands=list_commands,
            full=full,
        ),
    )
