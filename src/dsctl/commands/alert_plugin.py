from pathlib import Path
from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.alert_plugin import (
    create_alert_plugin_result,
    delete_alert_plugin_result,
    get_alert_plugin_result,
    get_alert_plugin_schema_result,
    list_alert_plugin_definitions_result,
    list_alert_plugins_result,
    send_test_alert_plugin_result,
    update_alert_plugin_result,
)

alert_plugin_app = typer.Typer(
    help="Manage DolphinScheduler alert plugin instances.",
    no_args_is_help=True,
)
alert_plugin_definition_app = typer.Typer(
    help="Discover supported DolphinScheduler alert plugin definitions.",
    no_args_is_help=True,
)


def register_alert_plugin_commands(app: typer.Typer) -> None:
    """Register the `alert-plugin` command group."""
    alert_plugin_app.add_typer(alert_plugin_definition_app, name="definition")
    app.add_typer(alert_plugin_app, name="alert-plugin")


@alert_plugin_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter alert-plugin instances by instance name.",
        ),
    ] = None,
    page_no: Annotated[
        int,
        typer.Option(
            "--page-no",
            min=1,
            help="Page number to fetch when not using --all.",
        ),
    ] = 1,
    page_size: Annotated[
        int,
        typer.Option(
            "--page-size",
            min=1,
            help="Page size to request from the upstream API.",
        ),
    ] = 100,
    all_pages: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Fetch all remaining pages up to the safety limit.",
        ),
    ] = False,
) -> None:
    """List alert-plugin instances with optional filtering and pagination."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.list",
        lambda: list_alert_plugins_result(
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@alert_plugin_app.command("get")
def get_command(
    ctx: typer.Context,
    alert_plugin: Annotated[
        str,
        typer.Argument(help="Alert-plugin instance name or numeric id."),
    ],
) -> None:
    """Get one alert-plugin instance by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.get",
        lambda: get_alert_plugin_result(alert_plugin, env_file=env_file),
    )


@alert_plugin_app.command("schema")
def schema_command(
    ctx: typer.Context,
    plugin: Annotated[
        str,
        typer.Argument(help="Alert UI plugin definition name or numeric id."),
    ],
) -> None:
    """Get one alert-plugin definition schema by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.schema",
        lambda: get_alert_plugin_schema_result(plugin, env_file=env_file),
    )


@alert_plugin_definition_app.command("list")
def list_definition_command(ctx: typer.Context) -> None:
    """List supported alert-plugin definitions, not configured instances."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.definition.list",
        lambda: list_alert_plugin_definitions_result(env_file=env_file),
    )


@alert_plugin_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Alert-plugin instance name.",
        ),
    ],
    plugin: Annotated[
        str,
        typer.Option(
            "--plugin",
            help="Alert UI plugin definition name or numeric id.",
        ),
    ],
    params_json: Annotated[
        str | None,
        typer.Option(
            "--params-json",
            help="DS-native alert-plugin UI params JSON array.",
        ),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help="Path to one DS-native alert-plugin UI params JSON file.",
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            help=(
                "Alert-plugin UI param in KEY=VALUE form. Repeat for multiple fields."
            ),
        ),
    ] = None,
) -> None:
    """Create one alert-plugin instance."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.create",
        lambda: create_alert_plugin_result(
            name=name,
            plugin=plugin,
            params_json=params_json,
            file=file,
            params=params,
            env_file=env_file,
        ),
    )


@alert_plugin_app.command("update")
def update_command(
    ctx: typer.Context,
    alert_plugin: Annotated[
        str,
        typer.Argument(help="Alert-plugin instance name or numeric id."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated alert-plugin instance name.",
        ),
    ] = None,
    params_json: Annotated[
        str | None,
        typer.Option(
            "--params-json",
            help="Replacement DS-native alert-plugin UI params JSON array.",
        ),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            "--file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help="Path to one replacement DS-native alert-plugin UI params JSON file.",
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    params: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            help=(
                "Replacement alert-plugin UI param in KEY=VALUE form. Repeat "
                "for multiple fields; omitted fields keep current values."
            ),
        ),
    ] = None,
) -> None:
    """Update one alert-plugin instance."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.update",
        lambda: update_alert_plugin_result(
            alert_plugin,
            name=name,
            params_json=params_json,
            file=file,
            params=params,
            env_file=env_file,
        ),
    )


@alert_plugin_app.command("delete")
def delete_command(
    ctx: typer.Context,
    alert_plugin: Annotated[
        str,
        typer.Argument(help="Alert-plugin instance name or numeric id."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm alert-plugin deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one alert-plugin instance."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.delete",
        lambda: delete_alert_plugin_result(
            alert_plugin,
            force=force,
            env_file=env_file,
        ),
    )


@alert_plugin_app.command("test")
def test_command(
    ctx: typer.Context,
    alert_plugin: Annotated[
        str,
        typer.Argument(help="Alert-plugin instance name or numeric id."),
    ],
) -> None:
    """Send one test alert using one existing alert-plugin instance."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.test",
        lambda: send_test_alert_plugin_result(
            alert_plugin,
            env_file=env_file,
        ),
    )
