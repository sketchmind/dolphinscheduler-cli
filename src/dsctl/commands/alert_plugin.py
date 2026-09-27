from pathlib import Path

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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
@bind_command("alert-plugin.list")
def list_command(
    ctx: typer.Context,
    *,
    search: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
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
@bind_command("alert-plugin.get")
def get_command(
    ctx: typer.Context,
    alert_plugin: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.get",
        lambda: get_alert_plugin_result(alert_plugin, env_file=env_file),
    )


@alert_plugin_app.command("schema")
@bind_command("alert-plugin.schema")
def schema_command(
    ctx: typer.Context,
    plugin: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.schema",
        lambda: get_alert_plugin_schema_result(plugin, env_file=env_file),
    )


@alert_plugin_definition_app.command("list")
@bind_command("alert-plugin.definition.list")
def list_definition_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.definition.list",
        lambda: list_alert_plugin_definitions_result(env_file=env_file),
    )


@alert_plugin_app.command("create")
@bind_command("alert-plugin.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    plugin: str,
    params_json: str | None,
    file: Path | None,
    params: list[str] | None,
) -> None:
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
@bind_command("alert-plugin.update")
def update_command(
    ctx: typer.Context,
    alert_plugin: str,
    *,
    name: str | None,
    params_json: str | None,
    file: Path | None,
    params: list[str] | None,
) -> None:
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
@bind_command("alert-plugin.delete")
def delete_command(
    ctx: typer.Context,
    alert_plugin: str,
    *,
    force: bool,
) -> None:
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
@bind_command("alert-plugin.test")
def test_command(
    ctx: typer.Context,
    alert_plugin: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-plugin.test",
        lambda: send_test_alert_plugin_result(
            alert_plugin,
            env_file=env_file,
        ),
    )
