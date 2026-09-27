from pathlib import Path
from typing import cast

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.cluster import (
    UNSET,
    DescriptionUpdate,
    create_cluster_result,
    delete_cluster_result,
    get_cluster_result,
    list_clusters_result,
    update_cluster_result,
)

cluster_app = typer.Typer(
    help="Manage DolphinScheduler clusters.",
    no_args_is_help=True,
)


def register_cluster_commands(app: typer.Typer) -> None:
    """Register the `cluster` command group."""
    app.add_typer(cluster_app, name="cluster")


@cluster_app.command("list")
@bind_command("cluster.list")
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
        "cluster.list",
        lambda: list_clusters_result(
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@cluster_app.command("get")
@bind_command("cluster.get")
def get_command(
    ctx: typer.Context,
    cluster: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "cluster.get",
        lambda: get_cluster_result(cluster, env_file=env_file),
    )


@cluster_app.command("create")
@bind_command("cluster.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    config: str | None,
    config_file: Path | None,
    description: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "cluster.create",
        lambda: create_cluster_result(
            name=name,
            config=cast(
                "str",
                _cluster_config_from_options(
                    config=config,
                    config_file=config_file,
                    required=True,
                ),
            ),
            description=description,
            env_file=env_file,
        ),
    )


@cluster_app.command("update")
@bind_command("cluster.update")
def update_command(
    ctx: typer.Context,
    cluster: str,
    *,
    name: str | None,
    config: str | None,
    config_file: Path | None,
    description: str | None,
    clear_description: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(message)
        if clear_description:
            description_update = None
        elif description is not None:
            description_update = description
        else:
            description_update = UNSET
        return update_cluster_result(
            cluster,
            name=name,
            config=_cluster_config_from_options(
                config=config,
                config_file=config_file,
                required=False,
            ),
            description=description_update,
            env_file=env_file,
        )

    emit_result("cluster.update", build_result)


@cluster_app.command("delete")
@bind_command("cluster.delete")
def delete_command(
    ctx: typer.Context,
    cluster: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "cluster.delete",
        lambda: delete_cluster_result(
            cluster,
            force=force,
            env_file=env_file,
        ),
    )


def _cluster_config_from_options(
    *,
    config: str | None,
    config_file: Path | None,
    required: bool,
) -> str | None:
    if config is not None and config_file is not None:
        message = "--config and --config-file are mutually exclusive"
        raise UserInputError(
            message,
            suggestion=(
                "Pass inline config with --config or read it from --config-file."
            ),
        )
    if config_file is not None:
        return config_file.read_text(encoding="utf-8")
    if config is not None:
        return config
    if required:
        message = "Cluster config is required"
        raise UserInputError(
            message,
            suggestion=(
                "Pass --config CONFIG or --config-file CONFIG_FILE. Run "
                "`dsctl template cluster` for an example JSON config."
            ),
        )
    return None
