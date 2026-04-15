from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter clusters by name using the upstream search value.",
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
    """List clusters with optional filtering and pagination controls."""
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
def get_command(
    ctx: typer.Context,
    cluster: Annotated[
        str,
        typer.Argument(help="Cluster name or numeric code."),
    ],
) -> None:
    """Get one cluster by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "cluster.get",
        lambda: get_cluster_result(cluster, env_file=env_file),
    )


@cluster_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Cluster name.",
        ),
    ],
    config: Annotated[
        str,
        typer.Option(
            "--config",
            help="Cluster config payload.",
        ),
    ],
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional cluster description.",
        ),
    ] = None,
) -> None:
    """Create one cluster."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "cluster.create",
        lambda: create_cluster_result(
            name=name,
            config=config,
            description=description,
            env_file=env_file,
        ),
    )


@cluster_app.command("update")
def update_command(
    ctx: typer.Context,
    cluster: Annotated[
        str,
        typer.Argument(help="Cluster name or numeric code."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated cluster name. Omit to keep the current name.",
        ),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config",
            help="Updated cluster config. Omit to keep the current config.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated cluster description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the stored cluster description.",
        ),
    ] = False,
) -> None:
    """Update one cluster."""
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
            config=config,
            description=description_update,
            env_file=env_file,
        )

    emit_result("cluster.update", build_result)


@cluster_app.command("delete")
def delete_command(
    ctx: typer.Context,
    cluster: Annotated[
        str,
        typer.Argument(help="Cluster name or numeric code."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm cluster deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one cluster."""
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
