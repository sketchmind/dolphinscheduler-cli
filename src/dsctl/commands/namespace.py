from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.namespace import (
    create_namespace_result,
    delete_namespace_result,
    get_namespace_result,
    list_available_namespaces_result,
    list_namespaces_result,
)

namespace_app = typer.Typer(
    help="Manage DolphinScheduler namespaces.",
    no_args_is_help=True,
)

NAMESPACE_HELP = (
    "Namespace name or numeric id. Run `dsctl namespace list` to discover values."
)


def register_namespace_commands(app: typer.Typer) -> None:
    """Register the `namespace` command group."""
    app.add_typer(namespace_app, name="namespace")


@namespace_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter namespaces by namespace name using the upstream search value.",
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
    """List namespaces with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.list",
        lambda: list_namespaces_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@namespace_app.command("get")
def get_command(
    ctx: typer.Context,
    namespace: Annotated[
        str,
        typer.Argument(help=NAMESPACE_HELP),
    ],
) -> None:
    """Get one namespace by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.get",
        lambda: get_namespace_result(namespace, env_file=env_file),
    )


@namespace_app.command("available")
def available_command(ctx: typer.Context) -> None:
    """List namespaces available to the current login user."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.available",
        lambda: list_available_namespaces_result(env_file=env_file),
    )


@namespace_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    namespace: Annotated[
        str,
        typer.Option(
            "--namespace",
            help="Namespace name.",
        ),
    ],
    cluster_code: Annotated[
        int,
        typer.Option(
            "--cluster-code",
            min=1,
            help="Owning cluster code. Run `dsctl cluster list` to discover codes.",
        ),
    ],
) -> None:
    """Create one namespace."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.create",
        lambda: create_namespace_result(
            namespace=namespace,
            cluster_code=cluster_code,
            env_file=env_file,
        ),
    )


@namespace_app.command("delete")
def delete_command(
    ctx: typer.Context,
    namespace: Annotated[
        str,
        typer.Argument(help=NAMESPACE_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm namespace deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one namespace."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.delete",
        lambda: delete_namespace_result(
            namespace,
            force=force,
            env_file=env_file,
        ),
    )
