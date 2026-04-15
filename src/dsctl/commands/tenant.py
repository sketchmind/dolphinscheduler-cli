from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.tenant import (
    UNSET,
    DescriptionUpdate,
    create_tenant_result,
    delete_tenant_result,
    get_tenant_result,
    list_tenants_result,
    update_tenant_result,
)

tenant_app = typer.Typer(
    help="Manage DolphinScheduler tenants.",
    no_args_is_help=True,
)


def register_tenant_commands(app: typer.Typer) -> None:
    """Register the `tenant` command group."""
    app.add_typer(tenant_app, name="tenant")


@tenant_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter tenants by tenant code using the upstream search value.",
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
    """List tenants with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "tenant.list",
        lambda: list_tenants_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@tenant_app.command("get")
def get_command(
    ctx: typer.Context,
    tenant: Annotated[
        str,
        typer.Argument(help="Tenant code or numeric id."),
    ],
) -> None:
    """Get one tenant by code or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "tenant.get",
        lambda: get_tenant_result(tenant, env_file=env_file),
    )


@tenant_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    tenant_code: Annotated[
        str,
        typer.Option(
            "--tenant-code",
            help="Tenant code.",
        ),
    ],
    queue: Annotated[
        str,
        typer.Option(
            "--queue",
            help="Queue name or numeric id to bind to this tenant.",
        ),
    ],
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional tenant description.",
        ),
    ] = None,
) -> None:
    """Create one tenant."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "tenant.create",
        lambda: create_tenant_result(
            tenant_code=tenant_code,
            queue=queue,
            description=description,
            env_file=env_file,
        ),
    )


@tenant_app.command("update")
def update_command(
    ctx: typer.Context,
    tenant: Annotated[
        str,
        typer.Argument(help="Tenant code or numeric id."),
    ],
    *,
    tenant_code: Annotated[
        str | None,
        typer.Option(
            "--tenant-code",
            help="Updated tenant code. Omit to keep the current tenant code.",
        ),
    ] = None,
    queue: Annotated[
        str | None,
        typer.Option(
            "--queue",
            help="Updated queue name or numeric id. Omit to keep the current queue.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated tenant description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the stored tenant description.",
        ),
    ] = False,
) -> None:
    """Update one tenant."""
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

        return update_tenant_result(
            tenant,
            tenant_code=tenant_code,
            queue=queue,
            description=description_update,
            env_file=env_file,
        )

    emit_result("tenant.update", build_result)


@tenant_app.command("delete")
def delete_command(
    ctx: typer.Context,
    tenant: Annotated[
        str,
        typer.Argument(help="Tenant code or numeric id."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm tenant deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one tenant."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "tenant.delete",
        lambda: delete_tenant_result(
            tenant,
            force=force,
            env_file=env_file,
        ),
    )
