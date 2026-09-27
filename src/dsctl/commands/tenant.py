import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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
@bind_command("tenant.list")
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
@bind_command("tenant.get")
def get_command(
    ctx: typer.Context,
    tenant: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "tenant.get",
        lambda: get_tenant_result(tenant, env_file=env_file),
    )


@tenant_app.command("create")
@bind_command("tenant.create")
def create_command(
    ctx: typer.Context,
    *,
    tenant_code: str,
    queue: str,
    description: str | None,
) -> None:
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
@bind_command("tenant.update")
def update_command(
    ctx: typer.Context,
    tenant: str,
    *,
    tenant_code: str | None,
    queue: str | None,
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

        return update_tenant_result(
            tenant,
            tenant_code=tenant_code,
            queue=queue,
            description=description_update,
            env_file=env_file,
        )

    emit_result("tenant.update", build_result)


@tenant_app.command("delete")
@bind_command("tenant.delete")
def delete_command(
    ctx: typer.Context,
    tenant: str,
    *,
    force: bool,
) -> None:
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
