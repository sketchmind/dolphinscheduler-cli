import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.worker_group import (
    UNSET,
    AddressesUpdate,
    DescriptionUpdate,
    create_worker_group_result,
    delete_worker_group_result,
    get_worker_group_result,
    list_worker_groups_result,
    update_worker_group_result,
)

worker_group_app = typer.Typer(
    help="Manage DolphinScheduler worker groups.",
    no_args_is_help=True,
)


def register_worker_group_commands(app: typer.Typer) -> None:
    """Register the `worker-group` command group."""
    app.add_typer(worker_group_app, name="worker-group")


@worker_group_app.command("list")
@bind_command("worker-group.list")
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
        "worker-group.list",
        lambda: list_worker_groups_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@worker_group_app.command("get")
@bind_command("worker-group.get")
def get_command(
    ctx: typer.Context,
    worker_group: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "worker-group.get",
        lambda: get_worker_group_result(worker_group, env_file=env_file),
    )


@worker_group_app.command("create")
@bind_command("worker-group.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    addresses: list[str] | None,
    description: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "worker-group.create",
        lambda: create_worker_group_result(
            name=name,
            addresses=addresses,
            description=description,
            env_file=env_file,
        ),
    )


@worker_group_app.command("update")
@bind_command("worker-group.update")
def update_command(
    ctx: typer.Context,
    worker_group: str,
    *,
    name: str | None,
    addresses: list[str] | None,
    clear_addrs: bool,
    description: str | None,
    clear_description: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        addresses_update: AddressesUpdate
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(message)
        if addresses is not None and clear_addrs:
            message = "--addr and --clear-addrs cannot be used together"
            raise UserInputError(message)

        if clear_description:
            description_update = None
        elif description is not None:
            description_update = description
        else:
            description_update = UNSET

        if clear_addrs:
            addresses_update = []
        elif addresses is not None:
            addresses_update = addresses
        else:
            addresses_update = UNSET

        return update_worker_group_result(
            worker_group,
            name=name,
            addresses=addresses_update,
            description=description_update,
            env_file=env_file,
        )

    emit_result("worker-group.update", build_result)


@worker_group_app.command("delete")
@bind_command("worker-group.delete")
def delete_command(
    ctx: typer.Context,
    worker_group: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "worker-group.delete",
        lambda: delete_worker_group_result(
            worker_group,
            force=force,
            env_file=env_file,
        ),
    )
