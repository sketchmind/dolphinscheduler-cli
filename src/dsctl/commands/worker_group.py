from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help=("Filter UI worker groups by name using the upstream search value."),
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
    """List worker groups with optional filtering and pagination controls."""
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
def get_command(
    ctx: typer.Context,
    worker_group: Annotated[
        str,
        typer.Argument(help="Worker-group name or numeric id."),
    ],
) -> None:
    """Get one worker group by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "worker-group.get",
        lambda: get_worker_group_result(worker_group, env_file=env_file),
    )


@worker_group_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Worker-group name.",
        ),
    ],
    addresses: Annotated[
        list[str] | None,
        typer.Option(
            "--addr",
            help="Worker address to include in addrList. Repeat as needed.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional worker-group description.",
        ),
    ] = None,
) -> None:
    """Create one worker group."""
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
def update_command(
    ctx: typer.Context,
    worker_group: Annotated[
        str,
        typer.Argument(help="Worker-group name or numeric id."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated worker-group name. Omit to keep the current name.",
        ),
    ] = None,
    addresses: Annotated[
        list[str] | None,
        typer.Option(
            "--addr",
            help=(
                "Replacement worker address list. Repeat as needed. Omit to keep "
                "the current addrList."
            ),
        ),
    ] = None,
    clear_addrs: Annotated[
        bool,
        typer.Option(
            "--clear-addrs",
            help="Clear the current addrList.",
        ),
    ] = False,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated worker-group description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the current worker-group description.",
        ),
    ] = False,
) -> None:
    """Update one worker group."""
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
def delete_command(
    ctx: typer.Context,
    worker_group: Annotated[
        str,
        typer.Argument(help="Worker-group name or numeric id."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm worker-group deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one worker group."""
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
