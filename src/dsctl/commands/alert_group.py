from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.alert_group import (
    UNSET,
    DescriptionUpdate,
    InstanceIdsUpdate,
    create_alert_group_result,
    delete_alert_group_result,
    get_alert_group_result,
    list_alert_groups_result,
    update_alert_group_result,
)

alert_group_app = typer.Typer(
    help="Manage DolphinScheduler alert groups.",
    no_args_is_help=True,
)


def register_alert_group_commands(app: typer.Typer) -> None:
    """Register the `alert-group` command group."""
    app.add_typer(alert_group_app, name="alert-group")


@alert_group_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter alert groups by group name using the upstream search value.",
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
    """List alert groups with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.list",
        lambda: list_alert_groups_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@alert_group_app.command("get")
def get_command(
    ctx: typer.Context,
    alert_group: Annotated[
        str,
        typer.Argument(help="Alert-group name or numeric id."),
    ],
) -> None:
    """Get one alert group by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.get",
        lambda: get_alert_group_result(alert_group, env_file=env_file),
    )


@alert_group_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Alert-group name.",
        ),
    ],
    instance_ids: Annotated[
        list[int] | None,
        typer.Option(
            "--instance-id",
            min=1,
            help="Alert plugin instance id to bind to this group. Repeat as needed.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional alert-group description.",
        ),
    ] = None,
) -> None:
    """Create one alert group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.create",
        lambda: create_alert_group_result(
            name=name,
            instance_ids=instance_ids,
            description=description,
            env_file=env_file,
        ),
    )


@alert_group_app.command("update")
def update_command(
    ctx: typer.Context,
    alert_group: Annotated[
        str,
        typer.Argument(help="Alert-group name or numeric id."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated alert-group name. Omit to keep the current name.",
        ),
    ] = None,
    instance_ids: Annotated[
        list[int] | None,
        typer.Option(
            "--instance-id",
            min=1,
            help="Alert plugin instance id to bind to this group. Repeat as needed.",
        ),
    ] = None,
    clear_instance_ids: Annotated[
        bool,
        typer.Option(
            "--clear-instance-ids",
            help="Clear all bound alert plugin instance ids.",
        ),
    ] = False,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated alert-group description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the stored alert-group description.",
        ),
    ] = False,
) -> None:
    """Update one alert group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        instance_ids_update: InstanceIdsUpdate

        if instance_ids is not None and clear_instance_ids:
            message = "--instance-id and --clear-instance-ids cannot be used together"
            raise UserInputError(message)
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(message)

        if clear_instance_ids:
            instance_ids_update = []
        elif instance_ids is not None:
            instance_ids_update = instance_ids
        else:
            instance_ids_update = UNSET

        if clear_description:
            description_update = None
        elif description is not None:
            description_update = description
        else:
            description_update = UNSET

        return update_alert_group_result(
            alert_group,
            name=name,
            description=description_update,
            instance_ids=instance_ids_update,
            env_file=env_file,
        )

    emit_result("alert-group.update", build_result)


@alert_group_app.command("delete")
def delete_command(
    ctx: typer.Context,
    alert_group: Annotated[
        str,
        typer.Argument(help="Alert-group name or numeric id."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm alert-group deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one alert group."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.delete",
        lambda: delete_alert_group_result(
            alert_group,
            force=force,
            env_file=env_file,
        ),
    )
