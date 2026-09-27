import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.alert_group import (
    UNSET,
    DescriptionUpdate,
    GroupTypeUpdate,
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
@bind_command("alert-group.list")
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
@bind_command("alert-group.get")
def get_command(
    ctx: typer.Context,
    alert_group: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.get",
        lambda: get_alert_group_result(alert_group, env_file=env_file),
    )


@alert_group_app.command("create")
@bind_command("alert-group.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    instance_ids: list[int] | None,
    group_type: str | None,
    description: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "alert-group.create",
        lambda: create_alert_group_result(
            name=name,
            instance_ids=instance_ids,
            group_type=group_type,
            description=description,
            env_file=env_file,
        ),
    )


@alert_group_app.command("update")
@bind_command("alert-group.update")
def update_command(
    ctx: typer.Context,
    alert_group: str,
    *,
    name: str | None,
    instance_ids: list[int] | None,
    clear_instance_ids: bool,
    group_type: str | None,
    description: str | None,
    clear_description: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        instance_ids_update: InstanceIdsUpdate
        group_type_update: GroupTypeUpdate

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

        group_type_update = UNSET if group_type is None else group_type

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
            group_type=group_type_update,
            env_file=env_file,
        )

    emit_result("alert-group.update", build_result)


@alert_group_app.command("delete")
@bind_command("alert-group.delete")
def delete_command(
    ctx: typer.Context,
    alert_group: str,
    *,
    force: bool,
) -> None:
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
