import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.project import (
    UNSET,
    DescriptionUpdate,
    create_project_result,
    delete_project_result,
    get_project_result,
    list_projects_result,
    update_project_result,
)

project_app = typer.Typer(
    help="Manage DolphinScheduler projects.",
    no_args_is_help=True,
)


def register_project_commands(app: typer.Typer) -> None:
    """Register the `project` command group."""
    app.add_typer(project_app, name="project")


@project_app.command("list")
@bind_command("project.list")
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
        "project.list",
        lambda: list_projects_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@project_app.command("get")
@bind_command("project.get")
def get_command(
    ctx: typer.Context,
    project: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project.get",
        lambda: get_project_result(project, env_file=env_file),
    )


@project_app.command("create")
@bind_command("project.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    description: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project.create",
        lambda: create_project_result(
            name=name,
            description=description,
            env_file=env_file,
        ),
    )


@project_app.command("update")
@bind_command("project.update")
def update_command(
    ctx: typer.Context,
    project: str,
    *,
    name: str | None,
    description: str | None,
    clear_description: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(
                message,
                suggestion=(
                    "Use either --description VALUE or --clear-description, not both."
                ),
            )
        if clear_description:
            description_update = None
        elif description is not None:
            description_update = description
        else:
            description_update = UNSET
        return update_project_result(
            project,
            name=name,
            description=description_update,
            env_file=env_file,
        )

    emit_result(
        "project.update",
        build_result,
    )


@project_app.command("delete")
@bind_command("project.delete")
def delete_command(
    ctx: typer.Context,
    project: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project.delete",
        lambda: delete_project_result(
            project,
            force=force,
            env_file=env_file,
        ),
    )
