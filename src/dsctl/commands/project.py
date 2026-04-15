from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter projects by name using the upstream search value.",
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
    """List projects with optional filtering and pagination controls."""
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
def get_command(
    ctx: typer.Context,
    project: Annotated[
        str,
        typer.Argument(help="Project name or numeric code."),
    ],
) -> None:
    """Get one project by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project.get",
        lambda: get_project_result(project, env_file=env_file),
    )


@project_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Project name.",
        ),
    ],
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional project description.",
        ),
    ] = None,
) -> None:
    """Create a project."""
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
def update_command(
    ctx: typer.Context,
    project: Annotated[
        str,
        typer.Argument(help="Project name or numeric code."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated project name. Omit to keep the current name.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated project description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the stored project description.",
        ),
    ] = False,
) -> None:
    """Update a project."""
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
def delete_command(
    ctx: typer.Context,
    project: Annotated[
        str,
        typer.Argument(help="Project name or numeric code."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm project deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete a project."""
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
