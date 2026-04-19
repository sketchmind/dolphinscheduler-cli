from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.output import CommandResult
from dsctl.services.project_parameter import (
    UNSET,
    ValueUpdate,
    create_project_parameter_result,
    delete_project_parameter_result,
    get_project_parameter_result,
    list_project_parameters_result,
    update_project_parameter_result,
)

project_parameter_app = typer.Typer(
    help="Manage DolphinScheduler project parameters.",
    no_args_is_help=True,
)

PROJECT_PARAMETER_HELP = (
    "Project parameter name or numeric code. Run `dsctl project-parameter list` "
    "in the selected project to discover values."
)


def register_project_parameter_commands(app: typer.Typer) -> None:
    """Register the `project-parameter` command group."""
    app.add_typer(project_parameter_app, name="project-parameter")


@project_parameter_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter project parameters by name using the upstream search value.",
        ),
    ] = None,
    data_type: Annotated[
        str | None,
        typer.Option(
            "--data-type",
            help=(
                "Filter by DS projectParameterDataType. Run `dsctl enum list "
                "data-type` to discover values."
            ),
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
    """List project parameters inside one selected project."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-parameter.list",
        lambda: list_project_parameters_result(
            project=project,
            search=search,
            data_type=data_type,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@project_parameter_app.command("get")
def get_command(
    ctx: typer.Context,
    project_parameter: Annotated[
        str,
        typer.Argument(help=PROJECT_PARAMETER_HELP),
    ],
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
) -> None:
    """Get one project parameter by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-parameter.get",
        lambda: get_project_parameter_result(
            project_parameter,
            project=project,
            env_file=env_file,
        ),
    )


@project_parameter_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Project parameter name.",
        ),
    ],
    value: Annotated[
        str,
        typer.Option(
            "--value",
            help="Project parameter value.",
        ),
    ],
    data_type: Annotated[
        str,
        typer.Option(
            "--data-type",
            help=(
                "DS projectParameterDataType value. Run `dsctl enum list "
                "data-type` to discover values."
            ),
        ),
    ] = "VARCHAR",
) -> None:
    """Create one project parameter."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-parameter.create",
        lambda: create_project_parameter_result(
            project=project,
            name=name,
            value=value,
            data_type=data_type,
            env_file=env_file,
        ),
    )


@project_parameter_app.command("update")
def update_command(
    ctx: typer.Context,
    project_parameter: Annotated[
        str,
        typer.Argument(help=PROJECT_PARAMETER_HELP),
    ],
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated parameter name. Omit to keep the current name.",
        ),
    ] = None,
    value: Annotated[
        str | None,
        typer.Option(
            "--value",
            help="Updated parameter value. Omit to keep the current value.",
        ),
    ] = None,
    data_type: Annotated[
        str | None,
        typer.Option(
            "--data-type",
            help=(
                "Updated DS projectParameterDataType value. Run `dsctl enum "
                "list data-type` to discover values."
            ),
        ),
    ] = None,
) -> None:
    """Update one project parameter."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        value_update: ValueUpdate = UNSET if value is None else value
        return update_project_parameter_result(
            project_parameter,
            project=project,
            name=name,
            value=value_update,
            data_type=data_type,
            env_file=env_file,
        )

    emit_result("project-parameter.update", build_result)


@project_parameter_app.command("delete")
def delete_command(
    ctx: typer.Context,
    project_parameter: Annotated[
        str,
        typer.Argument(help=PROJECT_PARAMETER_HELP),
    ],
    *,
    project: Annotated[
        str | None,
        typer.Option(
            "--project",
            help=(
                "Project name or code. Run `dsctl project list` to discover "
                "values; falls back to stored project context."
            ),
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm project parameter deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one project parameter."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "project-parameter.delete",
        lambda: delete_project_parameter_result(
            project_parameter,
            project=project,
            force=force,
            env_file=env_file,
        ),
    )
