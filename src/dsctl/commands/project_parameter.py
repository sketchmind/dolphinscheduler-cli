import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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


def register_project_parameter_commands(app: typer.Typer) -> None:
    """Register the `project-parameter` command group."""
    app.add_typer(project_parameter_app, name="project-parameter")


@project_parameter_app.command("list")
@bind_command("project-parameter.list")
def list_command(
    ctx: typer.Context,
    *,
    project: str | None,
    search: str | None,
    data_type: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
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
@bind_command("project-parameter.get")
def get_command(
    ctx: typer.Context,
    project_parameter: str,
    *,
    project: str | None,
) -> None:
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
@bind_command("project-parameter.create")
def create_command(
    ctx: typer.Context,
    *,
    project: str | None,
    name: str,
    value: str,
    data_type: str,
) -> None:
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
@bind_command("project-parameter.update")
def update_command(
    ctx: typer.Context,
    project_parameter: str,
    *,
    project: str | None,
    name: str | None,
    value: str | None,
    data_type: str | None,
) -> None:
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
@bind_command("project-parameter.delete")
def delete_command(
    ctx: typer.Context,
    project_parameter: str,
    *,
    project: str | None,
    force: bool,
) -> None:
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
