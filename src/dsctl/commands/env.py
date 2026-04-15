from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.env import (
    UNSET,
    DescriptionUpdate,
    WorkerGroupsUpdate,
    create_environment_result,
    delete_environment_result,
    get_environment_result,
    list_environments_result,
    update_environment_result,
)

env_app = typer.Typer(
    help="Manage DolphinScheduler environments.",
    no_args_is_help=True,
)


def register_env_commands(app: typer.Typer) -> None:
    """Register the `env` command group."""
    app.add_typer(env_app, name="env")


@env_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter environments by name using the upstream search value.",
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
    """List environments with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "env.list",
        lambda: list_environments_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@env_app.command("get")
def get_command(
    ctx: typer.Context,
    environment: Annotated[
        str,
        typer.Argument(help="Environment name or numeric code."),
    ],
) -> None:
    """Get one environment by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "env.get",
        lambda: get_environment_result(environment, env_file=env_file),
    )


@env_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    name: Annotated[
        str,
        typer.Option(
            "--name",
            help="Environment name.",
        ),
    ],
    config: Annotated[
        str,
        typer.Option(
            "--config",
            help="Environment config payload.",
        ),
    ],
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Optional environment description.",
        ),
    ] = None,
    worker_groups: Annotated[
        list[str] | None,
        typer.Option(
            "--worker-group",
            help="Worker group to bind to this environment. Repeat as needed.",
        ),
    ] = None,
) -> None:
    """Create one environment."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "env.create",
        lambda: create_environment_result(
            name=name,
            config=config,
            description=description,
            worker_groups=worker_groups,
            env_file=env_file,
        ),
    )


@env_app.command("update")
def update_command(
    ctx: typer.Context,
    environment: Annotated[
        str,
        typer.Argument(help="Environment name or numeric code."),
    ],
    *,
    name: Annotated[
        str | None,
        typer.Option(
            "--name",
            help="Updated environment name. Omit to keep the current name.",
        ),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option(
            "--config",
            help="Updated environment config. Omit to keep the current config.",
        ),
    ] = None,
    description: Annotated[
        str | None,
        typer.Option(
            "--description",
            help="Updated environment description.",
        ),
    ] = None,
    clear_description: Annotated[
        bool,
        typer.Option(
            "--clear-description",
            help="Clear the stored environment description.",
        ),
    ] = False,
    worker_groups: Annotated[
        list[str] | None,
        typer.Option(
            "--worker-group",
            help="Worker group to bind to this environment. Repeat as needed.",
        ),
    ] = None,
    clear_worker_groups: Annotated[
        bool,
        typer.Option(
            "--clear-worker-groups",
            help="Clear all bound worker groups.",
        ),
    ] = False,
) -> None:
    """Update one environment."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)

    def build_result() -> CommandResult:
        description_update: DescriptionUpdate
        worker_groups_update: WorkerGroupsUpdate
        if description is not None and clear_description:
            message = "--description and --clear-description cannot be used together"
            raise UserInputError(message)
        if worker_groups is not None and clear_worker_groups:
            message = "--worker-group and --clear-worker-groups cannot be used together"
            raise UserInputError(message)

        if clear_description:
            description_update = None
        elif description is not None:
            description_update = description
        else:
            description_update = UNSET

        if clear_worker_groups:
            worker_groups_update = []
        elif worker_groups is not None:
            worker_groups_update = worker_groups
        else:
            worker_groups_update = UNSET

        return update_environment_result(
            environment,
            name=name,
            config=config,
            description=description_update,
            worker_groups=worker_groups_update,
            env_file=env_file,
        )

    emit_result("env.update", build_result)


@env_app.command("delete")
def delete_command(
    ctx: typer.Context,
    environment: Annotated[
        str,
        typer.Argument(help="Environment name or numeric code."),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm environment deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one environment."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "env.delete",
        lambda: delete_environment_result(
            environment,
            force=force,
            env_file=env_file,
        ),
    )
