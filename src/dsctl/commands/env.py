from pathlib import Path
from typing import Annotated, cast

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
    """Register the `environment` command group."""
    app.add_typer(env_app, name="environment")


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
        "environment.list",
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
        typer.Argument(
            help="Environment name or numeric code. Use list to discover values."
        ),
    ],
) -> None:
    """Get one environment by name or code."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "environment.get",
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
        str | None,
        typer.Option(
            "--config",
            help=(
                "Inline DS environment shell/export config. Prefer "
                "--config-file for multiline configs; run "
                "`dsctl template environment` "
                "for an example."
            ),
        ),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config-file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to a DS environment shell/export config file. Run "
                "`dsctl template environment` for an example."
            ),
            readable=True,
            resolve_path=True,
        ),
    ] = None,
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
    """Create one environment; pass --config or --config-file."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "environment.create",
        lambda: create_environment_result(
            name=name,
            config=cast(
                "str",
                _environment_config_from_options(
                    config=config,
                    config_file=config_file,
                    required=True,
                ),
            ),
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
        typer.Argument(
            help="Environment name or numeric code. Use list to discover values."
        ),
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
            help=(
                "Updated inline DS environment shell/export config. Omit to "
                "keep the current config; prefer --config-file for multiline "
                "configs."
            ),
        ),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config-file",
            dir_okay=False,
            exists=True,
            file_okay=True,
            help=(
                "Path to an updated DS environment shell/export config file. "
                "Omit both config options to keep the current config."
            ),
            readable=True,
            resolve_path=True,
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
    """Update one environment; config may come from --config-file."""
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
            config=_environment_config_from_options(
                config=config,
                config_file=config_file,
                required=False,
            ),
            description=description_update,
            worker_groups=worker_groups_update,
            env_file=env_file,
        )

    emit_result("environment.update", build_result)


@env_app.command("delete")
def delete_command(
    ctx: typer.Context,
    environment: Annotated[
        str,
        typer.Argument(
            help="Environment name or numeric code. Use list to discover values."
        ),
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
        "environment.delete",
        lambda: delete_environment_result(
            environment,
            force=force,
            env_file=env_file,
        ),
    )


def _environment_config_from_options(
    *,
    config: str | None,
    config_file: Path | None,
    required: bool,
) -> str | None:
    if config is not None and config_file is not None:
        message = "--config and --config-file are mutually exclusive"
        raise UserInputError(
            message,
            suggestion=(
                "Pass inline config with --config or read it from --config-file."
            ),
        )
    if config_file is not None:
        return config_file.read_text(encoding="utf-8")
    if config is not None:
        return config
    if required:
        message = "Environment config is required"
        raise UserInputError(
            message,
            suggestion=(
                "Pass --config TEXT or --config-file PATH. Run "
                "`dsctl template environment` for an example shell/export config."
            ),
        )
    return None
