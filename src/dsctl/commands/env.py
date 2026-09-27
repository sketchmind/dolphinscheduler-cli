from pathlib import Path
from typing import cast

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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
@bind_command("environment.list")
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
@bind_command("environment.get")
def get_command(
    ctx: typer.Context,
    environment: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "environment.get",
        lambda: get_environment_result(environment, env_file=env_file),
    )


@env_app.command("create")
@bind_command("environment.create")
def create_command(
    ctx: typer.Context,
    *,
    name: str,
    config: str | None,
    config_file: Path | None,
    description: str | None,
    worker_groups: list[str] | None,
) -> None:
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
@bind_command("environment.update")
def update_command(
    ctx: typer.Context,
    environment: str,
    *,
    name: str | None,
    config: str | None,
    config_file: Path | None,
    description: str | None,
    clear_description: bool,
    worker_groups: list[str] | None,
    clear_worker_groups: bool,
) -> None:
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
@bind_command("environment.delete")
def delete_command(
    ctx: typer.Context,
    environment: str,
    *,
    force: bool,
) -> None:
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
                "Pass --config CONFIG or --config-file CONFIG_FILE. Run "
                "`dsctl template environment` for an example shell/export config."
            ),
        )
    return None
