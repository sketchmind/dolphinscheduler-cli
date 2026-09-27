from __future__ import annotations

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.lint import (
    lint_workflow_instance_patch_result,
    lint_workflow_patch_result,
    lint_workflow_result,
)

lint_app = typer.Typer(
    help="Run local design-time checks without contacting DolphinScheduler.",
    no_args_is_help=True,
)


def register_lint_commands(app: typer.Typer) -> None:
    """Register the `lint` command group."""
    app.add_typer(lint_app, name="lint")


@lint_app.command("workflow")
@bind_command("lint.workflow")
def workflow_command(
    ctx: typer.Context,
    file: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "lint.workflow",
        lambda: lint_workflow_result(file=file, env_file=env_file),
    )


@lint_app.command("workflow-patch")
@bind_command("lint.workflow-patch")
def workflow_patch_command(
    ctx: typer.Context,
    file: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "lint.workflow-patch",
        lambda: lint_workflow_patch_result(file=file, env_file=env_file),
    )


@lint_app.command("workflow-instance-patch")
@bind_command("lint.workflow-instance-patch")
def workflow_instance_patch_command(
    ctx: typer.Context,
    file: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "lint.workflow-instance-patch",
        lambda: lint_workflow_instance_patch_result(file=file, env_file=env_file),
    )


__all__ = ["register_lint_commands"]
