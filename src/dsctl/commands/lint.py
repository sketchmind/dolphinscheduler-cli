from __future__ import annotations

from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result
from dsctl.services.lint import lint_workflow_result

lint_app = typer.Typer(
    help="Run local design-time checks without contacting DolphinScheduler.",
    no_args_is_help=True,
)


def register_lint_commands(app: typer.Typer) -> None:
    """Register the `lint` command group."""
    app.add_typer(lint_app, name="lint")


@lint_app.command("workflow")
def workflow_command(
    file: Annotated[
        str,
        typer.Argument(
            dir_okay=False,
            exists=True,
            file_okay=True,
            help="Workflow YAML file to lint.",
            readable=True,
            resolve_path=True,
        ),
    ],
) -> None:
    """Lint one workflow YAML file using the local spec and compile pipeline."""
    emit_result("lint.workflow", lambda: lint_workflow_result(file=file))


__all__ = ["register_lint_commands"]
