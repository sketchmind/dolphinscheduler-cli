from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result
from dsctl.services.template import (
    parameter_syntax_result,
    supported_parameter_syntax_topics,
    task_template_result,
    task_template_types_result,
    workflow_template_result,
)

template_app = typer.Typer(
    help="Emit stable YAML templates for workflow authoring.",
    no_args_is_help=True,
)


def register_template_commands(app: typer.Typer) -> None:
    """Register the `template` command group."""
    app.add_typer(template_app, name="template")


@template_app.command("workflow")
def workflow_command(
    with_schedule: Annotated[
        bool | None,
        typer.Option(
            "--with-schedule",
            help="Include one optional schedule block in the emitted template.",
        ),
    ] = None,
) -> None:
    """Emit the stable workflow YAML template."""
    emit_result(
        "template.workflow",
        lambda: workflow_template_result(with_schedule=bool(with_schedule)),
    )


@template_app.command("params")
def params_command(
    topic: Annotated[
        str | None,
        typer.Option(
            "--topic",
            help=(
                "Parameter syntax topic. Run without --topic for compact "
                f"discovery. Supported: "
                f"{', '.join(supported_parameter_syntax_topics())}."
            ),
        ),
    ] = None,
) -> None:
    """Emit stable DS parameter syntax metadata and examples."""
    emit_result("template.params", lambda: parameter_syntax_result(topic=topic))


@template_app.command("task")
def task_command(
    task_type: Annotated[
        str | None,
        typer.Argument(
            help="Task type to template, for example SHELL, PYTHON, SQL, or HTTP.",
        ),
    ] = None,
    list_types: Annotated[
        bool | None,
        typer.Option(
            "--list",
            help="List supported stable task template types instead of emitting YAML.",
        ),
    ] = None,
    variant: Annotated[
        str | None,
        typer.Option(
            "--variant",
            help=(
                "Task template scenario, for example minimal, resource, "
                "post-json, or branching."
            ),
        ),
    ] = None,
) -> None:
    """Emit one task YAML template."""
    if list_types:
        emit_result("template.task_types", task_template_types_result)
        return
    emit_result(
        "template.task",
        lambda: task_template_result(task_type or "", variant=variant),
    )
