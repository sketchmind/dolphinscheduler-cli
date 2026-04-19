from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result
from dsctl.services.template import (
    cluster_config_template_result,
    datasource_template_result,
    environment_config_template_result,
    parameter_syntax_result,
    supported_datasource_types,
    supported_parameter_syntax_topics,
    task_template_result,
    task_template_types_result,
    workflow_template_result,
)

template_app = typer.Typer(
    help="Emit stable templates for workflow authoring and DS-native payloads.",
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


@template_app.command("environment")
def environment_command() -> None:
    """Emit a DS environment shell/export config template."""
    emit_result("template.environment", environment_config_template_result)


@template_app.command("cluster")
def cluster_command() -> None:
    """Emit a DS cluster config JSON template."""
    emit_result("template.cluster", cluster_config_template_result)


@template_app.command("datasource")
def datasource_command(
    datasource_type: Annotated[
        str | None,
        typer.Option(
            "--type",
            help=(
                "Datasource type to template. Omit for compact type discovery. "
                "Run `dsctl template datasource` or `dsctl enum list db-type` "
                "for all values. Common: "
                f"{', '.join(supported_datasource_types()[:6])}."
            ),
        ),
    ] = None,
) -> None:
    """Emit datasource JSON payload-template type discovery or one template."""
    emit_result(
        "template.datasource",
        lambda: datasource_template_result(datasource_type=datasource_type),
    )


@template_app.command("task")
def task_command(
    task_type: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Task type to template. Required unless --list. Run "
                "`dsctl template task --list` to inspect supported values."
            ),
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
                "Task template scenario. Valid choices depend on the selected "
                "task type. Known variants include minimal, params, resource, "
                "post-json, pre-post-statements, branching, condition-routing, "
                "workflow-dependency, child-workflow, and datasource; inspect "
                "per-type values with `dsctl template task --list`."
            ),
        ),
    ] = None,
) -> None:
    """Emit one task YAML template or list supported task types."""
    if list_types:
        emit_result("template.task", task_template_types_result)
        return
    emit_result(
        "template.task",
        lambda: task_template_result(task_type or "", variant=variant),
    )
