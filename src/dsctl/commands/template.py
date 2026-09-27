import typer

from dsctl.cli_runtime import emit_raw_result, emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.errors import UserInputError
from dsctl.output import CommandResult
from dsctl.services.template import (
    cluster_config_template_result,
    datasource_template_result,
    environment_config_template_result,
    parameter_syntax_result,
    task_template_result,
    task_template_types_result,
    workflow_instance_patch_template_result,
    workflow_patch_template_result,
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
@bind_command("template.workflow")
def workflow_command(
    ctx: typer.Context,
    with_schedule: bool | None,
    raw: bool | None,
    example: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    if raw:
        emit_raw_result(
            "template.workflow",
            lambda: workflow_template_result(
                with_schedule=bool(with_schedule), env_file=env_file, example=example
            ),
            _template_yaml,
        )
        return
    emit_result(
        "template.workflow",
        lambda: workflow_template_result(
            with_schedule=bool(with_schedule), env_file=env_file, example=example
        ),
    )


@template_app.command("workflow-patch")
@bind_command("template.workflow-patch")
def workflow_patch_command(
    raw: bool | None,
) -> None:
    if raw:
        emit_raw_result(
            "template.workflow-patch",
            workflow_patch_template_result,
            _template_yaml,
        )
        return
    emit_result("template.workflow-patch", workflow_patch_template_result)


@template_app.command("workflow-instance-patch")
@bind_command("template.workflow-instance-patch")
def workflow_instance_patch_command(
    raw: bool | None,
) -> None:
    if raw:
        emit_raw_result(
            "template.workflow-instance-patch",
            workflow_instance_patch_template_result,
            _template_yaml,
        )
        return
    emit_result(
        "template.workflow-instance-patch",
        workflow_instance_patch_template_result,
    )


@template_app.command("params")
@bind_command("template.params")
def params_command(
    ctx: typer.Context,
    topic: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "template.params",
        lambda: parameter_syntax_result(topic=topic, env_file=env_file),
    )


@template_app.command("environment")
@bind_command("template.environment")
def environment_command() -> None:
    emit_result("template.environment", environment_config_template_result)


@template_app.command("cluster")
@bind_command("template.cluster")
def cluster_command() -> None:
    emit_result("template.cluster", cluster_config_template_result)


@template_app.command("datasource")
@bind_command("template.datasource")
def datasource_command(
    ctx: typer.Context,
    datasource_type: str | None,
    ds_version: str | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "template.datasource",
        lambda: datasource_template_result(
            datasource_type=datasource_type,
            ds_version=ds_version,
            env_file=env_file,
        ),
    )


@template_app.command("task")
@bind_command("template.task")
def task_command(
    ctx: typer.Context,
    task_type: str | None,
    variant: str | None,
    raw: bool | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    if task_type is None:
        if raw:
            emit_result("template.task", _task_template_raw_requires_type)
            return
        emit_result(
            "template.task",
            lambda: task_template_types_result(env_file=env_file),
        )
        return
    if raw:
        emit_raw_result(
            "template.task",
            lambda: task_template_result(
                task_type,
                variant=variant,
                env_file=env_file,
            ),
            _template_yaml,
        )
        return
    emit_result(
        "template.task",
        lambda: task_template_result(
            task_type,
            variant=variant,
            env_file=env_file,
        ),
    )


def _task_template_raw_requires_type() -> CommandResult:
    message = "--raw requires TASK_TYPE."
    raise UserInputError(
        message,
        details={"discovery_command": "dsctl template task"},
        suggestion=(
            "Run `dsctl template task` to choose a task type, then "
            "`dsctl template task TYPE --raw`."
        ),
    )


def _template_yaml(result: CommandResult) -> str:
    data = result.data
    if not isinstance(data, dict):
        message = "template result data must be an object"
        raise TypeError(message)
    yaml_text = data.get("yaml")
    if not isinstance(yaml_text, str):
        message = "template result data is missing yaml"
        raise TypeError(message)
    return yaml_text
