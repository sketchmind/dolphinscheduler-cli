import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.audit import (
    list_audit_logs_result,
    list_audit_model_types_result,
    list_audit_operation_types_result,
)

audit_app = typer.Typer(
    help="Inspect DolphinScheduler audit logs and audit filter metadata.",
    no_args_is_help=True,
)


def register_audit_commands(app: typer.Typer) -> None:
    """Register the `audit` command group."""
    app.add_typer(audit_app, name="audit")


@audit_app.command("list")
@bind_command("audit.list")
def list_command(
    ctx: typer.Context,
    *,
    model_types: list[str] | None,
    operation_types: list[str] | None,
    start: str | None,
    end: str | None,
    user_name: str | None,
    model_name: str | None,
    page_no: int,
    page_size: int,
    all_pages: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "audit.list",
        lambda: list_audit_logs_result(
            model_types=model_types,
            operation_types=operation_types,
            start=start,
            end=end,
            user_name=user_name,
            model_name=model_name,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
            env_file=env_file,
        ),
    )


@audit_app.command("model-types")
@bind_command("audit.model-types")
def model_types_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "audit.model-types",
        lambda: list_audit_model_types_result(env_file=env_file),
    )


@audit_app.command("operation-types")
@bind_command("audit.operation-types")
def operation_types_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "audit.operation-types",
        lambda: list_audit_operation_types_result(env_file=env_file),
    )
