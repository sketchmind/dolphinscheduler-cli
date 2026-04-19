from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
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
def list_command(
    ctx: typer.Context,
    *,
    model_types: Annotated[
        list[str] | None,
        typer.Option(
            "--model-type",
            help=(
                "Audit model type filter. Repeat as needed; run "
                "`dsctl audit model-types` to discover values."
            ),
        ),
    ] = None,
    operation_types: Annotated[
        list[str] | None,
        typer.Option(
            "--operation-type",
            help=(
                "Audit operation type filter. Repeat as needed; run "
                "`dsctl audit operation-types` to discover values."
            ),
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Start datetime in DS format 'YYYY-MM-DD HH:MM:SS'.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option(
            "--end",
            help="End datetime in DS format 'YYYY-MM-DD HH:MM:SS'.",
        ),
    ] = None,
    user_name: Annotated[
        str | None,
        typer.Option(
            "--user-name",
            help="Filter by audit actor user name.",
        ),
    ] = None,
    model_name: Annotated[
        str | None,
        typer.Option(
            "--model-name",
            help="Filter by audited model name.",
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
    """List audit-log rows with optional filters."""
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
def model_types_command(ctx: typer.Context) -> None:
    """List DS audit model types."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "audit.model-types",
        lambda: list_audit_model_types_result(env_file=env_file),
    )


@audit_app.command("operation-types")
def operation_types_command(ctx: typer.Context) -> None:
    """List DS audit operation types."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "audit.operation-types",
        lambda: list_audit_operation_types_result(env_file=env_file),
    )
