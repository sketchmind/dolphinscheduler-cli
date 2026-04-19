from typing import Annotated

import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.services.queue import (
    create_queue_result,
    delete_queue_result,
    get_queue_result,
    list_queues_result,
    update_queue_result,
)

queue_app = typer.Typer(
    help="Manage DolphinScheduler queues.",
    no_args_is_help=True,
)

QUEUE_HELP = "Queue name or numeric id. Run `dsctl queue list` to discover values."


def register_queue_commands(app: typer.Typer) -> None:
    """Register the `queue` command group."""
    app.add_typer(queue_app, name="queue")


@queue_app.command("list")
def list_command(
    ctx: typer.Context,
    *,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            help="Filter queues by queue name using the upstream search value.",
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
    """List queues with optional filtering and pagination controls."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.list",
        lambda: list_queues_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@queue_app.command("get")
def get_command(
    ctx: typer.Context,
    queue: Annotated[
        str,
        typer.Argument(help=QUEUE_HELP),
    ],
) -> None:
    """Get one queue by name or id."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.get",
        lambda: get_queue_result(queue, env_file=env_file),
    )


@queue_app.command("create")
def create_command(
    ctx: typer.Context,
    *,
    queue_name: Annotated[
        str,
        typer.Option(
            "--queue-name",
            help="Human-facing DS queue name used as the selector label.",
        ),
    ],
    queue: Annotated[
        str,
        typer.Option(
            "--queue",
            help="Underlying YARN queue value stored in DolphinScheduler.",
        ),
    ],
) -> None:
    """Create one queue."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.create",
        lambda: create_queue_result(
            queue_name=queue_name,
            queue=queue,
            env_file=env_file,
        ),
    )


@queue_app.command("update")
def update_command(
    ctx: typer.Context,
    queue_identifier: Annotated[
        str,
        typer.Argument(
            help=QUEUE_HELP,
            metavar="QUEUE",
        ),
    ],
    *,
    queue_name: Annotated[
        str | None,
        typer.Option(
            "--queue-name",
            help=(
                "Updated human-facing DS queue name. Omit to keep the current "
                "queue name."
            ),
        ),
    ] = None,
    queue: Annotated[
        str | None,
        typer.Option(
            "--queue",
            help=(
                "Updated underlying YARN queue value. Omit to keep the current "
                "queue value."
            ),
        ),
    ] = None,
) -> None:
    """Update one queue."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.update",
        lambda: update_queue_result(
            queue_identifier,
            queue_name=queue_name,
            queue=queue,
            env_file=env_file,
        ),
    )


@queue_app.command("delete")
def delete_command(
    ctx: typer.Context,
    queue: Annotated[
        str,
        typer.Argument(help=QUEUE_HELP),
    ],
    *,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Confirm queue deletion without prompting.",
        ),
    ] = False,
) -> None:
    """Delete one queue."""
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.delete",
        lambda: delete_queue_result(
            queue,
            force=force,
            env_file=env_file,
        ),
    )
