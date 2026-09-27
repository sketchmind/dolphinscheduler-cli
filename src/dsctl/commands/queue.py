import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
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


def register_queue_commands(app: typer.Typer) -> None:
    """Register the `queue` command group."""
    app.add_typer(queue_app, name="queue")


@queue_app.command("list")
@bind_command("queue.list")
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
@bind_command("queue.get")
def get_command(
    ctx: typer.Context,
    queue: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "queue.get",
        lambda: get_queue_result(queue, env_file=env_file),
    )


@queue_app.command("create")
@bind_command("queue.create")
def create_command(
    ctx: typer.Context,
    *,
    queue_name: str,
    queue: str,
) -> None:
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
@bind_command("queue.update")
def update_command(
    ctx: typer.Context,
    queue_identifier: str,
    *,
    queue_name: str | None,
    queue: str | None,
) -> None:
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
@bind_command("queue.delete")
def delete_command(
    ctx: typer.Context,
    queue: str,
    *,
    force: bool,
) -> None:
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
