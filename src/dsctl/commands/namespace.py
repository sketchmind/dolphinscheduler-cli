import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.namespace import (
    create_namespace_result,
    delete_namespace_result,
    get_namespace_result,
    list_available_namespaces_result,
    list_namespaces_result,
)

namespace_app = typer.Typer(
    help="Manage DolphinScheduler namespaces.",
    no_args_is_help=True,
)


def register_namespace_commands(app: typer.Typer) -> None:
    """Register the `namespace` command group."""
    app.add_typer(namespace_app, name="namespace")


@namespace_app.command("list")
@bind_command("namespace.list")
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
        "namespace.list",
        lambda: list_namespaces_result(
            env_file=env_file,
            search=search,
            page_no=page_no,
            page_size=page_size,
            all_pages=all_pages,
        ),
    )


@namespace_app.command("get")
@bind_command("namespace.get")
def get_command(
    ctx: typer.Context,
    namespace: str,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.get",
        lambda: get_namespace_result(namespace, env_file=env_file),
    )


@namespace_app.command("available")
@bind_command("namespace.available")
def available_command(ctx: typer.Context) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.available",
        lambda: list_available_namespaces_result(env_file=env_file),
    )


@namespace_app.command("create")
@bind_command("namespace.create")
def create_command(
    ctx: typer.Context,
    *,
    namespace: str,
    cluster_code: int | None,
    k8s: str | None,
    limits_cpu: float | None,
    limits_memory: int | None,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.create",
        lambda: create_namespace_result(
            namespace=namespace,
            cluster_code=cluster_code,
            k8s=k8s,
            limits_cpu=limits_cpu,
            limits_memory=limits_memory,
            env_file=env_file,
        ),
    )


@namespace_app.command("delete")
@bind_command("namespace.delete")
def delete_command(
    ctx: typer.Context,
    namespace: str,
    *,
    force: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "namespace.delete",
        lambda: delete_namespace_result(
            namespace,
            force=force,
            env_file=env_file,
        ),
    )
