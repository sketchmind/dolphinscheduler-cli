import typer

from dsctl.cli_runtime import emit_result, get_app_state
from dsctl.commands._contract_adapter import bind_command
from dsctl.services.capabilities import (
    CAPABILITIES_SECTION_CHOICES,
    get_capabilities_result,
)

CAPABILITIES_SECTION_HELP = (
    "Return one top-level capability section. Supported: "
    f"{', '.join(CAPABILITIES_SECTION_CHOICES)}. Discover values with "
    "`dsctl schema --command capabilities`."
)


def register_capabilities_commands(app: typer.Typer) -> None:
    """Register the top-level `capabilities` command."""
    app.command("capabilities")(capabilities_command)


@bind_command("capabilities")
def capabilities_command(
    ctx: typer.Context,
    *,
    summary: bool,
    section: str | None,
    action: str | None,
    full: bool,
) -> None:
    state = get_app_state(ctx)
    env_file = None if state.env_file is None else str(state.env_file)
    emit_result(
        "capabilities",
        lambda: get_capabilities_result(
            env_file=env_file,
            summary=summary,
            section=section,
            full=full,
            action=action,
        ),
    )
