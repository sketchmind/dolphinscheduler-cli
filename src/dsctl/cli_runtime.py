from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

import typer

from dsctl.errors import DsctlError
from dsctl.output import CommandResult, error_payload, success_payload
from dsctl.output_formats import RenderOptions, render_payload, validate_render_options


@dataclass(frozen=True)
class AppState:
    """Global CLI state shared across commands."""

    env_file: Path | None
    render_options: RenderOptions = field(default_factory=RenderOptions)


_DEFAULT_APP_STATE = AppState(env_file=None)
_CURRENT_APP_STATE: ContextVar[AppState] = ContextVar(
    "dsctl_current_app_state",
    default=_DEFAULT_APP_STATE,
)


def get_app_state(ctx: typer.Context) -> AppState:
    """Return the initialized CLI state object."""
    state = ctx.obj
    if isinstance(state, AppState):
        return state
    message = "CLI app state is not initialized"
    raise RuntimeError(message)


def set_app_state(state: AppState) -> None:
    """Store the active app state for shared emitters."""
    _CURRENT_APP_STATE.set(state)


def emit_result(action: str, builder: Callable[[], CommandResult]) -> None:
    """Render a command result with the active global display settings."""
    render_options = _CURRENT_APP_STATE.get().render_options
    try:
        try:
            validate_render_options(render_options)
            result = builder()
            payload = success_payload(action, result)
        except DsctlError as exc:
            payload = error_payload(action, exc)
            typer.echo(
                render_payload(payload, action=action, options=render_options),
            )
            raise typer.Exit(code=1) from None

        try:
            rendered = render_payload(payload, action=action, options=render_options)
        except DsctlError as exc:
            typer.echo(
                render_payload(
                    error_payload(action, exc),
                    action=action,
                    options=render_options,
                ),
            )
            raise typer.Exit(code=1) from None
        typer.echo(rendered)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)


def emit_raw_result(
    action: str,
    builder: Callable[[], CommandResult],
    selector: Callable[[CommandResult], str],
) -> None:
    """Emit one command artifact body without the standard success envelope."""
    render_options = _CURRENT_APP_STATE.get().render_options
    try:
        try:
            result = builder()
        except DsctlError as exc:
            payload = error_payload(action, exc)
            typer.echo(
                render_payload(payload, action=action, options=render_options),
            )
            raise typer.Exit(code=1) from None
        typer.echo(selector(result), nl=False)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)
