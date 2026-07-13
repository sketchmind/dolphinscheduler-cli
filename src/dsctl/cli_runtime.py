from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

import typer

from dsctl.errors import DsctlError
from dsctl.output import CommandResult, error_payload, success_payload
from dsctl.output_formats import (
    RenderedCommand,
    RenderOptions,
    render_command,
    render_raw_command,
    validate_render_options,
)


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
    state = _CURRENT_APP_STATE.get()
    render_options = state.render_options
    try:
        try:
            validate_render_options(render_options)
            result = builder()
            payload = success_payload(
                action,
                result,
                env_file=None if state.env_file is None else str(state.env_file),
            )
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
        except DsctlError as exc:
            payload = error_payload(action, exc)
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
        _emit_rendered(rendered)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)


def emit_raw_result(
    action: str,
    builder: Callable[[], CommandResult],
    selector: Callable[[CommandResult], str],
) -> None:
    """Emit one command artifact body without the standard success envelope."""
    state = _CURRENT_APP_STATE.get()
    render_options = state.render_options
    try:
        try:
            validate_render_options(render_options)
            result = builder()
            rendered = render_raw_command(
                selector(result),
                payload=success_payload(
                    action,
                    result,
                    env_file=None if state.env_file is None else str(state.env_file),
                ),
            )
        except DsctlError as exc:
            payload = error_payload(action, exc)
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
        _emit_rendered(rendered)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)


def _emit_rendered(rendered: RenderedCommand) -> None:
    """Write one rendered command without altering its exact channel text."""
    if rendered.stdout:
        typer.echo(rendered.stdout, nl=False)
    if rendered.stderr:
        typer.echo(rendered.stderr, err=True, nl=False)
    if rendered.exit_code:
        raise typer.Exit(code=rendered.exit_code)
