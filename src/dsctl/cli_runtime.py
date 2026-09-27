from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path

import typer

from dsctl.command_contract import COMMAND_CATALOG
from dsctl.errors import DsctlError
from dsctl.output import (
    CommandResult,
    JsonObject,
    annotate_error_navigation,
    error_payload,
    result_payload,
)
from dsctl.output_formats import (
    RenderedCommand,
    RenderOptions,
    render_command,
    render_raw_command,
    validate_action_render_options,
    validate_render_options,
)

ActionPreflight = Callable[[str, Path | None], frozenset[str] | None]


def _unchanged_result(result: CommandResult, _env_file: Path | None) -> CommandResult:
    return result


def _unchanged_error(payload: JsonObject, _env_file: Path | None) -> JsonObject:
    return payload


@dataclass(frozen=True)
class AppState:
    """Global CLI state shared across commands."""

    env_file: Path | None
    render_options: RenderOptions = field(default_factory=RenderOptions)
    action_preflight: ActionPreflight | None = None
    invocation_scope: Callable[[], AbstractContextManager[None]] = nullcontext
    result_postprocess: Callable[[CommandResult, Path | None], CommandResult] = (
        _unchanged_result
    )
    error_postprocess: Callable[[JsonObject, Path | None], JsonObject] = (
        _unchanged_error
    )


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


def emit_result(
    action: str,
    builder: Callable[[], CommandResult],
    *,
    local_validate: Callable[[], None] | None = None,
) -> None:
    """Render a command result with the active global display settings."""
    state = _CURRENT_APP_STATE.get()
    try:
        with state.invocation_scope():
            _emit_result(action, builder, state, local_validate=local_validate)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)


def _emit_result(
    action: str,
    builder: Callable[[], CommandResult],
    state: AppState,
    *,
    local_validate: Callable[[], None] | None,
) -> None:
    render_options = state.render_options
    try:
        validate_render_options(render_options)
        validate_action_render_options(action, render_options)
        if local_validate is not None:
            local_validate()
        available_actions = _preflight_selected_action(state, action)
        result = state.result_postprocess(builder(), state.env_file)
        payload = result_payload(
            action,
            result,
            env_file=None if state.env_file is None else str(state.env_file),
            available_actions=available_actions,
        )
    except DsctlError as exc:
        payload = _failed_command_payload(state, action=action, error=exc)
        rendered = render_command(
            payload,
            action=action,
            options=render_options,
        )
    else:
        try:
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
        except DsctlError as exc:
            payload = state.error_postprocess(
                _post_result_render_error_payload(action, payload, exc),
                state.env_file,
            )
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
    _emit_rendered(rendered)


def emit_raw_result(
    action: str,
    builder: Callable[[], CommandResult],
    selector: Callable[[CommandResult], str],
) -> None:
    """Emit one command artifact body without the standard success envelope."""
    state = _CURRENT_APP_STATE.get()
    try:
        with state.invocation_scope():
            _emit_raw_result(action, builder, selector, state)
    finally:
        _CURRENT_APP_STATE.set(_DEFAULT_APP_STATE)


def _emit_raw_result(
    action: str,
    builder: Callable[[], CommandResult],
    selector: Callable[[CommandResult], str],
    state: AppState,
) -> None:
    render_options = state.render_options
    try:
        validate_render_options(render_options)
        available_actions = _preflight_selected_action(state, action)
        result = state.result_postprocess(builder(), state.env_file)
        payload = result_payload(
            action,
            result,
            env_file=None if state.env_file is None else str(state.env_file),
            available_actions=available_actions,
        )
    except DsctlError as exc:
        payload = _failed_command_payload(state, action=action, error=exc)
        rendered = render_command(
            payload,
            action=action,
            options=render_options,
        )
    else:
        if payload.get("ok") is not True:
            rendered = render_command(
                payload,
                action=action,
                options=render_options,
            )
        else:
            try:
                rendered = render_raw_command(selector(result), payload=payload)
            except DsctlError as exc:
                payload = state.error_postprocess(
                    _post_result_render_error_payload(action, payload, exc),
                    state.env_file,
                )
                rendered = render_command(
                    payload,
                    action=action,
                    options=render_options,
                )
    _emit_rendered(rendered)


def _failed_command_payload(
    state: AppState,
    *,
    action: str,
    error: DsctlError,
) -> JsonObject:
    payload = state.error_postprocess(error_payload(action, error), state.env_file)
    return annotate_error_navigation(
        payload,
        env_file=None if state.env_file is None else str(state.env_file),
    )


def _post_result_render_error_payload(
    action: str,
    completed: JsonObject,
    error: DsctlError,
) -> JsonObject:
    """Preserve a completed operation result when only output rendering fails."""
    error_data = error.to_payload()
    original_details = error_data.get("details")
    details: JsonObject = (
        dict(original_details) if isinstance(original_details, dict) else {}
    )
    details.update(
        {
            "phase": "output_render",
            "result_available": True,
            "operation_returned_success": completed.get("ok") is True,
        }
    )
    error_data["details"] = details
    effects = COMMAND_CATALOG.command(action).effects
    has_side_effects = effects.remote == "write" or effects.local != "none"
    original_suggestion = error_data.get("suggestion")
    completed_guidance = (
        "The operation already returned a result. Do not repeat the command solely "
        "to change its output. Inspect the preserved `data`, `resolved`, and any "
        "receipt below."
    )
    if not has_side_effects and isinstance(original_suggestion, str):
        error_data["suggestion"] = f"{completed_guidance} {original_suggestion}"
    else:
        error_data["suggestion"] = (
            f"{completed_guidance} Run `dsctl schema --command {action}` before a "
            "later invocation."
        )
    return {
        **completed,
        "ok": False,
        "error": error_data,
    }


def _preflight_selected_action(
    state: AppState,
    action: str,
) -> frozenset[str] | None:
    """Invoke the action policy injected by the CLI composition root."""
    if state.action_preflight is None:
        return None
    return state.action_preflight(action, state.env_file)


def _emit_rendered(rendered: RenderedCommand) -> None:
    """Write one rendered command without altering its exact channel text."""
    if rendered.stdout:
        typer.echo(rendered.stdout, nl=False)
    if rendered.stderr:
        typer.echo(rendered.stderr, err=True, nl=False)
    if rendered.exit_code:
        raise typer.Exit(code=rendered.exit_code)
