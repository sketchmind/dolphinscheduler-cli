from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.context import (
    get_default_context,
    set_default_context,
    unset_default_context,
)
from dsctl.errors import DsctlError, UserInputError
from dsctl.output import CommandResult
from dsctl.services.meta import get_context_result

if TYPE_CHECKING:
    from dsctl.support.yaml_io import JsonObject


def _validate_key(key: str) -> None:
    if key != "default-context":
        message = f"Unsupported configuration key: {key}"
        raise UserInputError(
            message,
            suggestion="Use the supported key default-context.",
        )


def get_config_result(key: str) -> CommandResult:
    """Read the saved default independently of effective target selection."""
    _validate_key(key)
    return CommandResult(
        data={"key": key, "value": get_default_context()},
        resolved={"remote_validation": "not_performed"},
    )


def set_config_result(
    key: str, value: str, *, env_file: str | None = None
) -> CommandResult:
    """Save a default and distinguish it from any higher-priority selection."""
    _validate_key(key)
    registry = set_default_context(value)
    return _default_write_result(registry.default_context, env_file=env_file)


def unset_config_result(key: str, *, env_file: str | None = None) -> CommandResult:
    """Clear the saved default and inspect the remaining effective selection."""
    _validate_key(key)
    registry = unset_default_context()
    return _default_write_result(registry.default_context, env_file=env_file)


def _default_write_result(value: str | None, *, env_file: str | None) -> CommandResult:
    data: JsonObject = {"key": "default-context", "value": value}
    resolved: JsonObject = {"saved": True, "remote_validation": "not_performed"}
    try:
        effective = get_context_result(env_file=env_file)
    except DsctlError as error:
        message = (
            "Default configuration saved, but the effective target "
            "could not be resolved."
        )
        resolved["effective"] = None
        return CommandResult(
            data=data,
            resolved=resolved,
            warnings=[message],
            warning_details=[
                {
                    "code": "default_context_readback_failed",
                    "message": message,
                    "error": error.to_payload(),
                    "suggestion": error.suggestion,
                }
            ],
        )
    resolved["effective"] = effective.resolved.get("selection")
    return CommandResult(data=data, resolved=resolved)
