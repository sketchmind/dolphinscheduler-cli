from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, cast

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.task_spec import (
    validate_sqoop_argument,
    validate_sqoop_argument_ascii,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskParameterProjectionError,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

_SQOOP_CANONICAL_FIELDS = frozenset({"subcommand", "args"})
_SQOOP_NATIVE_FIELDS = frozenset({"jobType", "localParams", "customShell"})


def _encode_sqoop_literal_command(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project one closed command intent onto upstream SQOOP CUSTOM mode."""
    canonical = _validate_sqoop_canonical(
        payload,
        version=version,
        direction="encode",
    )
    subcommand = cast("str", canonical["subcommand"])
    arguments = cast("list[str]", canonical["args"])
    return {
        "jobType": "CUSTOM",
        "localParams": [],
        "customShell": shlex.join(["sqoop", subcommand, *arguments]),
    }


def _decode_sqoop_literal_command(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode only the exact compiler-owned SQOOP CUSTOM command wire."""
    unexpected = sorted(set(payload) - _SQOOP_NATIVE_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="richer-native-state",
            message=f"SQOOP typed native wire does not own fields: {names}",
        )
    missing = sorted(_SQOOP_NATIVE_FIELDS - set(payload))
    if missing:
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-compiler-owned-field",
            message=f"SQOOP typed native wire requires task_params.{missing[0]}",
        )
    if payload["jobType"] != "CUSTOM":
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.jobType",
            reason="alternate-native-mode",
            message="SQOOP typed native wire requires jobType=CUSTOM",
        )
    if payload["localParams"] != []:
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.localParams",
            reason="parameters-outside-reviewed-subset",
            message="SQOOP typed native wire requires localParams=[]",
        )
    custom_shell = payload["customShell"]
    if not isinstance(custom_shell, str):
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.customShell",
            reason="invalid-type",
            message="SQOOP customShell must be one string",
        )
    try:
        tokens = shlex.split(custom_shell, comments=False, posix=True)
    except ValueError as exc:
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.customShell",
            reason="invalid-posix-shell-quoting",
            message="SQOOP customShell is not one valid POSIX-quoted command",
        ) from exc
    if len(tokens) < 3 or tokens[0] != "sqoop":
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.customShell",
            reason="outside-reviewed-literal-command-subset",
            message="SQOOP customShell is not one compiler-owned sqoop command",
        )
    canonical = _validate_sqoop_canonical(
        {"subcommand": tokens[1], "args": tokens[2:]},
        version=version,
        direction="decode",
    )
    if _encode_sqoop_literal_command(canonical, version=version) != payload:
        raise _sqoop_projection_error(
            version=version,
            direction="decode",
            field="task_params.customShell",
            reason="noncanonical-posix-shell-quoting",
            message="SQOOP customShell is outside the exact reversible typed wire",
        )
    return canonical


def _validate_sqoop_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate the closed literal import/export command package."""
    unexpected = sorted(set(payload) - _SQOOP_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _sqoop_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-command-subset",
            message=f"SQOOP literal command projection does not own fields: {names}",
        )
    subcommand = payload.get("subcommand")
    if not isinstance(subcommand, str) or subcommand not in {"import", "export"}:
        reason = (
            "missing-required-field"
            if "subcommand" not in payload
            else "unsupported-subcommand"
        )
        raise _sqoop_projection_error(
            version=version,
            direction=direction,
            field="task_params.subcommand",
            reason=reason,
            message="SQOOP subcommand must be exactly import or export",
        )
    raw_arguments = payload.get("args")
    if not isinstance(raw_arguments, list) or not raw_arguments:
        reason = "missing-required-field" if "args" not in payload else "invalid-type"
        raise _sqoop_projection_error(
            version=version,
            direction=direction,
            field="task_params.args",
            reason=reason,
            message="SQOOP args must be one nonempty strict list",
        )
    argument_validator = (
        validate_sqoop_argument_ascii
        if get_task_authoring_surface(version).sqoop.script_encoding
        == "platform-default"
        else validate_sqoop_argument
    )
    arguments: list[JsonValue] = []
    for index, argument in enumerate(raw_arguments):
        if not isinstance(argument, str):
            raise _sqoop_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.args[{index}]",
                reason="invalid-item-type",
                message="SQOOP args items must be strings",
            )
        try:
            argument_validator(argument)
        except ValueError as exc:
            raise _sqoop_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.args[{index}]",
                reason="unsafe-literal-argument",
                message=f"SQOOP {exc}",
            ) from exc
        arguments.append(argument)
    return {"subcommand": subcommand, "args": arguments}


def _decode_opaque_sqoop_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize only the exact SQOOP command wire and preserve richer state."""
    _require_sqoop_plugin_present(version=version, direction="decode")
    try:
        projected = _decode_sqoop_literal_command(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("SQOOP", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("SQOOP", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _require_sqoop_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Require one of the 15 exact profiles where upstream registers SQOOP."""
    if version in TARGET_DS_VERSIONS:
        return
    raise _sqoop_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"SQOOP does not exist in DolphinScheduler {version}",
    )


def _sqoop_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type="SQOOP",
        field=field,
        reason=reason,
        message=message,
    )
