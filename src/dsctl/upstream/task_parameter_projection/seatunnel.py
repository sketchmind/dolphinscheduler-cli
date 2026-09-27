from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    SeatunnelLiteralLocalConfigTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import (
    SeatunnelAuthoringSurface,
    get_task_authoring_surface,
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

_SEATUNNEL_CANONICAL_FIELDS = frozenset({"rawScript"})
_SEATUNNEL_LEGACY_WIRE_FIELDS = frozenset({"localParams", "resourceList", "rawScript"})
_SEATUNNEL_ENGINE_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "engine",
        "useCustom",
        "rawScript",
        "resourceList",
        "deployMode",
    }
)
_SEATUNNEL_STARTUP_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "startupScript",
        "useCustom",
        "rawScript",
        "resourceList",
        "deployMode",
        "others",
    }
)
_SEATUNNEL_LEGACY_SCRIPT_PREFIX = "#!/bin/sh\nset -eu\nprintf '%s' "
_SEATUNNEL_LEGACY_SCRIPT_SUFFIX = (
    " > .dsctl-seatunnel.conf\n"
    'exec sh "$SEATUNNEL_HOME/bin/start-seatunnel-spark.sh" '
    "--config .dsctl-seatunnel.conf --deploy-mode client --master local\n"
)


def _seatunnel_posix_single_quote(value: str) -> str:
    """Quote one literal as a deterministic POSIX single shell argument."""
    return "'" + value.replace("'", "'\"'\"'") + "'"


def _seatunnel_legacy_local_config_script(raw_script: str) -> str:
    """Build the compiler-owned 3.0.x SeaTunnel SPARK/local wrapper."""
    return (
        _SEATUNNEL_LEGACY_SCRIPT_PREFIX
        + _seatunnel_posix_single_quote(raw_script)
        + _SEATUNNEL_LEGACY_SCRIPT_SUFFIX
    )


def _decode_seatunnel_legacy_local_config_script(
    raw_script: JsonValue,
    *,
    version: str,
) -> str:
    """Recover the literal config only from the exact compiler-owned wrapper."""
    if (
        not isinstance(raw_script, str)
        or not raw_script.startswith(_SEATUNNEL_LEGACY_SCRIPT_PREFIX)
        or not raw_script.endswith(_SEATUNNEL_LEGACY_SCRIPT_SUFFIX)
    ):
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL rawScript is not the exact compiler-owned wrapper",
        )
    quoted_config = raw_script[
        len(_SEATUNNEL_LEGACY_SCRIPT_PREFIX) : -len(_SEATUNNEL_LEGACY_SCRIPT_SUFFIX)
    ]
    try:
        tokens = shlex.split(quoted_config, comments=False, posix=True)
    except ValueError as exc:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="invalid-compiler-owned-shell-quoting",
            message="SEATUNNEL compiler-owned config quoting is invalid",
        ) from exc
    if len(tokens) != 1:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="invalid-compiler-owned-shell-quoting",
            message="SEATUNNEL compiler-owned config must decode to one argument",
        )
    config = tokens[0]
    if _seatunnel_legacy_local_config_script(config) != raw_script:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="noncanonical-compiler-owned-shell-quoting",
            message="SEATUNNEL rawScript does not use canonical compiler quoting",
        )
    return config


def _validate_seatunnel_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> str:
    """Validate the one-field literal config canonical intent."""
    unexpected = sorted(set(payload) - _SEATUNNEL_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _seatunnel_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-local-config-subset",
            message=f"SEATUNNEL literal config projection does not own fields: {names}",
        )
    try:
        normalized = SeatunnelLiteralLocalConfigTaskParamsSpec.model_validate(
            payload
        ).to_payload()
    except ValidationError as exc:
        first_error = exc.errors()[0]
        detail = str(first_error.get("msg", "invalid literal config"))
        raise _seatunnel_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason="invalid-literal-config",
            message=f"SEATUNNEL typed rawScript is invalid: {detail}",
        ) from exc
    raw_script = normalized["rawScript"]
    if not isinstance(raw_script, str):
        raise _seatunnel_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason="invalid-type",
            message="SEATUNNEL rawScript must be one string",
        )
    return raw_script


def _encode_seatunnel_literal_local_config(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project one literal local config across the exact native launcher epochs."""
    surface = _require_seatunnel_available(version=version, direction="encode")
    raw_script = _validate_seatunnel_canonical(
        payload,
        version=version,
        direction="encode",
    )
    if surface.wire_epoch == "legacy-raw-shell":
        return {
            "localParams": [],
            "resourceList": [],
            "rawScript": _seatunnel_legacy_local_config_script(raw_script),
        }
    if surface.wire_epoch in {"engine-custom-config", "engine-explicit-local-master"}:
        native: JsonObject = {
            "localParams": [],
            "engine": "SPARK",
            "useCustom": True,
            "rawScript": raw_script,
            "resourceList": [],
            "deployMode": "local",
        }
        if surface.wire_epoch == "engine-explicit-local-master":
            native["deployMode"] = "client"
            native["master"] = "LOCAL"
        return native
    if surface.wire_epoch == "startup-script-custom-config":
        return {
            "localParams": [],
            "startupScript": "seatunnel.sh",
            "useCustom": True,
            "rawScript": raw_script,
            "resourceList": [],
            "deployMode": "local",
            "others": "",
        }
    raise _seatunnel_projection_error(
        version=version,
        direction="encode",
        field="task_type",
        reason="missing-reviewed-wire-epoch",
        message=f"SEATUNNEL {version} lacks one reviewed literal-config wire epoch",
    )


def _decode_seatunnel_literal_local_config(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode only exact compiler-owned SeaTunnel local-config fixed points."""
    surface = _require_seatunnel_available(version=version, direction="decode")
    expected_fields = _seatunnel_native_wire_fields(surface, version=version)
    if set(payload) != expected_fields:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL native wire is not the exact compiler-owned field set",
        )
    if payload["localParams"] != [] or payload["resourceList"] != []:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.localParams|resourceList",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL typed native parameters and resources must be empty",
        )
    raw_script = _decode_seatunnel_native_raw_script(
        payload,
        surface=surface,
        version=version,
    )
    canonical: JsonObject = {"rawScript": raw_script}
    validated_raw_script = _validate_seatunnel_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    result: JsonObject = {"rawScript": validated_raw_script}
    if _encode_seatunnel_literal_local_config(result, version=version) != payload:
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="noncanonical-native-fixed-point",
            message="SEATUNNEL native wire is not the compiler-owned fixed point",
        )
    return result


def _seatunnel_native_wire_fields(
    surface: SeatunnelAuthoringSurface,
    *,
    version: str,
) -> frozenset[str]:
    """Select the exact compiler-owned native field set for one wire epoch."""
    if surface.wire_epoch == "legacy-raw-shell":
        return _SEATUNNEL_LEGACY_WIRE_FIELDS
    if surface.wire_epoch == "engine-custom-config":
        return _SEATUNNEL_ENGINE_WIRE_FIELDS
    if surface.wire_epoch == "engine-explicit-local-master":
        return _SEATUNNEL_ENGINE_WIRE_FIELDS | {"master"}
    if surface.wire_epoch == "startup-script-custom-config":
        return _SEATUNNEL_STARTUP_WIRE_FIELDS
    raise _seatunnel_projection_error(
        version=version,
        direction="decode",
        field="task_type",
        reason="missing-reviewed-wire-epoch",
        message=f"SEATUNNEL {version} lacks one reviewed literal-config wire epoch",
    )


def _decode_seatunnel_native_raw_script(
    payload: JsonObject,
    *,
    surface: SeatunnelAuthoringSurface,
    version: str,
) -> str:
    """Validate exact epoch selectors and recover the literal config string."""
    if surface.wire_epoch == "legacy-raw-shell":
        return _decode_seatunnel_legacy_local_config_script(
            payload["rawScript"],
            version=version,
        )
    if surface.wire_epoch == "engine-custom-config" and (
        payload["engine"] != "SPARK"
        or payload["useCustom"] is not True
        or payload["deployMode"] != "local"
    ):
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.engine|useCustom|deployMode",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL native engine wire is outside SPARK custom local",
        )
    if surface.wire_epoch == "engine-explicit-local-master" and (
        payload["engine"] != "SPARK"
        or payload["useCustom"] is not True
        or payload["deployMode"] != "client"
        or payload["master"] != "LOCAL"
    ):
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.engine|useCustom|deployMode|master",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL native engine wire is outside SPARK client/local",
        )
    if surface.wire_epoch == "startup-script-custom-config" and (
        payload["startupScript"] != "seatunnel.sh"
        or payload["useCustom"] is not True
        or payload["deployMode"] != "local"
        or payload["others"] != ""
    ):
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.startupScript|useCustom|deployMode|others",
            reason="outside-reviewed-literal-local-config-subset",
            message="SEATUNNEL native startup wire is outside custom local",
        )
    raw_script = payload["rawScript"]
    if not isinstance(raw_script, str):
        raise _seatunnel_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="invalid-type",
            message="SEATUNNEL native rawScript must be one string",
        )
    return raw_script


def _decode_opaque_seatunnel_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize only the exact executable SeaTunnel local-config wire."""
    try:
        projected = _decode_seatunnel_literal_local_config(
            payload,
            version=version,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("SEATUNNEL", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("SEATUNNEL", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _require_seatunnel_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> SeatunnelAuthoringSurface:
    """Require one exact release with the reviewed SeaTunnel plugin surface."""
    surface = get_task_authoring_surface(version).seatunnel
    if surface.available and surface.wire_epoch is not None:
        return surface
    raise _seatunnel_projection_error(
        version=version,
        direction=direction,
        field="task_type",
        reason="upstream-absent",
        message=f"SEATUNNEL does not exist in DolphinScheduler {version}",
    )


def _guard_seatunnel_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Guard opaque preservation without exposing the selected surface."""
    _require_seatunnel_available(version=version, direction=direction)


def _seatunnel_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return TaskParameterProjectionError(
        message,
        version=version,
        direction=direction,
        task_type="SEATUNNEL",
        field=field,
        reason=reason,
    )
