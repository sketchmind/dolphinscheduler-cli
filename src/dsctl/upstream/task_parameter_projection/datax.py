from __future__ import annotations

from copy import deepcopy

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DataxLiteralCustomJsonJobTaskParamsSpec,
)
from dsctl.models.task_spec.datax import validate_datax_inline_job_presence
from dsctl.support.json_types import JsonObject, require_json_object
from dsctl.upstream.task_authoring_surface import (
    DataxAuthoringSurface,
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

_DATAX_UI_RESOURCE_LIST_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)


def _encode_datax(payload: JsonObject, *, version: str) -> JsonObject:
    """Project one literal custom-JSON job onto its minimal exact wire."""
    surface = _require_datax_typed_available(version=version, direction="encode")
    validated = _validate_datax_canonical(
        payload,
        version=version,
        direction="encode",
    )
    projected: JsonObject = {
        "customConfig": 1,
        "json": validated.json_text,
    }
    if surface.wire_epoch == "custom-json-jvm-memory":
        projected.update({"xms": 1, "xmx": 1})
    return projected


def _decode_datax(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact custom job plus harmless empty UI residue."""
    surface = _require_datax_typed_available(version=version, direction="decode")
    allowed = {"customConfig", "json", "localParams"}
    if surface.wire_epoch == "custom-json-jvm-memory":
        allowed.update({"xms", "xmx"})
    if version in _DATAX_UI_RESOURCE_LIST_VERSIONS:
        allowed.add("resourceList")
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        names = ", ".join(unexpected)
        raise _datax_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-custom-json-subset",
            message=f"DATAX custom-JSON projection does not own fields: {names}",
        )

    _require_datax_integer_constant(
        payload,
        key="customConfig",
        version=version,
        direction="decode",
    )
    if surface.wire_epoch == "custom-json-jvm-memory":
        for key in ("xms", "xmx"):
            _require_datax_integer_constant(
                payload,
                key=key,
                version=version,
                direction="decode",
            )
    for key in ("localParams", "resourceList"):
        if key not in payload:
            continue
        if payload[key] != []:
            raise _datax_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{key}",
                reason="nonempty-ui-only-field",
                message=(
                    f"DATAX native {key} is typed only as the exact empty UI convention"
                ),
            )

    canonical: JsonObject = {}
    if "json" in payload:
        canonical["json"] = deepcopy(payload["json"])
    validated = _validate_datax_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    return require_json_object(
        validated.to_payload(),
        label="validated DATAX task_params",
    )


def _require_datax_integer_constant(
    payload: JsonObject,
    *,
    key: str,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Require one compiler-owned strict integer constant equal to one."""
    value = payload.get(key)
    if isinstance(value, int) and not isinstance(value, bool) and value == 1:
        return
    reason = (
        "missing-required-wire-field" if key not in payload else "invalid-wire-constant"
    )
    raise _datax_projection_error(
        version=version,
        direction=direction,
        field=f"task_params.{key}",
        reason=reason,
        message=f"DATAX native task_params.{key} must be the exact integer 1",
    )


def _decode_opaque_datax_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Recognize exact safe fixed points and preserve every richer native job."""
    surface = _require_datax_registered(version=version, direction="decode")
    if not surface.typed_custom_json_available:
        return DecodedTaskParameters(
            ProjectedTask("DATAX", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    try:
        projected = _decode_datax(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("DATAX", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("DATAX", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _validate_datax_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> DataxLiteralCustomJsonJobTaskParamsSpec:
    """Apply the closed literal custom-JSON model with stable error details."""
    try:
        validated = DataxLiteralCustomJsonJobTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params"
        if location:
            field_name = "json" if location[0] == "json_text" else str(location[0])
            field = f"task_params.{field_name}"
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-custom-json-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _datax_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"DATAX typed {field} is invalid: {error['msg']}",
        ) from exc
    try:
        validate_datax_inline_job_presence(
            validated.json_text,
            empty_json_object_is_absent=(
                get_task_authoring_surface(version).datax.empty_json_object_is_absent
            ),
        )
    except ValueError as exc:
        raise _datax_projection_error(
            version=version,
            direction=direction,
            field="task_params.json",
            reason="inline-json-selects-resource-fallback",
            message=str(exc),
        ) from exc
    return validated


def _require_datax_registered(
    *,
    version: str,
    direction: ProjectionDirection,
) -> DataxAuthoringSurface:
    """Require the exact profile to register the native DATAX plugin."""
    surface = get_task_authoring_surface(version).datax
    if surface.registered:
        return surface
    raise _datax_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DATAX does not exist in DolphinScheduler {version}",
    )


def _require_datax_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Require DATAX registration when the resolved surface is not needed."""
    _require_datax_registered(version=version, direction=direction)


def _require_datax_typed_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> DataxAuthoringSurface:
    """Require the custom-JSON runtime to satisfy the reviewed typed contract."""
    surface = _require_datax_registered(version=version, direction=direction)
    if surface.typed_custom_json_available:
        return surface
    reason = surface.exclusion_reason or "exact custom-JSON runtime unavailable"
    raise _datax_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason=reason,
        message=(
            f"DATAX typed authoring is unavailable on DolphinScheduler {version}: "
            f"{reason}"
        ),
    )


def _datax_projection_error(
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
        task_type="DATAX",
        field=field,
        reason=reason,
        message=message,
    )
