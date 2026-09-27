from __future__ import annotations

from copy import deepcopy

from pydantic import ValidationError

from dsctl.models.task_spec import (
    ChunJunLiteralLocalJsonJobTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, require_json_object
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


def _encode_chunjun(payload: JsonObject, *, version: str) -> JsonObject:
    """Project one literal JSON job onto worker-local ChunJun execution."""
    _require_chunjun_available(version=version, direction="encode")
    validated = _validate_chunjun_canonical(
        payload,
        version=version,
        direction="encode",
    )
    return {
        "customConfig": 1,
        "json": validated.json_text,
        "deployMode": "local",
    }


def _decode_chunjun(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode one exact local custom-JSON wire plus harmless empty UI residue."""
    _require_chunjun_available(version=version, direction="decode")
    allowed = {
        "customConfig",
        "json",
        "deployMode",
        "others",
        "localParams",
        "resourceList",
    }
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        names = ", ".join(unexpected)
        raise _chunjun_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-local-json-subset",
            message=f"CHUNJUN local JSON projection does not own fields: {names}",
        )
    custom_config = payload.get("customConfig")
    if not (
        isinstance(custom_config, int)
        and not isinstance(custom_config, bool)
        and custom_config == 1
    ):
        reason = (
            "missing-required-wire-field"
            if "customConfig" not in payload
            else "invalid-wire-constant"
        )
        raise _chunjun_projection_error(
            version=version,
            direction="decode",
            field="task_params.customConfig",
            reason=reason,
            message=(
                "CHUNJUN native task_params.customConfig must be the exact integer 1"
            ),
        )
    if payload.get("deployMode") != "local":
        reason = (
            "missing-required-wire-field"
            if "deployMode" not in payload
            else "nonlocal-deploy-mode"
        )
        raise _chunjun_projection_error(
            version=version,
            direction="decode",
            field="task_params.deployMode",
            reason=reason,
            message="CHUNJUN typed native task_params.deployMode must be exact local",
        )
    if "others" in payload and payload["others"] != "":
        raise _chunjun_projection_error(
            version=version,
            direction="decode",
            field="task_params.others",
            reason="nonempty-ui-only-field",
            message="CHUNJUN native others is typed only as the exact empty UI default",
        )
    for key in ("localParams", "resourceList"):
        if key in payload and payload[key] != []:
            raise _chunjun_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{key}",
                reason="nonempty-ui-only-field",
                message=(
                    f"CHUNJUN native {key} is typed only as the exact empty UI "
                    "convention"
                ),
            )
    canonical: JsonObject = {}
    if "json" in payload:
        canonical["json"] = deepcopy(payload["json"])
    validated = _validate_chunjun_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    return require_json_object(
        validated.to_payload(),
        label="validated CHUNJUN task_params",
    )


def _decode_opaque_chunjun_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize only the exact local job and preserve richer native state."""
    _require_chunjun_available(version=version, direction="decode")
    try:
        projected = _decode_chunjun(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("CHUNJUN", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("CHUNJUN", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _validate_chunjun_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> ChunJunLiteralLocalJsonJobTaskParamsSpec:
    """Apply the closed literal local-JSON model with stable error details."""
    try:
        return ChunJunLiteralLocalJsonJobTaskParamsSpec.model_validate(payload)
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
            else "outside-reviewed-local-json-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _chunjun_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"CHUNJUN typed {field} is invalid: {error['msg']}",
        ) from exc


def _require_chunjun_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).chunjun.available:
        return
    raise _chunjun_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"CHUNJUN does not exist in DolphinScheduler {version}",
    )


def _chunjun_projection_error(
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
        task_type="CHUNJUN",
        field=field,
        reason=reason,
        message=message,
    )
