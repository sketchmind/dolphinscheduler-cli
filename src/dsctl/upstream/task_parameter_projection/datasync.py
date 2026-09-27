from __future__ import annotations

from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DatasyncTaskParamsSpec,
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

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_DATASYNC_NORMAL_FIELDS = frozenset(
    {
        "jsonFormat",
        "name",
        "sourceLocationArn",
        "destinationLocationArn",
        "cloudWatchLogGroupArn",
    }
)
_DATASYNC_UI_EMPTY_FIELDS = frozenset({"localParams", "resourceList"})
_DATASYNC_MODEL_FIELD_ALIASES: Mapping[str, str] = MappingProxyType(
    {
        "json_format": "jsonFormat",
        "source_location_arn": "sourceLocationArn",
        "destination_location_arn": "destinationLocationArn",
        "cloud_watch_log_group_arn": "cloudWatchLogGroupArn",
        "json_text": "json",
    }
)


def _encode_datasync(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate and emit one exact normal-mode DataSync public wire."""
    _require_datasync_available(version=version, direction="encode")
    return _validate_datasync_normal(
        payload,
        version=version,
        direction="encode",
    )


def _decode_datasync(payload: JsonObject, *, version: str) -> JsonObject:
    """Canonicalize only the reviewed normal wire plus exact empty UI fields."""
    _require_datasync_available(version=version, direction="decode")
    unexpected = sorted(
        set(payload) - _DATASYNC_NORMAL_FIELDS - _DATASYNC_UI_EMPTY_FIELDS
    )
    if unexpected:
        raise _datasync_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-normal-mode",
            message=(
                "DATASYNC normal projection does not own fields: "
                f"{', '.join(unexpected)}"
            ),
        )
    canonical = deepcopy(payload)
    for field_name in _DATASYNC_UI_EMPTY_FIELDS:
        if field_name not in canonical:
            continue
        if canonical[field_name] != []:
            raise _datasync_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="nonempty-ui-only-field",
                message=(
                    f"DATASYNC native {field_name} is typed only when it is the "
                    "exact empty UI convention"
                ),
            )
        canonical.pop(field_name)
    if canonical.get("cloudWatchLogGroupArn") == "":
        canonical.pop("cloudWatchLogGroupArn")
    return _validate_datasync_normal(
        canonical,
        version=version,
        direction="decode",
    )


def _decode_opaque_datasync_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Keep raw/richer state opaque and recognize only safe normal fixed points."""
    _require_datasync_available(version=version, direction="decode")
    if payload.get("jsonFormat") is True:
        return DecodedTaskParameters(
            ProjectedTask("DATASYNC", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    try:
        projected = _decode_datasync(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("DATASYNC", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("DATASYNC", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _validate_datasync_normal(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Apply the unified public model, then require its normal discriminator."""
    try:
        validated = DatasyncTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        message = str(error["msg"])
        location = error["loc"]
        field = _datasync_validation_field(location, message=message)
        reason = (
            "missing-required-field"
            if error["type"] == "missing" or "requires fields" in message
            else "outside-reviewed-normal-mode"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _datasync_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"DATASYNC typed task params are invalid: {message}",
        ) from exc
    if validated.json_format:
        raise _datasync_projection_error(
            version=version,
            direction=direction,
            field="task_params.jsonFormat",
            reason="raw-json-requires-opaque-provenance",
            message=(
                "DATASYNC raw JSON is an explicit selector-bound opaque mode, "
                "not typed normal authoring"
            ),
        )
    return require_json_object(
        validated.to_payload(),
        label="validated DATASYNC task_params",
    )


def _datasync_validation_field(
    location: Sequence[str | int],
    *,
    message: str,
) -> str:
    """Translate model validation locations to stable DS-native field paths."""
    if location:
        return "task_params" + "".join(
            (
                f"[{part}]"
                if isinstance(part, int)
                else f".{_DATASYNC_MODEL_FIELD_ALIASES.get(part, part)}"
            )
            for part in location
        )
    for field_name in (
        "jsonFormat",
        "sourceLocationArn",
        "destinationLocationArn",
        "cloudWatchLogGroupArn",
        "name",
        "json",
    ):
        if field_name in message:
            return f"task_params.{field_name}"
    return "task_params"


def _require_datasync_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).datasync.available:
        return
    raise _datasync_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DATASYNC does not exist in DolphinScheduler {version}",
    )


def _datasync_projection_error(
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
        task_type="DATASYNC",
        field=field,
        reason=reason,
        message=message,
    )
