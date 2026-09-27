from __future__ import annotations

from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    SagemakerStartPipelineExecutionTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, require_json_object
from dsctl.upstream.task_authoring_surface import (
    SagemakerAuthoringSurface,
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

_SAGEMAKER_CANONICAL_FIELDS = frozenset(
    {"sagemakerRequestJson", "localParams", "datasource"}
)
_SAGEMAKER_COMPILER_FIELDS = frozenset({"resourceList", "type"})
_SAGEMAKER_DATASOURCE_UI_RESIDUE_FIELDS = frozenset(
    {"username", "password", "awsRegion"}
)
_SAGEMAKER_MODEL_FIELD_ALIASES: Mapping[str, str] = MappingProxyType(
    {
        "sagemaker_request_json": "sagemakerRequestJson",
        "local_params": "localParams",
    }
)


def _encode_sagemaker(payload: JsonObject, *, version: str) -> JsonObject:
    """Project one canonical pipeline request onto the exact SageMaker epoch."""
    surface = _require_sagemaker_available(version=version, direction="encode")
    validated = _validate_sagemaker_canonical(
        payload,
        version=version,
        direction="encode",
    )
    datasource_epoch = surface.datasource_required
    if datasource_epoch and validated.datasource is None:
        raise _sagemaker_projection_error(
            version=version,
            direction="encode",
            field="task_params.datasource",
            reason="missing-required-field",
            message=(f"SAGEMAKER requires one positive datasource id on {version}"),
        )
    if not datasource_epoch and "datasource" in payload:
        raise _sagemaker_projection_error(
            version=version,
            direction="encode",
            field="task_params.datasource",
            reason="field-absent-in-wire-epoch",
            message=(f"SAGEMAKER datasource does not exist on the {version} task wire"),
        )

    projected = require_json_object(
        validated.to_payload(),
        label="validated SAGEMAKER task_params",
    )
    projected["localParams"] = deepcopy(projected.get("localParams", []))
    projected["resourceList"] = []
    if datasource_epoch:
        projected["type"] = "SAGEMAKER"
    else:
        projected.pop("datasource", None)
    return projected


def _decode_sagemaker(payload: JsonObject, *, version: str) -> JsonObject:
    """Canonicalize only safe native SageMaker request-template fixed points."""
    surface = _require_sagemaker_available(version=version, direction="decode")
    datasource_epoch = surface.datasource_required
    allowed = set(_SAGEMAKER_CANONICAL_FIELDS | _SAGEMAKER_COMPILER_FIELDS)
    if datasource_epoch:
        allowed.update(_SAGEMAKER_DATASOURCE_UI_RESIDUE_FIELDS)
    else:
        allowed.discard("datasource")
        allowed.discard("type")
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        names = ", ".join(unexpected)
        raise _sagemaker_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-start-pipeline-execution-subset",
            message=(
                "SAGEMAKER StartPipelineExecution projection does not own fields: "
                f"{names}"
            ),
        )

    canonical = _strip_sagemaker_compiler_fields(
        payload,
        version=version,
        datasource_epoch=datasource_epoch,
    )

    canonical.setdefault("localParams", [])
    validated = _validate_sagemaker_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    if datasource_epoch and validated.datasource is None:
        raise _sagemaker_projection_error(
            version=version,
            direction="decode",
            field="task_params.datasource",
            reason="missing-required-wire-field",
            message=(
                "SAGEMAKER native wire requires one positive datasource id on "
                f"{version}"
            ),
        )
    return require_json_object(
        validated.to_payload(),
        label="validated SAGEMAKER task_params",
    )


def _decode_opaque_sagemaker_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize a safe SageMaker wire and preserve every richer shape."""
    _require_sagemaker_when_selected(
        task_type="SAGEMAKER", version=version, direction="decode"
    )
    try:
        projected = _decode_sagemaker(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("SAGEMAKER", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("SAGEMAKER", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _strip_sagemaker_compiler_fields(
    payload: JsonObject,
    *,
    version: str,
    datasource_epoch: bool,
) -> JsonObject:
    """Validate and remove exact compiler/UI-only SageMaker wire fields."""
    canonical = deepcopy(payload)
    if "resourceList" in canonical:
        if canonical["resourceList"] != []:
            raise _sagemaker_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList",
                reason="nonempty-compiler-owned-field",
                message=(
                    "SAGEMAKER resourceList is canonical only as the exact empty "
                    "compiler-owned list"
                ),
            )
        canonical.pop("resourceList")
    if not datasource_epoch:
        return canonical
    _strip_sagemaker_datasource_epoch_fields(canonical, version=version)
    return canonical


def _strip_sagemaker_datasource_epoch_fields(
    canonical: JsonObject,
    *,
    version: str,
) -> None:
    """Remove exact datasource discriminator and empty UI credential residue."""
    if canonical.get("type") != "SAGEMAKER":
        reason = (
            "missing-required-wire-field"
            if "type" not in canonical
            else "invalid-compiler-owned-value"
        )
        raise _sagemaker_projection_error(
            version=version,
            direction="decode",
            field="task_params.type",
            reason=reason,
            message=(
                "SAGEMAKER datasource-backed native wire requires type='SAGEMAKER'"
            ),
        )
    canonical.pop("type")
    for field_name in _SAGEMAKER_DATASOURCE_UI_RESIDUE_FIELDS:
        if field_name not in canonical:
            continue
        if canonical[field_name] not in ("", None):
            raise _sagemaker_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="nonempty-runtime-credential-residue",
                message=(
                    f"SAGEMAKER native {field_name} is typed only as an empty "
                    "or null UI residue"
                ),
            )
        canonical.pop(field_name)


def _validate_sagemaker_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> SagemakerStartPipelineExecutionTaskParamsSpec:
    """Apply the closed canonical model and preserve stable projection errors."""
    try:
        return SagemakerStartPipelineExecutionTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        message = str(error["msg"])
        location = error["loc"]
        field = _sagemaker_validation_field(location, message=message)
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-start-pipeline-execution-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _sagemaker_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"SAGEMAKER typed {field} is invalid: {message}",
        ) from exc


def _sagemaker_validation_field(
    location: Sequence[str | int],
    *,
    message: str,
) -> str:
    """Translate model locations and root validators to DS-native paths."""
    if location:
        return "task_params" + "".join(
            (
                f"[{part}]"
                if isinstance(part, int)
                else f".{_SAGEMAKER_MODEL_FIELD_ALIASES.get(part, part)}"
            )
            for part in location
        )
    for field_name in ("sagemakerRequestJson", "localParams", "datasource"):
        if field_name in message:
            return f"task_params.{field_name}"
    return "task_params"


def _require_sagemaker_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> SagemakerAuthoringSurface:
    try:
        surface = get_task_authoring_surface(version).sagemaker
    except ValueError:
        surface = None
    if surface is not None and surface.available:
        return surface
    if surface is not None and surface.exclusion_reason is not None:
        raise _sagemaker_projection_error(
            version=version,
            direction=direction,
            field="task.type",
            reason=surface.exclusion_reason,
            message=(
                "SAGEMAKER typed authoring is unavailable in DolphinScheduler "
                f"{version}"
            ),
        )
    raise _sagemaker_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"SAGEMAKER does not exist in DolphinScheduler {version}",
    )


def _require_sagemaker_when_selected(
    *,
    task_type: str,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Gate SageMaker existence without complicating shared dispatch."""
    if (
        task_type == "SAGEMAKER"
        and get_task_authoring_surface(version).sagemaker.exclusion_reason is None
    ):
        _require_sagemaker_available(version=version, direction=direction)


def _sagemaker_projection_error(
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
        task_type="SAGEMAKER",
        field=field,
        reason=reason,
        message=message,
    )
