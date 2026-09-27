from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_waterdrop_config_resource,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskParameterProjectionError,
    TaskResourceRefIndex,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue

_WATERDROP_CANONICAL_FIELDS = frozenset({"configResource"})
_WATERDROP_NATIVE_FIELDS = frozenset({"localParams", "resourceList", "rawScript"})
_WATERDROP_LOCAL_CONFIG_SCRIPT_PREFIX = (
    'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
    "--deploy-mode client --queue default --config "
)


def _waterdrop_local_config_script(config_resource: str) -> str:
    """Build the sole compiler-owned local/client/default launcher line."""
    return (
        f"{_WATERDROP_LOCAL_CONFIG_SCRIPT_PREFIX}{config_resource.removeprefix('/')}\n"
    )


def _decode_waterdrop_native_candidate(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
) -> tuple[int, str]:
    """Validate the exact native shape before resolving its FILE identity."""
    _require_waterdrop_available(version=version, direction="decode")
    unexpected = sorted(set(payload) - _WATERDROP_NATIVE_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-local-config-subset",
            message=f"WATERDROP local config projection does not own fields: {names}",
        )
    missing = sorted(_WATERDROP_NATIVE_FIELDS - set(payload))
    if missing:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=f"WATERDROP local config wire requires task_params.{missing[0]}",
        )
    if payload["localParams"] != []:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.localParams",
            reason="outside-reviewed-literal-local-config-subset",
            message="WATERDROP typed native localParams is owned only as []",
        )
    resource_list = payload["resourceList"]
    if (
        not isinstance(resource_list, list)
        or len(resource_list) != 1
        or not isinstance(resource_list[0], dict)
        or set(resource_list[0]) != {"id"}
    ):
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList",
            reason="invalid-resource-reference",
            message="WATERDROP resourceList must contain one exact positive-id object",
        )
    resource_id = resource_list[0]["id"]
    if (
        not isinstance(resource_id, int)
        or isinstance(resource_id, bool)
        or resource_id <= 0
    ):
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList[0].id",
            reason="invalid-positive-resource-id",
            message="WATERDROP resourceList id must be one positive integer",
        )
    raw_script = payload["rawScript"]
    if (
        not isinstance(raw_script, str)
        or not raw_script.startswith(_WATERDROP_LOCAL_CONFIG_SCRIPT_PREFIX)
        or not raw_script.endswith("\n")
    ):
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="outside-reviewed-literal-local-config-subset",
            message="WATERDROP rawScript is not the exact compiler-owned launcher",
        )
    relative_config = raw_script[len(_WATERDROP_LOCAL_CONFIG_SCRIPT_PREFIX) : -1]
    script_config_resource = f"/{relative_config}"
    try:
        validate_waterdrop_config_resource(script_config_resource)
    except ValueError as exc:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="unsafe-resource-name",
            message="WATERDROP rawScript config path is outside the safe subset",
        ) from exc
    return resource_id, script_config_resource


def waterdrop_typed_resource_id_candidate(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
) -> int | None:
    """Return the id only when a native wire could decode to the typed facet."""
    try:
        resource_id, _config_resource = _decode_waterdrop_native_candidate(
            payload,
            version=version,
        )
    except TaskParameterProjectionError:
        return None
    return resource_id


def _encode_waterdrop_literal_local_config_job(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Project one local Waterdrop config onto the exact ShellParameters wire."""
    _require_waterdrop_available(version=version, direction="encode")
    unexpected = sorted(set(payload) - _WATERDROP_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _waterdrop_projection_error(
            version=version,
            direction="encode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-local-config-subset",
            message=f"WATERDROP local config projection does not own fields: {names}",
        )
    config_resource = payload.get("configResource")
    if not isinstance(config_resource, str):
        reason = (
            "missing-required-field"
            if "configResource" not in payload
            else "invalid-type"
        )
        raise _waterdrop_projection_error(
            version=version,
            direction="encode",
            field="task_params.configResource",
            reason=reason,
            message="WATERDROP configResource must be one DS resource fullName",
        )
    try:
        validate_waterdrop_config_resource(config_resource)
    except ValueError as exc:
        raise _waterdrop_projection_error(
            version=version,
            direction="encode",
            field="task_params.configResource",
            reason="unsafe-resource-name",
            message=f"WATERDROP {exc}",
        ) from exc
    refs = _require_waterdrop_resource_refs(
        resource_refs,
        version=version,
        direction="encode",
    )
    resource_id = refs.id_by_full_name.get(config_resource)
    if resource_id is None:
        raise _waterdrop_projection_error(
            version=version,
            direction="encode",
            field="task_params.configResource",
            reason="resource-full-name-not-resolved",
            message=(
                f"WATERDROP configResource {config_resource!r} was not resolved "
                f"to one positive FILE id for DolphinScheduler {version}"
            ),
        )
    return {
        "localParams": [],
        "resourceList": [{"id": resource_id}],
        "rawScript": _waterdrop_local_config_script(config_resource),
    }


def _decode_waterdrop_literal_local_config_job(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Decode only the exact compiler-owned local Waterdrop launcher wire."""
    resource_id, script_config_resource = _decode_waterdrop_native_candidate(
        payload,
        version=version,
    )
    refs = _require_waterdrop_resource_refs(
        resource_refs,
        version=version,
        direction="decode",
    )
    config_resource = refs.full_name_by_id.get(resource_id)
    if config_resource is None:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList[0].id",
            reason="resource-id-not-resolved",
            message=(
                f"WATERDROP resource id {resource_id} was not resolved to one "
                f"fullName for DolphinScheduler {version}"
            ),
        )
    try:
        validate_waterdrop_config_resource(config_resource)
    except ValueError as exc:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.configResource",
            reason="unsafe-resource-name",
            message="WATERDROP config resource is outside the safe absolute subset",
        ) from exc
    if config_resource != script_config_resource:
        raise _waterdrop_projection_error(
            version=version,
            direction="decode",
            field="task_params.rawScript",
            reason="outside-reviewed-literal-local-config-subset",
            message="WATERDROP rawScript is not the exact compiler-owned launcher",
        )
    return {"configResource": config_resource}


def _decode_opaque_waterdrop_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> DecodedTaskParameters:
    """Canonicalize only the exact executable Waterdrop wire."""
    try:
        projected = _decode_waterdrop_literal_local_config_job(
            payload,
            version=version,
            resource_refs=resource_refs,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("WATERDROP", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("WATERDROP", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _require_waterdrop_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Require the sole exact release whose worker registers the Shell alias."""
    surface = get_task_authoring_surface(version).waterdrop
    if surface.available and surface.channel_registered:
        return
    reason = surface.exclusion_reason or "upstream-absent"
    raise _waterdrop_projection_error(
        version=version,
        direction=direction,
        field="task_type",
        reason=reason,
        message=(
            f"WATERDROP typed execution is unavailable in DolphinScheduler "
            f"{version}: {reason}"
        ),
    )


def _require_waterdrop_resource_refs(
    resource_refs: TaskResourceRefIndex | None,
    *,
    version: str,
    direction: ProjectionDirection,
) -> TaskResourceRefIndex:
    if resource_refs is not None:
        return resource_refs
    raise _waterdrop_projection_error(
        version=version,
        direction=direction,
        field="task_params.configResource",
        reason="resource-reference-context-required",
        message="WATERDROP projection requires resolved task resource identities",
    )


def _waterdrop_projection_error(
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
        task_type="WATERDROP",
        field=field,
        reason=reason,
    )
