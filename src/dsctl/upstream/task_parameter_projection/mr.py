from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.task_spec import (
    validate_java_argument,
    validate_java_resource_name,
    validate_mr_main_class,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.resource_info import (
    decode_resource_info,
    encode_resource_info,
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
    TaskResourceRefIndex,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue

_MR_CANONICAL_FIELDS = frozenset({"mainJar", "mainClass", "mainArgs"})
_MR_MODERN_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "mainJar",
        "mainClass",
        "mainArgs",
        "others",
        "appName",
        "yarnQueue",
        "resourceList",
        "programType",
    }
)
_MR_LEGACY_POSITIVE_ID_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "mainJar",
        "mainClass",
        "mainArgs",
        "others",
        "appName",
        "resourceList",
        "programType",
    }
)


def _encode_mr_literal_java_jar_job(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Project one literal Java MapReduce JAR job onto its exact resource wire."""
    _require_mr_plugin_present(version=version, direction="encode")
    canonical = _validate_mr_canonical(
        payload,
        version=version,
        direction="encode",
    )
    main_jar = cast("str", canonical["mainJar"])
    projected: JsonObject = {
        "localParams": [],
        "mainClass": cast("str", canonical["mainClass"]),
        "mainArgs": " ".join(cast("list[str]", canonical["mainArgs"])),
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "JAVA",
    }
    if get_task_authoring_surface(version).mr.wire_epoch == "resource-name":
        projected["mainJar"] = encode_resource_info(
            main_jar,
            version=version,
            task_type="MR",
            field="task_params.mainJar",
            resource_refs=resource_refs,
        )
        projected["yarnQueue"] = ""
        return projected
    resolved_refs = _require_mr_resource_refs(
        resource_refs,
        version=version,
        direction="encode",
    )
    resource_id = resolved_refs.id_by_full_name.get(main_jar)
    if resource_id is None:
        raise _mr_projection_error(
            version=version,
            direction="encode",
            field="task_params.mainJar",
            reason="resource-full-name-not-resolved",
            message=(
                f"MR mainJar {main_jar!r} was not resolved to one positive resource "
                f"id for DolphinScheduler {version}"
            ),
        )
    projected["mainJar"] = {"id": resource_id}
    return projected


def _decode_mr_literal_java_jar_job(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Decode only the exact closed Java MapReduce resource wire."""
    _require_mr_plugin_present(version=version, direction="decode")
    modern_wire = get_task_authoring_surface(version).mr.wire_epoch == "resource-name"
    _validate_mr_wire_shape(payload, version=version, modern_wire=modern_wire)
    main_jar = _decode_mr_main_jar(
        payload,
        version=version,
        modern_wire=modern_wire,
        resource_refs=resource_refs,
    )
    main_class = payload["mainClass"]
    if not isinstance(main_class, str):
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainClass",
            reason="invalid-type",
            message="MR mainClass must be one string",
        )
    main_args = _decode_mr_main_args(payload, version=version)
    return _validate_mr_canonical(
        {
            "mainJar": main_jar,
            "mainClass": main_class,
            "mainArgs": main_args,
        },
        version=version,
        direction="decode",
    )


def _validate_mr_wire_shape(
    payload: JsonObject,
    *,
    version: str,
    modern_wire: bool,
) -> None:
    """Require every compiler-owned MR field for the selected resource epoch."""
    wire_fields = (
        _MR_MODERN_WIRE_FIELDS if modern_wire else _MR_LEGACY_POSITIVE_ID_WIRE_FIELDS
    )
    unexpected = sorted(set(payload) - wire_fields)
    if unexpected:
        names = ", ".join(unexpected)
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-java-jar-subset",
            message=f"MR literal Java JAR projection does not own fields: {names}",
        )
    missing = sorted(wire_fields - set(payload))
    if missing:
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=f"MR literal Java JAR wire requires task_params.{missing[0]}",
        )
    fixed_fields: dict[str, JsonValue] = {
        "localParams": [],
        "others": "",
        "appName": "",
        "resourceList": [],
        "programType": "JAVA",
    }
    if modern_wire:
        fixed_fields["yarnQueue"] = ""
    for field_name, expected in fixed_fields.items():
        if payload[field_name] != expected:
            raise _mr_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="outside-reviewed-literal-java-jar-subset",
                message=(f"MR typed native {field_name} is owned only as {expected!r}"),
            )


def _decode_mr_main_jar(
    payload: JsonObject,
    *,
    version: str,
    modern_wire: bool,
    resource_refs: TaskResourceRefIndex | None,
) -> str:
    """Decode one exact fullName or resolved positive-id MR resource reference."""
    main_jar_object = payload["mainJar"]
    expected_resource_field = "resourceName" if modern_wire else "id"
    if not isinstance(main_jar_object, dict) or set(main_jar_object) != {
        expected_resource_field
    }:
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainJar",
            reason="invalid-resource-reference",
            message=(
                "MR mainJar must be one exact ResourceInfo."
                f"{expected_resource_field} object"
            ),
        )
    if modern_wire:
        main_jar_value = main_jar_object["resourceName"]
        if not isinstance(main_jar_value, str):
            raise _mr_projection_error(
                version=version,
                direction="decode",
                field="task_params.mainJar.resourceName",
                reason="invalid-type",
                message="MR mainJar.resourceName must be one string",
            )
        return decode_resource_info(
            main_jar_object,
            version=version,
            task_type="MR",
            field="task_params.mainJar",
            resource_refs=resource_refs,
        )
    resource_id = main_jar_object["id"]
    if (
        not isinstance(resource_id, int)
        or isinstance(resource_id, bool)
        or resource_id <= 0
    ):
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainJar.id",
            reason="invalid-positive-resource-id",
            message="MR legacy mainJar.id must be one positive integer",
        )
    resolved_refs = _require_mr_resource_refs(
        resource_refs,
        version=version,
        direction="decode",
    )
    main_jar = resolved_refs.full_name_by_id.get(resource_id)
    if main_jar is None:
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainJar.id",
            reason="resource-id-not-resolved",
            message=(
                f"MR resource id {resource_id} was not resolved to one fullName "
                f"for DolphinScheduler {version}"
            ),
        )
    return main_jar


def _decode_mr_main_args(payload: JsonObject, *, version: str) -> list[JsonValue]:
    """Split only the reversible single-space MR application-argument subset."""
    wire_args = payload["mainArgs"]
    if not isinstance(wire_args, str):
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainArgs",
            reason="invalid-type",
            message="MR native mainArgs must be one string",
        )
    main_args: list[JsonValue] = [] if wire_args == "" else list(wire_args.split(" "))
    if any(not item for item in main_args):
        raise _mr_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainArgs",
            reason="ambiguous-argument-spacing",
            message="MR native mainArgs must use one space between safe tokens",
        )
    return main_args


def _validate_mr_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate the closed literal Java MapReduce authoring package."""
    unexpected = sorted(set(payload) - _MR_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-java-jar-subset",
            message=f"MR literal Java JAR projection does not own fields: {names}",
        )
    main_jar = payload.get("mainJar")
    if not isinstance(main_jar, str):
        reason = (
            "missing-required-field" if "mainJar" not in payload else "invalid-type"
        )
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainJar",
            reason=reason,
            message="MR mainJar must be one DS resource fullName string",
        )
    main_class = payload.get("mainClass")
    if not isinstance(main_class, str):
        reason = (
            "missing-required-field" if "mainClass" not in payload else "invalid-type"
        )
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainClass",
            reason=reason,
            message="MR mainClass must be one literal Java class name string",
        )
    try:
        validate_java_resource_name(main_jar)
    except ValueError as exc:
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainJar",
            reason="unsafe-resource-name",
            message="MR mainJar is outside the safe absolute JAR resource subset",
        ) from exc
    try:
        validate_mr_main_class(main_class)
    except ValueError as exc:
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainClass",
            reason="unsafe-main-class",
            message="MR mainClass is outside the safe literal Java class subset",
        ) from exc
    raw_args = payload.get("mainArgs", [])
    if not isinstance(raw_args, list):
        raise _mr_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainArgs",
            reason="invalid-type",
            message="MR mainArgs must be one strict list of safe tokens",
        )
    arguments: list[JsonValue] = []
    for index, argument in enumerate(raw_args):
        if not isinstance(argument, str):
            raise _mr_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.mainArgs[{index}]",
                reason="invalid-item-type",
                message="MR mainArgs items must be strings",
            )
        try:
            validate_java_argument(argument)
        except ValueError as exc:
            raise _mr_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.mainArgs[{index}]",
                reason="unsafe-shell-token",
                message="MR mainArgs contains an unsafe shell token",
            ) from exc
        arguments.append(argument)
    return {
        "mainJar": main_jar,
        "mainClass": main_class,
        "mainArgs": arguments,
    }


def _require_mr_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Require one of the 15 exact profiles where upstream registers MR."""
    if version in TARGET_DS_VERSIONS:
        return
    raise _mr_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"MR does not exist in DolphinScheduler {version}",
    )


def _require_mr_resource_refs(
    resource_refs: TaskResourceRefIndex | None,
    *,
    version: str,
    direction: ProjectionDirection,
) -> TaskResourceRefIndex:
    """Require remote resource identities for an old id-only MR wire."""
    if resource_refs is not None:
        return resource_refs
    raise _mr_projection_error(
        version=version,
        direction=direction,
        field="task_params.mainJar",
        reason="positive-resource-id-binding-required",
        message=(
            f"MR typed projection on {version} requires an exact positive resource "
            "id/fullName binding"
        ),
    )


def _decode_opaque_mr_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> DecodedTaskParameters:
    """Canonicalize only an exact reviewed MR Java resource wire."""
    _require_mr_plugin_present(version=version, direction="decode")
    try:
        projected = _decode_mr_literal_java_jar_job(
            payload,
            version=version,
            resource_refs=resource_refs,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("MR", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("MR", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _mr_projection_error(
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
        task_type="MR",
        field=field,
        reason=reason,
        message=message,
    )
