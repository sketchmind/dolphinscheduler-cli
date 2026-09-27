from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, cast

from dsctl.models.task_spec import (
    validate_java_argument,
    validate_java_resource_name,
)
from dsctl.upstream.task_authoring_surface import (
    JavaAuthoringSurface,
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
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import TaskResourceRefIndex

_JAVA_CANONICAL_FIELDS = frozenset({"mainJar", "mainArgs"})
_JAVA_COMMON_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "mainJar",
        "runType",
        "mainArgs",
        "jvmArgs",
        "isModulePath",
        "resourceList",
    }
)


def _encode_java_literal_fat_jar(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Project one portable literal fat-JAR intent onto the exact JAVA wire."""
    surface = _require_java_typed_fat_jar_available(
        version=version,
        direction="encode",
    )
    canonical = _validate_java_canonical(
        payload,
        version=version,
        direction="encode",
    )
    main_jar = cast("str", canonical["mainJar"])
    main_args = cast("list[str]", canonical["mainArgs"])
    resource = encode_resource_info(
        main_jar,
        version=version,
        task_type="JAVA",
        field="task_params.mainJar",
        resource_refs=resource_refs,
    )
    projected: JsonObject = {
        "localParams": [],
        "mainJar": deepcopy(resource),
        "runType": surface.fat_jar_run_type,
        "mainArgs": " ".join(main_args),
        "jvmArgs": "",
        "isModulePath": False,
        "resourceList": [deepcopy(resource)],
    }
    if surface.wire_epoch == "source-and-jar":
        projected["rawScript"] = ""
    elif surface.wire_epoch == "fat-and-normal-jar":
        projected["mainClass"] = ""
    else:
        raise _java_projection_error(
            version=version,
            direction="encode",
            field="task.type",
            reason="missing-reviewed-wire-epoch",
            message=f"JAVA {version} lacks one reviewed fat-JAR wire epoch",
        )
    return projected


def _decode_java_literal_fat_jar(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Decode only the exact closed fat-JAR wire into canonical intent."""
    surface = _require_java_typed_fat_jar_available(
        version=version,
        direction="decode",
    )
    _validate_java_wire_shape(payload, version=version, surface=surface)
    _decode_java_main_jar(payload, version=version)
    main_jar = decode_resource_info(
        payload["mainJar"],
        version=version,
        task_type="JAVA",
        field="task_params.mainJar",
        resource_refs=resource_refs,
    )
    main_args = _decode_java_main_args(payload, version=version)
    return _validate_java_canonical(
        {"mainJar": main_jar, "mainArgs": main_args},
        version=version,
        direction="decode",
    )


def _validate_java_wire_shape(
    payload: JsonObject,
    *,
    version: str,
    surface: JavaAuthoringSurface,
) -> None:
    """Require every compiler-owned discriminator and empty native field."""
    epoch_field = "rawScript" if surface.wire_epoch == "source-and-jar" else "mainClass"
    allowed = _JAVA_COMMON_WIRE_FIELDS | {epoch_field}
    unexpected = sorted(set(payload) - allowed)
    if unexpected:
        names = ", ".join(unexpected)
        raise _java_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-fat-jar-subset",
            message=f"JAVA literal fat-JAR projection does not own fields: {names}",
        )
    required = set(allowed)
    missing = sorted(required - set(payload))
    if missing:
        raise _java_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=f"JAVA literal fat-JAR wire requires task_params.{missing[0]}",
        )
    if payload["runType"] != surface.fat_jar_run_type:
        raise _java_projection_error(
            version=version,
            direction="decode",
            field="task_params.runType",
            reason="wrong-mode",
            message=(
                "JAVA literal fat-JAR projection requires "
                f"runType={surface.fat_jar_run_type} on {version}"
            ),
        )
    for field_name, expected in {
        "localParams": [],
        "jvmArgs": "",
        "isModulePath": False,
        epoch_field: "",
    }.items():
        actual = payload[field_name]
        matches_expected = (
            actual is expected if isinstance(expected, bool) else actual == expected
        )
        if not matches_expected:
            raise _java_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="outside-reviewed-literal-fat-jar-subset",
                message=(
                    f"JAVA typed native {field_name} is owned only as {expected!r}"
                ),
            )


def _decode_java_main_jar(payload: JsonObject, *, version: str) -> str:
    """Require one exact ResourceInfo duplicated into the download list."""
    main_jar_object = payload["mainJar"]
    if not isinstance(main_jar_object, dict) or set(main_jar_object) != {
        "resourceName"
    }:
        raise _java_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainJar",
            reason="invalid-resource-reference",
            message="JAVA mainJar must be one exact ResourceInfo.resourceName object",
        )
    main_jar = main_jar_object["resourceName"]
    if not isinstance(main_jar, str):
        raise _java_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainJar.resourceName",
            reason="invalid-type",
            message="JAVA mainJar.resourceName must be one string",
        )
    resources = payload["resourceList"]
    if resources != [main_jar_object]:
        raise _java_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList",
            reason="main-jar-download-invariant",
            message=(
                "JAVA resourceList must contain exactly the same mainJar so the "
                "worker downloads it"
            ),
        )
    return main_jar


def _decode_java_main_args(payload: JsonObject, *, version: str) -> list[JsonValue]:
    """Split only the reversible single-space native argument subset."""
    wire_args = payload["mainArgs"]
    if not isinstance(wire_args, str):
        raise _java_projection_error(
            version=version,
            direction="decode",
            field="task_params.mainArgs",
            reason="invalid-type",
            message="JAVA native mainArgs must be one string",
        )
    main_args: list[JsonValue]
    if wire_args == "":
        main_args = []
    else:
        main_args = list(wire_args.split(" "))
        if any(not item for item in main_args):
            raise _java_projection_error(
                version=version,
                direction="decode",
                field="task_params.mainArgs",
                reason="ambiguous-argument-spacing",
                message="JAVA native mainArgs must use one space between safe tokens",
            )
    return main_args


def _validate_java_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    unexpected = sorted(set(payload) - _JAVA_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _java_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-fat-jar-subset",
            message=f"JAVA literal fat-JAR projection does not own fields: {names}",
        )
    main_jar = payload.get("mainJar")
    if not isinstance(main_jar, str):
        reason = (
            "missing-required-field" if "mainJar" not in payload else "invalid-type"
        )
        raise _java_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainJar",
            reason=reason,
            message="JAVA mainJar must be one DS resource fullName string",
        )
    try:
        validate_java_resource_name(main_jar)
    except ValueError as exc:
        raise _java_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainJar",
            reason="unsafe-resource-name",
            message="JAVA mainJar is outside the safe absolute JAR resource subset",
        ) from exc
    raw_args = payload.get("mainArgs", [])
    if not isinstance(raw_args, list):
        raise _java_projection_error(
            version=version,
            direction=direction,
            field="task_params.mainArgs",
            reason="invalid-type",
            message="JAVA mainArgs must be one strict list of safe tokens",
        )
    arguments: list[JsonValue] = []
    for index, argument in enumerate(raw_args):
        if not isinstance(argument, str):
            raise _java_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.mainArgs[{index}]",
                reason="invalid-item-type",
                message="JAVA mainArgs items must be strings",
            )
        try:
            validate_java_argument(argument)
        except ValueError as exc:
            raise _java_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.mainArgs[{index}]",
                reason="unsafe-shell-token",
                message="JAVA mainArgs contains an unsafe shell token",
            ) from exc
        arguments.append(argument)
    return {"mainJar": main_jar, "mainArgs": arguments}


def _require_java_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).java.available:
        return
    raise _java_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"JAVA does not exist in DolphinScheduler {version}",
    )


def _require_java_typed_fat_jar_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> JavaAuthoringSurface:
    surface = get_task_authoring_surface(version).java
    if not surface.available:
        _require_java_plugin_present(version=version, direction=direction)
    if not surface.typed_fat_jar_supported or surface.fat_jar_run_type is None:
        reason = surface.exclusion_reason or "typed-fat-jar-unavailable"
        raise _java_projection_error(
            version=version,
            direction=direction,
            field="task.type",
            reason=reason,
            message=(
                f"JAVA literal fat-JAR typed authoring is unavailable on {version}: "
                f"{reason}"
            ),
        )
    return surface


def _decode_opaque_java_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> DecodedTaskParameters:
    """Canonicalize only the exact safe JAVA fat-JAR package."""
    _require_java_plugin_present(version=version, direction="decode")
    surface = get_task_authoring_surface(version).java
    if not surface.typed_fat_jar_supported:
        return DecodedTaskParameters(
            ProjectedTask("JAVA", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    try:
        projected = _decode_java_literal_fat_jar(
            payload, version=version, resource_refs=resource_refs
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("JAVA", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("JAVA", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _java_projection_error(
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
        task_type="JAVA",
        field=field,
        reason=reason,
        message=message,
    )
