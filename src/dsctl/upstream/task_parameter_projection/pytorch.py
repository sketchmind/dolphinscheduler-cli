from __future__ import annotations

from typing import TYPE_CHECKING, cast

from dsctl.models.task_spec import (
    validate_pytorch_argument,
    validate_pytorch_python_executable,
    validate_pytorch_script_resource,
)
from dsctl.upstream.task_authoring_surface import (
    PytorchAuthoringSurface,
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
    TaskResourceRefIndex,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue

_PYTORCH_CANONICAL_FIELDS = frozenset(
    {"pythonExecutable", "scriptResource", "scriptArgs"}
)
_PYTORCH_COMMON_WIRE_FIELDS = frozenset(
    {
        "localParams",
        "isCreateEnvironment",
        "pythonPath",
        "script",
        "scriptParams",
        "pythonEnvTool",
        "requirements",
        "condaPythonVersion",
        "resourceList",
    }
)


def _encode_pytorch_literal_resource_script(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Project one portable downloaded Python script onto the exact wire."""
    surface = _require_pytorch_available(version=version, direction="encode")
    canonical = _validate_pytorch_canonical(
        payload,
        version=version,
        direction="encode",
    )
    python_executable = cast("str", canonical["pythonExecutable"])
    script_resource = cast("str", canonical["scriptResource"])
    script_args = cast("list[str]", canonical["scriptArgs"])
    launcher_field: str
    resource: JsonObject
    if surface.wire_epoch == "positive-resource-id-python-home":
        launcher_field = "pythonCommand"
        refs = _require_pytorch_resource_refs(
            resource_refs,
            version=version,
            direction="encode",
        )
        resource_id = refs.id_by_full_name.get(script_resource)
        if resource_id is None:
            raise _pytorch_projection_error(
                version=version,
                direction="encode",
                field="task_params.scriptResource",
                reason="resource-full-name-not-resolved",
                message=(
                    f"PYTORCH scriptResource {script_resource!r} was not resolved "
                    f"to one positive FILE id for DolphinScheduler {version}"
                ),
            )
        resource = {"id": resource_id}
    elif surface.wire_epoch == "resource-name-python-launcher":
        launcher_field = "pythonLauncher"
        refs = _require_pytorch_resource_refs(
            resource_refs,
            version=version,
            direction="encode",
        )
        wire_full_name = refs.wire_full_name_by_full_name.get(script_resource)
        if wire_full_name is None:
            raise _pytorch_projection_error(
                version=version,
                direction="encode",
                field="task_params.scriptResource",
                reason="resource-full-name-not-resolved",
                message=(
                    f"PYTORCH scriptResource {script_resource!r} was not resolved "
                    f"to one exact FILE fullName for DolphinScheduler {version}"
                ),
            )
        if not wire_full_name.endswith(f"/resources{script_resource}"):
            raise _pytorch_projection_error(
                version=version,
                direction="encode",
                field="task_params.scriptResource",
                reason="resource-wire-name-invalid",
                message=(
                    "PYTORCH resolved resourceName must be one storage path whose "
                    "FILE base ends in /resources before scriptResource"
                ),
            )
        resource = {"resourceName": wire_full_name}
    else:
        raise _pytorch_projection_error(
            version=version,
            direction="encode",
            field="task.type",
            reason="missing-reviewed-wire-epoch",
            message=f"PYTORCH {version} lacks one reviewed resource-script epoch",
        )
    return {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": script_resource.removeprefix("/"),
        "scriptParams": " ".join(script_args),
        launcher_field: python_executable,
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
        "resourceList": [resource],
    }


def _decode_pytorch_literal_resource_script(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Decode only the exact compiler-owned PYTORCH resource-script wire."""
    (
        surface,
        python_executable,
        wire_script_resource,
        script_args,
        resource_id,
        wire_resource_name,
    ) = _decode_pytorch_native_candidate(payload, version=version)
    script_resource = wire_script_resource
    if surface.wire_epoch == "positive-resource-id-python-home":
        if resource_id is None:
            message = "legacy PYTORCH candidate must carry a resource id"
            raise AssertionError(message)
        refs = _require_pytorch_resource_refs(
            resource_refs,
            version=version,
            direction="decode",
        )
        resolved = refs.full_name_by_id.get(resource_id)
        if resolved is None:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0].id",
                reason="resource-id-not-resolved",
                message=(
                    f"PYTORCH resource id {resource_id} was not resolved to one "
                    f"fullName for DolphinScheduler {version}"
                ),
            )
        if resolved != wire_script_resource:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.script",
                reason="script-resource-mismatch",
                message=(
                    "PYTORCH native script must identify the one resolved "
                    "resourceList entry"
                ),
            )
        script_resource = resolved
    elif surface.wire_epoch == "resource-name-python-launcher":
        if wire_resource_name is None:
            message = "modern PYTORCH candidate must carry one resourceName"
            raise AssertionError(message)
        refs = _require_pytorch_resource_refs(
            resource_refs,
            version=version,
            direction="decode",
        )
        expected_wire_name = refs.wire_full_name_by_full_name.get(script_resource)
        if expected_wire_name is None:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0].resourceName",
                reason="resource-full-name-not-resolved",
                message=(
                    "PYTORCH resourceName was not verified against the selected "
                    "profile's exact FILE storage identity"
                ),
            )
        if wire_resource_name != expected_wire_name:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0].resourceName",
                reason="resource-wire-name-mismatch",
                message=(
                    "PYTORCH resourceName does not match the exact resolved "
                    "storage identity for the staged script"
                ),
            )
    return _validate_pytorch_canonical(
        {
            "pythonExecutable": python_executable,
            "scriptResource": script_resource,
            "scriptArgs": script_args,
        },
        version=version,
        direction="decode",
    )


def _decode_pytorch_native_candidate(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
) -> tuple[
    PytorchAuthoringSurface,
    str,
    str,
    list[JsonValue],
    int | None,
    str | None,
]:
    """Validate the exact fixed-point native wire without resolving old ids."""
    surface = _require_pytorch_available(version=version, direction="decode")
    launcher_field = _pytorch_launcher_field(surface, version=version)
    _validate_pytorch_native_fixed_fields(
        payload,
        version=version,
        launcher_field=launcher_field,
    )
    python_executable = _decode_pytorch_python_executable(
        payload[launcher_field],
        version=version,
        launcher_field=launcher_field,
    )
    script_resource = _decode_pytorch_script_resource(
        payload["script"],
        version=version,
    )
    script_args = _decode_pytorch_script_args(payload["scriptParams"], version=version)
    resource_id, wire_resource_name = _decode_pytorch_resource_reference(
        payload["resourceList"],
        version=version,
        surface=surface,
        script_resource=script_resource,
    )
    return (
        surface,
        python_executable,
        script_resource,
        script_args,
        resource_id,
        wire_resource_name,
    )


def _pytorch_launcher_field(
    surface: PytorchAuthoringSurface,
    *,
    version: str,
) -> str:
    """Select the only executable field recognized by one exact wire epoch."""
    if surface.wire_epoch == "positive-resource-id-python-home":
        return "pythonCommand"
    if surface.wire_epoch == "resource-name-python-launcher":
        return "pythonLauncher"
    raise _pytorch_projection_error(
        version=version,
        direction="decode",
        field="task.type",
        reason="missing-reviewed-wire-epoch",
        message=f"PYTORCH {version} lacks one reviewed resource-script epoch",
    )


def _validate_pytorch_native_fixed_fields(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    launcher_field: str,
) -> None:
    """Require the exact key set and every compiler-owned fixed native value."""
    expected_fields = _PYTORCH_COMMON_WIRE_FIELDS | {launcher_field}
    unexpected = sorted(set(payload) - expected_fields)
    if unexpected:
        names = ", ".join(unexpected)
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-resource-script-subset",
            message=f"PYTORCH resource-script projection does not own fields: {names}",
        )
    missing = sorted(expected_fields - set(payload))
    if missing:
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=f"PYTORCH native wire requires task_params.{missing[0]}",
        )
    for field_name, expected in {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
    }.items():
        actual = payload[field_name]
        matches_expected = (
            actual is expected if isinstance(expected, bool) else actual == expected
        )
        if not matches_expected:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="outside-reviewed-literal-resource-script-subset",
                message=(
                    f"PYTORCH typed native {field_name} is owned only as {expected!r}"
                ),
            )


def _decode_pytorch_python_executable(
    value: JsonValue,
    *,
    version: str,
    launcher_field: str,
) -> str:
    """Decode one exact launcher-field value into canonical executable intent."""
    python_executable = value
    if not isinstance(python_executable, str):
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{launcher_field}",
            reason="invalid-type",
            message=f"PYTORCH {launcher_field} must be one string",
        )
    try:
        validate_pytorch_python_executable(python_executable)
    except ValueError as exc:
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{launcher_field}",
            reason="unsafe-python-executable",
            message=f"PYTORCH {launcher_field} is outside the safe absolute subset",
        ) from exc
    return python_executable


def _decode_pytorch_script_resource(
    value: JsonValue,
    *,
    version: str,
) -> str:
    """Decode the relative native script into one absolute DS resource name."""
    script = value
    if not isinstance(script, str) or script.startswith("/"):
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.script",
            reason="invalid-relative-script",
            message="PYTORCH native script must be one relative resource path",
        )
    script_resource = f"/{script}"
    try:
        validate_pytorch_script_resource(script_resource)
    except ValueError as exc:
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.script",
            reason="unsafe-script-resource",
            message="PYTORCH native script is outside the safe resource subset",
        ) from exc
    return script_resource


def _decode_pytorch_resource_reference(
    value: JsonValue,
    *,
    version: str,
    surface: PytorchAuthoringSurface,
    script_resource: str,
) -> tuple[int | None, str | None]:
    """Decode the one exact id or resourceName reference for the script."""
    resources = value
    if not isinstance(resources, list) or len(resources) != 1:
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList",
            reason="invalid-resource-reference",
            message="PYTORCH resourceList must contain exactly one script resource",
        )
    resource = resources[0]
    if not isinstance(resource, dict):
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.resourceList[0]",
            reason="invalid-resource-reference",
            message="PYTORCH resourceList entry must be one exact object",
        )
    if surface.wire_epoch == "positive-resource-id-python-home":
        if set(resource) != {"id"}:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0]",
                reason="invalid-resource-reference",
                message="PYTORCH 3.1.x resourceList must contain one id-only object",
            )
        raw_id = resource["id"]
        if not isinstance(raw_id, int) or isinstance(raw_id, bool) or raw_id <= 0:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0].id",
                reason="invalid-positive-resource-id",
                message="PYTORCH resource id must be one positive integer",
            )
        return raw_id, None
    if surface.wire_epoch == "resource-name-python-launcher":
        if set(resource) != {"resourceName"}:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0]",
                reason="invalid-resource-reference",
                message=(
                    "PYTORCH resourceList must contain one resourceName-only object"
                ),
            )
        resource_name = resource["resourceName"]
        storage_suffix = f"/resources{script_resource}"
        if not isinstance(resource_name, str) or not resource_name.endswith(
            storage_suffix
        ):
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field="task_params.resourceList[0].resourceName",
                reason="script-resource-mismatch",
                message=(
                    "PYTORCH resourceName must be one storage path whose FILE "
                    "base ends in /resources before the staged script resource"
                ),
            )
        return None, resource_name
    message = "available PYTORCH surface must select one reviewed wire epoch"
    raise AssertionError(message)


def _decode_pytorch_script_args(
    wire_args: JsonValue,
    *,
    version: str,
) -> list[JsonValue]:
    """Split only the reversible exact single-space native argument subset."""
    if not isinstance(wire_args, str):
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.scriptParams",
            reason="invalid-type",
            message="PYTORCH native scriptParams must be one string",
        )
    if wire_args == "":
        return []
    arguments = list(wire_args.split(" "))
    if any(not item for item in arguments):
        raise _pytorch_projection_error(
            version=version,
            direction="decode",
            field="task_params.scriptParams",
            reason="ambiguous-argument-spacing",
            message="PYTORCH scriptParams must use one space between safe tokens",
        )
    for index, argument in enumerate(arguments):
        try:
            validate_pytorch_argument(argument)
        except ValueError as exc:
            raise _pytorch_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.scriptParams[{index}]",
                reason="unsafe-shell-token",
                message="PYTORCH scriptParams contains an unsafe shell token",
            ) from exc
    return cast("list[JsonValue]", arguments)


def _validate_pytorch_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate the closed canonical PYTORCH resource-script intent."""
    unexpected = sorted(set(payload) - _PYTORCH_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _pytorch_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-resource-script-subset",
            message=f"PYTORCH resource-script projection does not own fields: {names}",
        )
    values_and_validators = (
        (
            "pythonExecutable",
            validate_pytorch_python_executable,
            "unsafe-python-executable",
        ),
        ("scriptResource", validate_pytorch_script_resource, "unsafe-script-resource"),
    )
    validated: JsonObject = {}
    for field_name, validator, unsafe_reason in values_and_validators:
        value = payload.get(field_name)
        if not isinstance(value, str):
            reason = (
                "missing-required-field"
                if field_name not in payload
                else "invalid-type"
            )
            raise _pytorch_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{field_name}",
                reason=reason,
                message=f"PYTORCH {field_name} must be one string",
            )
        try:
            validator(value)
        except ValueError as exc:
            raise _pytorch_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{field_name}",
                reason=unsafe_reason,
                message=f"PYTORCH {field_name} is outside the safe literal subset",
            ) from exc
        validated[field_name] = value
    raw_args = payload.get("scriptArgs", [])
    if not isinstance(raw_args, list):
        raise _pytorch_projection_error(
            version=version,
            direction=direction,
            field="task_params.scriptArgs",
            reason="invalid-type",
            message="PYTORCH scriptArgs must be one strict list of safe tokens",
        )
    arguments: list[JsonValue] = []
    for index, argument in enumerate(raw_args):
        if not isinstance(argument, str):
            raise _pytorch_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.scriptArgs[{index}]",
                reason="invalid-item-type",
                message="PYTORCH scriptArgs items must be strings",
            )
        try:
            validate_pytorch_argument(argument)
        except ValueError as exc:
            raise _pytorch_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.scriptArgs[{index}]",
                reason="unsafe-shell-token",
                message="PYTORCH scriptArgs contains an unsafe shell token",
            ) from exc
        arguments.append(argument)
    validated["scriptArgs"] = arguments
    return validated


def pytorch_typed_resource_id_candidate(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
) -> int | None:
    """Return an old id only when the full native package is a typed candidate."""
    try:
        surface, _launcher, _script, _args, resource_id, _wire_name = (
            _decode_pytorch_native_candidate(payload, version=version)
        )
    except TaskParameterProjectionError:
        return None
    if surface.wire_epoch != "positive-resource-id-python-home":
        return None
    return resource_id


def pytorch_typed_resource_file_candidate(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
) -> tuple[str, str] | None:
    """Return one modern canonical/wire pair only for an exact typed candidate."""
    try:
        surface, _launcher, script, _args, _resource_id, wire_name = (
            _decode_pytorch_native_candidate(payload, version=version)
        )
    except TaskParameterProjectionError:
        return None
    if surface.wire_epoch != "resource-name-python-launcher" or wire_name is None:
        return None
    return script, wire_name


def _decode_opaque_pytorch_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    resource_refs: TaskResourceRefIndex | None,
) -> DecodedTaskParameters:
    """Canonicalize only the exact PYTORCH resource-script fixed point."""
    _require_pytorch_available(version=version, direction="decode")
    try:
        projected = _decode_pytorch_literal_resource_script(
            payload,
            version=version,
            resource_refs=resource_refs,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("PYTORCH", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("PYTORCH", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _require_pytorch_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> PytorchAuthoringSurface:
    surface = get_task_authoring_surface(version).pytorch
    if surface.available and surface.wire_epoch is not None:
        return surface
    raise _pytorch_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"PYTORCH does not exist in DolphinScheduler {version}",
    )


def _require_pytorch_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    """Guard opaque preservation without exposing the selected wire surface."""
    _require_pytorch_available(version=version, direction=direction)


def _require_pytorch_resource_refs(
    resource_refs: TaskResourceRefIndex | None,
    *,
    version: str,
    direction: ProjectionDirection,
) -> TaskResourceRefIndex:
    if resource_refs is not None:
        return resource_refs
    raise _pytorch_projection_error(
        version=version,
        direction=direction,
        field="task_params.scriptResource",
        reason="resource-reference-context-required",
        message="PYTORCH projection requires resolved exact task resource identities",
    )


def _pytorch_projection_error(
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
        task_type="PYTORCH",
        field=field,
        reason=reason,
        message=message,
    )
