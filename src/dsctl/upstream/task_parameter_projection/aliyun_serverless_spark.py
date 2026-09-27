from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_aliyun_serverless_spark_argument,
    validate_aliyun_serverless_spark_entry_point,
    validate_aliyun_serverless_spark_literal,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_ALIYUN_SERVERLESS_SPARK_CANONICAL_FIELDS = frozenset(
    {
        "datasource",
        "workspaceId",
        "resourceQueueId",
        "jobName",
        "entryPoint",
        "entryPointArguments",
        "sparkSubmitParameters",
        "isProduction",
    }
)
_ALIYUN_SERVERLESS_SPARK_LITERAL_FIELDS = (
    "workspaceId",
    "resourceQueueId",
    "jobName",
    "sparkSubmitParameters",
)
_ALIYUN_SERVERLESS_SPARK_WIRE_FIELDS = _ALIYUN_SERVERLESS_SPARK_CANONICAL_FIELDS | {
    "type",
    "codeType",
}


def _encode_aliyun_serverless_spark(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project literal JAR intent onto the exact hash-delimited native wire."""
    _require_aliyun_serverless_spark_available(
        version=version,
        direction="encode",
    )
    canonical = _validate_aliyun_serverless_spark_canonical(
        payload,
        version=version,
        direction="encode",
    )
    raw_arguments = canonical.pop("entryPointArguments")
    if not isinstance(raw_arguments, list):
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction="encode",
            field="task_params.entryPointArguments",
            reason="invalid-type",
            message="entryPointArguments must be one strict list",
        )
    canonical["entryPointArguments"] = "#".join(raw_arguments)
    canonical["type"] = "ALIYUN_SERVERLESS_SPARK"
    canonical["codeType"] = "JAR"
    return canonical


def _decode_aliyun_serverless_spark(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode only the exact reviewed literal JAR wire into canonical intent."""
    _require_aliyun_serverless_spark_available(
        version=version,
        direction="decode",
    )
    unexpected = sorted(set(payload) - _ALIYUN_SERVERLESS_SPARK_WIRE_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-jar-subset",
            message=(
                "ALIYUN_SERVERLESS_SPARK literal JAR projection does not own "
                f"fields: {names}"
            ),
        )
    for key, expected in {
        "type": "ALIYUN_SERVERLESS_SPARK",
        "codeType": "JAR",
    }.items():
        if payload.get(key) != expected:
            reason = (
                "missing-native-discriminator" if key not in payload else "wrong-mode"
            )
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{key}",
                reason=reason,
                message=(
                    f"ALIYUN_SERVERLESS_SPARK literal JAR requires {key}={expected}"
                ),
            )
    raw_arguments = payload.get("entryPointArguments")
    if not isinstance(raw_arguments, str):
        reason = (
            "missing-required-field"
            if "entryPointArguments" not in payload
            else "invalid-type"
        )
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction="decode",
            field="task_params.entryPointArguments",
            reason=reason,
            message="Native entryPointArguments must be one hash-delimited string",
        )
    canonical = {
        key: deepcopy(value)
        for key, value in payload.items()
        if key not in {"type", "codeType", "entryPointArguments"}
    }
    canonical["entryPointArguments"] = raw_arguments.split("#")
    return _validate_aliyun_serverless_spark_canonical(
        canonical,
        version=version,
        direction="decode",
    )


def _validate_aliyun_serverless_spark_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    _reject_aliyun_serverless_spark_unowned_fields(
        payload,
        version=version,
        direction=direction,
    )
    projected = deepcopy(payload)
    _validate_aliyun_serverless_spark_datasource(
        projected,
        version=version,
        direction=direction,
    )
    _validate_aliyun_serverless_spark_literals(
        projected,
        version=version,
        direction=direction,
    )
    _validate_aliyun_serverless_spark_entry_point_field(
        projected,
        version=version,
        direction=direction,
    )
    _validate_aliyun_serverless_spark_arguments(
        projected,
        version=version,
        direction=direction,
    )
    _validate_aliyun_serverless_spark_production(
        projected,
        version=version,
        direction=direction,
    )
    return projected


def _reject_aliyun_serverless_spark_unowned_fields(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    unexpected = sorted(set(payload) - _ALIYUN_SERVERLESS_SPARK_CANONICAL_FIELDS)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    raise _aliyun_serverless_spark_projection_error(
        version=version,
        direction=direction,
        field=f"task_params.{unexpected[0]}",
        reason="outside-reviewed-literal-jar-subset",
        message=(
            "ALIYUN_SERVERLESS_SPARK literal JAR projection does not own "
            f"fields: {names}"
        ),
    )


def _validate_aliyun_serverless_spark_datasource(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    datasource = payload.get("datasource")
    if (
        not isinstance(datasource, int)
        or isinstance(datasource, bool)
        or datasource <= 0
    ):
        reason = (
            "missing-required-field" if "datasource" not in payload else "invalid-type"
        )
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.datasource",
            reason=reason,
            message="datasource must be one strict positive integer id",
        )


def _validate_aliyun_serverless_spark_literals(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    for key in _ALIYUN_SERVERLESS_SPARK_LITERAL_FIELDS:
        value = payload.get(key)
        if not isinstance(value, str):
            reason = "missing-required-field" if key not in payload else "invalid-type"
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason=reason,
                message=f"{key} must be one literal string",
            )
        try:
            validate_aliyun_serverless_spark_literal(value, field=key)
        except ValueError as exc:
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason="unsafe-literal",
                message=f"{key} is outside the safe literal task subset",
            ) from exc


def _validate_aliyun_serverless_spark_entry_point_field(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    entry_point = payload.get("entryPoint")
    if not isinstance(entry_point, str):
        reason = (
            "missing-required-field" if "entryPoint" not in payload else "invalid-type"
        )
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.entryPoint",
            reason=reason,
            message="entryPoint must be one safe absolute oss:// object URI",
        )
    try:
        validate_aliyun_serverless_spark_entry_point(entry_point)
    except ValueError as exc:
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.entryPoint",
            reason="unsafe-oss-entry-point",
            message="entryPoint is outside the safe absolute OSS URI subset",
        ) from exc


def _validate_aliyun_serverless_spark_arguments(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    arguments = payload.get("entryPointArguments")
    if not isinstance(arguments, list) or not arguments:
        reason = (
            "missing-required-field"
            if "entryPointArguments" not in payload
            else "invalid-type"
        )
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.entryPointArguments",
            reason=reason,
            message="entryPointArguments must be one non-empty strict list",
        )
    for argument in arguments:
        if not isinstance(argument, str):
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction=direction,
                field="task_params.entryPointArguments",
                reason="invalid-item-type",
                message="entryPointArguments items must be literal strings",
            )
        try:
            validate_aliyun_serverless_spark_argument(argument)
        except ValueError as exc:
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction=direction,
                field="task_params.entryPointArguments",
                reason="ambiguous-or-unsafe-argument",
                message=(
                    "entryPointArguments contains an ambiguous or unsafe literal item"
                ),
            ) from exc


def _validate_aliyun_serverless_spark_production(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if "isProduction" not in payload:
        if direction == "encode":
            payload["isProduction"] = False
        else:
            raise _aliyun_serverless_spark_projection_error(
                version=version,
                direction=direction,
                field="task_params.isProduction",
                reason="missing-required-wire-field",
                message="Native literal JAR wire requires explicit isProduction",
            )
    production = payload["isProduction"]
    if not isinstance(production, bool):
        raise _aliyun_serverless_spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.isProduction",
            reason="invalid-type",
            message="isProduction must be one strict boolean",
        )


def _require_aliyun_serverless_spark_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).aliyun_serverless_spark.available:
        return
    raise _aliyun_serverless_spark_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=(
            f"ALIYUN_SERVERLESS_SPARK does not exist in DolphinScheduler {version}"
        ),
    )


def _aliyun_serverless_spark_projection_error(
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
        task_type="ALIYUN_SERVERLESS_SPARK",
        field=field,
        reason=reason,
        message=message,
    )
