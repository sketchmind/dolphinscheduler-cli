from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_spark_inline_local_sql,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _decode_canonical_native,
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
    from dsctl.support.json_types import JsonObject


def _decode_opaque_spark_with_provenance(
    payload: JsonObject, *, version: str
) -> DecodedTaskParameters:
    """Canonicalize Spark SQL only on its explicitly reviewed exact profiles."""
    if not get_task_authoring_surface(version).spark_inline_sql.available:
        return DecodedTaskParameters(
            ProjectedTask("SPARK", payload), ProjectionSource.OPAQUE_PRESERVE
        )
    return _decode_canonical_native(
        payload,
        version=version,
        task_type="SPARK",
        decoder=_decode_spark_inline_local_sql,
    )


_SPARK_CANONICAL_FIELDS = frozenset({"rawScript"})


def _encode_spark_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project one portable literal SQL script onto the exact Spark wire."""
    wire_epoch = _require_spark_inline_sql_available(
        version=version,
        direction="encode",
    )
    _reject_spark_unowned_fields(
        payload,
        allowed=_SPARK_CANONICAL_FIELDS,
        version=version,
        direction="encode",
    )
    raw_script = _spark_safe_raw_script(
        payload,
        version=version,
        direction="encode",
    )
    encoded: JsonObject = {
        "programType": "SQL",
        "rawScript": raw_script,
        "deployMode": "local",
    }
    if wire_epoch == "legacy-spark2":
        encoded["sparkVersion"] = "SPARK2"
    else:
        encoded["sqlExecutionType"] = "SCRIPT"
    if wire_epoch == "script-master":
        encoded["master"] = "local"
    return encoded


def _decode_spark_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode only exact worker-local inline SQL evidence into canonical intent."""
    wire_epoch = _require_spark_inline_sql_available(
        version=version,
        direction="decode",
    )
    required_values: dict[str, str] = {
        "programType": "SQL",
        "deployMode": "local",
    }
    if wire_epoch == "legacy-spark2":
        required_values["sparkVersion"] = "SPARK2"
    else:
        required_values["sqlExecutionType"] = "SCRIPT"
    allowed = _SPARK_CANONICAL_FIELDS | frozenset(required_values)
    if wire_epoch == "script-master":
        allowed |= {"master"}
    _reject_spark_unowned_fields(
        payload,
        allowed=allowed,
        version=version,
        direction="decode",
    )
    for key, expected in required_values.items():
        if payload.get(key) == expected:
            continue
        reason = "missing-native-discriminator" if key not in payload else "wrong-mode"
        raise _spark_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{key}",
            reason=reason,
            message=f"SPARK inline local SQL requires {key}={expected}",
        )
    if wire_epoch == "script-master" and payload.get("master", "") not in {
        "",
        "local",
    }:
        raise _spark_projection_error(
            version=version,
            direction="decode",
            field="task_params.master",
            reason="non-local-master",
            message="SPARK inline local SQL requires an empty or local master",
        )
    return {
        "rawScript": _spark_safe_raw_script(
            payload,
            version=version,
            direction="decode",
        )
    }


def _spark_safe_raw_script(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> str:
    value = payload.get("rawScript")
    if not isinstance(value, str):
        reason = (
            "missing-required-field" if "rawScript" not in payload else "invalid-type"
        )
        raise _spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason=reason,
            message="SPARK inline local SQL requires one literal rawScript string",
        )
    try:
        return validate_spark_inline_local_sql(value)
    except ValueError as exc:
        raise _spark_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason="unsafe-inline-sql",
            message="SPARK rawScript is outside the safe literal SQL subset",
        ) from exc


def _reject_spark_unowned_fields(
    payload: JsonObject,
    *,
    allowed: frozenset[str],
    version: str,
    direction: ProjectionDirection,
) -> None:
    unexpected = sorted(set(payload) - allowed)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    raise _spark_projection_error(
        version=version,
        direction=direction,
        field=f"task_params.{unexpected[0]}",
        reason="outside-reviewed-inline-local-sql-subset",
        message=f"SPARK inline local SQL projection does not own fields: {names}",
    )


def _require_spark_inline_sql_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> str:
    surface = get_task_authoring_surface(version).spark_inline_sql
    if surface.available and surface.wire_epoch is not None:
        return surface.wire_epoch
    raise _spark_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="typed-facet-absent-in-version",
        message=f"SPARK inline local SQL is unavailable in DolphinScheduler {version}",
    )


def _spark_projection_error(
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
        task_type="SPARK",
        field=field,
        reason=reason,
        message=message,
    )
