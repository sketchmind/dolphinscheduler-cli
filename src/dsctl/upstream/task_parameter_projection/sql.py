from __future__ import annotations

from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from pydantic import ValidationError

from dsctl.models.task_spec import (
    Sql139InlineTaskParamsSpec,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _json_value_exact_equal,
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
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue


def _decode_opaque_sql_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize only the exact safe SQL package and preserve richer state."""
    try:
        projected = _decode_sql(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("SQL", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("SQL", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


_SQL_139_CANONICAL_FIELDS = frozenset(
    {
        "type",
        "datasource",
        "sql",
        "sqlType",
        "displayRows",
        "connParams",
        "preStatements",
        "postStatements",
        "limit",
        "localParams",
    }
)
_SQL_139_FIXED_NATIVE_VALUES: Mapping[str, JsonValue] = MappingProxyType(
    {
        "sendEmail": False,
        "udfs": "",
        "showType": "TABLE",
        "title": "",
        "receivers": "",
        "receiversCc": "",
    }
)
_SQL_139_NATIVE_FIELDS = _SQL_139_CANONICAL_FIELDS | frozenset(
    _SQL_139_FIXED_NATIVE_VALUES
)


def is_sql_139_complete_native_package(
    payload: Mapping[str, JsonValue],
) -> bool:
    """Recognize an intentional full native 1.3.9 SQL parameter package."""
    return _SQL_139_NATIVE_FIELDS.issubset(payload)


def _encode_sql(payload: JsonObject, *, version: str) -> JsonObject:
    """Project canonical SQL onto the closed exact 1.3.9 legacy package."""
    if version != "1.3.9":
        return payload
    canonical = _validate_sql_139_canonical(
        payload,
        direction="encode",
    )
    projected = deepcopy(canonical)
    projected.update(deepcopy(dict(_SQL_139_FIXED_NATIVE_VALUES)))
    return projected


def _decode_sql(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact compiler-owned 1.3.9 SQL native package."""
    if version != "1.3.9":
        return payload
    missing = sorted(_SQL_139_NATIVE_FIELDS - set(payload))
    unexpected = sorted(set(payload) - _SQL_139_NATIVE_FIELDS)
    if missing or unexpected:
        field = missing[0] if missing else unexpected[0]
        reason = "missing-native-field" if missing else "outside-reviewed-sql-subset"
        message = (
            f"SQL 1.3.9 native task params are missing field {field!r}"
            if missing
            else f"SQL 1.3.9 typed projection does not own field {field!r}"
        )
        raise _sql_139_projection_error(
            direction="decode",
            field=f"task_params.{field}",
            reason=reason,
            message=message,
        )
    for field, expected in _SQL_139_FIXED_NATIVE_VALUES.items():
        if not _json_value_exact_equal(payload[field], expected):
            raise _sql_139_projection_error(
                direction="decode",
                field=f"task_params.{field}",
                reason="richer-native-sql-state",
                message=(
                    f"SQL 1.3.9 native field {field!r} is outside the safe typed "
                    "package and must remain opaque"
                ),
            )
    canonical_input = {
        field: deepcopy(payload[field]) for field in _SQL_139_CANONICAL_FIELDS
    }
    canonical = _validate_sql_139_canonical(canonical_input, direction="decode")
    if not _json_value_exact_equal(
        _encode_sql(canonical, version="1.3.9"),
        payload,
    ):
        raise _sql_139_projection_error(
            direction="decode",
            field="task_params",
            reason="non-canonical-native-spelling",
            message=(
                "SQL 1.3.9 native task params would change under typed "
                "normalization and must remain opaque"
            ),
        )
    return canonical


def _validate_sql_139_canonical(
    payload: JsonObject,
    *,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate and normalize the closed exact 1.3.9 SQL canonical shape."""
    unexpected = sorted(set(payload) - _SQL_139_CANONICAL_FIELDS)
    if unexpected:
        field = unexpected[0]
        raise _sql_139_projection_error(
            direction=direction,
            field=f"task_params.{field}",
            reason="outside-reviewed-sql-subset",
            message=f"SQL 1.3.9 typed projection does not own field {field!r}",
        )
    try:
        validated = Sql139InlineTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params" + "".join(
            f"[{part}]" if isinstance(part, int) else f".{part}" for part in location
        )
        raise _sql_139_projection_error(
            direction=direction,
            field=field,
            reason=(
                "missing-required-field"
                if error["type"] == "missing"
                else "invalid-canonical-value"
            ),
            message=f"SQL 1.3.9 typed task params are invalid: {error['msg']}",
        ) from exc
    normalized = validated.to_payload()
    if direction == "decode" and normalized != payload:
        raise _sql_139_projection_error(
            direction=direction,
            field="task_params",
            reason="non-canonical-native-spelling",
            message=(
                "SQL 1.3.9 native task params would change under typed "
                "normalization and must remain opaque"
            ),
        )
    return cast("JsonObject", normalized)


def _sql_139_projection_error(
    *,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version="1.3.9",
        direction=direction,
        task_type="SQL",
        field=field,
        reason=reason,
        message=message,
    )
