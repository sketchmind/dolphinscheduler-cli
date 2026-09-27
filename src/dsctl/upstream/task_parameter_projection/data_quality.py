from __future__ import annotations

import re
from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec,
    DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, JsonValue, require_json_object
from dsctl.upstream.task_authoring_surface import (
    DataQualityAuthoringSurface,
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _copy_json_object,
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

_DATA_QUALITY_NATIVE_FIELDS = frozenset(
    {"localParams", "ruleId", "ruleInputParameter", "sparkParameters"}
)
_DATA_QUALITY_RULE_INPUT_FIELDS = frozenset(
    {
        "src_connector_type",
        "src_datasource_id",
        "src_table",
        "src_filter",
        "statistics_name",
        "comparison_type",
        "comparison_name",
        "check_type",
        "threshold",
        "failure_strategy",
        "operator",
    }
)
_DATA_QUALITY_SPARK_FIELDS = frozenset({"deployMode", "programType", "mainClass"})
_DATA_QUALITY_MAIN_CLASS = (
    "org.apache.dolphinscheduler.data.quality.DataQualityApplication"
)
_DATA_QUALITY_CANONICAL_DECIMAL_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_DATA_QUALITY_POSITIVE_DECIMAL_PATTERN = re.compile(r"[1-9][0-9]*\Z")
_DATA_QUALITY_MODEL_FIELD_ALIASES: Mapping[str, str] = MappingProxyType(
    {"expected_row_count": "expectedRowCount"}
)


def _encode_data_quality(payload: JsonObject, *, version: str) -> JsonObject:
    """Compile one closed table-count equality intent to its exact DQ wire."""
    validated = _validate_data_quality_canonical(
        payload,
        version=version,
        direction="encode",
    )
    return _data_quality_native_wire(validated, version=version)


def _decode_data_quality(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact compiler-owned DQ table-count fixed point."""
    surface = _require_data_quality_available(version=version, direction="decode")
    if set(payload) != _DATA_QUALITY_NATIVE_FIELDS:
        differences = sorted(
            set(payload).symmetric_difference(_DATA_QUALITY_NATIVE_FIELDS)
        )
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="outside-reviewed-native-fixed-point",
            message=(
                "DATA_QUALITY native task params do not match the reviewed fixed "
                f"point: {', '.join(differences)}"
            ),
        )
    rule_input = _data_quality_native_object(
        payload.get("ruleInputParameter"),
        version=version,
        field="ruleInputParameter",
    )
    expected_rule_fields = set(_DATA_QUALITY_RULE_INPUT_FIELDS)
    if surface.database_wire_required:
        expected_rule_fields.add("src_database")
    if set(rule_input) != expected_rule_fields:
        differences = sorted(set(rule_input).symmetric_difference(expected_rule_fields))
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field="task_params.ruleInputParameter",
            reason="outside-reviewed-rule-input-fixed-point",
            message=(
                "DATA_QUALITY native ruleInputParameter does not match the reviewed "
                f"fixed point: {', '.join(differences)}"
            ),
        )
    _data_quality_native_object(
        payload.get("sparkParameters"),
        version=version,
        field="sparkParameters",
        expected_fields=_DATA_QUALITY_SPARK_FIELDS,
    )
    canonical: JsonObject = {
        "datasource": _data_quality_native_decimal(
            rule_input.get("src_datasource_id"),
            version=version,
            field="src_datasource_id",
            positive=True,
        ),
        "table": deepcopy(rule_input.get("src_table")),
        "expectedRowCount": _data_quality_native_decimal(
            rule_input.get("comparison_name"),
            version=version,
            field="comparison_name",
            positive=False,
        ),
    }
    if surface.database_wire_required:
        canonical["database"] = deepcopy(rule_input.get("src_database"))
    validated = _validate_data_quality_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    expected = _data_quality_native_wire(validated, version=version)
    if payload != expected:
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="noncanonical-native-wire",
            message=(
                "DATA_QUALITY native task params are not this projector's exact "
                "table-count equality fixed point"
            ),
        )
    return require_json_object(
        validated.to_payload(),
        label="validated DATA_QUALITY task_params",
    )


def _decode_opaque_data_quality_with_provenance(
    payload: JsonObject,
    *,
    version: str,
) -> DecodedTaskParameters:
    """Canonicalize only the exact DQ fixed point and preserve richer state."""
    _require_data_quality_available(version=version, direction="decode")
    try:
        projected = _decode_data_quality(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("DATA_QUALITY", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("DATA_QUALITY", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _data_quality_native_wire(
    validated: (
        DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec
        | DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec
    ),
    *,
    version: str,
) -> JsonObject:
    """Materialize every compiler-owned native field for one exact epoch."""
    surface = _require_data_quality_available(version=version, direction="encode")
    if surface.result_operator == "EQ":
        operator = "0"
    elif surface.result_operator == "NE":
        operator = "5"
    else:
        operator = None
    if (
        surface.rule_id != 10
        or surface.main_class != _DATA_QUALITY_MAIN_CLASS
        or not surface.fixed_value_comparison
        or not surface.blocking_failure_strategy
        or operator is None
    ):
        message = "Available DATA_QUALITY surface lacks its fixed projection contract"
        raise RuntimeError(message)
    rule_input: JsonObject = {
        "src_connector_type": "0",
        "src_datasource_id": str(validated.datasource),
        "src_table": validated.table,
        "src_filter": "",
        "statistics_name": "table_count.total",
        "comparison_type": "1",
        "comparison_name": str(validated.expected_row_count),
        "check_type": "0",
        "threshold": "0",
        "failure_strategy": "1",
        "operator": operator,
    }
    if surface.database_wire_required:
        if not isinstance(
            validated,
            DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec,
        ):
            message = "Database-wire DATA_QUALITY epoch used the legacy model"
            raise RuntimeError(message)
        rule_input["src_database"] = validated.database
    return {
        "localParams": [],
        "ruleId": surface.rule_id,
        "ruleInputParameter": rule_input,
        "sparkParameters": {
            "deployMode": "local",
            "programType": "JAVA",
            "mainClass": surface.main_class,
        },
    }


def _data_quality_native_object(
    value: JsonValue,
    *,
    version: str,
    field: str,
    expected_fields: frozenset[str] | None = None,
) -> JsonObject:
    """Require one nested native DQ object and optionally its exact keys."""
    if not isinstance(value, dict):
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{field}",
            reason="invalid-native-object",
            message=f"DATA_QUALITY native {field} must be one JSON object",
        )
    projected = _copy_json_object(value, label=f"DATA_QUALITY native {field}")
    if expected_fields is not None and set(projected) != expected_fields:
        differences = sorted(set(projected).symmetric_difference(expected_fields))
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{field}",
            reason="outside-reviewed-native-fixed-point",
            message=(
                f"DATA_QUALITY native {field} does not match the reviewed fixed "
                f"point: {', '.join(differences)}"
            ),
        )
    return projected


def _data_quality_native_decimal(
    value: JsonValue,
    *,
    version: str,
    field: str,
    positive: bool,
) -> int:
    """Parse only the canonical decimal spelling emitted by this projector."""
    pattern = (
        _DATA_QUALITY_POSITIVE_DECIMAL_PATTERN
        if positive
        else _DATA_QUALITY_CANONICAL_DECIMAL_PATTERN
    )
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise _data_quality_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.ruleInputParameter.{field}",
            reason="invalid-native-decimal",
            message=(
                f"DATA_QUALITY native {field} must use the compiler-owned decimal "
                "integer spelling"
            ),
        )
    return int(value)


def _validate_data_quality_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> (
    DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec
    | DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec
):
    """Apply the closed exact-epoch DQ canonical model with stable errors."""
    surface = _require_data_quality_available(version=version, direction=direction)
    model = (
        DataQualityLocalMysqlTableRowCountEqualsTaskParamsSpec
        if surface.database_wire_required
        else DataQualityLegacyLocalMysqlTableRowCountEqualsTaskParamsSpec
    )
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params"
        for part in location:
            if isinstance(part, int):
                field += f"[{part}]"
            else:
                field += f".{_DATA_QUALITY_MODEL_FIELD_ALIASES.get(part, part)}"
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-table-count-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _data_quality_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"DATA_QUALITY typed task params are invalid: {error['msg']}",
        ) from exc


def _require_data_quality_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> DataQualityAuthoringSurface:
    """Return the exact DQ surface or reject upstream-absent profiles."""
    surface = get_task_authoring_surface(version).data_quality
    if surface.available:
        return surface
    raise _data_quality_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DATA_QUALITY does not exist in DolphinScheduler {version}",
    )


def _data_quality_projection_error(
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
        task_type="DATA_QUALITY",
        field=field,
        reason=reason,
        message=message,
    )


def _guard_data_quality_present(
    *, version: str, direction: ProjectionDirection
) -> None:
    """Require the reviewed plugin without exposing its surface lookup result."""
    _require_data_quality_available(version=version, direction=direction)
