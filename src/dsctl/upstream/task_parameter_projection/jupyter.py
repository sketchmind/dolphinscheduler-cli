from __future__ import annotations

import json
import re
from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN,
    JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
    JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN,
    JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
    JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN,
    JUPYTER_SECRET_PARAMETER_NAMES,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_JUPYTER_CONDA_ENV_NAME_PATTERN = re.compile(
    JUPYTER_CONDA_ENV_NAME_JSON_SCHEMA_PATTERN,
)
_JUPYTER_NOTEBOOK_PATH_PATTERN = re.compile(
    JUPYTER_NOTEBOOK_PATH_JSON_SCHEMA_PATTERN,
)
_JUPYTER_OPTION_TOKEN_PATTERN = re.compile(
    JUPYTER_OPTION_TOKEN_JSON_SCHEMA_PATTERN,
)
_JUPYTER_PARAMETER_KEY_PATTERN = re.compile(
    JUPYTER_PARAMETER_KEY_JSON_SCHEMA_PATTERN,
)
_JUPYTER_PARAMETER_VALUE_PATTERN = re.compile(
    JUPYTER_PARAMETER_VALUE_JSON_SCHEMA_PATTERN,
)


def _looks_canonical_jupyter(*, version: str, payload: JsonObject) -> bool:
    """Recognize markers whose types cannot occur on the exact native wire."""
    if set(payload) - _JUPYTER_OWNED_FIELDS:
        return False
    surface = get_task_authoring_surface(version).jupyter
    parameters = payload.get("parameters")
    canonical_timeout = any(
        isinstance(payload.get(key), int) and not isinstance(payload.get(key), bool)
        for key in ("executionTimeout", "startTimeout")
    )
    return surface.supports_preinstalled_notebook and (
        isinstance(parameters, Mapping) or canonical_timeout
    )


def _encode_jupyter(payload: JsonObject, *, version: str) -> JsonObject:
    """Project portable notebook intent onto the plugin's string-valued wire."""
    _require_jupyter_available(version=version, direction="encode")
    encoded = deepcopy(payload)
    _reject_jupyter_unowned_fields(
        encoded,
        version=version,
        direction="encode",
    )
    _validate_jupyter_canonical_values(
        encoded,
        version=version,
        direction="encode",
        allow_none_options=True,
    )
    parameters_present = "parameters" in encoded
    raw_parameters = encoded.pop("parameters", None)
    if parameters_present:
        if not isinstance(raw_parameters, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in raw_parameters.items()
        ):
            raise _jupyter_projection_error(
                version=version,
                direction="encode",
                field="task_params.parameters",
                reason="invalid-canonical-parameters",
                message="JUPYTER parameters must contain only string keys and values",
            )
        parameters = dict(raw_parameters)
        if parameters:
            encoded["parameters"] = json.dumps(
                parameters,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
    for key in ("kernel", "engine", "executionTimeout", "startTimeout"):
        value = encoded.get(key)
        if value is None:
            encoded.pop(key, None)
            continue
        if key in {"kernel", "engine"}:
            continue
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise _jupyter_projection_error(
                version=version,
                direction="encode",
                field=f"task_params.{key}",
                reason="invalid-canonical-timeout",
                message=f"JUPYTER {key} must be one strict positive integer",
            )
        encoded[key] = str(value)
    return encoded


def _decode_jupyter(payload: JsonObject, *, version: str) -> JsonObject:
    """Restore the exact string-valued wire to portable notebook intent."""
    _require_jupyter_available(version=version, direction="decode")
    decoded = deepcopy(payload)
    _reject_jupyter_unowned_fields(
        decoded,
        version=version,
        direction="decode",
    )
    raw_parameters = decoded.pop("parameters", None)
    if raw_parameters is None or raw_parameters == "":
        decoded["parameters"] = {}
    elif not isinstance(raw_parameters, str):
        raise _jupyter_projection_error(
            version=version,
            direction="decode",
            field="task_params.parameters",
            reason="invalid-native-parameters",
            message="JUPYTER native parameters must be one JSON object string",
        )
    else:
        try:
            parsed_parameters = json.loads(
                raw_parameters,
                parse_constant=_reject_jupyter_json_constant,
                object_pairs_hook=_jupyter_json_object,
            )
        except (json.JSONDecodeError, ValueError) as exc:
            raise _jupyter_projection_error(
                version=version,
                direction="decode",
                field="task_params.parameters",
                reason="invalid-native-parameters",
                message="JUPYTER native parameters must be one JSON object string",
            ) from exc
        if not isinstance(parsed_parameters, dict) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in parsed_parameters.items()
        ):
            raise _jupyter_projection_error(
                version=version,
                direction="decode",
                field="task_params.parameters",
                reason="invalid-native-parameters",
                message="JUPYTER native parameters must contain string values",
            )
        decoded["parameters"] = parsed_parameters
    for key in ("executionTimeout", "startTimeout"):
        if key not in decoded:
            continue
        value = decoded[key]
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not value.isdecimal()
            or value.startswith("0")
        ):
            raise _jupyter_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{key}",
                reason="invalid-native-timeout",
                message=f"JUPYTER native {key} must be one positive decimal string",
            )
        decoded[key] = int(value)
    _validate_jupyter_canonical_values(
        decoded,
        version=version,
        direction="decode",
        allow_none_options=False,
    )
    return decoded


_JUPYTER_OWNED_FIELDS = frozenset(
    {
        "condaEnvName",
        "engine",
        "executionTimeout",
        "inputNotePath",
        "kernel",
        "outputNotePath",
        "parameters",
        "startTimeout",
    }
)


def _reject_jupyter_unowned_fields(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    unexpected = sorted(set(payload) - _JUPYTER_OWNED_FIELDS)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    raise _jupyter_projection_error(
        version=version,
        direction=direction,
        field=f"task_params.{unexpected[0]}",
        reason="outside-reviewed-notebook-subset",
        message=f"JUPYTER notebook projection does not own fields: {names}",
    )


def _validate_jupyter_canonical_values(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
    allow_none_options: bool,
) -> None:
    _validate_jupyter_required_fields_and_paths(
        payload,
        version=version,
        direction=direction,
    )
    if "parameters" in payload:
        _validate_jupyter_parameter_map(
            payload["parameters"],
            version=version,
            direction=direction,
        )
    _validate_jupyter_option_fields(
        payload,
        version=version,
        direction=direction,
        allow_none=allow_none_options,
    )
    _validate_jupyter_timeout_fields(
        payload,
        version=version,
        direction=direction,
        allow_none=allow_none_options,
    )


def _validate_jupyter_required_fields_and_paths(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    required_patterns = {
        "condaEnvName": _JUPYTER_CONDA_ENV_NAME_PATTERN,
        "inputNotePath": _JUPYTER_NOTEBOOK_PATH_PATTERN,
        "outputNotePath": _JUPYTER_NOTEBOOK_PATH_PATTERN,
    }
    for key, pattern in required_patterns.items():
        value = payload.get(key)
        if not isinstance(value, str) or not pattern.fullmatch(value):
            reason = "missing-required-field" if key not in payload else "unsafe-value"
            raise _jupyter_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason=reason,
                message=f"JUPYTER {key} is outside the safe typed subset",
            )
    if payload["inputNotePath"] == payload["outputNotePath"]:
        raise _jupyter_projection_error(
            version=version,
            direction=direction,
            field="task_params.outputNotePath",
            reason="conflicting-notebook-paths",
            message="JUPYTER inputNotePath and outputNotePath must be different",
        )


def _validate_jupyter_option_fields(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
    allow_none: bool,
) -> None:
    for key in ("kernel", "engine"):
        if key not in payload:
            continue
        value = payload[key]
        if value is None and allow_none:
            continue
        if not isinstance(value, str) or not _JUPYTER_OPTION_TOKEN_PATTERN.fullmatch(
            value
        ):
            raise _jupyter_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason="unsafe-value",
                message=f"JUPYTER {key} must be one safe option token",
            )


def _validate_jupyter_timeout_fields(
    payload: Mapping[str, JsonValue],
    *,
    version: str,
    direction: ProjectionDirection,
    allow_none: bool,
) -> None:
    for key in ("executionTimeout", "startTimeout"):
        if key not in payload:
            continue
        value = payload[key]
        if value is None and allow_none:
            continue
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise _jupyter_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason="invalid-canonical-timeout",
                message=f"JUPYTER {key} must be one strict positive integer",
            )


def _validate_jupyter_parameter_map(
    value: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if not isinstance(value, Mapping):
        raise _jupyter_projection_error(
            version=version,
            direction=direction,
            field="task_params.parameters",
            reason="invalid-parameters",
            message="JUPYTER parameters must be one string-to-string object",
        )
    for key, parameter_value in value.items():
        if (
            not isinstance(key, str)
            or not _JUPYTER_PARAMETER_KEY_PATTERN.fullmatch(key)
            or key.lower() in JUPYTER_SECRET_PARAMETER_NAMES
        ):
            field = (
                f"task_params.parameters.{key}"
                if isinstance(key, str)
                and key.lower() in JUPYTER_SECRET_PARAMETER_NAMES
                else "task_params.parameters"
            )
            raise _jupyter_projection_error(
                version=version,
                direction=direction,
                field=field,
                reason="unsafe-parameter-name",
                message="JUPYTER parameter name is outside the safe bounded subset",
            )
        if not isinstance(
            parameter_value, str
        ) or not _JUPYTER_PARAMETER_VALUE_PATTERN.fullmatch(parameter_value):
            raise _jupyter_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.parameters.{key}",
                reason="unsafe-parameter-value",
                message="JUPYTER parameter value is not one safe shell token",
            )


def _reject_jupyter_json_constant(value: str) -> JsonValue:
    message = f"Non-standard JSON constant is unsupported: {value}"
    raise ValueError(message)


def _jupyter_json_object(pairs: list[tuple[str, JsonValue]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            message = f"Duplicate JUPYTER parameter key is unsupported: {key}"
            raise ValueError(message)
        result[key] = value
    return result


def _require_jupyter_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).jupyter.supports_preinstalled_notebook:
        return
    raise _jupyter_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=(
            "JUPYTER does not expose the reviewed preinstalled-notebook contract "
            f"in DolphinScheduler {version}"
        ),
    )


def _jupyter_projection_error(
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
        task_type="JUPYTER",
        field=field,
        reason=reason,
        message=message,
    )
