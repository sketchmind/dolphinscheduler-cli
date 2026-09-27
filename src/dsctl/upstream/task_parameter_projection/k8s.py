from __future__ import annotations

import json
from copy import deepcopy

from pydantic import ValidationError

from dsctl.models.task_spec import (
    k8s_literal_container_job_params_model,
)
from dsctl.support.json_types import JsonObject, JsonValue, require_json_object
from dsctl.upstream.task_authoring_surface import (
    K8sAuthoringSurface,
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _compact_json_array,
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


def _decode_opaque_k8s_with_provenance(
    payload: JsonObject, *, version: str
) -> DecodedTaskParameters:
    """Keep native K8S state across the explicitly excluded executor coordinate."""
    if not get_task_authoring_surface(version).k8s.available:
        _require_k8s_plugin_present(version=version, direction="decode")
        return DecodedTaskParameters(
            ProjectedTask("K8S", payload), ProjectionSource.OPAQUE_PRESERVE
        )
    return _decode_canonical_native(
        payload, version=version, task_type="K8S", decoder=_decode_k8s
    )


_K8S_PLUGIN_VERSIONS = frozenset(
    {
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    }
)
_K8S_ADVANCED_CANONICAL_FIELDS = frozenset(
    {
        "command",
        "args",
        "pullSecret",
        "imagePullPolicy",
        "customizedLabels",
        "nodeSelectors",
    }
)
_K8S_ADVANCED_WIRE_FIELDS = frozenset(
    {
        "command",
        "args",
        "pullSecret",
        "imagePullPolicy",
        "customizedLabels",
        "nodeSelectors",
    }
)


def _encode_k8s(payload: JsonObject, *, version: str) -> JsonObject:
    """Compile one stable literal container job to the exact native wire."""
    validated = _validate_k8s_canonical(
        payload,
        version=version,
        direction="encode",
    )
    return _k8s_native_wire(validated, version=version)


def _decode_k8s(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the deterministic K8S wire emitted by this projector."""
    _require_k8s_available(version=version, direction="decode")
    surface = get_task_authoring_surface(version).k8s
    canonical, expected_fields = _decode_k8s_connection(
        payload,
        version=version,
        surface=surface,
    )
    expected_fields.update({"image", "minCpuCores", "minMemorySpace", "localParams"})
    if surface.advanced_container_fields:
        expected_fields.update(_K8S_ADVANCED_WIRE_FIELDS - {"pullSecret"})
        if "pullSecret" in payload:
            expected_fields.add("pullSecret")
    if set(payload) != expected_fields:
        unexpected = sorted(set(payload).symmetric_difference(expected_fields))
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="outside-reviewed-native-fixed-point",
            message=(
                "K8S native task params do not match the reviewed fixed point: "
                f"{', '.join(unexpected)}"
            ),
        )
    canonical.update(
        {
            "image": deepcopy(payload["image"]),
            "minCpuCores": deepcopy(payload["minCpuCores"]),
            "minMemorySpace": deepcopy(payload["minMemorySpace"]),
        }
    )
    environment, outputs = _decode_k8s_local_params(
        payload["localParams"],
        version=version,
    )
    canonical["environment"] = environment
    if surface.output_transport_supported:
        canonical["outputs"] = outputs
    elif outputs:
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params.localParams",
            reason="output-transport-unavailable",
            message=f"K8S OUT localParams are not transported on {version}",
        )
    if surface.advanced_container_fields:
        canonical.update(_decode_k8s_advanced_wire(payload, version=version))
    validated = _validate_k8s_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    expected = _k8s_native_wire(validated, version=version)
    if payload != expected:
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="noncanonical-native-wire",
            message="K8S native task params are not this projector's exact fixed point",
        )
    return validated


def _decode_k8s_connection(
    payload: JsonObject,
    *,
    version: str,
    surface: K8sAuthoringSurface,
) -> tuple[JsonObject, set[str]]:
    """Decode the exact connection discriminator and its native fields."""
    expected_fields = {
        "namespace",
    }
    if surface.connection_mode == "NAMESPACE":
        namespace_value = payload.get("namespace")
        if not isinstance(namespace_value, str):
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field="task_params.namespace",
                reason="invalid-native-namespace",
                message="K8S native namespace must be one compact JSON string",
            )
        namespace = _parse_k8s_json_object(
            namespace_value,
            version=version,
            field="task_params.namespace",
        )
        if set(namespace) != {"name", "cluster"} or not all(
            isinstance(namespace.get(key), str) for key in ("name", "cluster")
        ):
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field="task_params.namespace",
                reason="invalid-native-namespace",
                message="K8S native namespace must contain exact name and cluster",
            )
        return (
            {
                "connectionMode": "NAMESPACE",
                "namespace": deepcopy(namespace["name"]),
                "cluster": deepcopy(namespace["cluster"]),
            },
            expected_fields,
        )
    if surface.connection_mode != "DATASOURCE":
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task.type",
            reason="task-type-absent-in-version",
            message=f"K8S typed decoding is unavailable for {version}",
        )
    expected_fields.update({"datasource", "type", "kubeConfig"})
    if (
        payload.get("type") != "K8S"
        or payload.get("namespace") != ""
        or payload.get("kubeConfig") != ""
    ):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params.connectionMode",
            reason="invalid-native-datasource-wire",
            message=(
                "K8S datasource wire requires type='K8S' and empty compiler-owned "
                "namespace and kubeConfig"
            ),
        )
    return (
        {
            "connectionMode": "DATASOURCE",
            "datasource": deepcopy(payload.get("datasource")),
        },
        expected_fields,
    )


def _validate_k8s_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate one exact-profile K8S canonical payload and publish defaults."""
    _require_k8s_available(version=version, direction=direction)
    surface = get_task_authoring_surface(version).k8s
    model = k8s_literal_container_job_params_model(
        advanced_container_fields=surface.advanced_container_fields,
        empty_customized_labels_supported=(surface.empty_customized_labels_supported),
        output_transport_supported=surface.output_transport_supported,
    )
    try:
        validated = model.model_validate(payload).to_payload()
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = ".".join(str(part) for part in error["loc"])
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-literal-container-job-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _k8s_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{location}" if location else "task_params",
            reason=reason,
            message=f"K8S typed task params are invalid: {error['msg']}",
        ) from exc
    mode = validated.get("connectionMode")
    if mode != surface.connection_mode:
        raise _k8s_projection_error(
            version=version,
            direction=direction,
            field="task_params.connectionMode",
            reason="connection-mode-absent-in-version",
            message=f"K8S {version} requires connectionMode={surface.connection_mode}",
        )
    advanced = sorted(_K8S_ADVANCED_CANONICAL_FIELDS.intersection(validated))
    if not surface.advanced_container_fields and advanced:
        raise _k8s_projection_error(
            version=version,
            direction=direction,
            field="task_params",
            reason="container-options-absent-in-version",
            message=f"K8S {version} does not support: {', '.join(advanced)}",
        )
    if not surface.output_transport_supported and "outputs" in validated:
        raise _k8s_projection_error(
            version=version,
            direction=direction,
            field="task_params.outputs",
            reason="output-transport-unavailable",
            message=f"K8S task outputs are unavailable on {version}",
        )
    return require_json_object(validated, label="validated K8S task_params")


def _k8s_native_wire(payload: JsonObject, *, version: str) -> JsonObject:
    """Build one deterministic exact native K8S task-parameter object."""
    surface = get_task_authoring_surface(version).k8s
    wire: JsonObject = {}
    if surface.connection_mode == "NAMESPACE":
        wire["namespace"] = json.dumps(
            {"name": payload["namespace"], "cluster": payload["cluster"]},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    else:
        wire.update(
            {
                "datasource": deepcopy(payload["datasource"]),
                "type": "K8S",
                "namespace": "",
                "kubeConfig": "",
            }
        )
    wire.update(
        {
            "image": deepcopy(payload["image"]),
            "minCpuCores": deepcopy(payload["minCpuCores"]),
            "minMemorySpace": deepcopy(payload["minMemorySpace"]),
            "localParams": _encode_k8s_local_params(payload),
        }
    )
    if surface.advanced_container_fields:
        wire.update(
            {
                "command": _compact_json_array(payload["command"]),
                "args": _compact_json_array(payload["args"]),
                "imagePullPolicy": deepcopy(payload["imagePullPolicy"]),
                "customizedLabels": deepcopy(payload["customizedLabels"]),
                "nodeSelectors": _encode_k8s_node_selectors(payload["nodeSelectors"]),
            }
        )
        if "pullSecret" in payload:
            wire["pullSecret"] = deepcopy(payload["pullSecret"])
    return wire


def _encode_k8s_local_params(payload: JsonObject) -> list[JsonValue]:
    environment = payload.get("environment")
    outputs = payload.get("outputs", [])
    if not isinstance(environment, list) or not isinstance(outputs, list):
        message = "Validated K8S environment and outputs must be arrays"
        raise TypeError(message)
    result: list[JsonValue] = []
    for item in environment:
        if not isinstance(item, dict):
            message = "Validated K8S environment item must be an object"
            raise TypeError(message)
        result.append(
            {
                "prop": deepcopy(item["name"]),
                "direct": "IN",
                "type": "VARCHAR",
                "value": deepcopy(item["value"]),
            }
        )
    for item in outputs:
        if not isinstance(item, dict):
            message = "Validated K8S output item must be an object"
            raise TypeError(message)
        result.append(
            {
                "prop": deepcopy(item["name"]),
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "",
            }
        )
    return result


def _decode_k8s_local_params(
    value: JsonValue,
    *,
    version: str,
) -> tuple[list[JsonValue], list[JsonValue]]:
    if not isinstance(value, list):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params.localParams",
            reason="invalid-native-local-params",
            message="K8S native localParams must be an array",
        )
    environment: list[JsonValue] = []
    outputs: list[JsonValue] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {
            "prop",
            "direct",
            "type",
            "value",
        }:
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.localParams.{index}",
                reason="invalid-native-local-param",
                message="K8S localParams entries must use the exact four-field wire",
            )
        name = item.get("prop")
        direct = item.get("direct")
        if not isinstance(name, str) or item.get("type") != "VARCHAR":
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.localParams.{index}",
                reason="invalid-native-local-param",
                message="K8S localParams require string names and VARCHAR type",
            )
        if direct == "IN" and isinstance(item.get("value"), str):
            environment.append({"name": name, "value": deepcopy(item["value"])})
        elif direct == "OUT" and item.get("value") == "":
            outputs.append({"name": name})
        else:
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.localParams.{index}",
                reason="invalid-native-local-param",
                message="K8S localParams are outside the reviewed IN/OUT VARCHAR wire",
            )
    return environment, outputs


def _decode_k8s_advanced_wire(payload: JsonObject, *, version: str) -> JsonObject:
    command = _decode_k8s_json_array(
        payload["command"], version=version, field="command"
    )
    args = _decode_k8s_json_array(payload["args"], version=version, field="args")
    labels = payload["customizedLabels"]
    selectors = payload["nodeSelectors"]
    if not isinstance(labels, list) or not isinstance(selectors, list):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field="task_params",
            reason="invalid-native-container-options",
            message="K8S labels and nodeSelectors must be arrays",
        )
    canonical: JsonObject = {
        "command": command,
        "args": args,
        "imagePullPolicy": deepcopy(payload["imagePullPolicy"]),
        "customizedLabels": deepcopy(labels),
        "nodeSelectors": _decode_k8s_node_selectors(selectors, version=version),
    }
    if "pullSecret" in payload:
        canonical["pullSecret"] = deepcopy(payload["pullSecret"])
    return canonical


def _decode_k8s_json_array(
    value: JsonValue,
    *,
    version: str,
    field: str,
) -> list[JsonValue]:
    if not isinstance(value, str):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{field}",
            reason="invalid-native-argv",
            message=f"K8S native {field} must be one compact JSON array string",
        )
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{field}",
            reason="invalid-native-argv",
            message=f"K8S native {field} must be valid JSON",
        ) from exc
    if not isinstance(parsed, list):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{field}",
            reason="invalid-native-argv",
            message=f"K8S native {field} must decode to an array",
        )
    return deepcopy(parsed)


def _encode_k8s_node_selectors(value: JsonValue) -> list[JsonValue]:
    if not isinstance(value, list):
        message = "Validated K8S nodeSelectors must be an array"
        raise TypeError(message)
    encoded: list[JsonValue] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("values"), list):
            message = "Validated K8S nodeSelector must be an object with values"
            raise TypeError(message)
        values = item["values"]
        if not all(isinstance(part, str) for part in values):
            message = "Validated K8S nodeSelector values must be strings"
            raise TypeError(message)
        encoded.append(
            {
                "key": deepcopy(item["key"]),
                "operator": deepcopy(item["operator"]),
                "values": ",".join(values),
            }
        )
    return encoded


def _decode_k8s_node_selectors(
    value: list[JsonValue],
    *,
    version: str,
) -> list[JsonValue]:
    decoded: list[JsonValue] = []
    for index, item in enumerate(value):
        if (
            not isinstance(item, dict)
            or set(item) != {"key", "operator", "values"}
            or not isinstance(item.get("values"), str)
        ):
            raise _k8s_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.nodeSelectors.{index}",
                reason="invalid-native-node-selector",
                message="K8S nodeSelectors require exact key/operator/values strings",
            )
        raw_values = item["values"]
        operator = item.get("operator")
        decoded.append(
            {
                "key": deepcopy(item["key"]),
                "operator": deepcopy(operator),
                "values": (
                    [""]
                    if raw_values == "" and operator in {"In", "NotIn"}
                    else []
                    if raw_values == ""
                    else raw_values.split(",")
                ),
            }
        )
    return decoded


def _parse_k8s_json_object(
    value: str,
    *,
    version: str,
    field: str,
) -> JsonObject:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field=field,
            reason="invalid-native-json",
            message=f"K8S native {field} must be valid JSON",
        ) from exc
    if not isinstance(parsed, dict) or not all(isinstance(key, str) for key in parsed):
        raise _k8s_projection_error(
            version=version,
            direction="decode",
            field=field,
            reason="invalid-native-json",
            message=f"K8S native {field} must decode to one object",
        )
    return deepcopy(parsed)


def _require_k8s_plugin_present(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if version in _K8S_PLUGIN_VERSIONS:
        return
    raise _k8s_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"K8S does not exist in DolphinScheduler {version}",
    )


def _require_k8s_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).k8s.available:
        return
    _require_k8s_plugin_present(version=version, direction=direction)
    raise _k8s_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="upstream-runtime-hole",
        message=f"K8S typed authoring is unsafe on DolphinScheduler {version}",
    )


def _k8s_projection_error(
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
        task_type="K8S",
        field=field,
        reason=reason,
        message=message,
    )
