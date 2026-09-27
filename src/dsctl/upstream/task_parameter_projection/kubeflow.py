from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    KubeflowTfjobManifestTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, require_json_object
from dsctl.upstream.kubeflow_manifest import kubeflow_tfjob_identity_template
from dsctl.upstream.task_authoring_surface import (
    KubeflowAuthoringSurface,
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_KUBEFLOW_NATIVE_FIELDS = frozenset({"namespace", "yamlContent"})
_KUBEFLOW_UI_EMPTY_FIELDS = frozenset({"localParams", "resourceList"})


def _encode_kubeflow(payload: JsonObject, *, version: str) -> JsonObject:
    """Project one guarded TFJob manifest onto the legacy namespace wire."""
    _require_kubeflow_available(version=version, direction="encode")
    validated = _validate_kubeflow_canonical(
        payload,
        version=version,
        direction="encode",
    )
    return {
        "yamlContent": validated.yaml_content,
        "namespace": json.dumps(
            {"name": validated.namespace, "cluster": validated.cluster},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


def _decode_kubeflow(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode the exact native TFJob wire and harmless empty UI residue."""
    _require_kubeflow_available(version=version, direction="decode")
    unexpected = sorted(
        set(payload) - _KUBEFLOW_NATIVE_FIELDS - _KUBEFLOW_UI_EMPTY_FIELDS
    )
    if unexpected:
        names = ", ".join(unexpected)
        raise _kubeflow_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-tfjob-subset",
            message=f"KUBEFLOW native task params contain unowned fields: {names}",
        )
    for field_name in _KUBEFLOW_UI_EMPTY_FIELDS:
        if field_name in payload and payload[field_name] != []:
            raise _kubeflow_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{field_name}",
                reason="nonempty-ui-only-field",
                message=(
                    f"KUBEFLOW native {field_name} is typed only as the exact "
                    "empty UI default"
                ),
            )
    namespace_wire = payload.get("namespace")
    if not isinstance(namespace_wire, str):
        raise _kubeflow_projection_error(
            version=version,
            direction="decode",
            field="task_params.namespace",
            reason="missing-or-invalid-native-namespace",
            message="KUBEFLOW native namespace must be one compact JSON string",
        )
    try:
        selector = json.loads(namespace_wire)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise _kubeflow_projection_error(
            version=version,
            direction="decode",
            field="task_params.namespace",
            reason="invalid-native-namespace-json",
            message="KUBEFLOW native namespace must be valid compact JSON",
        ) from exc
    if (
        not isinstance(selector, dict)
        or set(selector) != {"name", "cluster"}
        or not isinstance(selector.get("name"), str)
        or not isinstance(selector.get("cluster"), str)
    ):
        raise _kubeflow_projection_error(
            version=version,
            direction="decode",
            field="task_params.namespace",
            reason="invalid-native-namespace-shape",
            message=(
                "KUBEFLOW native namespace must contain exact string name and cluster"
            ),
        )
    expected_selector = json.dumps(
        {"name": selector["name"], "cluster": selector["cluster"]},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if namespace_wire != expected_selector:
        raise _kubeflow_projection_error(
            version=version,
            direction="decode",
            field="task_params.namespace",
            reason="noncanonical-native-namespace-json",
            message=(
                "KUBEFLOW native namespace must use the exact compact name/cluster wire"
            ),
        )
    canonical: JsonObject = {
        "namespace": selector["name"],
        "cluster": selector["cluster"],
    }
    if "yamlContent" in payload:
        canonical["yamlContent"] = deepcopy(payload["yamlContent"])
    validated = _validate_kubeflow_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    return require_json_object(
        validated.to_payload(),
        label="validated KUBEFLOW task_params",
    )


def _validate_kubeflow_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> KubeflowTfjobManifestTaskParamsSpec:
    """Apply the closed TFJob model and translate stable field-level errors."""
    try:
        validated = KubeflowTfjobManifestTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params"
        if location:
            field_name = (
                "yamlContent" if location[0] == "yaml_content" else str(location[0])
            )
            field = f"task_params.{field_name}"
        reason = (
            "missing-required-field"
            if error["type"] == "missing"
            else "outside-reviewed-tfjob-subset"
            if error["type"] == "extra_forbidden"
            else "invalid-canonical-value"
        )
        raise _kubeflow_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason=reason,
            message=f"KUBEFLOW typed {field} is invalid: {error['msg']}",
        ) from exc
    try:
        kubeflow_tfjob_identity_template(
            validated.yaml_content,
            namespace=validated.namespace,
            cluster=validated.cluster,
        )
    except ValueError as exc:
        raise _kubeflow_projection_error(
            version=version,
            direction=direction,
            field="task_params.yamlContent",
            reason="invalid-kubeflow-manifest",
            message=(f"KUBEFLOW typed task_params.yamlContent is invalid: {exc}"),
        ) from exc
    return validated


def _require_kubeflow_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    surface: KubeflowAuthoringSurface = get_task_authoring_surface(version).kubeflow
    if surface.available:
        return
    raise _kubeflow_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"KUBEFLOW does not exist in DolphinScheduler {version}",
    )


def _kubeflow_projection_error(
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
        task_type="KUBEFLOW",
        field=field,
        reason=reason,
        message=message,
    )
