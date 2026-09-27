from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.output import result_payload
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.patch import apply_workflow_patch
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.legacy_workflow_graph import prepare_legacy_workflow_graph
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_TASK_TYPE = "K8S"
_FACET = "K8S/literal_container_job"
_UNICODE_DIGIT = "\N{ARABIC-INDIC DIGIT ONE}"
_TYPED_VERSIONS = (
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_REFS = TaskRefIndex.from_code_by_name({})
_SCHEMA_EPOCHS = [
    (
        "3.1.9",
        frozenset(
            {
                "connectionMode",
                "namespace",
                "cluster",
                "image",
                "minCpuCores",
                "minMemorySpace",
                "environment",
            }
        ),
        frozenset({"connectionMode", "namespace", "cluster", "image"}),
        "NAMESPACE",
        {
            "minCpuCores": 0.0,
            "minMemorySpace": 0.0,
            "environment": [],
        },
    ),
    (
        "3.2.0",
        frozenset(
            {
                "connectionMode",
                "namespace",
                "cluster",
                "image",
                "minCpuCores",
                "minMemorySpace",
                "environment",
                "outputs",
                "command",
                "args",
                "pullSecret",
                "imagePullPolicy",
                "customizedLabels",
                "nodeSelectors",
            }
        ),
        frozenset(
            {"connectionMode", "namespace", "cluster", "image", "customizedLabels"}
        ),
        "NAMESPACE",
        {
            "minCpuCores": 0.0,
            "minMemorySpace": 0.0,
            "environment": [],
            "outputs": [],
            "command": [],
            "args": [],
            "imagePullPolicy": "IfNotPresent",
            "nodeSelectors": [],
        },
    ),
    (
        "3.3.1",
        frozenset(
            {
                "connectionMode",
                "datasource",
                "image",
                "minCpuCores",
                "minMemorySpace",
                "environment",
                "command",
                "args",
                "pullSecret",
                "imagePullPolicy",
                "customizedLabels",
                "nodeSelectors",
            }
        ),
        frozenset({"connectionMode", "datasource", "image"}),
        "DATASOURCE",
        {
            "minCpuCores": 0.0,
            "minMemorySpace": 0.0,
            "environment": [],
            "command": [],
            "args": [],
            "imagePullPolicy": "IfNotPresent",
            "customizedLabels": [],
            "nodeSelectors": [],
        },
    ),
    (
        "3.4.2",
        frozenset(
            {
                "connectionMode",
                "datasource",
                "image",
                "minCpuCores",
                "minMemorySpace",
                "environment",
                "outputs",
                "command",
                "args",
                "pullSecret",
                "imagePullPolicy",
                "customizedLabels",
                "nodeSelectors",
            }
        ),
        frozenset({"connectionMode", "datasource", "image"}),
        "DATASOURCE",
        {
            "minCpuCores": 0.0,
            "minMemorySpace": 0.0,
            "environment": [],
            "outputs": [],
            "command": [],
            "args": [],
            "imagePullPolicy": "IfNotPresent",
            "customizedLabels": [],
            "nodeSelectors": [],
        },
    ),
]


def _encode(version: str, params: YamlObject) -> YamlObject:
    encoded = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", encoded.task_params)


def _decode(
    version: str,
    params: YamlObject,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    return (
        cast("YamlObject", decoded.task.task_params),
        decoded.reencode_source,
    )


def _task_schema(version: str) -> JsonObject:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    return cast("JsonObject", result.data["schema"])


def _task_params_schema(version: str) -> JsonObject:
    schema = _task_schema(version)
    definitions = cast("JsonObject", schema["$defs"])
    return cast("JsonObject", definitions["task_params"])


def _resolve_schema(root: JsonObject, node: JsonObject) -> JsonObject:
    reference = node.get("$ref")
    if not isinstance(reference, str):
        return node
    definitions = cast("JsonObject", root["$defs"])
    return cast("JsonObject", definitions[reference.rsplit("/", maxsplit=1)[-1]])


def _schema_patterns(value: object) -> tuple[str, ...]:
    if isinstance(value, dict):
        own = (value["pattern"],) if isinstance(value.get("pattern"), str) else ()
        return own + tuple(
            pattern for child in value.values() for pattern in _schema_patterns(child)
        )
    if isinstance(value, list):
        return tuple(pattern for child in value for pattern in _schema_patterns(child))
    return ()


def _workflow_spec(
    version: str,
    params: YamlObject,
    *,
    intent: TaskAuthoringIntent = TaskAuthoringIntent.TYPED_CREATE,
    task_name: str = "run-container-job",
) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"k8s-create-{version}"},
            "tasks": [
                {
                    "name": task_name,
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=intent,
        ),
    )


def _workflow_create_wire(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    spec = _workflow_spec(version, params)
    if version == "1.3.9":
        legacy_prepared = prepare_legacy_workflow_graph(
            spec,
            task_id_factory=lambda _task_name: "tasks-k8s",
        )
        process_definition = json.loads(
            legacy_prepared.materialize()["processDefinitionJson"]
        )
        task = process_definition["tasks"][0]
        assert task["type"] == _TASK_TYPE
        native = task["params"]
    else:
        modern_prepared = prepare_workflow_create_compilation(spec, catalog=catalog)
        task_definitions = json.loads(
            modern_prepared.materialize([34_200])["taskDefinitionJson"]
        )
        task = task_definitions[0]
        assert task["taskType"] == _TASK_TYPE
        native = json.loads(task["taskParams"])
    assert isinstance(native, dict)
    return cast("YamlObject", native)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_k8s_catalog_exposes_the_literal_container_job_facet(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    membership = catalog.require_facet(_TASK_TYPE, _FACET)
    assert membership.profile_version == version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_k8s_public_json_schema_envelope_stays_bounded(version: str) -> None:
    payload = result_payload(
        "task-type.schema",
        task_type_schema_result(
            _TASK_TYPE,
            json_schema=True,
            catalog=get_task_authoring_catalog(version),
        ),
    )
    compact = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()

    assert len(compact) < 10 * 1024


def test_k8s_319_minimal_namespace_job_normalizes_and_projects_exactly() -> None:
    authored: YamlObject = {
        "connectionMode": "NAMESPACE",
        "namespace": "analytics",
        "cluster": "production",
        "image": "busybox:1.36",
    }
    expected: YamlObject = {
        **authored,
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "environment": [],
    }
    catalog = get_task_authoring_catalog("3.1.9")

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == expected
    assert _encode("3.1.9", normalized) == {
        "namespace": '{"name":"analytics","cluster":"production"}',
        "image": "busybox:1.36",
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "localParams": [],
    }


def test_k8s_320_advanced_job_roundtrips_the_exact_safe_native_wire() -> None:
    canonical: YamlObject = {
        "connectionMode": "NAMESPACE",
        "namespace": "analytics",
        "cluster": "production",
        "image": "registry.example/worker:3.2",
        "minCpuCores": 0.5,
        "minMemorySpace": 256.0,
        "environment": [{"name": "REGION", "value": "cn-east"}],
        "outputs": [{"name": "RESULT"}],
        "command": ["/bin/sh", "-c"],
        "args": ["echo $REGION"],
        "pullSecret": "registry-credentials",
        "imagePullPolicy": "Always",
        "customizedLabels": [{"label": "app.kubernetes.io/name", "value": "dsctl-job"}],
        "nodeSelectors": [
            {
                "key": "topology.kubernetes.io/zone",
                "operator": "In",
                "values": ["cn-east-1a", "cn-east-1b"],
            }
        ],
    }
    native: YamlObject = {
        "namespace": '{"name":"analytics","cluster":"production"}',
        "image": "registry.example/worker:3.2",
        "minCpuCores": 0.5,
        "minMemorySpace": 256.0,
        "localParams": [
            {
                "prop": "REGION",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "cn-east",
            },
            {
                "prop": "RESULT",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "",
            },
        ],
        "command": '["/bin/sh","-c"]',
        "args": '["echo $REGION"]',
        "pullSecret": "registry-credentials",
        "imagePullPolicy": "Always",
        "customizedLabels": [{"label": "app.kubernetes.io/name", "value": "dsctl-job"}],
        "nodeSelectors": [
            {
                "key": "topology.kubernetes.io/zone",
                "operator": "In",
                "values": "cn-east-1a,cn-east-1b",
            }
        ],
    }
    catalog = get_task_authoring_catalog("3.2.0")

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    decoded, source = _decode("3.2.0", native)

    assert normalized == canonical
    assert _encode("3.2.0", normalized) == native
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("version", ["3.2.1", "3.2.2"])
def test_k8s_32x_set_value_outputs_roundtrip_with_empty_labels(
    version: str,
) -> None:
    authored: YamlObject = {
        "connectionMode": "NAMESPACE",
        "namespace": "analytics",
        "cluster": "production",
        "image": "registry.example/worker:3.2",
        "outputs": [{"name": "RESULT"}],
    }
    canonical: YamlObject = {
        **authored,
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "environment": [],
        "command": [],
        "args": [],
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    native: YamlObject = {
        "namespace": '{"name":"analytics","cluster":"production"}',
        "image": "registry.example/worker:3.2",
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "localParams": [
            {
                "prop": "RESULT",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "",
            }
        ],
        "command": "[]",
        "args": "[]",
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    decoded, source = _decode(version, native)

    assert normalized == canonical
    assert _encode(version, normalized) == native
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1"])
def test_k8s_datasource_transport_hole_has_one_exact_safe_fixed_point(
    version: str,
) -> None:
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.3",
    }
    canonical: YamlObject = {
        **authored,
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "environment": [],
        "command": [],
        "args": [],
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    native: YamlObject = {
        "datasource": 17,
        "type": "K8S",
        "namespace": "",
        "kubeConfig": "",
        "image": "registry.example/worker:3.3",
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "localParams": [],
        "command": "[]",
        "args": "[]",
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    decoded, source = _decode(version, native)

    assert normalized == canonical
    assert _encode(version, normalized) == native
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING

    rejected_outputs: tuple[tuple[YamlValue, type[Exception]], ...] = (
        ([{"name": "RESULT"}], UnsupportedFeatureError),
        ([], UnsupportedFeatureError),
        (None, ValueError),
    )
    for outputs, expected_error in rejected_outputs:
        with pytest.raises(expected_error):
            catalog.normalize_task_params(
                _TASK_TYPE,
                {**authored, "outputs": outputs},
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )
        with pytest.raises(TaskParameterProjectionError):
            _encode(version, {**canonical, "outputs": outputs})

    resolved_kubeconfig: YamlObject = {**native, "kubeConfig": "apiVersion: v1"}
    richer_native: YamlObject = {**native, "futureField": {"preserve": True}}
    for opaque in (resolved_kubeconfig, richer_native):
        decoded_opaque, opaque_source = _decode(version, opaque)

        assert decoded_opaque == opaque
        assert opaque_source is ProjectionSource.OPAQUE_PRESERVE


def test_k8s_342_datasource_set_value_output_restores_the_exact_runtime() -> None:
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
        "environment": [{"name": "REGION", "value": "cn-east"}],
        "outputs": [{"name": "RESULT"}],
    }
    canonical: YamlObject = {
        **authored,
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "command": [],
        "args": [],
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    native: YamlObject = {
        "datasource": 17,
        "type": "K8S",
        "namespace": "",
        "kubeConfig": "",
        "image": "registry.example/worker:3.4.2",
        "minCpuCores": 0.0,
        "minMemorySpace": 0.0,
        "localParams": [
            {
                "prop": "REGION",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "cn-east",
            },
            {
                "prop": "RESULT",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "",
            },
        ],
        "command": "[]",
        "args": "[]",
        "imagePullPolicy": "IfNotPresent",
        "customizedLabels": [],
        "nodeSelectors": [],
    }
    catalog = get_task_authoring_catalog("3.4.2")
    surface = get_task_authoring_surface("3.4.2").k8s

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    decoded, source = _decode("3.4.2", native)
    image_field = next(
        field
        for field in catalog.require_facet(_TASK_TYPE, _FACET).contract.fields
        if field.path == "task_params.image"
    )
    guidance = image_field.description.lower()

    assert normalized == canonical
    assert _encode("3.4.2", normalized) == native
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING
    assert surface.connection_mode == "DATASOURCE"
    assert surface.output_protocol == "set-value"
    assert surface.output_transport_supported is True
    assert surface.output_declarations_injected_as_environment is True
    assert surface.namespace_context_propagated is True
    assert surface.resolved_kubeconfig_logged is True
    assert "including kubeconfig" in guidance
    assert (
        "out declarations are also injected into the pod as empty-valued "
        "environment entries"
    ) in guidance
    assert "propagates the selected namespace to pod-log/output lookup" in guidance


@pytest.mark.parametrize(
    ("version", "expected_fields", "expected_required", "mode", "expected_defaults"),
    _SCHEMA_EPOCHS,
)
def test_k8s_public_json_schema_matches_each_exact_runtime_epoch(
    version: str,
    expected_fields: frozenset[str],
    expected_required: frozenset[str],
    mode: str,
    expected_defaults: dict[str, object],
) -> None:
    task_params = _task_params_schema(version)
    properties = cast("JsonObject", task_params["properties"])
    required = cast("list[str]", task_params["required"])
    defaults = {
        name: field_schema["default"]
        for name, field_schema in properties.items()
        if isinstance(field_schema, dict) and "default" in field_schema
    }

    assert task_params["additionalProperties"] is False
    assert set(properties) == expected_fields
    assert set(required) == expected_required
    assert defaults == expected_defaults
    expected_runtime_validations = {"environment names must be unique"}
    if "outputs" in expected_fields:
        expected_runtime_validations.add(
            "output names must be unique and disjoint from environment names"
        )
    if "customizedLabels" in expected_fields:
        expected_runtime_validations.add("customizedLabels label keys must be unique")
    assert set(cast("list[str]", task_params["x-dsctl-runtime-validations"])) == (
        expected_runtime_validations
    )

    connection_mode = _resolve_schema(
        task_params,
        cast("JsonObject", properties["connectionMode"]),
    )
    assert connection_mode["type"] == "string"
    assert connection_mode["enum"] == [mode]

    if mode == "NAMESPACE":
        assert "datasource" not in properties
        for field_name in ("namespace", "cluster"):
            field_schema = _resolve_schema(
                task_params,
                cast("JsonObject", properties[field_name]),
            )
            assert field_schema["type"] == "string"
            assert "anyOf" not in field_schema
    else:
        assert "namespace" not in properties
        assert "cluster" not in properties
        datasource_schema = _resolve_schema(
            task_params,
            cast("JsonObject", properties["datasource"]),
        )
        assert datasource_schema["anyOf"] == [
            {"minimum": 1, "type": "integer"},
            {"pattern": r"\S", "type": "string"},
        ]
        assert "type" not in datasource_schema
        assert "default" not in datasource_schema

    if version == "3.1.9":
        return
    image_pull_policy = _resolve_schema(
        task_params,
        cast("JsonObject", properties["imagePullPolicy"]),
    )
    assert image_pull_policy["enum"] == ["IfNotPresent", "Always", "Never"]
    selectors = _resolve_schema(
        task_params,
        cast("JsonObject", properties["nodeSelectors"]),
    )
    selector = _resolve_schema(
        task_params,
        cast("JsonObject", selectors["items"]),
    )
    selector_properties = cast("JsonObject", selector["properties"])
    operator = _resolve_schema(
        task_params,
        cast("JsonObject", selector_properties["operator"]),
    )
    assert set(cast("list[str]", selector["required"])) == {
        "key",
        "operator",
        "values",
    }
    assert operator["enum"] == ["In", "NotIn", "Exists", "DoesNotExist", "Gt", "Lt"]
    labels = _resolve_schema(
        task_params,
        cast("JsonObject", properties["customizedLabels"]),
    )
    assert (labels.get("minItems") == 1) is (version == "3.2.0")


def test_k8s_nested_schema_and_runtime_reject_the_same_unsafe_selectors() -> None:
    version = "3.4.2"
    task_params = _task_params_schema(version)
    properties = cast("JsonObject", task_params["properties"])
    catalog = get_task_authoring_catalog(version)
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
    }

    labels = _resolve_schema(
        task_params,
        cast("JsonObject", properties["customizedLabels"]),
    )
    label_item = _resolve_schema(
        task_params,
        cast("JsonObject", labels["items"]),
    )
    label_properties = cast("JsonObject", label_item["properties"])
    label_key = _resolve_schema(
        task_params,
        cast("JsonObject", label_properties["label"]),
    )
    label_pattern = cast("str", label_key["pattern"])
    assert re.search(label_pattern, "app.kubernetes.io/name") is not None
    assert re.search(label_pattern, "dolphinscheduler-label") is None
    with pytest.raises(ValueError, match="reserved"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            {
                **authored,
                "customizedLabels": [
                    {"label": "dolphinscheduler-label", "value": "owned"}
                ],
            },
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    selectors = _resolve_schema(
        task_params,
        cast("JsonObject", properties["nodeSelectors"]),
    )
    selector = _resolve_schema(
        task_params,
        cast("JsonObject", selectors["items"]),
    )
    selector_properties = cast("JsonObject", selector["properties"])
    values = _resolve_schema(
        task_params,
        cast("JsonObject", selector_properties["values"]),
    )
    value_item = _resolve_schema(
        task_params,
        cast("JsonObject", values["items"]),
    )
    assert values["uniqueItems"] is True
    assert "pattern" not in value_item
    label_patterns = [
        pattern
        for pattern in _schema_patterns(selector)
        if re.search(pattern, "cn-east-1a") is not None
        and re.search(pattern, "") is None
        and re.search(pattern, "cn east") is None
        and re.search(pattern, "cn/east") is None
    ]
    assert label_patterns
    numeric_patterns = [
        pattern
        for pattern in _schema_patterns(selector)
        if re.search(pattern, "123") is not None
        and re.search(pattern, "abc") is None
        and re.search(pattern, _UNICODE_DIGIT) is None
    ]
    assert numeric_patterns

    exists_native: YamlObject = {
        **_encode(
            version,
            catalog.normalize_task_params(
                _TASK_TYPE,
                authored,
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        ),
        "nodeSelectors": [{"key": "zone", "operator": "Exists", "values": ""}],
    }
    decoded_exists, exists_source = _decode(version, exists_native)
    expected_exists: YamlObject = {
        **catalog.normalize_task_params(
            _TASK_TYPE,
            authored,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
        "nodeSelectors": [{"key": "zone", "operator": "Exists", "values": []}],
    }
    assert decoded_exists == expected_exists
    assert exists_source is ProjectionSource.TYPED_AUTHORING

    invalid_selectors: list[YamlObject] = [
        {"key": "zone", "operator": "In", "values": [""]},
        {"key": "capacity", "operator": "Gt", "values": [_UNICODE_DIGIT]},
        {"key": "zone", "operator": "In", "values": ["cn east"]},
        {"key": "zone", "operator": "In", "values": ["cn/east"]},
        {
            "key": "zone",
            "operator": "In",
            "values": ["cn-east-1a", "cn-east-1a"],
        },
    ]
    for node_selector in invalid_selectors:
        with pytest.raises(ValueError):
            catalog.normalize_task_params(
                _TASK_TYPE,
                {**authored, "nodeSelectors": [node_selector]},
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )

    with pytest.raises(ValueError, match="values"):
        _workflow_create_wire(
            version,
            {
                **authored,
                "nodeSelectors": [{"key": "zone", "operator": "Exists"}],
            },
        )


def test_k8s_argv_schema_and_runtime_reject_the_same_unsafe_literals() -> None:
    version = "3.4.2"
    task_params = _task_params_schema(version)
    properties = cast("JsonObject", task_params["properties"])
    catalog = get_task_authoring_catalog(version)
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
    }

    for field_name in ("command", "args"):
        argv = _resolve_schema(
            task_params,
            cast("JsonObject", properties[field_name]),
        )
        item = _resolve_schema(
            task_params,
            cast("JsonObject", argv["items"]),
        )
        pattern = cast("str", item["pattern"])
        for safe_value in ("", "echo", "--flag=value"):
            assert re.fullmatch(pattern, safe_value) is not None
            normalized = catalog.normalize_task_params(
                _TASK_TYPE,
                {**authored, field_name: [safe_value]},
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )
            assert normalized[field_name] == [safe_value]
        for unsafe_value in ("${secret}", "$[biz.date]", "line\nbreak", "\ud800"):
            assert re.fullmatch(pattern, unsafe_value) is None
            with pytest.raises(ValueError):
                catalog.normalize_task_params(
                    _TASK_TYPE,
                    {**authored, field_name: [unsafe_value]},
                    intent=TaskAuthoringIntent.TYPED_CREATE,
                )


def test_k8s_pull_secret_schema_and_runtime_require_one_dns_subdomain() -> None:
    version = "3.4.2"
    task_params = _task_params_schema(version)
    properties = cast("JsonObject", task_params["properties"])
    pull_secret = _resolve_schema(
        task_params,
        cast("JsonObject", properties["pullSecret"]),
    )
    pattern = cast("str", pull_secret["pattern"])
    catalog = get_task_authoring_catalog(version)
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
    }

    assert re.fullmatch(pattern, "registry-credentials.prod") is not None
    assert re.fullmatch(pattern, "bad secret") is None
    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        {**authored, "pullSecret": "registry-credentials.prod"},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized["pullSecret"] == "registry-credentials.prod"
    with pytest.raises(ValueError, match="DNS-subdomain"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            {**authored, "pullSecret": "bad secret"},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("invalid", "message"),
    [
        (
            {
                "environment": [
                    {"name": "DUPLICATE", "value": "first"},
                    {"name": "DUPLICATE", "value": "second"},
                ]
            },
            "environment names must be unique",
        ),
        (
            {"outputs": [{"name": "result"}, {"name": "result"}]},
            "outputs names must be unique",
        ),
        (
            {
                "environment": [{"name": "result", "value": "input"}],
                "outputs": [{"name": "result"}],
            },
            "environment and outputs names must be disjoint",
        ),
        (
            {
                "customizedLabels": [
                    {"label": "team", "value": "one"},
                    {"label": "team", "value": "two"},
                ]
            },
            "customizedLabels label keys must be unique",
        ),
    ],
)
def test_k8s_named_runtime_only_validations_fail_closed(
    invalid: YamlObject,
    message: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
    }

    with pytest.raises(ValueError, match=message):
        catalog.normalize_task_params(
            _TASK_TYPE,
            {**authored, **invalid},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("version", "expected_scope", "guidance_fragment"),
    [
        ("3.1.9", "none", "does not support customized labels"),
        ("3.2.0", "job-only", "job metadata only"),
        ("3.2.1", "job-and-pod", "job and pod metadata"),
        ("3.4.2", "job-and-pod", "job and pod metadata"),
    ],
)
def test_k8s_surface_discloses_the_exact_custom_label_scope(
    version: str,
    expected_scope: str,
    guidance_fragment: str,
) -> None:
    surface = get_task_authoring_surface(version).k8s
    catalog = get_task_authoring_catalog(version)
    image_field = next(
        field
        for field in catalog.require_facet(_TASK_TYPE, _FACET).contract.fields
        if field.path == "task_params.image"
    )

    assert surface.customized_label_scope == expected_scope
    assert guidance_fragment in image_field.description.lower()


@pytest.mark.parametrize(
    ("version", "empty_supported", "equals_preserved", "guidance_fragments"),
    [
        (
            "3.2.0",
            False,
            False,
            ("nonempty", "first '=' segment", "$varpool$"),
        ),
        ("3.2.1", False, True, ("nonempty", "preserves '='")),
        ("3.2.2", True, True, ("empty values", "preserves '='")),
        ("3.4.2", True, True, ("empty values", "preserves '='")),
    ],
)
def test_k8s_surface_discloses_exact_output_value_parsing(
    version: str,
    *,
    empty_supported: bool,
    equals_preserved: bool,
    guidance_fragments: tuple[str, ...],
) -> None:
    surface = get_task_authoring_surface(version).k8s
    catalog = get_task_authoring_catalog(version)
    output_field = next(
        field
        for field in catalog.require_facet(_TASK_TYPE, _FACET).contract.fields
        if field.path == "task_params.outputs"
    )
    guidance = output_field.description.lower()

    assert surface.empty_output_value_supported is empty_supported
    assert surface.output_value_equals_preserved is equals_preserved
    for fragment in guidance_fragments:
        assert fragment in guidance


@pytest.mark.parametrize("version", ["3.1.9", "3.4.2"])
@pytest.mark.parametrize(
    "task_name",
    [
        "A",
        "Run-Container-Job",
        "a" * 52,
        "a" * 51 + "-",
    ],
)
def test_k8s_typed_workflow_names_fit_the_derived_kubernetes_identity(
    version: str,
    task_name: str,
) -> None:
    canonical: YamlObject = (
        {
            "connectionMode": "NAMESPACE",
            "namespace": "analytics",
            "cluster": "production",
            "image": "busybox:1.36",
        }
        if version == "3.1.9"
        else {
            "connectionMode": "DATASOURCE",
            "datasource": 17,
            "image": "busybox:1.36",
        }
    )

    spec = _workflow_spec(version, canonical, task_name=task_name)

    assert spec.tasks[0].name == task_name


@pytest.mark.parametrize("version", ["3.1.9", "3.4.2"])
@pytest.mark.parametrize(
    "task_name",
    [
        "a" * 53,
        "run_container",
        "run.container",
        "run container",
        "任务",
    ],
)
def test_k8s_typed_workflow_names_fail_closed_before_transport(
    version: str,
    task_name: str,
) -> None:
    canonical: YamlObject = (
        {
            "connectionMode": "NAMESPACE",
            "namespace": "analytics",
            "cluster": "production",
            "image": "busybox:1.36",
        }
        if version == "3.1.9"
        else {
            "connectionMode": "DATASOURCE",
            "datasource": 17,
            "image": "busybox:1.36",
        }
    )

    with pytest.raises(ValueError, match="Kubernetes runtime name"):
        _workflow_spec(version, canonical, task_name=task_name)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_k8s_public_schema_matches_typed_workflow_task_name_validation(
    version: str,
) -> None:
    task_schema = _task_schema(version)
    properties = cast("JsonObject", task_schema["properties"])
    name_schema = cast("JsonObject", properties["name"])
    pattern = cast("str", name_schema["pattern"])
    canonical: YamlObject = (
        {
            "connectionMode": "NAMESPACE",
            "namespace": "analytics",
            "cluster": "production",
            "image": "busybox:1.36",
        }
        if version in {"3.1.9", "3.2.0", "3.2.1", "3.2.2"}
        else {
            "connectionMode": "DATASOURCE",
            "datasource": 17,
            "image": "busybox:1.36",
        }
    )
    if version == "3.2.0":
        canonical["customizedLabels"] = [
            {"label": "app.kubernetes.io/managed-by", "value": "dolphinscheduler"}
        ]

    assert pattern == r"^[A-Za-z0-9][A-Za-z0-9-]{0,51}$"
    assert name_schema["maxLength"] == 52
    for task_name in ("A", "Run-Container-Job", "a" * 52):
        assert re.fullmatch(pattern, task_name) is not None
        spec = _workflow_spec(version, canonical, task_name=task_name)
        assert spec.tasks[0].name == task_name
    for task_name in ("run_container", "a" * 53):
        assert re.fullmatch(pattern, task_name) is None
        with pytest.raises(ValueError, match="Kubernetes runtime name"):
            _workflow_spec(version, canonical, task_name=task_name)


def test_k8s_310_generic_opaque_name_is_not_reinterpreted_as_typed_identity() -> None:
    task_schema = _task_schema("3.1.0")
    properties = cast("JsonObject", task_schema["properties"])
    name_schema = cast("JsonObject", properties["name"])

    assert "pattern" not in name_schema
    assert "maxLength" not in name_schema

    spec = _workflow_spec(
        "3.1.0",
        {
            "namespace": '{"name":"analytics","cluster":"production"}',
            "image": "busybox:1.36",
        },
        task_name="legacy_name.with-native-spelling",
    )

    assert spec.tasks[0].name == "legacy_name.with-native-spelling"


def test_k8s_310_public_workflow_never_downgrades_canonical_input() -> None:
    canonical: YamlObject = {
        "connectionMode": "NAMESPACE",
        "namespace": "analytics",
        "cluster": "production",
        "image": "busybox:1.36",
    }

    with pytest.raises(UnsupportedFeatureError, match=r"K8S.*3\.1\.0"):
        _workflow_spec("3.1.0", canonical)


def test_k8s_310_public_workflow_requires_explicit_native_namespace() -> None:
    native: YamlObject = {
        "namespace": '{"name":"analytics","cluster":"production"}',
        "image": "busybox:1.36",
    }

    assert _workflow_create_wire("3.1.0", native) == native


@pytest.mark.parametrize(
    "namespace",
    [
        '{ "name": "analytics", "cluster": "production" }',
        '{"cluster":"production","name":"analytics"}',
        '{"name":"analytics","cluster":"production","future":true}',
        '{"name":"wrong","name":"analytics","cluster":"production"}',
    ],
)
def test_k8s_310_native_opaque_selector_requires_exact_compact_namespace(
    namespace: str,
) -> None:
    with pytest.raises(UnsupportedFeatureError, match=r"K8S.*3\.1\.0"):
        _workflow_spec(
            "3.1.0",
            {"namespace": namespace, "image": "busybox:1.36"},
        )


@pytest.mark.parametrize("version", ["3.1.9", "3.4.2"])
def test_k8s_patch_rename_validates_the_new_runtime_identity(version: str) -> None:
    canonical: YamlObject = (
        {
            "connectionMode": "NAMESPACE",
            "namespace": "analytics",
            "cluster": "production",
            "image": "busybox:1.36",
        }
        if version == "3.1.9"
        else {
            "connectionMode": "DATASOURCE",
            "datasource": 17,
            "image": "busybox:1.36",
        }
    )
    baseline = _workflow_spec(version, canonical)
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "rename": [{"from": "run-container-job", "to": "invalid_name"}]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="Kubernetes runtime name"):
        apply_workflow_patch(
            baseline,
            patch,
            edge_builder=lambda _tasks: [],
            catalog=get_task_authoring_catalog(version),
        )


@pytest.mark.parametrize(
    ("version", "canonical", "native"),
    [
        (
            "3.1.9",
            {
                "connectionMode": "NAMESPACE",
                "namespace": "analytics",
                "cluster": "production",
                "image": "busybox:1.36",
            },
            {
                "namespace": '{"name":"analytics","cluster":"production"}',
                "image": "busybox:1.36",
                "minCpuCores": 0.0,
                "minMemorySpace": 0.0,
                "localParams": [],
            },
        ),
        (
            "3.4.2",
            {
                "connectionMode": "DATASOURCE",
                "datasource": 17,
                "image": "registry.example/worker:3.4.2",
            },
            {
                "datasource": 17,
                "type": "K8S",
                "namespace": "",
                "kubeConfig": "",
                "image": "registry.example/worker:3.4.2",
                "minCpuCores": 0.0,
                "minMemorySpace": 0.0,
                "localParams": [],
                "command": "[]",
                "args": "[]",
                "imagePullPolicy": "IfNotPresent",
                "customizedLabels": [],
                "nodeSelectors": [],
            },
        ),
    ],
)
def test_k8s_high_level_workflow_create_compiles_the_exact_native_wire(
    version: str,
    canonical: YamlObject,
    native: YamlObject,
) -> None:
    assert _workflow_create_wire(version, canonical) == native


@pytest.mark.parametrize("version", ["3.1.9", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_k8s_high_level_authoring_has_no_explicit_opaque_create_or_edit(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    opaque: YamlObject = {
        "namespace": "native-only",
        "image": "busybox:1.36",
        "futureField": {"preserve": True},
    }

    with pytest.raises(UnsupportedFeatureError) as captured:
        _workflow_spec(version, opaque, intent=intent)

    assert str(captured.value) == (
        f"K8S opaque authoring is unsupported for DolphinScheduler {version}."
    )
    assert captured.value.details == {
        "selected_version": version,
        "task_type": "K8S",
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {version} policy permits only opaque "
            "preservation for K8S."
        ),
    }


@pytest.mark.parametrize(
    ("version", "invalid"),
    [
        (
            "3.1.9",
            {
                "connectionMode": "DATASOURCE",
                "datasource": 17,
                "image": "busybox:1.36",
            },
        ),
        (
            "3.4.2",
            {
                "connectionMode": "NAMESPACE",
                "namespace": "analytics",
                "cluster": "production",
                "image": "busybox:1.36",
            },
        ),
        (
            "3.1.9",
            {
                "connectionMode": "NAMESPACE",
                "namespace": "analytics",
                "cluster": "production",
                "image": "busybox:1.36",
                "futureField": {"native": True},
            },
        ),
        (
            "3.4.2",
            {
                "connectionMode": "DATASOURCE",
                "datasource": 17,
                "image": "busybox:1.36",
                "futureField": {"native": True},
            },
        ),
        (
            "3.1.9",
            {
                "connectionMode": "NAMESPACE",
                "namespace": "analytics",
                "cluster": "production",
                "image": "busybox:1.36",
                "pullSecret": None,
            },
        ),
    ],
)
def test_k8s_invalid_high_level_create_never_downgrades_to_opaque(
    version: str,
    invalid: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=invalid,
        )
        is TaskAuthoringIntent.TYPED_CREATE
    )
    with pytest.raises((UnsupportedFeatureError, ValueError)):
        _workflow_create_wire(version, invalid)


def test_k8s_node_selectors_use_int64_and_exact_duplicate_boundaries() -> None:
    version = "3.4.2"
    int64_max = "9223372036854775807"
    int64_overflow = "9223372036854775808"
    long_zero = "0" * 64
    authored: YamlObject = {
        "connectionMode": "DATASOURCE",
        "datasource": 17,
        "image": "registry.example/worker:3.4.2",
    }
    catalog = get_task_authoring_catalog(version)
    task_params = _task_params_schema(version)
    properties = cast("JsonObject", task_params["properties"])
    selectors = _resolve_schema(
        task_params,
        cast("JsonObject", properties["nodeSelectors"]),
    )
    selector = _resolve_schema(
        task_params,
        cast("JsonObject", selectors["items"]),
    )
    bounded_numeric_patterns = [
        pattern
        for pattern in _schema_patterns(selector)
        if re.search(pattern, int64_max) is not None
        and re.search(pattern, int64_overflow) is None
    ]

    assert selectors["uniqueItems"] is True
    assert bounded_numeric_patterns
    assert any(
        re.search(pattern, long_zero) is not None
        for pattern in bounded_numeric_patterns
    )

    for operator in ("Gt", "Lt"):
        maximum: YamlObject = {
            **authored,
            "nodeSelectors": [
                {"key": "rank", "operator": operator, "values": [int64_max]}
            ],
        }
        normalized_maximum = catalog.normalize_task_params(
            _TASK_TYPE,
            maximum,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        encoded_maximum = _encode(version, normalized_maximum)
        assert encoded_maximum["nodeSelectors"] == [
            {"key": "rank", "operator": operator, "values": int64_max}
        ]
        decoded_maximum, maximum_source = _decode(version, encoded_maximum)
        assert decoded_maximum == normalized_maximum
        assert maximum_source is ProjectionSource.TYPED_AUTHORING

        long_zero_value: YamlObject = {
            **authored,
            "nodeSelectors": [
                {"key": "rank", "operator": operator, "values": [long_zero]}
            ],
        }
        normalized_long_zero = catalog.normalize_task_params(
            _TASK_TYPE,
            long_zero_value,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        assert _encode(version, normalized_long_zero)["nodeSelectors"] == [
            {"key": "rank", "operator": operator, "values": long_zero}
        ]

        with pytest.raises(ValueError):
            catalog.normalize_task_params(
                _TASK_TYPE,
                {
                    **authored,
                    "nodeSelectors": [
                        {
                            "key": "rank",
                            "operator": operator,
                            "values": [int64_overflow],
                        }
                    ],
                },
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )

    range_selectors: list[YamlValue] = [
        {"key": "rank", "operator": "Gt", "values": ["1"]},
        {"key": "rank", "operator": "Lt", "values": ["10"]},
    ]
    normalized_range = catalog.normalize_task_params(
        _TASK_TYPE,
        {**authored, "nodeSelectors": range_selectors},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    encoded_range = _encode(version, normalized_range)
    assert encoded_range["nodeSelectors"] == [
        {"key": "rank", "operator": "Gt", "values": "1"},
        {"key": "rank", "operator": "Lt", "values": "10"},
    ]
    decoded_range, range_source = _decode(version, encoded_range)
    assert decoded_range == normalized_range
    assert range_source is ProjectionSource.TYPED_AUTHORING

    with pytest.raises(ValueError):
        catalog.normalize_task_params(
            _TASK_TYPE,
            {**authored, "nodeSelectors": [range_selectors[0], range_selectors[0]]},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
