"""Exact middle-release admission: real catalog/projection seams and runtime holes."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

import pytest
import yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject

_NEW_COUNTS = {
    "2.0.1": 14,
    "2.0.2": 14,
    "2.0.3": 14,
    "2.0.4": 14,
    "2.0.5": 14,
    "2.0.6": 14,
    "2.0.7": 14,
    "2.0.8": 14,
    "3.0.1": 20,
    "3.0.2": 20,
    "3.0.3": 20,
    "3.0.4": 20,
    "3.0.5": 20,
    "3.1.1": 27,
    "3.1.2": 27,
    "3.1.3": 29,
    "3.1.4": 30,
    "3.1.5": 31,
    "3.1.6": 31,
    "3.1.7": 31,
    "3.1.8": 31,
}
_REFS = TaskRefIndex.from_code_by_name({"selected": 101, "fallback": 102})


@pytest.mark.parametrize("version", list(_NEW_COUNTS))
def test_every_admitted_exact_template_validates_with_its_real_catalog(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    assert len(catalog.reviewed_typed_task_types) == _NEW_COUNTS[version]
    for task_type in catalog.reviewed_typed_task_types:
        result = task_template_result(task_type, catalog=catalog)
        assert isinstance(result.data, dict)
        yaml_text = result.data["yaml"]
        assert isinstance(yaml_text, str)
        document = yaml.safe_load(yaml_text)
        params = document.get("task_params", {"rawScript": document.get("command")})
        normalized = catalog.normalize_task_params(
            task_type, params, intent=TaskAuthoringIntent.TYPED_CREATE
        )
        assert isinstance(normalized, dict), (version, task_type)
        fact = catalog.task_type_fact(
            catalog.source_task_type_for_cli(task_type) or task_type
        )
        assert fact is not None
        assert fact.typed_authoring_review is not None


@pytest.mark.parametrize(
    ("version", "task_type", "opaque_allowed"),
    [
        ("3.1.1", "FLINK", True),
        ("3.1.1", "FLINK_STREAM", True),
        ("3.1.2", "FLINK_STREAM", True),
        ("3.1.3", "FLINK_STREAM", True),
        ("3.1.4", "FLINK_STREAM", True),
        ("3.1.1", "K8S", True),
        ("3.1.2", "K8S", True),
        ("3.1.3", "K8S", True),
        ("3.1.2", "OPENMLDB", False),
        ("3.1.1", "SAGEMAKER", False),
        ("3.1.2", "SAGEMAKER", False),
    ],
)
def test_registered_runtime_holes_do_not_accept_typed_intent_as_opaque(
    version: str, task_type: str, *, opaque_allowed: bool
) -> None:
    catalog = get_task_authoring_catalog(version)
    assert task_type in catalog.upstream_task_types
    assert not catalog.supports_typed_authoring(task_type)
    assert catalog.supports_opaque_authoring(task_type) is opaque_allowed
    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        # An undifferentiated object cannot use a native escape hatch.
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params(
                task_type, {"rawScript": "SELECT 1"}, intent=intent
            )
    if opaque_allowed:
        template = task_template_result(task_type, catalog=catalog)
        assert isinstance(template.data, dict)
        yaml_text = template.data["yaml"]
        assert isinstance(yaml_text, str)
        task = yaml.safe_load(yaml_text)
        catalog.normalize_task_params(
            task_type, task["task_params"], intent=TaskAuthoringIntent.OPAQUE_CREATE
        )
    payload: YamlObject = {"futureNative": {"nested": [1, "exact", True]}}
    preserved = catalog.normalize_task_params(
        task_type, payload, intent=TaskAuthoringIntent.OPAQUE_PRESERVE
    )
    assert preserved == payload
    assert preserved is not payload
    assert preserved["futureNative"] is not payload["futureNative"]


@pytest.mark.parametrize(
    ("version", "task_type", "params"),
    [
        (
            "3.1.2",
            "OPENMLDB",
            {
                "zk": "localhost:2181",
                "zkPath": "/openmldb",
                "executeMode": "online",
                "sql": "SELECT 1;",
            },
        ),
        (
            "3.1.1",
            "SAGEMAKER",
            {
                "sagemakerRequestJson": '{"PipelineName":"example"}',
                "localParams": [],
                "resourceList": [],
            },
        ),
        (
            "3.1.2",
            "SAGEMAKER",
            {
                "sagemakerRequestJson": '{"PipelineName":"example"}',
                "localParams": [],
                "resourceList": [],
            },
        ),
    ],
)
def test_broken_executor_native_wire_remains_opaque_across_decode_encode(
    version: str, task_type: str, params: JsonObject
) -> None:
    original = deepcopy(params)
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=task_type,
        task_params=params,
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    encoded = encode_task_parameters(
        version=version,
        task_type=task_type,
        task_params=decoded.task.task_params,
        refs=_REFS,
        source=decoded.reencode_source,
    )
    assert encoded.task_params == original == params
    with pytest.raises(TaskParameterProjectionError):
        encode_task_parameters(
            version=version,
            task_type=task_type,
            task_params=params,
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("3.1.5", {"engine": "SPARK", "deployMode": "local"}),
        ("3.1.6", {"engine": "SPARK", "deployMode": "client", "master": "LOCAL"}),
        (
            "3.1.7",
            {"startupScript": "seatunnel.sh", "deployMode": "local", "others": ""},
        ),
    ],
)
def test_seatunnel_exact_transitional_wire_has_one_literal_config_fixed_point(
    version: str, expected: JsonObject
) -> None:
    canonical: JsonObject = {"rawScript": "source {}\nsink {}\n"}
    encoded = encode_task_parameters(
        version=version,
        task_type="SEATUNNEL",
        task_params=canonical,
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert encoded.task_params == {
        "localParams": [],
        "resourceList": [],
        "useCustom": True,
        "rawScript": canonical["rawScript"],
        **expected,
    }
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="SEATUNNEL",
        task_params=encoded.task_params,
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == canonical
    if version == "3.1.6":
        broken = encoded.task_params
        broken["deployMode"] = "local"
        broken.pop("master")
        preserved = decode_task_parameters_with_provenance(
            version=version,
            task_type="SEATUNNEL",
            task_params=broken,
            refs=_REFS,
            source=ProjectionSource.OPAQUE_PRESERVE,
        )
        assert preserved.reencode_source is ProjectionSource.OPAQUE_PRESERVE
        assert preserved.task.task_params == broken


@pytest.mark.parametrize("version", ["2.0.1", "2.0.7", "2.0.8"])
def test_switch_and_condition_result_reference_wire_changes_at_208(
    version: str,
) -> None:
    params: JsonObject = {
        "switchResult": {
            "dependTaskList": [{"condition": "true", "nextNode": "selected"}],
            "nextNode": "fallback",
        }
    }
    expected: JsonObject = {
        "switchResult": {
            "dependTaskList": [
                {"condition": "true", "nextNode": 101 if version == "2.0.8" else "101"}
            ],
            "nextNode": 102 if version == "2.0.8" else "102",
        }
    }
    encoded = encode_task_parameters(
        version=version,
        task_type="SWITCH",
        task_params=params,
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert encoded.task_params == expected
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="SWITCH",
        task_params=expected,
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert decoded.task.task_params == params


@pytest.mark.parametrize(
    ("version", "month_extended", "dq_operator"),
    [
        ("3.0.1", False, "NE"),
        ("3.0.2", True, "NE"),
        ("3.0.3", True, "NE"),
        ("3.0.4", True, "EQ"),
        ("3.1.0", False, "NE"),
        ("3.1.1", True, "NE"),
    ],
)
def test_dependent_month_windows_and_quality_operator_use_their_own_epochs(
    version: str, dq_operator: str, *, month_extended: bool
) -> None:
    surface = get_task_authoring_surface(version)
    assert (
        "thisMonthBegin" in surface.dependent.date_values("month")
    ) is month_extended
    assert surface.data_quality.result_operator == dq_operator


@pytest.mark.parametrize(
    ("version", "startup", "precedence", "output"),
    [
        (
            "2.0.2",
            "declared-workflow-globals",
            ("child-global", "parent-global"),
            "absent",
        ),
        (
            "2.0.3",
            "declared-workflow-globals",
            ("parent-global", "child-global"),
            "absent",
        ),
        (
            "2.0.6",
            "declared-workflow-globals",
            ("parent-global", "child-global"),
            "absent",
        ),
        (
            "2.0.7",
            "declared-workflow-globals",
            ("parent-global", "child-global"),
            "task-local-out-on-cancel",
        ),
        (
            "2.0.8",
            "any-key-as-varchar",
            ("parent-global", "child-global"),
            "task-local-out-on-cancel",
        ),
        (
            "2.0.9",
            "any-key-as-varchar",
            ("parent-global", "child-global"),
            "task-local-out-on-cancel",
        ),
    ],
)
def test_nested_input_and_output_do_not_inherit_a_neighbour_release(
    version: str, startup: str, precedence: tuple[str, ...], output: str
) -> None:
    semantics = get_parameter_semantics(version)
    assert semantics.startup.key_scope == startup
    assert semantics.nested_workflow.child_input_precedence == precedence
    assert semantics.nested_workflow.child_output_contract == output


def test_sagemaker_310_is_not_misclassified_with_the_311_polling_hole() -> None:
    assert get_task_authoring_catalog("3.1.0").supports_typed_authoring("SAGEMAKER")
    assert not get_task_authoring_catalog("3.1.1").supports_typed_authoring("SAGEMAKER")
    assert not get_task_authoring_catalog("3.1.2").supports_typed_authoring("SAGEMAKER")
    assert get_task_authoring_catalog("3.1.3").supports_typed_authoring("SAGEMAKER")
