from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.generated.task_profiles import TASK_PROFILES
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import (
    supported_task_template_types,
    task_template_metadata,
    task_template_result,
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskWorkflowRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


_FACET = "DYNAMIC/literal_single_dimension_fanout"
_NO_TASK_REFS = TaskRefIndex.from_code_by_name({})
_WORKFLOW_REFS = TaskWorkflowRefIndex.from_code_by_name({"child-orders": 9001})
_CANONICAL_FIELDS = {
    "childWorkflowName",
    "parameterName",
    "values",
    "degreeOfParallelism",
}


def _canonical() -> YamlObject:
    return {
        "childWorkflowName": "child-orders",
        "parameterName": "region",
        "values": ["east", "west"],
        "degreeOfParallelism": 2,
    }


def _native() -> JsonObject:
    return {
        "processDefinitionCode": 9001,
        "maxNumOfSubWorkflowInstances": 2,
        "degreeOfParallelism": 2,
        "filterCondition": "",
        "listParameters": [
            {
                "name": "region",
                "value": "east,west",
                "separator": ",",
            }
        ],
    }


def test_dynamic_source_membership_is_exactly_the_three_32x_releases() -> None:
    source_versions = {
        version
        for version, profile in TASK_PROFILES.items()
        if "DYNAMIC" in cast("dict[str, object]", profile["task_types"])
    }

    assert source_versions == {"3.2.0", "3.2.1", "3.2.2"}


def test_dynamic_322_catalog_exposes_one_closed_typed_fanout() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    entry = catalog.require_task_type("DYNAMIC")
    membership = entry.default

    assert entry.default_facet == _FACET
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert catalog.supports_typed_authoring("DYNAMIC") is True
    assert catalog.supports_opaque_authoring("DYNAMIC") is False
    assert "DYNAMIC" in catalog.authoring_task_types
    assert "DYNAMIC" in supported_task_template_types(catalog=catalog)


@pytest.mark.parametrize(
    ("version", "reason"),
    [
        ("3.2.0", "dynamic-child-tenant-not-forwarded"),
        (
            "3.2.1",
            "dynamic-child-tenant-not-forwarded-and-start-param-unguarded",
        ),
    ],
)
def test_dynamic_320_and_321_are_explicit_preserve_only_runtime_exclusions(
    version: str,
    reason: str,
) -> None:
    exclusions = cast(
        "dict[str, JsonObject]",
        TASK_PROFILES[version]["typed_authoring_exclusions"],
    )
    assert exclusions["DYNAMIC"]["reason"] == reason

    catalog = get_task_authoring_catalog(version)
    membership = catalog.require_task_type("DYNAMIC").default
    assert membership.typed_create is False
    assert membership.typed_edit is False
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert "DYNAMIC" in catalog.upstream_task_types
    assert "DYNAMIC" not in catalog.authoring_task_types
    assert "DYNAMIC" not in supported_task_template_types(catalog=catalog)

    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params(
                "DYNAMIC", cast("YamlObject", _native()), intent=intent
            )

    assert (
        catalog.normalize_task_params(
            "DYNAMIC",
            cast("YamlObject", _native()),
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )
        == _native()
    )


def test_dynamic_322_schema_is_closed_and_documents_the_compiler_owned_wire() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    result = task_type_schema_result("DYNAMIC", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert {
        path.removeprefix("task_params.")
        for path in fields
        if path.startswith("task_params.") and "[]" not in path
    } == _CANONICAL_FIELDS
    assert fields["task_params.childWorkflowName"]["choice_source"] == (
        "dsctl workflow list"
    )
    assert fields["task_params.childWorkflowName"]["choice_value"] == "name"
    assert fields["task_params.degreeOfParallelism"]["default"] == 1
    assert "maxNumOfSubWorkflowInstances" not in fields
    assert "filterCondition" not in fields
    assert "listParameters" not in fields

    json_schema_result = task_type_schema_result(
        "DYNAMIC",
        json_schema=True,
        catalog=catalog,
    )
    assert isinstance(json_schema_result.data, dict)
    task_params = json_schema_result.data["schema"]["$defs"]["task_params"]
    assert task_params["additionalProperties"] is False
    assert set(task_params["properties"]) == _CANONICAL_FIELDS
    assert set(task_params["required"]) == {
        "childWorkflowName",
        "parameterName",
        "values",
    }


def test_dynamic_322_template_publishes_only_the_literal_fanout_subset() -> None:
    metadata = task_template_metadata(catalog=get_task_authoring_catalog("3.2.2"))
    template = metadata["DYNAMIC"]
    result = task_template_result(
        "DYNAMIC",
        catalog=get_task_authoring_catalog("3.2.2"),
    )
    assert isinstance(result.data, dict)
    yaml_text = result.data["yaml"]

    assert "default_variant" not in template
    assert template["payload_modes"] == ["task_params"]
    assert "childWorkflowName:" in yaml_text
    assert "processDefinitionCode:" not in yaml_text
    assert "parameterName:" in yaml_text
    assert "values:" in yaml_text
    assert "degreeOfParallelism:" in yaml_text
    assert "maxNumOfSubWorkflowInstances:" not in yaml_text
    assert "filterCondition:" not in yaml_text
    assert "listParameters:" not in yaml_text


def test_dynamic_322_projector_emits_one_exact_bounded_native_dimension() -> None:
    native = encode_task_parameters(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=cast("JsonObject", _canonical()),
        refs=_NO_TASK_REFS,
        workflow_refs=_WORKFLOW_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params

    assert native == _native()


def test_dynamic_322_workflow_compilation_uses_the_exact_native_wire() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "dynamic-region-fanout"},
            "tasks": [
                {
                    "name": "fanout-region",
                    "type": "DYNAMIC",
                    "task_params": _canonical(),
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    compilation = prepare_workflow_create_compilation(spec, catalog=catalog)
    definition = json.loads(
        compilation.materialize([52_001], workflow_refs=_WORKFLOW_REFS)[
            "taskDefinitionJson"
        ]
    )[0]

    assert definition["taskType"] == "DYNAMIC"
    assert json.loads(definition["taskParams"]) == _native()


def test_dynamic_322_exact_wire_round_trips_as_typed_authoring() -> None:
    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=_native(),
        refs=_NO_TASK_REFS,
        workflow_refs=_WORKFLOW_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical()
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=decoded.task.task_params,
            refs=_NO_TASK_REFS,
            workflow_refs=_WORKFLOW_REFS,
            source=decoded.reencode_source,
        ).task_params
        == _native()
    )


def test_dynamic_322_unresolved_native_child_code_stays_opaque_and_lossless() -> None:
    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=_native(),
        refs=_NO_TASK_REFS,
        workflow_refs=TaskWorkflowRefIndex.from_code_by_name({}),
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == _native()


def test_dynamic_322_unknown_child_name_fails_closed() -> None:
    with pytest.raises(TaskParameterProjectionError) as raised:
        encode_task_parameters(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=cast("JsonObject", _canonical()),
            refs=_NO_TASK_REFS,
            workflow_refs=TaskWorkflowRefIndex.from_code_by_name({}),
            source=ProjectionSource.TYPED_AUTHORING,
        )

    assert raised.value.details["reason"] == "unresolved-child-workflow-name"


@pytest.mark.parametrize("empty_ui_field", ["localParams", "resourceList"])
def test_dynamic_322_empty_ui_residue_decodes_to_the_typed_fixed_point(
    empty_ui_field: str,
) -> None:
    native = {**_native(), empty_ui_field: []}

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=native,
        refs=_NO_TASK_REFS,
        workflow_refs=_WORKFLOW_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical()
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=decoded.task.task_params,
            refs=_NO_TASK_REFS,
            workflow_refs=_WORKFLOW_REFS,
            source=decoded.reencode_source,
        ).task_params
        == _native()
    )


def test_dynamic_322_exact_ui_residue_decodes_to_the_typed_fixed_point() -> None:
    native = {
        **_native(),
        "localParams": [],
        "resourceList": [],
    }
    native["listParameters"] = [
        {**cast("list[JsonObject]", native["listParameters"])[0], "disabled": True}
    ]

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=native,
        refs=_NO_TASK_REFS,
        workflow_refs=_WORKFLOW_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical()
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=decoded.task.task_params,
            refs=_NO_TASK_REFS,
            workflow_refs=_WORKFLOW_REFS,
            source=decoded.reencode_source,
        ).task_params
        == _native()
    )


@pytest.mark.parametrize("disabled", [False, None, "true"])
def test_dynamic_322_noncanonical_ui_disabled_state_stays_opaque(
    disabled: object,
) -> None:
    native = _native()
    native["listParameters"] = [
        {
            **cast("list[JsonObject]", native["listParameters"])[0],
            "disabled": cast("YamlValue", disabled),
        }
    ]

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=native,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native


@pytest.mark.parametrize(
    "extra",
    [
        {"localParams": [{"prop": "region", "value": "${region}"}]},
        {"resourceList": [{"resourceName": "/unsafe"}]},
        {"varPool": [{"prop": "dynamic.out(fanout)", "value": "secret"}]},
        {"futureField": {"preserve": True}},
    ],
)
def test_dynamic_322_richer_native_state_stays_opaque_and_lossless(
    extra: JsonObject,
) -> None:
    native = {**_native(), **extra}
    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=native,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native
    assert (
        encode_task_parameters(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=decoded.task.task_params,
            refs=_NO_TASK_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )


def test_dynamic_322_multidimensional_native_state_remains_opaque() -> None:
    native = _native()
    native["maxNumOfSubWorkflowInstances"] = 4
    native["listParameters"] = [
        {"name": "region", "value": "east,west", "separator": ","},
        {"name": "tier", "value": "gold,silver", "separator": ","},
    ]

    decoded = decode_task_parameters_with_provenance(
        version="3.2.2",
        task_type="DYNAMIC",
        task_params=native,
        refs=_NO_TASK_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("childWorkflowName", " child-orders"),
        ("processDefinitionCode", 0),
        ("processDefinitionCode", True),
        ("processDefinitionCode", 2**63),
        ("parameterName", ""),
        ("parameterName", "${region}"),
        ("parameterName", "region name"),
        ("parameterName", "system.biz.date"),
        ("values", []),
        ("values", ["east", "east"]),
        ("values", [" east"]),
        ("values", ["east,"]),
        ("values", ["${region}"]),
        ("values", [1]),
        ("degreeOfParallelism", 0),
        ("degreeOfParallelism", 3),
    ],
)
def test_dynamic_322_rejects_nonliteral_or_unbounded_canonical_values(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            "DYNAMIC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_dynamic_322_bounds_the_complete_comma_joined_native_value() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    params = _canonical()
    params["values"] = ["a" * 127, "b" * 128]

    normalized = catalog.normalize_task_params(
        "DYNAMIC",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized["values"] == params["values"]

    params["values"] = ["a" * 127, "b" * 129]
    with pytest.raises(ValueError, match="joined native representation"):
        catalog.normalize_task_params(
            "DYNAMIC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_dynamic_322_counts_astral_values_in_ui_utf16_code_units() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    params = _canonical()
    params["values"] = ["😀" * 128]
    params["degreeOfParallelism"] = 1

    normalized = catalog.normalize_task_params(
        "DYNAMIC",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized["values"] == params["values"]

    params["values"] = ["😀" * 129]
    with pytest.raises(ValueError, match="256 UTF-16 code units"):
        catalog.normalize_task_params(
            "DYNAMIC",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "field_name",
    [
        "maxNumOfSubWorkflowInstances",
        "filterCondition",
        "listParameters",
        "localParams",
        "resourceList",
        "varPool",
        "futureField",
    ],
)
def test_dynamic_322_typed_authoring_rejects_native_or_future_fields(
    field_name: str,
) -> None:
    params = _canonical()
    params[field_name] = [] if field_name.endswith("Params") else "native"

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            "DYNAMIC",
            params,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        )


def test_dynamic_322_typed_projector_rejects_nonexact_native_decode() -> None:
    native = _native()
    native["filterCondition"] = "west"

    with pytest.raises(TaskParameterProjectionError, match="filterCondition"):
        decode_task_parameters_with_provenance(
            version="3.2.2",
            task_type="DYNAMIC",
            task_params=native,
            refs=_NO_TASK_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    "global_params",
    [
        {"region": "parent"},
        [
            {
                "prop": "region",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "parent",
            }
        ],
    ],
)
def test_dynamic_322_rejects_parent_global_parameter_collision(
    global_params: YamlValue,
) -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {
                "name": "dynamic-region-fanout",
                "global_params": global_params,
            },
            "tasks": [
                {
                    "name": "fanout-region",
                    "type": "DYNAMIC",
                    "task_params": _canonical(),
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match="parent workflow global"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


def test_dynamic_322_rejects_task_retry_because_failed_children_are_not_reset() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "dynamic-region-fanout"},
            "tasks": [
                {
                    "name": "fanout-region",
                    "type": "DYNAMIC",
                    "task_params": _canonical(),
                    "retry": {"times": 1, "interval": 1},
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_create_compilation(spec, catalog=catalog)
