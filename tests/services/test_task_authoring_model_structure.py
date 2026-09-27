"""Current reviewed discovery fingerprints and fail-closed model seams."""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel, Field

from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.models.task_spec import (
    HttpTaskParamsSpec,
    JavaLiteralFatJarTaskParamsSpec,
    TaskParamsSpec,
    supported_typed_task_types,
    task_family_models,
    task_params_model_for_type,
)
from dsctl.services import task_authoring as authoring_module
from dsctl.services._task_field_structure import (
    UnknownFieldStructureError,
    field_structure,
    structure_from_schema,
)
from dsctl.services.task_authoring_catalog import (
    get_task_authoring_catalog,
    model_field,
)
from dsctl.services.task_authoring_catalog import registry as catalog_module

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


@pytest.mark.parametrize(
    "version",
    [
        "1.3.9",
        "2.0.0",
        "2.0.9",
        "3.0.0",
        "3.0.6",
        "3.1.0",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
    ],
)
def test_all_exact_discovery_facts_match_current_contract(version: str) -> None:
    baseline = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures/task_authoring/current_contract_fingerprints.json"
        ).read_text(encoding="utf-8")
    )
    catalog = get_task_authoring_catalog(version)
    entries = {}
    for task_type, profile in catalog.entries.items():
        facets = {}
        for facet, membership in profile.facets.items():
            contract = membership.contract
            model = contract.params_model
            fields = [item.to_data() for item in contract.fields]
            templates = [asdict(item) for item in contract.templates]
            facets[facet] = {
                "family": contract.family,
                "review": contract.review,
                "model": None if model is None else model.__name__,
                "model_schema": None
                if model is None
                else model.model_json_schema(by_alias=True),
                "fields": fields,
                "state_rules": [item.to_data() for item in contract.state_rules],
                "templates": templates,
                "parameter_data_types": contract.parameter_data_types,
                "parameter_directions": contract.parameter_directions,
                "allow_local_out_without_var_pool": (
                    contract.allow_local_out_without_var_pool
                ),
                "runtime_only_fields": contract.runtime_only_fields,
                "membership": {
                    key: getattr(membership, key)
                    for key in (
                        "typed_create",
                        "typed_edit",
                        "opaque_create",
                        "opaque_edit",
                        "opaque_preserve",
                        "constraint",
                        "runtime_only_fields",
                    )
                },
                "restrict_opaque_authoring_to_selector": (
                    contract.restrict_opaque_authoring_to_selector
                ),
            }
        entries[task_type] = {
            "category": profile.category,
            "kind": profile.kind,
            "default_facet": profile.default_facet,
            "facets": facets,
        }
    actual = {
        "surface": asdict(catalog.authoring_surface),
        "families": {
            "reviewed_types": sorted(catalog.reviewed_typed_task_types),
            "authorable": list(catalog.authorable_task_types),
            "opaque": sorted(catalog.opaque_authoring_task_types),
        },
        "entries": entries,
    }
    # Both receipts describe this same current contract. Only the supported
    # Pydantic JSON Schema serialization epochs differ; no historical contract
    # hash is accepted. Family tests separately verify preserved selectors.
    serialized = json.dumps(
        actual, sort_keys=True, separators=(",", ":"), default=sorted
    )
    assert hashlib.sha256(serialized.encode()).hexdigest() in (
        baseline["profiles"][version],
        baseline["pydantic_2_8_profiles"][version],
    )


def test_model_registry_is_the_complete_reviewed_family_identity_union() -> None:
    reviewed = set().union(
        *(
            get_task_authoring_catalog(v).reviewed_typed_task_types
            for v in TARGET_DS_VERSIONS
        )
    )
    families = task_family_models()
    assert len(families) == len(reviewed) == 42
    assert {family.task_type for family in families} == reviewed
    assert supported_typed_task_types() == tuple(
        sorted(get_task_authoring_catalog("3.4.1").reviewed_typed_task_types)
    )
    # A registered model does not broaden historical implicit normalization.
    assert task_params_model_for_type("FLINK_STREAM") is None
    # A registered JAVA model does not open its exact broken executor coordinate.
    assert task_params_model_for_type("JAVA") is JavaLiteralFatJarTaskParamsSpec
    assert "JAVA" not in get_task_authoring_catalog("3.2.1").reviewed_typed_task_types


@pytest.mark.parametrize("task_type", ["EMR_SERVERLESS", "SHELL", "LINKIS"])
def test_missing_authoring_registration_never_becomes_model_only(
    monkeypatch: pytest.MonkeyPatch, task_type: str
) -> None:
    # Exercise a materialized facet, an explicit model-only family and an
    # exclusion. A source review plus an installed model cannot select policy.
    builders = dict(catalog_module._AUTHORING_PROFILE_BUILDERS)
    del builders[task_type]
    monkeypatch.setattr(catalog_module, "_AUTHORING_PROFILE_BUILDERS", builders)
    with pytest.raises(
        ValueError,
        match=rf"3\.4\.2\.{task_type} has no registered authoring implementation",
    ):
        catalog_module._generated_catalog("3.4.2")


def test_model_only_registration_cannot_replace_an_exclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builders = dict(catalog_module._AUTHORING_PROFILE_BUILDERS)
    builders["LINKIS"] = None
    monkeypatch.setattr(catalog_module, "_AUTHORING_PROFILE_BUILDERS", builders)
    with pytest.raises(ValueError, match="LINKIS has no materialized authoring policy"):
        catalog_module._generated_catalog("3.4.2")


def test_exact_model_only_memberships_remain_explicit_and_review_gated() -> None:
    fallback_union = set().union(
        *(
            get_task_authoring_catalog(v).legacy_typed_task_types
            for v in TARGET_DS_VERSIONS
        )
    )
    assert fallback_union == {
        "CONDITIONS",
        "HTTP",
        "PYTHON",
        "REMOTESHELL",
        "SHELL",
        "SUB_WORKFLOW",
        "SWITCH",
    }
    assert (
        sum(
            len(get_task_authoring_catalog(v).reviewed_typed_task_types)
            for v in TARGET_DS_VERSIONS
        )
        == 902
    )


def test_model_defaults_are_visible_only_when_discovery_requests_them() -> None:
    hidden = model_field("task_params.mainArgs").bind_model(
        JavaLiteralFatJarTaskParamsSpec
    )
    visible = model_field("task_params.mainArgs", model_default=True).bind_model(
        JavaLiteralFatJarTaskParamsSpec
    )
    assert "default" not in hidden.to_data()
    assert visible.to_data()["default"] == []
    assert visible.to_data()["type"] == "array"
    with pytest.raises(ValueError, match="no model default"):
        model_field("task_params.mainJar", model_default=True).bind_model(
            JavaLiteralFatJarTaskParamsSpec
        )
    with pytest.raises(ValueError, match="explicit typed parameter model"):
        model_field("task_params.mainJar").bind_model(None)


def test_model_only_discovery_tracks_model_changes_without_redefining_display_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class HttpWithOptionalTimeout(HttpTaskParamsSpec):
        connect_timeout: int = Field(default=2500, alias="connectTimeout", gt=0)

    def selected_model(task_type: str) -> type[TaskParamsSpec] | None:
        if task_type == "HTTP":
            return HttpWithOptionalTimeout
        return task_params_model_for_type(task_type)

    monkeypatch.setattr(authoring_module, "task_params_model_for_type", selected_model)
    catalog = get_task_authoring_catalog("3.4.2")
    result = authoring_module.task_type_schema_result(
        "HTTP", field="task_params.connectTimeout", catalog=catalog
    )
    assert isinstance(result.data, dict)
    assert result.data["fields"] == [
        {
            "path": "task_params.connectTimeout",
            "type": "integer",
            "required": False,
            "default": 10000,
            "compile_path": "taskDefinitionJson[].taskParams.connectTimeout",
            "description": "Connection timeout in milliseconds.",
        }
    ]
    assert "HTTP" not in catalog.entries
    assert catalog.supports_typed_authoring("HTTP")


def test_model_structure_follows_aliases_refs_and_rejects_ambiguous_unions() -> None:
    class Item(BaseModel):
        token: str = Field(alias="nativeToken")

    class Parameters(BaseModel):
        entries: list[Item]
        variant: str | int

    shape = field_structure(Parameters, "task_params.entries[].nativeToken")
    assert shape.value_type == "string"
    assert shape.required is True
    with pytest.raises(UnknownFieldStructureError, match="Variant-dependent"):
        field_structure(Parameters, "task_params.variant")
    with pytest.raises(UnknownFieldStructureError, match="unambiguous model property"):
        field_structure(Parameters, "task_params.entries[].missing")


def test_single_allof_reference_keeps_default_and_matches_direct_reference() -> None:
    schema: YamlObject = {
        "$defs": {"Direction": {"type": "string", "enum": ["IN", "OUT"]}},
        "type": "object",
        "properties": {
            "direct": {"allOf": [{"$ref": "#/$defs/Direction"}], "default": "IN"},
            "modern": {"$ref": "#/$defs/Direction", "default": "IN"},
        },
    }
    legacy = structure_from_schema(schema, "task_params.direct")
    modern = structure_from_schema(schema, "task_params.modern")
    assert legacy == modern
    assert legacy.has_default is True
    assert legacy.default == "IN"
    assert legacy.choices == ("IN", "OUT")


def test_multiple_allof_constraints_are_not_guessed() -> None:
    with pytest.raises(UnknownFieldStructureError, match="Combined model constraints"):
        structure_from_schema(
            {
                "properties": {
                    "value": {"allOf": [{"type": "string"}, {"minLength": 1}]}
                }
            },
            "task_params.value",
        )


def test_redundant_literal_enum_does_not_create_discovery_choices() -> None:
    legacy = structure_from_schema(
        {"properties": {"value": {"type": "boolean", "const": True, "enum": [True]}}},
        "task_params.value",
    )
    modern = structure_from_schema(
        {"properties": {"value": {"type": "boolean", "const": True}}},
        "task_params.value",
    )
    assert legacy == modern
    assert legacy.value_type == "boolean"
    assert legacy.choices == ()
