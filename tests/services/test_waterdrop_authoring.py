from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from dsctl.errors import UnsupportedFeatureError
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
)
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject

_NO_TASK_REFS = TaskRefIndex.from_code_by_name({})


def test_waterdrop_209_schema_exposes_one_closed_local_config_intent() -> None:
    catalog = get_task_authoring_catalog("2.0.9")
    result = task_type_schema_result("WATERDROP", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert catalog.supports_typed_authoring("WATERDROP") is True
    assert {path for path in fields if path.startswith("task_params.")} == {
        "task_params.configResource",
    }
    assert fields["task_params.configResource"]["choice_source"] == (
        "dsctl resource list"
    )
    assert (
        fields["task_params.configResource"]["choice_value"]
        == "fullName relative to the FILE root, retaining one leading slash"
    )


def test_waterdrop_209_template_marks_the_config_as_a_resource_field() -> None:
    metadata = task_template_metadata(catalog=get_task_authoring_catalog("2.0.9"))

    assert metadata["WATERDROP"]["resource_fields"] == ["task_params.configResource"]


def test_waterdrop_209_json_schema_is_closed_and_ascii_resource_bound() -> None:
    result = task_type_schema_result(
        "WATERDROP",
        json_schema=True,
        catalog=get_task_authoring_catalog("2.0.9"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["configResource"]
    assert set(task_params["properties"]) == {"configResource"}
    assert task_params["properties"]["configResource"]["pattern"] == (
        r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
        r"[A-Za-z0-9_.:@%+=,\-]+(?:/[A-Za-z0-9_.:@%+=,\-]+)*$"
    )


def test_waterdrop_209_projector_emits_one_staged_local_launcher_line() -> None:
    native = encode_task_parameters(
        version="2.0.9",
        task_type="WATERDROP",
        task_params={"configResource": "/waterdrop/orders.conf"},
        refs=_NO_TASK_REFS,
        resource_refs=TaskResourceRefIndex.from_id_by_full_name(
            {"/waterdrop/orders.conf": 811}
        ),
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params

    assert native == {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }


def test_waterdrop_209_workflow_compilation_binds_the_config_resource() -> None:
    catalog = get_task_authoring_catalog("2.0.9")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "waterdrop-orders"},
            "tasks": [
                {
                    "name": "sync-orders",
                    "type": "WATERDROP",
                    "task_params": {
                        "configResource": "/waterdrop/orders.conf",
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    compilation = prepare_workflow_create_compilation(spec, catalog=catalog)

    assert compilation.required_resource_full_names == ("/waterdrop/orders.conf",)
    payload = compilation.materialize(
        [51_001],
        resource_refs=TaskResourceRefIndex.from_id_by_full_name(
            {"/waterdrop/orders.conf": 811}
        ),
    )
    definition = json.loads(payload["taskDefinitionJson"])[0]
    assert json.loads(definition["taskParams"]) == {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }


def test_waterdrop_209_exact_wire_round_trips_as_typed_authoring() -> None:
    refs = TaskResourceRefIndex.from_id_by_full_name({"/waterdrop/orders.conf": 811})
    native: JsonObject = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }

    exported = decode_task_parameters_with_provenance(
        version="2.0.9",
        task_type="WATERDROP",
        task_params=native,
        refs=_NO_TASK_REFS,
        resource_refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported.task.task_params == {"configResource": "/waterdrop/orders.conf"}
    assert (
        encode_task_parameters(
            version="2.0.9",
            task_type="WATERDROP",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            resource_refs=refs,
            source=exported.reencode_source,
        ).task_params
        == native
    )


def test_waterdrop_209_richer_native_state_remains_opaque_and_lossless() -> None:
    refs = TaskResourceRefIndex.from_id_by_full_name({"/waterdrop/orders.conf": 811})
    native: JsonObject = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            'sh "$WATERDROP_HOME/bin/start-waterdrop.sh" --master local '
            "--deploy-mode client --queue priority "
            "--config waterdrop/orders.conf\n"
        ),
    }

    exported = decode_task_parameters_with_provenance(
        version="2.0.9",
        task_type="WATERDROP",
        task_params=native,
        refs=_NO_TASK_REFS,
        resource_refs=refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert exported.task.task_params == native
    assert (
        encode_task_parameters(
            version="2.0.9",
            task_type="WATERDROP",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            resource_refs=refs,
            source=exported.reencode_source,
        ).task_params
        == native
    )


def test_waterdrop_209_raw_create_edit_is_closed_but_preservation_is_lossless() -> None:
    catalog = get_task_authoring_catalog("2.0.9")
    native: YamlObject = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            "sh ${WATERDROP_HOME}/bin/start-waterdrop.sh --master local "
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }

    assert catalog.supports_opaque_authoring("WATERDROP") is False
    for intent in (
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params("WATERDROP", native, intent=intent)

    preserved = catalog.normalize_task_params(
        "WATERDROP",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    assert preserved == native
    assert preserved is not native


def test_waterdrop_200_is_an_explicit_preserve_only_runtime_exclusion() -> None:
    exclusions = cast(
        "dict[str, JsonObject]",
        TASK_PROFILES["2.0.0"]["typed_authoring_exclusions"],
    )
    exclusion = exclusions["WATERDROP"]
    assert exclusion["reason"] == "waterdrop-channel-not-registered-to-shell"

    catalog = get_task_authoring_catalog("2.0.0")
    native: YamlObject = {
        "localParams": [],
        "resourceList": [{"id": 811}],
        "rawScript": (
            "sh ${WATERDROP_HOME}/bin/start-waterdrop.sh --master local "
            "--deploy-mode client --queue default "
            "--config waterdrop/orders.conf\n"
        ),
    }

    assert "WATERDROP" in catalog.upstream_task_types
    assert "WATERDROP" not in catalog.authoring_task_types
    assert "WATERDROP" not in supported_task_template_types(catalog=catalog)
    assert catalog.supports_typed_authoring("WATERDROP") is False
    assert catalog.supports_opaque_authoring("WATERDROP") is False
    membership = catalog.require_task_type("WATERDROP").default
    assert membership.typed_create is False
    assert membership.typed_edit is False
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
    ):
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params("WATERDROP", native, intent=intent)

    assert (
        catalog.normalize_task_params(
            "WATERDROP",
            native,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )
        == native
    )
