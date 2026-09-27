from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from pydantic import ValidationError
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.generated.task_profiles import TASK_PROFILES
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.mutation import (
    WorkflowMutationPlan,
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import supported_task_template_types
from dsctl.upstream.resolver import ResolvedProject
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
    from dsctl.models.common import YamlValue
    from dsctl.support.json_types import JsonObject


_FACET = "DATA_QUALITY/local_mysql_table_row_count_equals"
_TYPED_VERSIONS = (
    "3.0.0",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.0.6",
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
)
_DATABASE_WIRE_VERSIONS = ("3.2.0", "3.2.1", "3.2.2")
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_LEGACY_FIELDS = {"datasource", "table", "expectedRowCount"}
_DATABASE_FIELDS = _LEGACY_FIELDS | {"database"}
_NO_REFS = TaskRefIndex.from_code_by_name({})
_TASK_NAME = "verify-events-row-count"


def _canonical(version: str) -> JsonObject:
    canonical: JsonObject = {
        "datasource": 7,
        "table": "events",
        "expectedRowCount": 42,
    }
    if version in _DATABASE_WIRE_VERSIONS:
        canonical["database"] = "warehouse"
    return canonical


def _native(version: str) -> JsonObject:
    rule_input_parameter: JsonObject = {
        "src_connector_type": "0",
        "src_datasource_id": "7",
        "src_table": "events",
        "src_filter": "",
        "statistics_name": "table_count.total",
        "comparison_type": "1",
        "comparison_name": "42",
        "check_type": "0",
        "threshold": "0",
        "failure_strategy": "1",
        "operator": (
            "5"
            if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3"}
            or version.startswith("3.1.")
            else "0"
        ),
    }
    if version in _DATABASE_WIRE_VERSIONS:
        rule_input_parameter["src_database"] = "warehouse"
    return {
        "localParams": [],
        "ruleId": 10,
        "ruleInputParameter": rule_input_parameter,
        "sparkParameters": {
            "deployMode": "local",
            "programType": "JAVA",
            "mainClass": (
                "org.apache.dolphinscheduler.data.quality.DataQualityApplication"
            ),
        },
    }


def _richer_native(version: str) -> JsonObject:
    native = deepcopy(_native(version))
    native["futureField"] = {"preserve": [True, {"epoch": version}]}
    return native


def _workflow_spec(
    version: str,
    params: JsonObject | None = None,
) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {
                "name": f"data-quality-{version}",
                "project": "analytics",
            },
            "tasks": [
                {
                    "name": _TASK_NAME,
                    "type": "DATA_QUALITY",
                    "task_params": cast(
                        "YamlValue",
                        _canonical(version) if params is None else params,
                    ),
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _fake_dag(version: str, native: JsonObject, *, workflow_name: str) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name=_TASK_NAME,
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="DATA_QUALITY",
        task_params_value=json.dumps(native),
        worker_group_value="default",
        fail_retry_times_value=0,
        fail_retry_interval_value=0,
        timeout=3600,
    )
    if version in _DATABASE_WIRE_VERSIONS:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> JsonObject:
    definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])
    native = json.loads(definitions[0]["taskParams"])
    assert isinstance(native, dict)
    return cast("JsonObject", native)


def _metadata_plan(
    version: str,
    native: JsonObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(
        version,
        native,
        workflow_name=f"data-quality-metadata-{version}",
    )
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": _TASK_NAME},
                                "set": {"description": "metadata only"},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [
                current.model_copy(update={"description": "metadata only"}, deep=True)
            ]
        },
        deep=True,
    )
    return prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )


def _edit_opaque_task_params(version: str, *, input_mode: str) -> None:
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    native = _richer_native(version)
    changed = deepcopy(native)
    changed["futureField"] = {"edited": True}
    dag = _fake_dag(version, native, workflow_name=f"data-quality-edit-{version}")
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": _TASK_NAME},
                                "set": {"task_params": changed},
                            }
                        ]
                    }
                }
            }
        ).patch
        prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
        return
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [current.model_copy(update={"task_params": changed}, deep=True)]
        },
        deep=True,
    )
    prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )


def test_data_quality_source_membership_is_exactly_the_reviewed_releases() -> None:
    source_versions = {
        version
        for version, profile in TASK_PROFILES.items()
        if "DATA_QUALITY" in cast("dict[str, object]", profile["task_types"])
    }

    assert source_versions == set(_TYPED_VERSIONS)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_catalog_exposes_one_closed_typed_facet(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    entry = catalog.require_task_type("DATA_QUALITY")
    membership = entry.default

    assert entry.default_facet == _FACET
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert catalog.supports_typed_authoring("DATA_QUALITY") is True
    assert catalog.supports_opaque_authoring("DATA_QUALITY") is False
    assert "DATA_QUALITY" in catalog.authoring_task_types
    assert "DATA_QUALITY" in supported_task_template_types(catalog=catalog)


@pytest.mark.parametrize(
    ("version", "expected_fields"),
    [
        *((version, _LEGACY_FIELDS) for version in _TYPED_VERSIONS[:-3]),
        *((version, _DATABASE_FIELDS) for version in _DATABASE_WIRE_VERSIONS),
    ],
)
def test_data_quality_schema_exposes_only_exact_canonical_fields(
    version: str,
    expected_fields: set[str],
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_type_schema_result("DATA_QUALITY", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert {
        path.removeprefix("task_params.")
        for path in fields
        if path.startswith("task_params.")
    } == expected_fields
    assert fields["task_params.datasource"]["choice_source"] == (
        "dsctl datasource list"
    )
    assert fields["task_params.datasource"]["choice_value"] == "name or id"
    assert "ruleId" not in fields
    assert "ruleInputParameter" not in fields
    assert "sparkParameters" not in fields

    json_schema = task_type_schema_result(
        "DATA_QUALITY",
        json_schema=True,
        catalog=catalog,
    )
    assert isinstance(json_schema.data, dict)
    task_params = cast(
        "JsonObject",
        json_schema.data["schema"]["$defs"]["task_params"],
    )
    assert task_params["additionalProperties"] is False
    assert set(cast("dict[str, object]", task_params["properties"])) == expected_fields
    assert set(cast("list[str]", task_params["required"])) == expected_fields


@pytest.mark.parametrize(
    ("version", "params"),
    [
        (
            "3.0.0",
            {
                "datasource": True,
                "table": "events",
                "expectedRowCount": 42,
            },
        ),
        (
            "3.0.0",
            {
                "datasource": " ",
                "table": "events",
                "expectedRowCount": 42,
            },
        ),
        (
            "3.0.0",
            {"datasource": 0, "table": "events", "expectedRowCount": 42},
        ),
        (
            "3.0.0",
            {
                "datasource": 2_147_483_648,
                "table": "events",
                "expectedRowCount": 42,
            },
        ),
        (
            "3.0.0",
            {"datasource": 7, "table": "warehouse.events", "expectedRowCount": 42},
        ),
        (
            "3.0.0",
            {"datasource": 7, "table": "events", "expectedRowCount": True},
        ),
        (
            "3.0.0",
            {"datasource": 7, "table": "events", "expectedRowCount": "42"},
        ),
        (
            "3.0.0",
            {"datasource": 7, "table": "events", "expectedRowCount": -1},
        ),
        (
            "3.0.0",
            {
                "datasource": 7,
                "table": "events",
                "expectedRowCount": 9_007_199_254_740_992,
            },
        ),
        (
            "3.0.0",
            {
                "datasource": 7,
                "database": "warehouse",
                "table": "events",
                "expectedRowCount": 42,
            },
        ),
        (
            "3.0.0",
            {
                "datasource": 7,
                "table": "events",
                "expectedRowCount": 42,
                "futureField": True,
            },
        ),
        (
            "3.0.0",
            {
                "datasource": 7,
                "table": "events",
                "expected_row_count": 42,
            },
        ),
        (
            "3.2.2",
            {"datasource": 7, "table": "events", "expectedRowCount": 42},
        ),
        (
            "3.2.2",
            {
                "datasource": 7,
                "database": "warehouse-prod",
                "table": "events",
                "expectedRowCount": 42,
            },
        ),
    ],
)
def test_data_quality_typed_schema_rejects_outside_closed_subset(
    version: str,
    params: JsonObject,
) -> None:
    with pytest.raises(ValidationError, match="task_params"):
        _workflow_spec(version, params)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_surface_records_exact_result_operator_epoch(version: str) -> None:
    surface = get_task_authoring_surface(version).data_quality

    assert surface.available is True
    assert surface.rule_id == 10
    assert surface.main_class == (
        "org.apache.dolphinscheduler.data.quality.DataQualityApplication"
    )
    assert surface.fixed_value_comparison is True
    assert surface.blocking_failure_strategy is True
    assert surface.result_operator == (
        "NE"
        if version in {"3.0.0", "3.0.1", "3.0.2", "3.0.3"} or version.startswith("3.1.")
        else "EQ"
    )
    assert surface.database_wire_required is (version in _DATABASE_WIRE_VERSIONS)
    assert surface.database_field_required is (version in _DATABASE_WIRE_VERSIONS)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_projector_emits_the_exact_closed_native_wire(
    version: str,
) -> None:
    projected = encode_task_parameters(
        version=version,
        task_type="DATA_QUALITY",
        task_params=_canonical(version),
        refs=_NO_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert projected.task_type == "DATA_QUALITY"
    assert projected.task_params == _native(version)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_exact_wire_round_trips_with_typed_provenance(
    version: str,
) -> None:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="DATA_QUALITY",
        task_params=_native(version),
        refs=_NO_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical(version)
    assert encode_task_parameters(
        version=version,
        task_type="DATA_QUALITY",
        task_params=decoded.task.task_params,
        refs=_NO_REFS,
        source=decoded.reencode_source,
    ).task_params == _native(version)


@pytest.mark.parametrize(
    ("version", "mutation"),
    [
        *(
            (version, mutation)
            for version in _TYPED_VERSIONS
            for mutation in (
                "richer",
                "changed-operator",
                "changed-rule-id",
                "nonempty-local-params",
                "changed-rule-constant",
                "missing-rule-key",
                "changed-main-class",
                "spark-extra",
            )
        ),
        *((version, "invalid-database") for version in _DATABASE_WIRE_VERSIONS),
    ],
)
def test_data_quality_noncanonical_native_wire_remains_opaque(
    version: str,
    mutation: str,
) -> None:
    native = deepcopy(_native(version))
    if mutation == "richer":
        native["futureField"] = {"preserve": True}
    elif mutation == "changed-rule-id":
        native["ruleId"] = 11
    elif mutation == "nonempty-local-params":
        native["localParams"] = [{"prop": "unexpected"}]
    elif mutation in {
        "changed-operator",
        "changed-rule-constant",
        "missing-rule-key",
        "invalid-database",
    }:
        rule_input = cast("JsonObject", native["ruleInputParameter"])
        if mutation == "changed-operator":
            rule_input["operator"] = "0" if rule_input["operator"] == "5" else "5"
        elif mutation == "changed-rule-constant":
            rule_input["statistics_name"] = "table_count.changed"
        elif mutation == "missing-rule-key":
            rule_input.pop("src_filter")
        else:
            rule_input["src_database"] = "warehouse-prod"
    else:
        spark = cast("JsonObject", native["sparkParameters"])
        if mutation == "changed-main-class":
            spark["mainClass"] = "org.example.OtherApplication"
        else:
            spark["futureField"] = True

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="DATA_QUALITY",
        task_params=native,
        refs=_NO_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native
    assert (
        encode_task_parameters(
            version=version,
            task_type="DATA_QUALITY",
            task_params=decoded.task.task_params,
            refs=_NO_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )

    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters_with_provenance(
            version=version,
            task_type="DATA_QUALITY",
            task_params=native,
            refs=_NO_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_public_workflow_compile_emits_exact_native_wire(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _workflow_spec(version),
        catalog=catalog,
    )
    definitions = json.loads(
        prepared.materialize([71_000])["taskDefinitionJson"],
    )
    assert definitions[0]["taskType"] == "DATA_QUALITY"
    assert json.loads(definitions[0]["taskParams"]) == _native(version)


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_data_quality_public_workflow_create_rejects_upstream_absent_versions(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "DATA_QUALITY" not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring("DATA_QUALITY") is False
    with pytest.raises(UnsupportedFeatureError, match="DATA_QUALITY"):
        _workflow_spec(version)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_safe_native_baseline_and_export_are_canonical_typed(
    version: str,
) -> None:
    native = _native(version)
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(version, native, workflow_name=f"data-quality-safe-{version}")

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert baseline.projection_sources[_TASK_NAME] is ProjectionSource.TYPED_AUTHORING
    assert baseline.spec.tasks[0].task_params == _canonical(version)
    assert document["tasks"][0]["task_params"] == _canonical(version)
    assert set(baseline.spec.tasks[0].task_params) == (
        _DATABASE_FIELDS if version in _DATABASE_WIRE_VERSIONS else _LEGACY_FIELDS
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_data_quality_richer_native_baseline_and_export_remain_opaque(
    version: str,
) -> None:
    native = _richer_native(version)
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(version, native, workflow_name=f"data-quality-opaque-{version}")

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert baseline.projection_sources[_TASK_NAME] is ProjectionSource.OPAQUE_PRESERVE
    assert baseline.spec.tasks[0].task_params == native
    assert document["tasks"][0]["task_params"] == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_data_quality_metadata_edits_preserve_richer_native_wire(
    version: str,
    input_mode: str,
) -> None:
    native = _richer_native(version)

    assert (
        _compiled_from_plan(
            _metadata_plan(version, native, input_mode=input_mode),
        )
        == native
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_data_quality_opaque_task_param_edits_fail_closed(
    version: str,
    input_mode: str,
) -> None:
    with pytest.raises((UnsupportedFeatureError, UserInputError)):
        _edit_opaque_task_params(version, input_mode=input_mode)
