from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, ClassVar, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import (
    WorkflowMutationPlan,
    prepare_workflow_file_mutation_plan,
    prepare_workflow_mutation_plan,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject


TYPED_VERSIONS = (
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
)
REPRESENTATIVE_VERSIONS = ("3.2.0", "3.2.2", "3.4.2")

_PROJECT = ResolvedProject(code=7, name="analytics", description=None)
_REFS = TaskRefIndex.from_code_by_name({})


@dataclass(frozen=True)
class ClosedFacetContractCase:
    """Shared lifecycle harness for closed, exact task-authoring facets."""

    task_type: str
    facet: str
    task_name: str
    review: str
    family: str
    params_model_name: str
    fingerprints: Mapping[str, str]
    fields: frozenset[str]
    canonical: Callable[[], YamlObject]
    opaque_native: Callable[[], YamlObject]
    task_spec_extras: Mapping[str, YamlValue]
    timeout: int = 0

    @property
    def workflow_stem(self) -> str:
        return self.task_type.lower().replace("_", "-")

    def spec(
        self,
        version: str,
        params: YamlObject,
        *,
        intent: TaskAuthoringIntent = TaskAuthoringIntent.TYPED_CREATE,
        workflow_name: str | None = None,
        project: str | None = None,
    ) -> WorkflowSpec:
        task: YamlObject = {
            "name": self.task_name,
            "type": self.task_type,
            "task_params": params,
            **deepcopy(dict(self.task_spec_extras)),
        }
        workflow: YamlObject = {
            "name": workflow_name or f"{self.workflow_stem}-{version}"
        }
        if project is not None:
            workflow["project"] = project
        catalog = get_task_authoring_catalog(version)
        return validate_workflow_document(
            {"workflow": workflow, "tasks": [task]},
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=intent,
            ),
        )

    def compiled(self, version: str, params: YamlObject) -> YamlObject:
        catalog = get_task_authoring_catalog(version)
        prepared = prepare_workflow_create_compilation(
            self.spec(version, params),
            catalog=catalog,
        )
        definition = json.loads(prepared.materialize([36_000])["taskDefinitionJson"])[0]
        native = json.loads(definition["taskParams"])
        assert definition["taskType"] == self.task_type
        assert isinstance(native, dict)
        return cast("YamlObject", native)

    def encode(self, version: str, params: YamlObject) -> YamlObject:
        projected = encode_task_parameters(
            version=version,
            task_type=self.task_type,
            task_params=cast("JsonObject", deepcopy(params)),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
        return cast("YamlObject", projected.task_params)

    def decode(
        self,
        version: str,
        params: YamlObject,
        *,
        source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
    ) -> tuple[YamlObject, ProjectionSource]:
        decoded = decode_task_parameters_with_provenance(
            version=version,
            task_type=self.task_type,
            task_params=cast("JsonObject", deepcopy(params)),
            refs=_REFS,
            source=source,
        )
        return cast("YamlObject", decoded.task.task_params), decoded.reencode_source

    def fake_dag(
        self,
        version: str,
        params: YamlObject,
        *,
        workflow_name: str,
        task_name: str | None = None,
    ) -> FakeDag:
        task = self.make_fake_task(
            version, params, task_name=task_name or self.task_name
        )
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

    def make_fake_task(
        self,
        version: str,
        params: YamlObject,
        *,
        task_name: str,
        code: int = 101,
    ) -> FakeTaskDefinition:
        task = FakeTaskDefinition(
            code=code,
            name=task_name,
            project_code_value=7,
            project_name_value="analytics",
            task_type_value=self.task_type,
            task_params_value=json.dumps(params),
            worker_group_value="default",
            timeout=self.timeout,
        )
        if version in {"3.2.0", "3.2.1", "3.2.2"}:
            return replace(task, is_cache_value=FakeEnumValue("NO"))
        return task

    def edit_plan(
        self,
        version: str,
        params: YamlObject,
        *,
        input_mode: str,
    ) -> WorkflowMutationPlan:
        workflow_name = f"{self.workflow_stem}-edit"
        dag = self.fake_dag(
            version,
            self.canonical(),
            workflow_name=workflow_name,
        )
        catalog = get_task_authoring_catalog(version)
        if input_mode == "patch":
            return self.mutation_plan(
                version,
                dag,
                {
                    "update": [
                        {
                            "match": {"name": self.task_name},
                            "set": {"task_params": params},
                        }
                    ]
                },
            )
        desired = self.spec(
            version,
            params,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
            workflow_name=workflow_name,
            project="analytics",
        )
        return prepare_workflow_file_mutation_plan(
            dag,
            project=_PROJECT,
            desired=desired,
            release_state="OFFLINE",
            catalog=catalog,
        )

    def metadata_plan(
        self,
        version: str,
        native: YamlObject,
        *,
        input_mode: str,
    ) -> WorkflowMutationPlan:
        dag = self.fake_dag(
            version,
            native,
            workflow_name=f"{self.workflow_stem}-metadata-edit",
        )
        catalog = get_task_authoring_catalog(version)
        return authoring_prep.single_task_metadata_edit_plan(
            dag,
            project=_PROJECT,
            catalog=catalog,
            task_name=self.task_name,
            input_mode=input_mode,
        )

    @staticmethod
    def mutation_plan(
        version: str,
        dag: FakeDag,
        task_patch: YamlObject,
    ) -> WorkflowMutationPlan:
        patch = WorkflowPatchDocument.model_validate(
            {"patch": {"tasks": task_patch}}
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=_PROJECT,
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )

    @staticmethod
    def compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
        definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
        params = json.loads(definition["taskParams"])
        assert isinstance(params, dict)
        return cast("YamlObject", params)

    def template_yaml(self, version: str) -> str:
        return authoring_prep.template_yaml(self.task_type, version)

    def task_params_schema(self, version: str) -> dict[object, object]:
        return authoring_prep.task_params_schema(self.task_type, version)

    @staticmethod
    def resolved_field_schema(
        task_params_schema: dict[object, object],
        field_name: str,
    ) -> dict[object, object]:
        return authoring_prep.resolved_field_schema(task_params_schema, field_name)

    def runtime_accepts(self, field_name: str, value: YamlValue) -> bool:
        params = self.canonical()
        params[field_name] = value
        try:
            get_task_authoring_catalog("3.4.2").normalize_task_params(
                self.task_type,
                params,
                intent=TaskAuthoringIntent.TYPED_CREATE,
            )
        except (UnsupportedFeatureError, ValueError):
            return False
        return True

    def guidance(self, version: str) -> str:
        return authoring_prep.schema_guidance(self.task_type, version)

    def native_for_kind(self, kind: str) -> YamlObject:
        if kind == "safe":
            return self.canonical()
        assert kind == "opaque"
        return self.opaque_native()


class ClosedFacetContractSuite:
    """Reusable assertions for the lifecycle shared by closed exact facets."""

    case: ClassVar[ClosedFacetContractCase]

    @pytest.mark.parametrize("version", TYPED_VERSIONS)
    def test_catalog_exposes_one_exact_closed_facet(self, version: str) -> None:
        case = self.case
        catalog = get_task_authoring_catalog(version)
        profile = catalog.require_task_type(case.task_type)
        membership = catalog.require_facet(case.task_type, case.facet)
        fact = catalog.task_type_facts[case.task_type]
        review = fact.typed_authoring_review
        assert catalog.supports_typed_authoring(case.task_type) is True
        assert catalog.supports_opaque_authoring(case.task_type) is False
        assert profile.category == "Cloud"
        assert profile.default_facet == case.facet
        assert set(profile.facets) == {case.facet}
        assert fact.semantic_fingerprint == case.fingerprints[version]
        assert review is not None
        assert review.review == case.review
        assert review.semantic_fingerprint == fact.semantic_fingerprint
        assert membership.contract.review == review.review
        assert membership.contract.family == case.family
        assert membership.contract.params_model is not None
        assert membership.contract.params_model.__name__ == case.params_model_name
        assert membership.contract.opaque_authoring_selector is None
        assert membership.profile_version == version
        assert membership.typed_create is True
        assert membership.typed_edit is True
        assert membership.opaque_create is False
        assert membership.opaque_edit is False
        assert membership.opaque_preserve is True

    @pytest.mark.parametrize("version", ABSENT_VERSIONS)
    def test_is_upstream_absent_before_3_2(self, version: str) -> None:
        task_type = self.case.task_type
        catalog = get_task_authoring_catalog(version)
        assert task_type not in catalog.upstream_task_types
        assert catalog.supports_typed_authoring(task_type) is False
        assert catalog.supports_opaque_authoring(task_type) is False
        assert task_type not in catalog.authoring_task_types

    @pytest.mark.parametrize("version", TYPED_VERSIONS)
    def test_schema_exposes_only_owned_fields(self, version: str) -> None:
        case = self.case
        result = task_type_schema_result(
            case.task_type,
            catalog=get_task_authoring_catalog(version),
        )
        assert isinstance(result.data, dict)
        fields = {
            field["path"]: field
            for field in result.data["fields"]
            if isinstance(field, dict) and isinstance(field.get("path"), str)
        }
        task_fields = {
            path.removeprefix("task_params."): field
            for path, field in fields.items()
            if path.startswith("task_params.")
        }
        assert result.data["task_type"] == case.task_type
        assert result.data["category"] == "Cloud"
        assert result.data["kind"] == "typed"
        assert set(task_fields) == case.fields
        assert result.data["state_rules"] == []
        for field_name in case.fields:
            assert task_fields[field_name]["required"] is True
            assert task_fields[field_name]["compile_path"].endswith(
                f"taskParams.{field_name}"
            )

    @pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
    def test_json_schema_is_closed_and_requires_every_owned_field(
        self,
        version: str,
    ) -> None:
        case = self.case
        task_params = case.task_params_schema(version)
        properties = task_params["properties"]
        required = task_params["required"]
        assert isinstance(properties, dict)
        assert isinstance(required, list)
        assert task_params["additionalProperties"] is False
        assert set(properties) == case.fields
        assert set(required) == case.fields

    @pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
    def test_summary_and_mappings_publish_the_owned_interface(
        self,
        version: str,
    ) -> None:
        case = self.case
        catalog = get_task_authoring_catalog(version)
        summary = task_type_summary_data(case.task_type, catalog=catalog)
        result = task_type_schema_result(
            case.task_type,
            compile_mappings=True,
            catalog=catalog,
        )
        assert isinstance(result.data, dict)
        mappings = {
            mapping["authoring_path"]: mapping["ds_payload_path"]
            for mapping in result.data["compile_mappings"]
            if isinstance(mapping, dict)
            and isinstance(mapping.get("authoring_path"), str)
            and mapping["authoring_path"].startswith("task_params.")
        }
        assert summary["task_type"] == case.task_type
        assert summary["category"] == "Cloud"
        assert summary["kind"] == "typed"
        assert "default_variant" not in summary
        assert summary["variants"] == []
        assert set(summary["required_paths"]) == {
            "name",
            "type",
            "task_params",
            *{f"task_params.{field_name}" for field_name in case.fields},
        }
        assert set(mappings) == {
            f"task_params.{field_name}" for field_name in case.fields
        }
        for authoring_path, payload_path in mappings.items():
            field_name = authoring_path.removeprefix("task_params.")
            assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"

    @pytest.mark.parametrize("version", ABSENT_VERSIONS)
    def test_projector_rejects_versions_without_the_plugin(self, version: str) -> None:
        case = self.case
        with pytest.raises(TaskParameterProjectionError, match=case.task_type):
            case.encode(version, case.canonical())

    def test_typed_create_and_edit_require_every_owned_field(self) -> None:
        case = self.case
        for field_name in sorted(case.fields):
            params = case.canonical()
            params.pop(field_name)
            for intent in (
                TaskAuthoringIntent.TYPED_CREATE,
                TaskAuthoringIntent.TYPED_EDIT,
            ):
                with pytest.raises(ValueError, match=field_name):
                    get_task_authoring_catalog("3.4.2").normalize_task_params(
                        case.task_type,
                        params,
                        intent=intent,
                    )

    @pytest.mark.parametrize("version", REPRESENTATIVE_VERSIONS)
    @pytest.mark.parametrize(
        "intent",
        [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
    )
    def test_has_no_public_raw_opaque_create_or_edit_selector(
        self,
        version: str,
        intent: TaskAuthoringIntent,
    ) -> None:
        case = self.case
        with pytest.raises(UnsupportedFeatureError) as captured:
            get_task_authoring_catalog(version).normalize_task_params(
                case.task_type,
                case.opaque_native(),
                intent=intent,
            )
        assert str(captured.value) == (
            f"{case.task_type} opaque authoring is unsupported for "
            f"DolphinScheduler {version}."
        )
        assert captured.value.details == {
            "selected_version": version,
            "task_type": case.task_type,
            "intent": intent.value,
            "constraint": (
                f"Exact DolphinScheduler {version} policy permits only opaque "
                f"preservation for {case.task_type}."
            ),
        }

    @pytest.mark.parametrize("version", TYPED_VERSIONS)
    def test_safe_and_extra_native_payloads_have_explicit_provenance(
        self,
        version: str,
    ) -> None:
        case = self.case
        safe = case.canonical()
        decoded_safe, safe_source = case.decode(version, safe)
        opaque = case.opaque_native()
        decoded_opaque, opaque_source = case.decode(version, opaque)
        assert decoded_safe == safe
        assert safe_source is ProjectionSource.TYPED_AUTHORING
        assert decoded_opaque == opaque
        assert opaque_source is ProjectionSource.OPAQUE_PRESERVE

    @pytest.mark.parametrize("native_kind", ["safe", "opaque"])
    def test_export_distinguishes_safe_and_opaque_provenance(
        self,
        native_kind: str,
    ) -> None:
        case = self.case
        native = case.native_for_kind(native_kind)
        expected_source = (
            ProjectionSource.TYPED_AUTHORING
            if native_kind == "safe"
            else ProjectionSource.OPAQUE_PRESERVE
        )
        version = "3.4.2"
        dag = case.fake_dag(
            version,
            native,
            workflow_name=f"{case.workflow_stem}-export",
        )
        catalog = workflow_authoring_catalog_for_version(version)
        baseline = workflow_live_baseline(dag, project=_PROJECT, catalog=catalog)
        document = yaml.safe_load(
            workflow_yaml_document(
                dag,
                project=_PROJECT,
                attached_schedule=None,
                catalog=catalog,
            )
        )
        assert baseline.projection_sources[case.task_name] is expected_source
        assert baseline.spec.tasks[0].task_params == native
        assert document["tasks"][0]["task_params"] == native

    @pytest.mark.parametrize("input_mode", ["patch", "file"])
    @pytest.mark.parametrize("native_kind", ["safe", "opaque"])
    def test_metadata_edits_preserve_exact_native_wire(
        self,
        native_kind: str,
        input_mode: str,
    ) -> None:
        case = self.case
        native = case.native_for_kind(native_kind)
        plan = case.metadata_plan("3.4.2", native, input_mode=input_mode)
        assert case.compiled_from_plan(plan) == native

    @pytest.mark.parametrize("native_kind", ["safe", "opaque"])
    def test_rename_preserves_safe_and_opaque_wire(self, native_kind: str) -> None:
        case = self.case
        native = case.native_for_kind(native_kind)
        version = "3.4.2"
        dag = case.fake_dag(
            version,
            native,
            workflow_name=f"{case.workflow_stem}-rename",
        )
        renamed_task_name = f"renamed-{case.task_name}"
        plan = case.mutation_plan(
            version,
            dag,
            {"rename": [{"from": case.task_name, "to": renamed_task_name}]},
        )
        definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
        assert definition["name"] == renamed_task_name
        assert json.loads(definition["taskParams"]) == native

    def test_delete_drops_deleted_provenance_and_keeps_other_wire(self) -> None:
        case = self.case
        version = "3.4.2"
        kept = case.opaque_native()
        delete_name = f"delete-{case.workflow_stem}"
        keep_name = f"keep-{case.workflow_stem}"
        tasks = [
            case.make_fake_task(
                version, case.canonical(), task_name=delete_name, code=101
            ),
            case.make_fake_task(version, kept, task_name=keep_name, code=102),
        ]
        dag = FakeDag(
            workflow_definition_value=FakeWorkflow(
                code=11,
                name=f"{case.workflow_stem}-delete",
                project_code_value=7,
                project_name_value="analytics",
            ),
            task_definition_list_value=tasks,
            workflow_task_relation_list_value=[],
        )
        plan = case.mutation_plan(
            version,
            dag,
            {"delete": [delete_name]},
        )
        definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])
        assert [definition["name"] for definition in definitions] == [keep_name]
        assert json.loads(definitions[0]["taskParams"]) == kept

    @pytest.mark.parametrize("native_kind", ["safe", "opaque"])
    def test_unchanged_update_preserves_safe_and_opaque_wire(
        self,
        native_kind: str,
    ) -> None:
        case = self.case
        native = case.native_for_kind(native_kind)
        version = "3.4.2"
        catalog = workflow_authoring_catalog_for_version(version)
        baseline = workflow_live_baseline(
            case.fake_dag(
                version,
                native,
                workflow_name=f"{case.workflow_stem}-unchanged",
            ),
            project=_PROJECT,
            catalog=catalog,
        )
        payload = prepare_preserved_workflow_update_compilation(
            baseline.spec,
            release_state="OFFLINE",
            active_task_identities=baseline.task_identities,
            unavailable_task_identities=(),
            preserved_projection_sources=baseline.projection_sources,
            catalog=catalog,
        ).materialize([])
        compiled = json.loads(
            json.loads(payload["taskDefinitionJson"])[0]["taskParams"]
        )
        assert compiled == native

    @pytest.mark.parametrize("version", TYPED_VERSIONS)
    def test_opaque_preserve_is_a_deep_identity_copy(self, version: str) -> None:
        case = self.case
        native = case.opaque_native()
        expected = deepcopy(native)

        preserved = get_task_authoring_catalog(version).normalize_task_params(
            case.task_type,
            native,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )
        assert preserved == expected
        assert preserved is not native
        future = native["futureField"]
        assert isinstance(future, dict)
        nested = future["nested"]
        assert isinstance(nested, list)
        nested.append("mutated")
        assert preserved == expected
