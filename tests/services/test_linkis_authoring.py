from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow

from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.task_profiles import TASK_PROFILES
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.mutation import prepare_workflow_file_mutation_plan
from dsctl.services._workflow.render import workflow_live_baseline
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import supported_task_template_types
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import ProjectionSource

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


_SOURCE_VERSIONS = {
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
}
_RUNTIME_HOLE = "linkis-command-result-never-populated-and-status-not-polled"


def _native() -> YamlObject:
    return {
        "useCustom": True,
        "paramScript": [],
        "rawScript": "--submitUser analyst --proxyUser batch sample.sql",
        "localParams": [],
        "resourceList": [],
    }


def _dag() -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=101,
            name="linkis-existing",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=201,
                name="linkis-native",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value="LINKIS",
                task_params_value=json.dumps(_native()),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )


def test_linkis_exact_source_coordinates_are_preserve_only_runtime_exclusions() -> None:
    source_versions = {
        version
        for version, profile in TASK_PROFILES.items()
        if "LINKIS" in cast("dict[str, object]", profile["task_types"])
    }
    assert source_versions == _SOURCE_VERSIONS

    for version in sorted(_SOURCE_VERSIONS):
        exclusions = cast(
            "dict[str, dict[str, object]]",
            TASK_PROFILES[version]["typed_authoring_exclusions"],
        )
        assert exclusions["LINKIS"]["reason"] == _RUNTIME_HOLE

        catalog = get_task_authoring_catalog(version)
        membership = catalog.require_task_type("LINKIS").default
        assert membership.typed_create is False
        assert membership.typed_edit is False
        assert membership.opaque_create is False
        assert membership.opaque_edit is False
        assert membership.opaque_preserve is True
        assert "LINKIS" in catalog.upstream_task_types
        assert "LINKIS" not in catalog.authoring_task_types
        assert "LINKIS" not in supported_task_template_types(catalog=catalog)

        native = _native()
        for intent in (
            TaskAuthoringIntent.TYPED_CREATE,
            TaskAuthoringIntent.TYPED_EDIT,
            TaskAuthoringIntent.OPAQUE_CREATE,
            TaskAuthoringIntent.OPAQUE_EDIT,
        ):
            with pytest.raises(UnsupportedFeatureError):
                catalog.normalize_task_params("LINKIS", native, intent=intent)

        preserved = catalog.normalize_task_params(
            "LINKIS",
            native,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )
        assert preserved == _native()
        assert preserved is not native


def test_linkis_exact_surface_records_broken_result_and_single_status_check() -> None:
    for version in sorted(_SOURCE_VERSIONS):
        surface = get_task_authoring_surface(version).linkis
        assert surface.registered is True
        assert surface.typed_available is False
        assert surface.exclusion_reason == _RUNTIME_HOLE
        assert surface.command_result_populated is False
        assert surface.status_check_count == 1
        assert surface.status_polled_to_terminal is False
        assert surface.task_id_persisted_after_submit is False
        assert surface.cancel_after_submit_failure is False
        assert surface.durable_application_id is False
        assert surface.failover_supported is False
        assert surface.retry_can_duplicate is True

    absent = get_task_authoring_surface("3.1.9").linkis
    assert absent.registered is False
    assert absent.status_check_count == 0


def test_linkis_metadata_only_edit_preserves_the_exact_native_payload() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    project = ResolvedProject(code=7, name="analytics", description=None)
    dag = _dag()
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    desired = baseline.spec.model_copy(
        update={"workflow": baseline.spec.workflow.model_copy(update={"timeout": 45})},
        deep=True,
    )

    plan = prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )
    definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])

    assert (
        baseline.projection_sources["linkis-native"] is ProjectionSource.OPAQUE_PRESERVE
    )
    assert baseline.spec.tasks[0].task_params == _native()
    assert json.loads(definitions[0]["taskParams"]) == _native()


def test_linkis_workflow_task_param_edit_fails_closed() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    project = ResolvedProject(code=7, name="analytics", description=None)
    dag = _dag()
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    changed_params = dict(_native())
    changed_params["rawScript"] = "--submitUser analyst changed.sql"
    changed_task = baseline.spec.tasks[0].model_copy(
        update={"task_params": changed_params},
        deep=True,
    )
    desired = baseline.spec.model_copy(update={"tasks": [changed_task]}, deep=True)

    with pytest.raises(UnsupportedFeatureError):
        prepare_workflow_file_mutation_plan(
            dag,
            project=project,
            desired=desired,
            release_state="OFFLINE",
            catalog=catalog,
        )


def test_linkis_workflow_create_fails_closed_before_compilation() -> None:
    catalog = get_task_authoring_catalog("3.4.1")

    with pytest.raises(UnsupportedFeatureError):
        validate_workflow_document(
            {
                "workflow": {"name": "linkis-new", "project": "analytics"},
                "tasks": [
                    {
                        "name": "linkis-native",
                        "type": "LINKIS",
                        "task_params": _native(),
                    }
                ],
            },
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.TYPED_CREATE,
            ),
        )
