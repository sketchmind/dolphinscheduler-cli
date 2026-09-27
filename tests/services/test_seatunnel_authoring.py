from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from pydantic import ValidationError
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.task_spec import SeatunnelLiteralLocalConfigTaskParamsSpec
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import (
    preflight_seatunnel_runtime_activation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
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
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject

_CONFIG = """env {
  execution.parallelism = 1
}
source { FakeSource {} }
sink { Console {} }
"""
_NO_REFS = TaskRefIndex.from_code_by_name({})
_PROJECT = ResolvedProject(code=7, name="analytics", description=None)
_TYPED_VERSIONS = (
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
)


def _legacy_wrapper(config: str = _CONFIG) -> str:
    quoted = "'" + config.replace("'", "'\"'\"'") + "'"
    return (
        "#!/bin/sh\n"
        "set -eu\n"
        f"printf '%s' {quoted} > .dsctl-seatunnel.conf\n"
        'exec sh "$SEATUNNEL_HOME/bin/start-seatunnel-spark.sh" '
        "--config .dsctl-seatunnel.conf --deploy-mode client --master local\n"
    )


def _native(version: str, config: str = _CONFIG) -> JsonObject:
    if version in {"3.0.0", "3.0.6"}:
        return {
            "localParams": [],
            "resourceList": [],
            "rawScript": _legacy_wrapper(config),
        }
    if version == "3.1.0":
        return {
            "localParams": [],
            "engine": "SPARK",
            "useCustom": True,
            "rawScript": config,
            "resourceList": [],
            "deployMode": "local",
        }
    return {
        "localParams": [],
        "startupScript": "seatunnel.sh",
        "useCustom": True,
        "rawScript": config,
        "resourceList": [],
        "deployMode": "local",
        "others": "",
    }


def _fake_dag(
    native: JsonObject,
    *,
    workflow_name: str,
    global_params: bool = False,
) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
            global_params_value=(
                '[{"prop":"unsafe","value":"value"}]' if global_params else None
            ),
            global_param_map_value={"unsafe": "value"} if global_params else None,
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name="run-seatunnel",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value="SEATUNNEL",
                task_params_value=json.dumps(native),
                worker_group_value="default",
                timeout=0,
            )
        ],
        workflow_task_relation_list_value=[],
    )


def test_seatunnel_literal_config_model_is_closed_and_preserves_ascii_lf() -> None:
    params = SeatunnelLiteralLocalConfigTaskParamsSpec.model_validate(
        {"rawScript": _CONFIG}
    )

    assert params.to_payload() == {"rawScript": _CONFIG}

    for invalid in (
        "   ",
        "source { path = ${unsafe} }",
        "source { date = $[yyyyMMdd] }",
        'source { value = "snowman ☃" }',
        'source { value = "bad\rline" }',
        'source { value = "bad\x00line" }',
    ):
        with pytest.raises(ValidationError):
            SeatunnelLiteralLocalConfigTaskParamsSpec.model_validate(
                {"rawScript": invalid}
            )

    with pytest.raises(ValidationError):
        SeatunnelLiteralLocalConfigTaskParamsSpec.model_validate(
            {"rawScript": _CONFIG, "others": "--unsafe"}
        )


@pytest.mark.parametrize(
    ("version", "wire_epoch", "execution_epoch", "forwarding", "cancel_mode"),
    [
        (
            "3.0.0",
            "legacy-raw-shell",
            "legacy-shell-file",
            "none",
            "wrapper-kill",
        ),
        (
            "3.0.6",
            "legacy-raw-shell",
            "legacy-shell-file",
            "none",
            "wrapper-kill",
        ),
        (
            "3.1.0",
            "engine-custom-config",
            "legacy-shell-file",
            "none",
            "wrapper-kill",
        ),
        (
            "3.1.9",
            "startup-script-custom-config",
            "legacy-shell-file",
            "none",
            "wrapper-kill",
        ),
        (
            "3.2.0",
            "startup-script-custom-config",
            "shell-interceptor",
            "none",
            "direct-process-destroy",
        ),
        (
            "3.2.1",
            "startup-script-custom-config",
            "shell-interceptor",
            "none",
            "direct-process-destroy",
        ),
        (
            "3.2.2",
            "startup-script-custom-config",
            "shell-interceptor",
            "none",
            "direct-process-destroy",
        ),
        (
            "3.3.1",
            "startup-script-custom-config",
            "shell-interceptor",
            "unsafe-values",
            "process-tree-and-application",
        ),
        (
            "3.3.2",
            "startup-script-custom-config",
            "shell-interceptor",
            "unsafe-values",
            "process-tree-and-application",
        ),
        (
            "3.4.0",
            "startup-script-custom-config",
            "task-request",
            "unsafe-values",
            "process-tree-and-application",
        ),
        (
            "3.4.1",
            "startup-script-custom-config",
            "task-request",
            "quoted-values",
            "process-tree-and-application",
        ),
        (
            "3.4.2",
            "startup-script-custom-config",
            "task-request",
            "quoted-values",
            "process-tree-and-application",
        ),
    ],
)
def test_seatunnel_surface_locks_exact_runtime_epochs(
    version: str,
    wire_epoch: str,
    execution_epoch: str,
    forwarding: str,
    cancel_mode: str,
) -> None:
    surface = get_task_authoring_surface(version).seatunnel

    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.execution_epoch == execution_epoch
    assert surface.workflow_parameter_forwarding == forwarding
    assert surface.cancel_mode == cancel_mode
    assert surface.parameter_substitution is True
    assert surface.config_logged is True
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_seatunnel_surface_is_absent_before_300(version: str) -> None:
    assert get_task_authoring_surface(version).seatunnel.available is False


@pytest.mark.parametrize(
    "version",
    [
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
def test_seatunnel_exact_wire_round_trips_as_typed_authoring(version: str) -> None:
    encoded = encode_task_parameters(
        version=version,
        task_type="SEATUNNEL",
        task_params={"rawScript": _CONFIG},
        refs=_NO_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    assert encoded.task_params == _native(version)

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="SEATUNNEL",
        task_params=encoded.task_params,
        refs=_NO_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == {"rawScript": _CONFIG}


def test_seatunnel_legacy_wrapper_quotes_literal_single_quotes() -> None:
    config = "source { query = \"select 'literal'\" }\n"

    encoded = encode_task_parameters(
        version="3.0.0",
        task_type="SEATUNNEL",
        task_params={"rawScript": config},
        refs=_NO_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert encoded.task_params == _native("3.0.0", config)
    raw_script = encoded.task_params["rawScript"]
    assert isinstance(raw_script, str)
    assert "'\"'\"'literal'\"'\"'" in raw_script


def test_seatunnel_richer_native_state_stays_opaque_and_lossless() -> None:
    native = _native("3.4.2")
    native["others"] = "--async"

    decoded = decode_task_parameters_with_provenance(
        version="3.4.2",
        task_type="SEATUNNEL",
        task_params=native,
        refs=_NO_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native
    assert (
        encode_task_parameters(
            version="3.4.2",
            task_type="SEATUNNEL",
            task_params=decoded.task.task_params,
            refs=_NO_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_seatunnel_projection_rejects_upstream_absent_versions(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        encode_task_parameters(
            version=version,
            task_type="SEATUNNEL",
            task_params={"rawScript": _CONFIG},
            refs=_NO_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_seatunnel_catalog_exposes_one_closed_literal_local_config_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("SEATUNNEL")
    membership = catalog.require_facet(
        "SEATUNNEL",
        "SEATUNNEL/literal_local_config_job",
    )

    assert profile.category == "DataIntegration"
    assert profile.kind == "typed"
    assert profile.default_facet == "SEATUNNEL/literal_local_config_job"
    assert set(profile.facets) == {"SEATUNNEL/literal_local_config_job"}
    assert membership.contract.params_model is SeatunnelLiteralLocalConfigTaskParamsSpec
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_seatunnel_catalog_reports_upstream_absence(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "SEATUNNEL" not in catalog.upstream_task_types
    with pytest.raises(UnsupportedFeatureError, match="SEATUNNEL"):
        catalog.require_task_type("SEATUNNEL")


def test_seatunnel_schema_and_template_publish_only_literal_config() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    result = task_type_schema_result("SEATUNNEL", json_schema=True, catalog=catalog)
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["rawScript"]
    assert set(task_params["properties"]) == {"rawScript"}

    template = task_template_result("SEATUNNEL", catalog=catalog)
    assert isinstance(template.data, dict)
    task = yaml.safe_load(template.data["yaml"])
    assert task["type"] == "SEATUNNEL"
    assert set(task["task_params"]) == {"rawScript"}

    spec = validate_workflow_document(
        {"workflow": {"name": "seatunnel-template"}, "tasks": [task]},
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    compiled = prepare_workflow_create_compilation(spec, catalog=catalog)
    definition = json.loads(compiled.materialize([41_001])["taskDefinitionJson"])[0]
    assert json.loads(definition["taskParams"]) == _native(
        "3.4.2",
        task["task_params"]["rawScript"],
    )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"])
def test_seatunnel_compile_rejects_workflow_globals_forwarded_outside_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {
                "name": "seatunnel-parameter-free",
                "global_params": {"unsafe": "value"},
            },
            "tasks": [
                {
                    "name": "run-seatunnel",
                    "type": "SEATUNNEL",
                    "task_params": {"rawScript": _CONFIG},
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match="parameter-free"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


def test_seatunnel_322_keeps_workflow_globals_outside_the_native_command() -> None:
    catalog = get_task_authoring_catalog("3.2.2")
    spec = validate_workflow_document(
        {
            "workflow": {
                "name": "seatunnel-before-global-forwarding",
                "global_params": {"safe": "not-forwarded"},
            },
            "tasks": [
                {
                    "name": "run-seatunnel",
                    "type": "SEATUNNEL",
                    "task_params": {"rawScript": _CONFIG},
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    compiled = prepare_workflow_create_compilation(spec, catalog=catalog)
    definition = json.loads(compiled.materialize([41_002])["taskDefinitionJson"])[0]

    assert json.loads(definition["taskParams"]) == _native("3.2.2")


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_native("3.4.2"), {"rawScript": _CONFIG}, ProjectionSource.TYPED_AUTHORING),
        (
            {**_native("3.4.2"), "futureField": {"preserve": True}},
            {**_native("3.4.2"), "futureField": {"preserve": True}},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_seatunnel_workflow_and_instance_render_seam_preserves_provenance(
    native: JsonObject,
    exported: JsonObject,
    source: ProjectionSource,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    dag = _fake_dag(native, workflow_name="seatunnel-export")

    baseline = workflow_live_baseline(dag, project=_PROJECT, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_PROJECT,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert baseline.projection_sources["run-seatunnel"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported


def test_seatunnel_richer_baseline_metadata_edit_preserves_exact_native_wire() -> None:
    native: JsonObject = {
        **_native("3.4.2"),
        "futureField": {"preserve": True},
    }
    catalog = get_task_authoring_catalog("3.4.2")
    dag = _fake_dag(
        native,
        workflow_name="seatunnel-metadata-preserve",
        global_params=True,
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "run-seatunnel"},
                            "set": {"description": "metadata only"},
                        }
                    ]
                }
            }
        }
    ).patch

    plan = prepare_workflow_mutation_plan(
        dag,
        project=_PROJECT,
        patch=patch,
        release_state="OFFLINE",
        catalog=catalog,
    )
    definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])

    assert json.loads(definitions[0]["taskParams"]) == native


@pytest.mark.parametrize("version", ["3.2.2", "3.4.2"])
def test_seatunnel_richer_opaque_activation_fails_closed(version: str) -> None:
    native: JsonObject = {**_native(version), "others": "--async"}
    catalog = get_task_authoring_catalog(version)
    baseline = workflow_live_baseline(
        _fake_dag(native, workflow_name="seatunnel-opaque-online"),
        project=_PROJECT,
        catalog=catalog,
    )

    with pytest.raises(UserInputError, match="richer opaque state"):
        preflight_seatunnel_runtime_activation(
            baseline.spec,
            profile_version=version,
            projection_sources=baseline.projection_sources,
        )


@pytest.mark.parametrize("version", ["3.2.2", "3.4.2"])
def test_seatunnel_richer_opaque_runtime_edit_fails_closed(version: str) -> None:
    native: JsonObject = {**_native(version), "others": "--async"}
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(native, workflow_name="seatunnel-opaque-runtime-edit")
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "run-seatunnel"},
                            "set": {"worker_group": "alternate-workers"},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="richer opaque state"):
        prepare_workflow_mutation_plan(
            dag,
            project=_PROJECT,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_seatunnel_raw_opaque_create_and_edit_are_closed(
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")

    with pytest.raises(UnsupportedFeatureError, match="SEATUNNEL"):
        catalog.normalize_task_params(
            "SEATUNNEL",
            cast(
                "YamlObject",
                {**_native("3.4.2"), "futureField": "unsafe"},
            ),
            intent=intent,
        )
