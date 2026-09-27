from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import (
    _preview_task_resource_refs,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import (
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
from dsctl.services.template import task_template_metadata, task_template_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject

_REFS = TaskRefIndex.from_code_by_name({})
_RESOURCE_REFS = TaskResourceRefIndex.from_resolved_files(
    ["/ml/train.py"],
    id_by_full_name={"/ml/train.py": 71},
    wire_full_name_by_full_name={
        "/ml/train.py": "/tenant/resources/ml/train.py",
    },
)
_NAME_ONLY_RESOURCE_REFS = TaskResourceRefIndex.from_resolved_files(
    ["/ml/train.py"],
    id_by_full_name={},
    wire_full_name_by_full_name={
        "/ml/train.py": "/tenant/resources/ml/train.py",
    },
)
_PROJECT = ResolvedProject(code=7, name="analytics", description=None)
_TYPED_VERSIONS = (
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)


def _canonical() -> JsonObject:
    return {
        "pythonExecutable": "/opt/python/bin/python3",
        "scriptResource": "/ml/train.py",
        "scriptArgs": ["--epochs", "10"],
    }


def _native(version: str, *, richer: bool = False) -> JsonObject:
    native = encode_task_parameters(
        version=version,
        task_type="PYTORCH",
        task_params=_canonical(),
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params
    if richer:
        native["futureField"] = {"preserve": [True, {"epoch": version}]}
    return native


def _fake_dag(version: str, native: JsonObject, *, workflow_name: str) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name="train-model",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value="PYTORCH",
                task_params_value=json.dumps(native),
                worker_group_value="default",
                flag_value=FakeEnumValue("YES"),
                timeout=60,
                timeout_notify_strategy_value=FakeEnumValue("FAILED"),
                is_cache_value=(
                    FakeEnumValue("NO")
                    if version in {"3.2.0", "3.2.1", "3.2.2"}
                    else None
                ),
            )
        ],
        workflow_task_relation_list_value=[],
    )


def _compiled_task_params(
    plan: WorkflowMutationPlan,
    *,
    resource_refs: TaskResourceRefIndex = _RESOURCE_REFS,
) -> JsonObject:
    definitions = json.loads(
        plan.compilation.preview(resource_refs=resource_refs)["taskDefinitionJson"]
    )
    native = json.loads(definitions[0]["taskParams"])
    assert isinstance(native, dict)
    return cast("JsonObject", native)


def _metadata_plan(
    version: str,
    native: JsonObject,
    *,
    input_mode: str,
    resource_refs: TaskResourceRefIndex = _RESOURCE_REFS,
) -> WorkflowMutationPlan:
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(
        version,
        native,
        workflow_name=f"pytorch-metadata-{version}",
    )
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "train-model"},
                                "set": {"description": "metadata only"},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=_PROJECT,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
            resource_refs=resource_refs,
        )
    baseline = workflow_live_baseline(
        dag,
        project=_PROJECT,
        catalog=catalog,
        resource_refs=resource_refs,
    )
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
        project=_PROJECT,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
        resource_refs=resource_refs,
    )


def _edit_richer_task_params(version: str, *, input_mode: str) -> None:
    native = _native(version, richer=True)
    changed = deepcopy(native)
    changed["futureField"] = {"edited": True}
    dag = _fake_dag(
        version,
        native,
        workflow_name=f"pytorch-param-edit-{version}",
    )
    catalog = get_task_authoring_catalog(version)
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "train-model"},
                                "set": {"task_params": changed},
                            }
                        ]
                    }
                }
            }
        ).patch
        prepare_workflow_mutation_plan(
            dag,
            project=_PROJECT,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
            resource_refs=_RESOURCE_REFS,
        )
        return
    baseline = workflow_live_baseline(
        dag,
        project=_PROJECT,
        catalog=catalog,
        resource_refs=_RESOURCE_REFS,
    )
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"task_params": changed},
                    deep=True,
                )
            ]
        },
        deep=True,
    )
    prepare_workflow_file_mutation_plan(
        dag,
        project=_PROJECT,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
        resource_refs=_RESOURCE_REFS,
    )


def _edit_richer_runtime(version: str, *, input_mode: str) -> None:
    native = _native(version, richer=True)
    dag = _fake_dag(
        version,
        native,
        workflow_name=f"pytorch-runtime-edit-{version}",
    )
    catalog = get_task_authoring_catalog(version)
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "train-model"},
                                "set": {"worker_group": "gpu-workers"},
                            }
                        ]
                    }
                }
            }
        ).patch
        prepare_workflow_mutation_plan(
            dag,
            project=_PROJECT,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
            resource_refs=_RESOURCE_REFS,
        )
        return
    baseline = workflow_live_baseline(
        dag,
        project=_PROJECT,
        catalog=catalog,
        resource_refs=_RESOURCE_REFS,
    )
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [
                current.model_copy(update={"worker_group": "gpu-workers"}, deep=True)
            ]
        },
        deep=True,
    )
    prepare_workflow_file_mutation_plan(
        dag,
        project=_PROJECT,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
        resource_refs=_RESOURCE_REFS,
    )


def test_pytorch_schema_exposes_one_closed_resource_script_intent() -> None:
    result = task_type_schema_result(
        "PYTORCH",
        catalog=get_task_authoring_catalog("3.3.2"),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert {path for path in fields if path.startswith("task_params.")} == {
        "task_params.pythonExecutable",
        "task_params.scriptResource",
        "task_params.scriptArgs",
        "task_params.scriptArgs[]",
    }
    assert fields["task_params.scriptResource"]["choice_source"] == (
        "dsctl resource list"
    )
    assert fields["task_params.scriptResource"]["choice_value"] == (
        "fullName minus resolved.directory, retaining one leading slash"
    )
    assert (
        "do not pass the storage absolute fullName"
        in fields["task_params.scriptResource"]["description"]
    )
    assert fields["task_params.pythonExecutable"]["required"] is True
    assert fields["task_params.scriptArgs"]["default"] == []


def test_pytorch_json_schema_is_closed_and_ascii_shell_safe() -> None:
    result = task_type_schema_result(
        "PYTORCH",
        json_schema=True,
        catalog=get_task_authoring_catalog("3.3.2"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["pythonExecutable", "scriptResource"]
    assert set(task_params["properties"]) == {
        "pythonExecutable",
        "scriptResource",
        "scriptArgs",
    }
    assert task_params["properties"]["pythonExecutable"]["pattern"] == (
        r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
        r"[A-Za-z0-9_.:@%+=,\-]+(?:/[A-Za-z0-9_.:@%+=,\-]+)*$"
    )
    assert task_params["properties"]["scriptResource"]["pattern"] == (
        r"^/(?!-)(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
        r"[A-Za-z0-9_./:@%+=,\-]+\.py$"
    )
    assert task_params["properties"]["scriptArgs"]["default"] == []
    assert task_params["properties"]["scriptArgs"]["items"]["pattern"] == (
        r"^[A-Za-z0-9_./:@%+=,\-]+$"
    )


@pytest.mark.parametrize(
    "task_params",
    [
        {"scriptResource": "/ml/train.py"},
        {
            "pythonExecutable": "python3",
            "scriptResource": "/ml/train.py",
        },
        {
            "pythonExecutable": "/opt/python/../bin/python3",
            "scriptResource": "/ml/train.py",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3 -u",
            "scriptResource": "/ml/train.py",
        },
        {
            "pythonExecutable": "/opt/${python}/bin/python3",
            "scriptResource": "/ml/train.py",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "ml/train.py",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/ml/../train.py",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/-train.py",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/ml/train.py",
            "scriptArgs": "--epochs 10",
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/ml/train.py",
            "scriptArgs": ["two words"],
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/ml/train.py",
            "scriptArgs": ["$(id)"],
        },
        {
            "pythonExecutable": "/opt/python/bin/python3",
            "scriptResource": "/ml/train.py",
            "gitUrl": "https://example.invalid/model.git",
        },
    ],
)
def test_pytorch_model_rejects_unowned_or_shell_unsafe_input(
    task_params: YamlObject,
) -> None:
    with pytest.raises(ValueError):
        get_task_authoring_catalog("3.3.2").normalize_task_params(
            "PYTORCH",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_pytorch_minimal_template_validates_on_every_reviewed_profile(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result("PYTORCH", catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(result.data["yaml"])

    spec = validate_workflow_document(
        {
            "workflow": {"name": f"pytorch-template-{version}"},
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    assert spec.tasks[0].task_params == {
        "pythonExecutable": "/usr/bin/python3",
        "scriptResource": "/ml/train.py",
        "scriptArgs": ["--epochs", "10"],
    }


def test_pytorch_template_metadata_marks_only_the_script_as_a_ds_resource() -> None:
    metadata = task_template_metadata(catalog=get_task_authoring_catalog("3.3.2"))

    assert metadata["PYTORCH"]["resource_fields"] == ["task_params.scriptResource"]


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_pytorch_raw_create_edit_is_closed_but_preservation_is_available(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    native: YamlObject = {"futureField": {"preserve": True}}

    assert catalog.supports_typed_authoring("PYTORCH") is True
    assert catalog.supports_opaque_authoring("PYTORCH") is False
    for intent in (TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT):
        with pytest.raises(UnsupportedFeatureError):
            catalog.normalize_task_params("PYTORCH", native, intent=intent)
    assert (
        catalog.normalize_task_params(
            "PYTORCH",
            native,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        )
        == native
    )


@pytest.mark.parametrize(
    (
        "version",
        "wire_epoch",
        "encoding",
        "output_protocol",
        "stop_mode",
        "timeout_mode",
    ),
    [
        (
            "3.1.0",
            "positive-resource-id-python-home",
            "utf-8",
            "legacy-var-pool",
            "outer-process-best-effort",
            "process-tree-best-effort",
        ),
        (
            "3.1.9",
            "positive-resource-id-python-home",
            "platform-default",
            "legacy-var-pool",
            "outer-process-best-effort",
            "process-tree-best-effort",
        ),
        (
            "3.2.0",
            "resource-name-python-launcher",
            "platform-default",
            "legacy-var-pool",
            "outer-process-best-effort",
            "direct-process-best-effort",
        ),
        (
            "3.2.1",
            "resource-name-python-launcher",
            "platform-default",
            "task-output-unparsed",
            "outer-process-best-effort",
            "direct-process-best-effort",
        ),
        (
            "3.2.2",
            "resource-name-python-launcher",
            "platform-default",
            "task-output-unparsed",
            "outer-process-best-effort",
            "direct-process-best-effort",
        ),
        (
            "3.3.1",
            "resource-name-python-launcher",
            "platform-default",
            "task-output-unparsed",
            "plugin-cancel-noop",
            "output-future-before-timeout-check",
        ),
        (
            "3.3.2",
            "resource-name-python-launcher",
            "platform-default",
            "task-output-unparsed",
            "plugin-cancel-noop",
            "output-future-and-live-process-exit-value-hole",
        ),
    ],
)
def test_pytorch_catalog_and_surface_lock_the_exact_reviewed_epochs(
    version: str,
    wire_epoch: str,
    encoding: str,
    output_protocol: str,
    stop_mode: str,
    timeout_mode: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("PYTORCH")
    membership = catalog.require_facet(
        "PYTORCH",
        "PYTORCH/literal_resource_script",
    )
    surface = get_task_authoring_surface(version).pytorch

    assert profile.category == "MachineLearning"
    assert profile.default_facet == "PYTORCH/literal_resource_script"
    assert set(profile.facets) == {"PYTORCH/literal_resource_script"}
    assert membership.contract.family == "pytorch-literal-resource-script-v1"
    assert membership.contract.review == (
        "pytorch-literal-resource-script-exact-subset"
    )
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.script_encoding == encoding
    assert surface.parameter_substitution is True
    assert surface.resource_files_supported is True
    assert surface.git_projects_supported is True
    assert surface.environment_creation_supported is True
    assert surface.output_protocol == output_protocol
    assert surface.task_params_logged is True
    assert surface.command_logged is True
    assert surface.result_output_supported is False
    assert surface.cancel_supported is False
    assert surface.stop_mode == stop_mode
    assert surface.timeout_mode == timeout_mode
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_pytorch_is_upstream_absent_outside_the_seven_exact_profiles(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "PYTORCH" not in catalog.upstream_task_types
    assert get_task_authoring_surface(version).pytorch.available is False
    with pytest.raises(UnsupportedFeatureError, match="PYTORCH"):
        catalog.require_task_type("PYTORCH")


def test_pytorch_projection_absorbs_resource_and_launcher_wire_epochs() -> None:
    canonical: JsonObject = {
        "pythonExecutable": "/opt/python/bin/python3",
        "scriptResource": "/ml/train.py",
        "scriptArgs": ["--epochs", "10"],
    }

    legacy = encode_task_parameters(
        version="3.1.9",
        task_type="PYTORCH",
        task_params=canonical,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    modern = encode_task_parameters(
        version="3.2.0",
        task_type="PYTORCH",
        task_params=canonical,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    common = {
        "localParams": [],
        "isCreateEnvironment": False,
        "pythonPath": ".",
        "script": "ml/train.py",
        "scriptParams": "--epochs 10",
        "pythonEnvTool": "virtualenv",
        "requirements": "requirements.txt",
        "condaPythonVersion": "3.9",
    }
    assert legacy.task_params == {
        **common,
        "pythonCommand": "/opt/python/bin/python3",
        "resourceList": [{"id": 71}],
    }
    assert modern.task_params == {
        **common,
        "pythonLauncher": "/opt/python/bin/python3",
        "resourceList": [{"resourceName": "/tenant/resources/ml/train.py"}],
    }


def test_pytorch_31x_workflow_compilation_requires_and_binds_the_script_id() -> None:
    catalog = get_task_authoring_catalog("3.1.9")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "pytorch-training"},
            "tasks": [
                {
                    "name": "train-model",
                    "type": "PYTORCH",
                    "task_params": {
                        "pythonExecutable": "/opt/python/bin/python3",
                        "scriptResource": "/ml/train.py",
                        "scriptArgs": ["--epochs", "10"],
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

    assert compilation.required_resource_full_names == ("/ml/train.py",)
    with pytest.raises(UserInputError, match="resolved task resource identities"):
        compilation.materialize([52_001])
    payload = compilation.materialize(
        [52_001],
        resource_refs=_RESOURCE_REFS,
    )
    definition = json.loads(payload["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert native["resourceList"] == [{"id": 71}]
    assert native["script"] == "ml/train.py"


def test_pytorch_32x_workflow_compilation_verifies_resource_before_using_name() -> None:
    catalog = get_task_authoring_catalog("3.2.0")
    spec = validate_workflow_document(
        {
            "workflow": {"name": "pytorch-training-modern"},
            "tasks": [
                {
                    "name": "train-model",
                    "type": "PYTORCH",
                    "task_params": {
                        "pythonExecutable": "/opt/python/bin/python3",
                        "scriptResource": "/ml/train.py",
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

    assert compilation.required_resource_full_names == ("/ml/train.py",)
    with pytest.raises(UserInputError, match="resolved task resource identities"):
        compilation.materialize([52_002])
    payload = compilation.materialize(
        [52_002],
        resource_refs=_NAME_ONLY_RESOURCE_REFS,
    )
    definition = json.loads(payload["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert native["script"] == "ml/train.py"
    assert native["resourceList"] == [{"resourceName": "/tenant/resources/ml/train.py"}]


def test_pytorch_preview_refs_invent_ids_only_for_the_legacy_id_wire() -> None:
    modern = _preview_task_resource_refs(
        ("/ml/train.py",),
        id_required_full_names=(),
    )
    legacy = _preview_task_resource_refs(
        ("/ml/train.py",),
        id_required_full_names=("/ml/train.py",),
    )

    assert modern.verified_full_names == frozenset({"/ml/train.py"})
    assert dict(modern.id_by_full_name) == {}
    assert dict(modern.wire_full_name_by_full_name) == {
        "/ml/train.py": "/__dsctl_preview__/resources/ml/train.py"
    }
    assert legacy.verified_full_names == frozenset({"/ml/train.py"})
    assert set(legacy.id_by_full_name) == {"/ml/train.py"}


def test_pytorch_modern_projection_rejects_a_logical_name_as_the_wire_name() -> None:
    invalid_refs = TaskResourceRefIndex.from_resolved_files(
        ["/ml/train.py"],
        id_by_full_name={},
        wire_full_name_by_full_name={"/ml/train.py": "/ml/train.py"},
    )

    with pytest.raises(
        TaskParameterProjectionError,
        match="storage path whose FILE base ends in /resources",
    ):
        encode_task_parameters(
            version="3.2.0",
            task_type="PYTORCH",
            task_params=_canonical(),
            refs=_REFS,
            resource_refs=invalid_refs,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2"])
def test_pytorch_33x_typed_compile_disables_the_broken_timeout_path(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"pytorch-timeout-{version}"},
            "tasks": [
                {
                    "name": "train-model",
                    "type": "PYTORCH",
                    "task_params": {
                        "pythonExecutable": "/opt/python/bin/python3",
                        "scriptResource": "/ml/train.py",
                    },
                    "timeout": 60,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match="timeout must be 0"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_pytorch_exact_wire_round_trips_with_typed_provenance(version: str) -> None:
    canonical: JsonObject = {
        "pythonExecutable": "/opt/python/bin/python3",
        "scriptResource": "/ml/train.py",
        "scriptArgs": ["--epochs", "10"],
    }
    native = encode_task_parameters(
        version=version,
        task_type="PYTORCH",
        task_params=canonical,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="PYTORCH",
        task_params=native,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == canonical


@pytest.mark.parametrize(
    "resource_refs",
    [None, TaskResourceRefIndex.from_id_by_full_name({})],
)
def test_pytorch_modern_wire_without_verified_resource_refs_stays_opaque(
    resource_refs: TaskResourceRefIndex | None,
) -> None:
    native = _native("3.3.2")

    decoded = decode_task_parameters_with_provenance(
        version="3.3.2",
        task_type="PYTORCH",
        task_params=native,
        refs=_REFS,
        resource_refs=resource_refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_pytorch_richer_native_wire_stays_lossless_and_opaque(version: str) -> None:
    canonical: JsonObject = {
        "pythonExecutable": "/opt/python/bin/python3",
        "scriptResource": "/ml/train.py",
        "scriptArgs": [],
    }
    native = encode_task_parameters(
        version=version,
        task_type="PYTORCH",
        task_params=canonical,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params
    richer = deepcopy(native)
    richer["futureField"] = {"preserve": [True, {"epoch": "future"}]}

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="PYTORCH",
        task_params=richer,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == richer


@pytest.mark.parametrize(
    "case",
    [
        "stale-ui-python-command",
        "nonempty-local-params",
        "extra-resource",
        "environment-creation",
        "ambiguous-argument-spacing",
        "launcher-shell-fragment",
        "logical-resource-name",
    ],
)
def test_pytorch_adjacent_native_shapes_remain_opaque_without_normalization(
    case: str,
) -> None:
    version = "3.2.2"
    richer = deepcopy(_native(version))
    if case == "stale-ui-python-command":
        richer["pythonCommand"] = "/usr/bin/python3"
    elif case == "nonempty-local-params":
        richer["localParams"] = [
            {
                "prop": "epochs",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "10",
            }
        ]
    elif case == "extra-resource":
        richer["resourceList"] = [
            {"resourceName": "/tenant/resources/ml/train.py"},
            {"resourceName": "/tenant/resources/ml/config.json"},
        ]
    elif case == "environment-creation":
        richer["isCreateEnvironment"] = True
    elif case == "ambiguous-argument-spacing":
        richer["scriptParams"] = "--epochs  10"
    elif case == "launcher-shell-fragment":
        richer["pythonLauncher"] = "/usr/bin/python3 -u"
    elif case == "logical-resource-name":
        richer["resourceList"] = [{"resourceName": "/ml/train.py"}]
    else:
        raise AssertionError(case)

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="PYTORCH",
        task_params=richer,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == richer
    assert decoded.task.task_params is not richer
    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters_with_provenance(
            version=version,
            task_type="PYTORCH",
            task_params=richer,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("version", ["3.4.0", "3.4.1", "3.4.2"])
def test_pytorch_projection_cannot_cross_the_upstream_removal_boundary(
    version: str,
) -> None:
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        encode_task_parameters(
            version=version,
            task_type="PYTORCH",
            task_params=_canonical(),
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        decode_task_parameters_with_provenance(
            version=version,
            task_type="PYTORCH",
            task_params=_native("3.3.2"),
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.OPAQUE_PRESERVE,
        )


@pytest.mark.parametrize("version", ["3.1.9", "3.3.2"])
def test_pytorch_richer_server_baseline_exports_with_opaque_provenance(
    version: str,
) -> None:
    native = _native(version, richer=True)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(
        version,
        native,
        workflow_name=f"pytorch-export-{version}",
    )

    baseline = workflow_live_baseline(
        dag,
        project=_PROJECT,
        catalog=catalog,
        resource_refs=_RESOURCE_REFS,
    )
    exported = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_PROJECT,
            attached_schedule=None,
            catalog=catalog,
            resource_refs=_RESOURCE_REFS,
        )
    )

    assert baseline.projection_sources["train-model"] is (
        ProjectionSource.OPAQUE_PRESERVE
    )
    assert baseline.spec.tasks[0].task_params == native
    assert exported["tasks"][0]["task_params"] == native


@pytest.mark.parametrize("version", ["3.1.9", "3.3.2"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_pytorch_metadata_edits_preserve_richer_native_wire(
    version: str,
    input_mode: str,
) -> None:
    native = _native(version, richer=True)

    assert (
        _compiled_task_params(_metadata_plan(version, native, input_mode=input_mode))
        == native
    )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_pytorch_metadata_edits_preserve_canonical_33x_nonzero_timeout(
    input_mode: str,
) -> None:
    native = _native("3.3.2")

    assert (
        _compiled_task_params(_metadata_plan("3.3.2", native, input_mode=input_mode))
        == native
    )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_pytorch_metadata_edits_do_not_rebind_unverified_modern_resources(
    input_mode: str,
) -> None:
    native = _native("3.3.2")
    native["resourceList"] = [{"resourceName": "/foreign/resources/ml/train.py"}]
    empty_refs = TaskResourceRefIndex.from_id_by_full_name({})

    plan = _metadata_plan(
        "3.3.2",
        native,
        input_mode=input_mode,
        resource_refs=empty_refs,
    )

    assert _compiled_task_params(plan, resource_refs=empty_refs) == native


@pytest.mark.parametrize("version", ["3.1.9", "3.3.2"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_pytorch_richer_task_param_edits_fail_closed(
    version: str,
    input_mode: str,
) -> None:
    with pytest.raises((UnsupportedFeatureError, UserInputError)):
        _edit_richer_task_params(version, input_mode=input_mode)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_pytorch_richer_opaque_runtime_edits_fail_closed(input_mode: str) -> None:
    with pytest.raises(UserInputError, match="richer opaque PYTORCH"):
        _edit_richer_runtime("3.3.2", input_mode=input_mode)


def test_pytorch_standalone_opaque_export_cannot_be_reapplied_as_a_create() -> None:
    version = "3.3.2"
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(
        version,
        _native(version, richer=True),
        workflow_name="pytorch-standalone-export",
    )
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=_PROJECT,
            attached_schedule=None,
            catalog=catalog,
            resource_refs=_RESOURCE_REFS,
        )
    )
    standalone = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )

    with pytest.raises((UnsupportedFeatureError, UserInputError)):
        prepare_workflow_create_compilation(standalone, catalog=catalog)
