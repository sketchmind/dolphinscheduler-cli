from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from pydantic import ValidationError
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models import ReleaseState
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
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    DecodedTaskParameters,
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


_TASK_TYPE = "KUBEFLOW"
_FACET = "KUBEFLOW/tfjob_manifest"
_TYPED_VERSIONS = (
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
)
_WORKFLOW_INSTANCE_PLACEHOLDER = "${system.workflow.instance.id}"
_REFS = TaskRefIndex.from_code_by_name({})


def _yaml_content(*, namespace: str = "kubeflow-team") -> str:
    return (
        'apiVersion: "kubeflow.org/v1"\n'
        "kind: TFJob\n"
        "metadata:\n"
        f"  name: train-mnist-{_WORKFLOW_INSTANCE_PLACEHOLDER}\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        "  tfReplicaSpecs:\n"
        "    Worker:\n"
        "      replicas: 1\n"
        "      restartPolicy: OnFailure\n"
        "      template:\n"
        "        spec:\n"
        "          containers:\n"
        "            - name: tensorflow\n"
        "              image: registry.example/tf-mnist:1.0\n"
    )


def _canonical() -> YamlObject:
    return {
        "namespace": "kubeflow-team",
        "cluster": "production",
        "yamlContent": _yaml_content(),
    }


def _native(*, ui_residue: bool = False) -> YamlObject:
    native: YamlObject = {
        "yamlContent": _yaml_content(),
        "namespace": '{"name":"kubeflow-team","cluster":"production"}',
    }
    if ui_residue:
        native.update({"localParams": [], "resourceList": []})
    return native


def _richer_native() -> YamlObject:
    return {
        **_native(),
        "localParams": [
            {
                "prop": "unsafe",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "native",
            }
        ],
        "futureField": {"preserve": [True, {"epoch": "future"}]},
    }


def _spec(
    version: str,
    params: YamlObject,
    *,
    retry_times: int = 0,
    timeout: int = 60,
    timeout_notify_strategy: str | None = "FAILED",
    global_params: YamlValue | None = None,
) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    workflow: YamlObject = {
        "name": f"kubeflow-{version}",
        "project": "analytics",
    }
    if global_params is not None:
        workflow["global_params"] = global_params
    task: YamlObject = {
        "name": "train-kubeflow-model",
        "type": _TASK_TYPE,
        "task_params": params,
        "retry": {"times": retry_times, "interval": 1},
        "timeout": timeout,
    }
    if timeout_notify_strategy is not None:
        task["timeout_notify_strategy"] = timeout_notify_strategy
    return validate_workflow_document(
        {
            "workflow": workflow,
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([42_000])["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert definition["taskType"] == _TASK_TYPE
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decode(version: str, params: YamlObject) -> DecodedTaskParameters:
    return decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )


def _fake_dag(
    version: str,
    params: YamlObject,
    *,
    workflow_name: str,
    flag: str = "YES",
    retry_times: int = 0,
    timeout: int = 60,
    timeout_notify_strategy: str | None = "FAILED",
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name="train-kubeflow-model",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value=_TASK_TYPE,
        task_params_value=json.dumps(params),
        worker_group_value="default",
        flag_value=FakeEnumValue(flag),
        fail_retry_times_value=retry_times,
        fail_retry_interval_value=0,
        timeout=timeout,
        timeout_notify_strategy_value=(
            None
            if timeout_notify_strategy is None
            else FakeEnumValue(timeout_notify_strategy)
        ),
    )
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
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


def _metadata_plan(
    version: str,
    native: YamlObject,
    *,
    input_mode: str,
    retry_times: int = 0,
    timeout: int = 60,
    timeout_notify_strategy: str | None = "FAILED",
) -> WorkflowMutationPlan:
    dag = _fake_dag(
        version,
        native,
        workflow_name="kubeflow-metadata-edit",
        retry_times=retry_times,
        timeout=timeout,
        timeout_notify_strategy=timeout_notify_strategy,
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="train-kubeflow-model",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = _definition_from_plan(plan)
    params = json.loads(cast("str", definition["taskParams"]))
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _definition_from_plan(plan: WorkflowMutationPlan) -> dict[str, object]:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    assert isinstance(definition, dict)
    return cast("dict[str, object]", definition)


def _template_yaml(version: str) -> str:
    return authoring_prep.template_yaml(_TASK_TYPE, version, variant="minimal")


def _invalid_yaml(case: str) -> str:
    base = _yaml_content()
    root_prefix = base.split("spec:\n", maxsplit=1)[0]
    cases = {
        "wrong-api-version": base.replace("kubeflow.org/v1", "kubeflow.org/v1beta1"),
        "wrong-kind": base.replace("kind: TFJob", "kind: PyTorchJob"),
        "missing-metadata": base.replace("metadata:\n", "identity:\n", 1),
        "missing-spec": root_prefix,
        "missing-replica-specs": f"{root_prefix}spec: {{}}\n",
        "empty-replica-specs": f"{root_prefix}spec:\n  tfReplicaSpecs: {{}}\n",
        "root-status": f"{base}status:\n  conditions: []\n",
        "alias": (
            base.replace("metadata:\n", "metadata: &identity\n", 1)
            + "identityCopy: *identity\n"
        ),
        "anchor": base.replace("metadata:\n", "metadata: &identity\n", 1),
        "duplicate-key": base.replace(
            "kind: TFJob\n",
            "kind: TFJob\nkind: TFJob\n",
            1,
        ),
        "custom-tag": base.replace(
            "kind: TFJob\n",
            "kind: !reviewed TFJob\n",
            1,
        ),
        "timestamp-tag": f"{base}createdAt: 2026-08-30\n",
        "merge-key": (
            base.replace("metadata:\n", "metadata: &identity\n", 1)
            + "identityCopy:\n  <<: *identity\n"
        ),
        "multi-document": f"{base}---\nextra: document\n",
        "namespace-mismatch": _yaml_content(namespace="other-team"),
        "static-name": base.replace(_WORKFLOW_INSTANCE_PLACEHOLDER, "static"),
        "long-name-prefix": base.replace("train-mnist", "a" * 53, 1),
        "other-placeholder": f"{base}extra: ${{tenant}}\n",
        "bracket-placeholder": f"{base}extra: $[tenant]\n",
        "task-instance-placeholder": base.replace(
            _WORKFLOW_INSTANCE_PLACEHOLDER,
            "${system.task.instance.id}",
        ),
        "generate-name": base.replace(
            "  name: train-mnist-",
            "  generateName: train-mnist-\n  name: train-mnist-",
            1,
        ),
        "non-ascii": f"{base}description: \u8bad\u7ec3\n",
        "carriage-return": base.replace("kind: TFJob\n", "kind: TFJob\r\n", 1),
        "c0-control": f'{base}description: "unsafe\x1f"\n',
        "del-control": f'{base}description: "unsafe\x7f"\n',
        "excessive-depth": f"{base}deep: {'[' * 2500}0{']' * 2500}\n",
    }
    try:
        return cases[case]
    except KeyError as exc:
        message = f"Unknown invalid KUBEFLOW YAML case: {case}"
        raise AssertionError(message) from exc


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_catalog_and_surface_expose_one_guarded_tfjob_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)
    surface = get_task_authoring_surface(version).kubeflow

    assert profile.category == "MachineLearning"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True
    assert surface.available is True
    assert surface.wire_epoch == "legacy-namespace-tfjob-yaml"
    assert surface.script_encoding == "platform-default"
    assert surface.parameter_substitution is True
    assert surface.durable_application_id is False
    assert surface.durable_submission_marker is True
    assert surface.failover_tracking_supported is True
    assert surface.callback_persistence_gap is True
    assert surface.namespace_context_enforced is False
    assert surface.task_params_logged is True
    assert surface.manifest_logged is True
    assert surface.terminal_resource_logged is True
    assert surface.kubeconfig_logged is True
    assert surface.poll_interval_seconds == 3
    assert surface.success_statuses == ("Succeeded", "Available", "Bound")
    assert surface.failure_statuses == ("Failed",)
    assert surface.empty_conditions_runtime_failure is True
    assert surface.result_output_supported is False
    assert surface.cancel_supported is True
    assert surface.retry_supported is False
    assert surface.native_retry_behavior == (
        "reapply-same-manifest"
        if version in {"3.2.0", "3.2.1", "3.2.2"}
        else "reobserve-same-manifest"
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_projector_emits_the_exact_legacy_namespace_wire(
    version: str,
) -> None:
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", _canonical()),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )

    assert projected.task_type == _TASK_TYPE
    assert projected.task_params == {
        "yamlContent": _yaml_content(),
        "namespace": json.dumps(
            {"name": "kubeflow-team", "cluster": "production"},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_kubeflow_is_explicitly_absent_before_3_2(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    surface = get_task_authoring_surface(version).kubeflow

    assert _TASK_TYPE not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is False
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert _TASK_TYPE not in catalog.authoring_task_types
    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.script_encoding is None
    assert surface.parameter_substitution is False
    assert surface.namespace_context_enforced is False
    assert surface.task_params_logged is False
    assert surface.manifest_logged is False
    assert surface.terminal_resource_logged is False
    assert surface.kubeconfig_logged is False
    assert surface.poll_interval_seconds is None
    assert surface.success_statuses == ()
    assert surface.failure_statuses == ()
    assert surface.empty_conditions_runtime_failure is False
    assert surface.result_output_supported is False
    assert surface.cancel_supported is False
    assert surface.durable_application_id is False
    assert surface.durable_submission_marker is False
    assert surface.failover_tracking_supported is False
    assert surface.callback_persistence_gap is False
    assert surface.retry_supported is False
    assert surface.native_retry_behavior is None


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_schema_exposes_only_the_closed_manifest_interface(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_type_schema_result(_TASK_TYPE, catalog=catalog)
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

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "MachineLearning"
    assert result.data["kind"] == "typed"
    assert set(task_fields) == {"namespace", "cluster", "yamlContent"}
    assert all(field["required"] is True for field in task_fields.values())
    assert task_fields["namespace"]["choice_source"] == "dsctl namespace available"
    assert task_fields["cluster"]["choice_source"] == "dsctl cluster list"
    assert task_fields["namespace"]["compile_path"].endswith("taskParams.namespace")
    assert task_fields["cluster"]["compile_path"].endswith("taskParams.namespace")
    assert task_fields["yamlContent"]["compile_path"].endswith("taskParams.yamlContent")
    assert any(
        "retry.times" in json.dumps(rule, ensure_ascii=False)
        for rule in result.data["state_rules"]
    )
    guidance = json.dumps(result.data, ensure_ascii=False).lower().replace("_", " ")
    for term in (
        "workflow.instance.id",
        "metadata.namespace",
        "kubeconfig",
        "platform-default",
        "info",
        "not secret storage",
        "retry",
        "repeat running",
        "recover-failed",
        "timeout",
        "failover",
        "kubectl",
        "tfjob",
        "empty status.conditions",
        "runtime failure",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", ["3.2.0", "3.2.2", "3.4.2"])
def test_kubeflow_json_schema_is_closed_and_requires_all_fields(version: str) -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    task_params = cast(
        "JsonObject",
        result.data["schema"]["$defs"]["task_params"],
    )

    assert task_params["additionalProperties"] is False
    assert set(cast("dict[str, object]", task_params["properties"])) == {
        "namespace",
        "cluster",
        "yamlContent",
    }
    assert set(cast("list[str]", task_params["required"])) == {
        "namespace",
        "cluster",
        "yamlContent",
    }
    runtime_validations = cast(
        "list[str]",
        task_params["x-dsctl-runtime-validations"],
    )
    rendered = " ".join(runtime_validations)
    assert "tfReplicaSpecs" in rendered
    assert "root status" in rendered
    assert "server-owned metadata" in rendered
    assert "JSON-core string/null/bool/int/float" in rendered


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_minimal_template_uses_guarded_identity_and_compiles(
    version: str,
) -> None:
    yaml_text = _template_yaml(version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == _TASK_TYPE
    assert task["retry"] == {"times": 0, "interval": 0}
    assert task["timeout_notify_strategy"] == "FAILED"
    assert task["timeout"] == 60
    assert set(params) == {"namespace", "cluster", "yamlContent"}
    assert str(params["yamlContent"]).count(_WORKFLOW_INSTANCE_PLACEHOLDER) == 1
    assert "${system.task.instance.id}" not in str(params["yamlContent"])
    assert _compiled(version, cast("YamlObject", params)) == _native()


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_public_workflow_compile_emits_exact_native_wire(
    version: str,
) -> None:
    assert _compiled(version, _canonical()) == _native()


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("wrong-api-version", "requires apiVersion kubeflow.org/v1"),
        ("wrong-kind", "requires kind TFJob"),
        ("missing-metadata", "requires metadata"),
        ("missing-spec", "requires a spec mapping"),
        ("missing-replica-specs", "nonempty tfReplicaSpecs"),
        ("empty-replica-specs", "must not be empty"),
        ("root-status", "server-owned root status"),
        ("alias", "aliases"),
        ("anchor", "anchors"),
        ("duplicate-key", "duplicate key"),
        ("custom-tag", "JSON-core scalar tags"),
        ("timestamp-tag", "JSON-core scalar tags"),
        ("merge-key", "literal strings|aliases"),
        ("multi-document", "exactly one nonempty YAML document"),
        ("namespace-mismatch", "must exactly match"),
        ("static-name", "exactly one.*workflow.instance.id"),
        ("long-name-prefix", "DNS-safe prefix"),
        ("other-placeholder", "unsupported DolphinScheduler placeholder"),
        ("bracket-placeholder", "unsupported DolphinScheduler placeholder"),
        ("task-instance-placeholder", "exactly one.*workflow.instance.id"),
        ("generate-name", "generateName"),
        ("non-ascii", "ASCII LF text"),
        ("carriage-return", "ASCII LF text"),
        ("c0-control", "ASCII LF text"),
        ("del-control", "ASCII LF text"),
        ("excessive-depth", "valid, safely bounded YAML"),
    ],
)
def test_kubeflow_compile_rejects_unreviewed_yaml(
    case: str,
    message: str,
) -> None:
    params = _canonical()
    params["yamlContent"] = _invalid_yaml(case)

    with pytest.raises(UserInputError, match=message):
        _compiled("3.4.2", params)


@pytest.mark.parametrize(
    "field_name",
    [
        "uid",
        "resourceVersion",
        "generation",
        "creationTimestamp",
        "managedFields",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "selfLink",
    ],
)
def test_kubeflow_compile_rejects_server_owned_metadata(field_name: str) -> None:
    params = _canonical()
    params["yamlContent"] = _yaml_content().replace(
        "  namespace: kubeflow-team\n",
        f"  namespace: kubeflow-team\n  {field_name}: server-owned\n",
        1,
    )

    with pytest.raises(UserInputError, match="server-owned metadata"):
        _compiled("3.4.2", params)


@pytest.mark.parametrize("case", ["custom-tag", "timestamp-tag"])
def test_kubeflow_projection_seam_rejects_non_json_yaml_tags(case: str) -> None:
    params = _canonical()
    params["yamlContent"] = _invalid_yaml(case)

    with pytest.raises(TaskParameterProjectionError, match="JSON-core scalar tags"):
        _encode("3.4.2", params)


def test_kubeflow_compiler_requires_retry_times_zero() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    spec = _spec("3.4.2", _canonical(), retry_times=1)

    with pytest.raises(UserInputError, match=r"retry\.times must be 0") as exc_info:
        prepare_workflow_create_compilation(spec, catalog=catalog)
    assert exc_info.value.suggestion is not None
    assert "`dsctl workflow run`" in exc_info.value.suggestion
    assert "workflow-instance rerun" in exc_info.value.suggestion


def test_kubeflow_compiler_requires_a_positive_task_timeout() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    spec = _spec(
        "3.4.2",
        _canonical(),
        timeout=0,
        timeout_notify_strategy=None,
    )

    with pytest.raises(UserInputError, match="timeout must be a positive"):
        prepare_workflow_create_compilation(spec, catalog=catalog)

    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        _spec(
            "3.4.2",
            _canonical(),
            timeout=-1,
            timeout_notify_strategy=None,
        )


@pytest.mark.parametrize("strategy", [None, "WARN"])
def test_kubeflow_compiler_requires_a_terminating_timeout_strategy(
    strategy: str | None,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    spec = _spec(
        "3.4.2",
        _canonical(),
        timeout_notify_strategy=strategy,
    )

    with pytest.raises(UserInputError, match="FAILED or WARNFAILED"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


@pytest.mark.parametrize("strategy", ["FAILED", "WARNFAILED"])
def test_kubeflow_compiler_emits_a_terminating_minute_timeout(
    strategy: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    prepared = prepare_workflow_create_compilation(
        _spec(
            "3.4.2",
            _canonical(),
            timeout=60,
            timeout_notify_strategy=strategy,
        ),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([42_000])["taskDefinitionJson"])[0]

    assert definition["timeoutFlag"] == "OPEN"
    assert definition["timeoutNotifyStrategy"] == strategy
    assert definition["timeout"] == 60


def test_kubeflow_compiler_rejects_duplicate_workflow_resource_identity() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    task: YamlObject = {
        "type": _TASK_TYPE,
        "task_params": _canonical(),
        "retry": {"times": 0, "interval": 0},
        "timeout_notify_strategy": "FAILED",
        "timeout": 60,
    }
    spec = validate_workflow_document(
        {
            "workflow": {"name": "duplicate-kubeflow", "project": "analytics"},
            "tasks": [
                {"name": "train-first", **task},
                {"name": "train-second", **task},
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    with pytest.raises(UserInputError, match="same TFJob identity"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


@pytest.mark.parametrize(
    "global_params",
    [
        {"system.workflow.instance.id": "shadowed"},
        [
            {
                "prop": "system.workflow.instance.id",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "shadowed",
            }
        ],
    ],
)
def test_kubeflow_compiler_rejects_workflow_instance_id_shadowing(
    global_params: YamlValue,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    spec = _spec("3.4.2", _canonical(), global_params=global_params)

    with pytest.raises(UserInputError, match="shadows the built-in"):
        prepare_workflow_create_compilation(spec, catalog=catalog)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("residue_mode", ["exact", "ui-empty"])
def test_kubeflow_exact_native_wire_decodes_to_a_typed_fixed_point(
    version: str,
    residue_mode: str,
) -> None:
    decoded = _decode(version, _native(ui_residue=residue_mode == "ui-empty"))

    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert decoded.task.task_params == _canonical()
    reencoded = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=decoded.task.task_params,
        refs=_REFS,
        source=decoded.reencode_source,
    )
    assert reencoded.task_params == _native()


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_richer_native_wire_remains_opaque(version: str) -> None:
    native = _richer_native()
    decoded = _decode(version, native)

    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded.task.task_params == native
    preserved = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=decoded.task.task_params,
        refs=_REFS,
        source=decoded.reencode_source,
    )
    assert preserved.task_params == native
    assert preserved.task_params is not native

    with pytest.raises(TaskParameterProjectionError):
        decode_task_parameters_with_provenance(
            version=version,
            task_type=_TASK_TYPE,
            task_params=cast("JsonObject", native),
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_kubeflow_safe_and_richer_server_baselines_have_exact_provenance(
    version: str,
) -> None:
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    safe_dag = _fake_dag(version, _native(), workflow_name="kubeflow-safe")
    opaque_dag = _fake_dag(
        version,
        _richer_native(),
        workflow_name="kubeflow-opaque",
    )

    safe = workflow_live_baseline(safe_dag, project=project, catalog=catalog)
    opaque = workflow_live_baseline(opaque_dag, project=project, catalog=catalog)
    safe_export = yaml.safe_load(
        workflow_yaml_document(
            safe_dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    opaque_export = yaml.safe_load(
        workflow_yaml_document(
            opaque_dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert safe.projection_sources["train-kubeflow-model"] is (
        ProjectionSource.TYPED_AUTHORING
    )
    assert safe.spec.tasks[0].task_params == _canonical()
    assert safe_export["tasks"][0]["task_params"] == _canonical()
    assert opaque.projection_sources["train-kubeflow-model"] is (
        ProjectionSource.OPAQUE_PRESERVE
    )
    assert opaque.spec.tasks[0].task_params == _richer_native()
    assert opaque_export["tasks"][0]["task_params"] == _richer_native()


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_kubeflow_metadata_edits_preserve_richer_native_wire(
    version: str,
    input_mode: str,
) -> None:
    native = _richer_native()

    assert (
        _compiled_from_plan(_metadata_plan(version, native, input_mode=input_mode))
        == native
    )


def test_kubeflow_opaque_runtime_patch_fails_closed() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _richer_native(),
        workflow_name="kubeflow-opaque-runtime-edit",
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "train-kubeflow-model"},
                            "set": {"retry": {"times": 1, "interval": 0}},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError) as captured:
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )

    assert captured.value.details["reason"] == ("kubeflow-opaque-runtime-edit-closed")


def test_kubeflow_opaque_task_rename_preserves_exact_native_wire() -> None:
    version = "3.4.2"
    native = _richer_native()
    dag = _fake_dag(
        version,
        native,
        workflow_name="kubeflow-opaque-rename",
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "rename": [
                        {
                            "from": "train-kubeflow-model",
                            "to": "renamed-kubeflow-model",
                        }
                    ]
                }
            }
        }
    ).patch
    plan = prepare_workflow_mutation_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog(version),
    )
    definition = _definition_from_plan(plan)

    assert definition["name"] == "renamed-kubeflow-model"
    assert json.loads(cast("str", definition["taskParams"])) == native


def test_unrelated_shell_runtime_edit_rechecks_unsafe_kubeflow_retry() -> None:
    version = "3.4.2"
    base = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-unrelated-runtime-edit",
        retry_times=1,
    )
    assert base.task_definition_list_value is not None
    shell = FakeTaskDefinition(
        code=102,
        name="sidecar-shell",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="SHELL",
        task_params_value=json.dumps(
            {
                "rawScript": "echo sidecar",
                "localParams": [],
                "resourceList": [],
            }
        ),
        worker_group_value="default",
        flag_value=FakeEnumValue("YES"),
    )
    dag = replace(
        base,
        task_definition_list_value=[*base.task_definition_list_value, shell],
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "sidecar-shell"},
                            "set": {"flag": "NO"},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )


def test_kubeflow_opaque_full_file_runtime_edit_fails_closed() -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _fake_dag(
        version,
        _richer_native(),
        workflow_name="kubeflow-opaque-file-runtime-edit",
    )
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    current = baseline.spec.tasks[0]
    desired = baseline.spec.model_copy(
        update={
            "tasks": [
                current.model_copy(
                    update={"retry": current.retry.model_copy(update={"times": 1})},
                    deep=True,
                )
            ]
        },
        deep=True,
    )

    with pytest.raises(UserInputError) as captured:
        prepare_workflow_file_mutation_plan(
            dag,
            project=project,
            desired=desired,
            release_state="OFFLINE",
            catalog=catalog,
        )

    assert captured.value.details["reason"] == ("kubeflow-opaque-runtime-edit-closed")


@pytest.mark.parametrize("version", ["3.2.0", "3.4.2"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("retry_times", "timeout", "strategy", "expected_strategy"),
    [
        (1, 60, "FAILED", "FAILED"),
        (0, 0, None, None),
        (0, 60, "WARN", "WARN"),
    ],
)
def test_kubeflow_metadata_edits_preserve_unsafe_outer_runtime_state(
    version: str,
    input_mode: str,
    retry_times: int,
    timeout: int,
    strategy: str | None,
    expected_strategy: str | None,
) -> None:
    plan = _metadata_plan(
        version,
        _native(),
        input_mode=input_mode,
        retry_times=retry_times,
        timeout=timeout,
        timeout_notify_strategy=strategy,
    )
    definition = _definition_from_plan(plan)

    assert definition["failRetryTimes"] == retry_times
    assert definition["timeout"] == timeout
    assert definition["timeoutNotifyStrategy"] == expected_strategy
    assert json.loads(cast("str", definition["taskParams"])) == _native()


def test_kubeflow_authored_runtime_edit_rechecks_preserved_typed_task() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-runtime-edit",
        retry_times=1,
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "train-kubeflow-model"},
                            "set": {"retry": {"times": 1, "interval": 0}},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )


def test_kubeflow_authored_activation_rechecks_preserved_typed_task() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-activation-edit",
        flag="NO",
        retry_times=1,
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "train-kubeflow-model"},
                            "set": {"flag": "YES"},
                        }
                    ]
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )


def test_kubeflow_workflow_online_rechecks_preserved_typed_task() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-workflow-activation",
        retry_times=1,
        timeout=0,
        timeout_notify_strategy=None,
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "workflow": {
                    "set": {"release_state": "ONLINE"},
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="ONLINE",
            catalog=get_task_authoring_catalog(version),
        )


def test_kubeflow_full_file_workflow_online_rechecks_preserved_task() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-file-activation",
        retry_times=1,
        timeout=0,
        timeout_notify_strategy=None,
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    desired = baseline.spec.model_copy(
        update={
            "workflow": baseline.spec.workflow.model_copy(
                update={"release_state": ReleaseState.ONLINE}
            )
        },
        deep=True,
    )

    with pytest.raises(UserInputError, match=r"retry\.times must be 0"):
        prepare_workflow_file_mutation_plan(
            dag,
            project=project,
            desired=desired,
            release_state="ONLINE",
            catalog=catalog,
        )


def test_kubeflow_authored_global_shadow_rechecks_preserved_typed_task() -> None:
    version = "3.4.2"
    dag = _fake_dag(
        version,
        _native(),
        workflow_name="kubeflow-global-edit",
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "workflow": {
                    "set": {
                        "global_params": {"system.workflow.instance.id": "shadowed"}
                    }
                }
            }
        }
    ).patch

    with pytest.raises(UserInputError, match="shadows the built-in"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            patch=patch,
            release_state="OFFLINE",
            catalog=get_task_authoring_catalog(version),
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_kubeflow_raw_opaque_create_and_edit_are_closed(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError, match="opaque authoring"):
        get_task_authoring_catalog(version).normalize_task_params(
            _TASK_TYPE,
            _richer_native(),
            intent=intent,
        )


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_kubeflow_absent_versions_reject_projection_in_both_directions(
    version: str,
) -> None:
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        _encode(version, _canonical())
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        _decode(version, _native())


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_kubeflow_absent_versions_reject_public_workflow_authoring(
    version: str,
) -> None:
    with pytest.raises(UnsupportedFeatureError, match=_TASK_TYPE):
        _spec(version, _canonical())
