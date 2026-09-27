from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        KubeflowAuthoringSurface,
    )

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringStateRule,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    _family_model,
    model_field,
)

KUBEFLOW_TFJOB_MANIFEST_FACET = "KUBEFLOW/tfjob_manifest"


def _kubeflow_runtime_guidance(surface: KubeflowAuthoringSurface) -> str:
    """Describe the exact manifest, watcher, disclosure, and recovery limits."""
    if not surface.available:
        return "KUBEFLOW is absent from this exact DolphinScheduler profile."
    if (
        surface.wire_epoch != "legacy-namespace-tfjob-yaml"
        or surface.script_encoding != "platform-default"
        or surface.poll_interval_seconds != 3
        or not surface.kubeconfig_logged
        or not surface.empty_conditions_runtime_failure
        or surface.retry_supported
        or surface.native_retry_behavior
        not in {"reapply-same-manifest", "reobserve-same-manifest"}
    ):
        message = "Available KUBEFLOW surface lacks the reviewed TFJob contract"
        raise ValueError(message)
    native_retry_behavior = (
        "Exact 3.2.x native retry clears the submission sentinel and re-applies "
        "the same workflow-instance-scoped manifest."
        if surface.native_retry_behavior == "reapply-same-manifest"
        else (
            "Exact 3.3.1-and-newer native retry copies the submission sentinel and "
            "only re-observes the same workflow-instance-scoped manifest."
        )
    )
    return (
        "Cluster selects kubeconfig; kubectl omits -n, so metadata.namespace must "
        "match namespace. The worker uses platform-default charset and raw parameter "
        "replacement, then INFO-logs params, YAML, status, terminal JSON, and "
        "kubeconfig; not secret storage. Accept one ASCII kubeflow.org/v1 TFJob with "
        "nonempty "
        "spec.tfReplicaSpecs, no status/server metadata, and metadata.name ending in "
        "the sole ${system.workflow.instance.id} placeholder. Requires kubectl, "
        "kubeconfig, TFJob CRD/controller, network, and RBAC. Succeeded, Available, "
        "or Bound succeed; Failed fails. Missing status/conditions or nonterminal "
        "state may poll to timeout; empty status.conditions causes upstream runtime "
        "failure. No output or durable Kubernetes id. appIds is a submission "
        "sentinel: after callback, failover re-observes; a callback gap can re-apply. "
        f"{native_retry_behavior} Controller/pod restart policy may repeat work. "
        "REPEAT_RUNNING reuses workflowInstanceId and may re-observe a terminal "
        "TFJob; fresh training needs a new workflow instance. With KUBEFLOW in "
        "dagData, dsctl blocks rerun, recover-failed, and execute-task; UI/direct REST "
        "are outside that guard. Typed: retry.times=0; positive minute timeout with "
        "FAILED|WARNFAILED; unique cluster/namespace/name; no authored global "
        "system.workflow.instance.id shadow. External shadows are prerequisites."
    )


def _kubeflow_fields(
    surface: KubeflowAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    """Return the three canonical fields owned by the guarded TFJob facet."""
    if not surface.available:
        return ()
    runtime = _kubeflow_runtime_guidance(surface)
    return (
        model_field(
            "task_params.namespace",
            choice_source="dsctl namespace available",
            related_commands=("dsctl namespace available", "dsctl namespace list"),
            compile_path="taskDefinitionJson[].taskParams.namespace",
            description=(
                "Kubernetes DNS-1123 namespace encoded into the legacy native "
                "selector and required verbatim in manifest metadata.namespace."
            ),
        ),
        model_field(
            "task_params.cluster",
            choice_source="dsctl cluster list",
            related_commands=("dsctl cluster list", "dsctl cluster get CLUSTER"),
            compile_path="taskDefinitionJson[].taskParams.namespace",
            description=(
                "Literal DolphinScheduler cluster name encoded with namespace; "
                "the master uses it to resolve kubeconfig."
            ),
        ),
        model_field(
            "task_params.yamlContent",
            compile_path="taskDefinitionJson[].taskParams.yamlContent",
            description=f"Spelling-preserved guarded TFJob YAML. {runtime}",
        ),
    )


def _kubeflow_templates(
    surface: KubeflowAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    """Return one workflow-instance-named TFJob template for the exact wire."""
    runtime = _kubeflow_runtime_guidance(surface)
    body = task_template_with_runtime_controls(
        """# One guarded Kubeflow TFJob manifest
name: train-mnist
type: KUBEFLOW
description: Run one workflow-instance-scoped Kubeflow TFJob
task_params:
  namespace: kubeflow-team
  cluster: production
  yamlContent: |
    apiVersion: "kubeflow.org/v1"
    kind: TFJob
    metadata:
      name: train-mnist-${system.workflow.instance.id}
      namespace: kubeflow-team
    spec:
      tfReplicaSpecs:
        Worker:
          replicas: 1
          restartPolicy: OnFailure
          template:
            spec:
              containers:
                - name: tensorflow
                  image: registry.example/tf-mnist:1.0
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout_notify_strategy: FAILED
timeout: 60
"""
    )
    comments = (
        f"# Runtime prerequisites and limits: {runtime}\n"
        "# Other CRD kinds, static/generateName identities, parameters, resources, "
        "outputs, and richer native state remain unchanged/export opaque "
        "preservation only.\n"
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one workflow-instance-scoped Kubeflow TFJob manifest.",
            payload_modes=("task_params",),
            yaml=f"{comments}{body}",
        ),
    )


def _kubeflow_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    """Materialize the eight exact reviewed KUBEFLOW memberships."""
    surface = get_task_authoring_surface(profile_version).kubeflow
    if not surface.available:
        message = f"KUBEFLOW is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=KUBEFLOW_TFJOB_MANIFEST_FACET,
        family="kubeflow-workflow-instance-tfjob-manifest-v1",
        review="kubeflow-workflow-instance-tfjob-manifest-exact-subset",
        params_model=_family_model("KUBEFLOW"),
        fields=_kubeflow_fields(surface),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed KUBEFLOW/tfjob_manifest authoring",
                condition_paths=(
                    "retry.times",
                    "timeout",
                    "timeout_notify_strategy",
                    "workflow.global_params",
                ),
                active_paths=(
                    "task_params.namespace",
                    "task_params.cluster",
                    "task_params.yamlContent",
                ),
                compile_policy=(
                    ("retry.times", "require exactly 0"),
                    ("timeout", "require a positive number of minutes"),
                    (
                        "timeout_notify_strategy",
                        "require FAILED or WARNFAILED",
                    ),
                    (
                        "typed TFJob target identity",
                        "require unique cluster/namespace/name per workflow",
                    ),
                    (
                        "workflow.global_params",
                        "reserve system.workflow.instance.id",
                    ),
                ),
                description=(
                    "Keep one workflow-instance-scoped TFJob identity stable across "
                    "failover without claiming task-retry or REPEAT_RUNNING "
                    "resubmission."
                ),
            ),
        ),
        templates=_kubeflow_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "KUBEFLOW/tfjob_manifest closes raw opaque create/edit; richer native "
            "state is unchanged/export preserve-only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="KUBEFLOW",
        category="MachineLearning",
        kind="typed",
        default_facet=KUBEFLOW_TFJOB_MANIFEST_FACET,
        facets={KUBEFLOW_TFJOB_MANIFEST_FACET: membership},
    )
