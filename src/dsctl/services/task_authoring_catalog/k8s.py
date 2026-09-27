from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.task_spec import (
    k8s_literal_container_job_params_model,
)
from dsctl.upstream.task_authoring_surface import (
    K8sAuthoringSurface,
    get_task_authoring_surface,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.common import YamlObject, YamlValue

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
    model_field,
)

K8S_LITERAL_CONTAINER_JOB_FACET = "K8S/literal_container_job"
_K8S_RUNTIME_EXCLUSION_OPAQUE_FACET = "K8S/runtime_exclusion_opaque"


def _k8s_runtime_guidance(surface: K8sAuthoringSurface) -> str:
    """Describe exact connection, disclosure, and recovery behavior."""
    if not surface.available or surface.connection_mode is None:
        reason = surface.exclusion_reason or "upstream-absent"
        return f"K8S typed authoring is unavailable: {reason}."
    connection = (
        "The task carries a legacy namespace/cluster selector."
        if surface.connection_mode == "NAMESPACE"
        else (
            "The worker resolves the selected K8S datasource and logs the resolved "
            "task parameter object, including kubeconfig; use only a datasource "
            "whose resolved configuration is safe for task logs."
        )
    )
    namespace_context = (
        " This exact executor propagates the selected namespace to Pod-log/output "
        "lookup."
        if surface.namespace_context_propagated
        else (
            " This exact executor does not propagate the selected namespace to "
            "Pod-log/output lookup."
        )
    )
    output_environment = (
        " On this exact release, compiler-owned OUT declarations are also "
        "injected into the Pod as empty-valued environment entries."
        if surface.output_declarations_injected_as_environment
        else ""
    )
    output_values = _k8s_output_value_guidance(surface)
    container_options = (
        "Container command and args use the image ENTRYPOINT/CMD, and the plugin "
        "fixes imagePullPolicy=Always."
        if not surface.advanced_container_fields
        else (
            "Canonical command, args, pull secret name, image pull policy, labels, "
            "and node selectors are projected deterministically."
        )
    )
    task_logging = (
        " DolphinScheduler INFO-logs the complete task parameter object."
        if surface.task_params_logged and not surface.resolved_kubeconfig_logged
        else ""
    )
    label_scope = (
        "This epoch does not support customized labels."
        if surface.customized_label_scope == "none"
        else (
            "Customized labels are attached to Job metadata only."
            if surface.customized_label_scope == "job-only"
            else "Customized labels are attached to Job and Pod metadata."
        )
    )
    return (
        f"{connection} {container_options} {label_scope}"
        f"{task_logging} DolphinScheduler derives the Job, container, and tracking "
        "label name by lowercasing the authored task name and appending '-' plus "
        "taskInstanceId; typed names are limited to 52 ASCII alphanumeric/hyphen "
        "characters. DolphinScheduler injects "
        "the complete prepared parameter "
        "map, not only this task's environment entries, into the Pod environment. "
        "Workflow/global/built-in/varPool keys must also be valid Kubernetes "
        "environment names and must not collide with taskInstanceId; dsctl can "
        "statically validate only this task's authored environment list. "
        "Task fields, prepared values, Pod logs, and resolved configuration are "
        "not secret storage; dsctl does not detect or redact secrets. Image tags "
        "are accepted literally and may move; use a digest when immutability is "
        f"required. Job watch registration omits its namespace; the target must "
        f"match the kubeconfig current-context namespace.{namespace_context}"
        f"{output_environment}{output_values} The "
        "executor creates a deterministic-name batch/v1 Job with restartPolicy "
        "Never, backoffLimit zero, and TTL 300 seconds. The "
        "plugin exposes no durable Kubernetes job id or failover "
        "resume. Cancel requires the same worker's in-memory Job, while retry or "
        "worker loss may delete, recreate, or duplicate execution and its side effects."
    )


def _k8s_output_value_guidance(surface: K8sAuthoringSurface) -> str:
    """Describe exact stdout-value parsing without overstating transport."""
    if not surface.output_transport_supported:
        return ""
    if surface.output_protocol == "legacy-dsval":
        return (
            " Output markers must carry nonempty values; the legacy parser reserves "
            "$VarPool$ as a delimiter and keeps only the first '=' segment."
        )
    if surface.empty_output_value_supported:
        return " Output parsing supports empty values and preserves '=' in values."
    return (
        " Output markers must carry nonempty values; parsing preserves '=' in values."
    )


def _k8s_fields(surface: K8sAuthoringSurface) -> tuple[TaskAuthoringField, ...]:
    """Return the stable minimal container-job fields for one exact epoch."""
    if not surface.available or surface.connection_mode is None:
        return ()
    runtime = _k8s_runtime_guidance(surface)
    fields: list[TaskAuthoringField] = [
        model_field(
            "task_params.connectionMode",
            choices=(surface.connection_mode,),
            description=(
                "Canonical connection intent selected by the exact profile; the "
                "discriminator is projected onto native namespace or datasource wire."
            ),
        )
    ]
    if surface.connection_mode == "NAMESPACE":
        fields.extend(
            (
                model_field(
                    "task_params.namespace",
                    required=True,
                    choice_source="dsctl namespace list",
                    related_commands=("dsctl namespace get NAMESPACE",),
                    active_when="task_params.connectionMode == NAMESPACE",
                    compile_path="taskDefinitionJson[].taskParams.namespace",
                    description=(
                        "Kubernetes DNS-1123 namespace label encoded into the exact "
                        "legacy namespace JSON selector."
                    ),
                ),
                model_field(
                    "task_params.cluster",
                    required=True,
                    choice_source="dsctl cluster list",
                    related_commands=("dsctl cluster get CLUSTER",),
                    active_when="task_params.connectionMode == NAMESPACE",
                    compile_path="taskDefinitionJson[].taskParams.namespace",
                    description=(
                        "Literal legacy Kubernetes cluster selector encoded together "
                        "with namespace. INFO-visible; not secret storage."
                    ),
                ),
            )
        )
    else:
        fields.append(
            model_field(
                "task_params.datasource",
                "integer|string",
                required=True,
                choices=(),
                active_when="task_params.connectionMode == DATASOURCE",
                choice_source="dsctl datasource list",
                related_commands=(
                    "dsctl datasource list",
                    "dsctl datasource get DATASOURCE",
                ),
                compile_path="taskDefinitionJson[].taskParams.datasource",
                description=(
                    "Positive K8S datasource id or exact name. The worker resolves "
                    "namespace and kubeconfig; resolved values may be written to "
                    "task logs."
                ),
            )
        )
    fields.extend(
        (
            model_field(
                "task_params.image",
                compile_path="taskDefinitionJson[].taskParams.image",
                description=f"Literal Kubernetes image reference. {runtime}",
            ),
            model_field(
                "task_params.minCpuCores",
                model_default=True,
                compile_path="taskDefinitionJson[].taskParams.minCpuCores",
                description="Finite non-negative requested CPU cores.",
            ),
            model_field(
                "task_params.minMemorySpace",
                model_default=True,
                compile_path="taskDefinitionJson[].taskParams.minMemorySpace",
                description="Finite non-negative requested memory in MiB.",
            ),
            model_field(
                "task_params.environment",
                default=[],
                compile_path="taskDefinitionJson[].taskParams.localParams",
                description=(
                    "Ordered literal input environment values compiled to IN/VARCHAR "
                    "localParams. DolphinScheduler may inject additional prepared "
                    "values."
                ),
            ),
            model_field(
                "task_params.environment[]",
                description="One literal Kubernetes Pod environment input.",
            ),
            model_field(
                "task_params.environment[].name",
                description=(
                    "Unique Kubernetes environment name; taskInstanceId is reserved."
                ),
            ),
            model_field(
                "task_params.environment[].value",
                description="Literal INFO-visible value; not secret storage.",
            ),
        )
    )
    if surface.advanced_container_fields:
        fields.extend(
            (
                model_field(
                    "task_params.command",
                    default=[],
                    compile_path="taskDefinitionJson[].taskParams.command",
                    description=(
                        "Ordered container entrypoint argv encoded as one compact "
                        "native JSON array string."
                    ),
                ),
                model_field(
                    "task_params.command[]",
                    description="One literal argv item; empty strings are preserved.",
                ),
                model_field(
                    "task_params.args",
                    default=[],
                    compile_path="taskDefinitionJson[].taskParams.args",
                    description=(
                        "Ordered container arguments encoded as one compact native "
                        "JSON array string."
                    ),
                ),
                model_field(
                    "task_params.args[]",
                    description="One literal argv item; empty strings are preserved.",
                ),
                model_field(
                    "task_params.pullSecret",
                    compile_path="taskDefinitionJson[].taskParams.pullSecret",
                    description=(
                        "Optional Kubernetes Secret object name, never secret content."
                    ),
                ),
                model_field(
                    "task_params.imagePullPolicy",
                    model_default=True,
                    compile_path="taskDefinitionJson[].taskParams.imagePullPolicy",
                    description="Exact Kubernetes image pull policy.",
                ),
                model_field(
                    "task_params.nodeSelectors",
                    default=[],
                    compile_path="taskDefinitionJson[].taskParams.nodeSelectors",
                    description=(
                        "Ordered Kubernetes node-selector expressions; repeated "
                        "keys form logical AND, while exact duplicates are rejected."
                    ),
                ),
                model_field(
                    "task_params.nodeSelectors[]",
                    description="One exact node-selector expression.",
                ),
                model_field(
                    "task_params.nodeSelectors[].key",
                    description="Kubernetes qualified node-label key.",
                ),
                model_field(
                    "task_params.nodeSelectors[].operator",
                    description="Kubernetes selector operator.",
                ),
                model_field(
                    "task_params.nodeSelectors[].values",
                    description=(
                        "Operator-specific values joined with commas on the native "
                        "wire."
                    ),
                ),
                model_field(
                    "task_params.nodeSelectors[].values[]",
                    required=False,
                    description="One comma-free literal selector value.",
                ),
            )
        )
        label_field = (
            model_field(
                "task_params.customizedLabels",
                compile_path="taskDefinitionJson[].taskParams.customizedLabels",
                description=(
                    "At least one label is required on 3.2.0 because its executor "
                    "mutates the returned map and crashes when the list is empty."
                ),
            )
            if not surface.empty_customized_labels_supported
            else model_field(
                "task_params.customizedLabels",
                default=[],
                compile_path="taskDefinitionJson[].taskParams.customizedLabels",
                description="Ordered unique non-reserved Kubernetes labels.",
            )
        )
        fields.extend(
            (
                label_field,
                model_field(
                    "task_params.customizedLabels[]",
                    required=False,
                    description="One Kubernetes label entry.",
                ),
                model_field(
                    "task_params.customizedLabels[].label",
                    description="Unique non-reserved Kubernetes qualified label key.",
                ),
                model_field(
                    "task_params.customizedLabels[].value",
                    description="Kubernetes label value; an empty value is valid.",
                ),
            )
        )
    if surface.output_transport_supported:
        marker = (
            "${(name=value)dsVal} or #{(name=value)dsVal}"
            if surface.output_protocol == "legacy-dsval"
            else "${setValue(name=value)} or #{setValue(name=value)}"
        )
        fields.extend(
            (
                model_field(
                    "task_params.outputs",
                    default=[],
                    compile_path="taskDefinitionJson[].taskParams.localParams",
                    description=(
                        "Declared stdout-derived outputs. The container must print "
                        f"{marker}; missing markers do not produce stable values."
                        f"{_k8s_output_value_guidance(surface)}"
                    ),
                ),
                model_field(
                    "task_params.outputs[]",
                    description="One OUT/VARCHAR declaration with an empty wire value.",
                ),
                model_field(
                    "task_params.outputs[].name",
                    description=(
                        "Unique output name disjoint from environment input names."
                    ),
                ),
            )
        )
    return tuple(fields)


def _k8s_templates(
    surface: K8sAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    """Return one exact-version minimal literal container template."""
    runtime = _k8s_runtime_guidance(surface)
    if surface.connection_mode == "NAMESPACE":
        connection = """  connectionMode: NAMESPACE
  namespace: analytics
  cluster: production
"""
    elif surface.connection_mode == "DATASOURCE":
        connection = """  connectionMode: DATASOURCE
  datasource: 1
"""
    else:
        message = "Available K8S surface must declare one connection mode"
        raise ValueError(message)
    body = task_template_with_runtime_controls(
        f"""# Task template for one literal Kubernetes container job
name: run-container-job
type: K8S
description: Run one literal container job
task_params:
{connection}  image: busybox:1.36
  minCpuCores: 0.0
  minMemorySpace: 0.0
  environment: []
{_k8s_advanced_template_fields(surface)}
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
    )
    comments = (
        f"# Runtime prerequisite: {runtime}\n"
        "# Typed scope: one literal container job; native runtime and future state "
        "remain unchanged/export opaque preservation only.\n"
    )
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Run one literal Kubernetes container job.",
            payload_modes=("task_params",),
            yaml=f"{comments}{body}",
        ),
    )


def _k8s_advanced_template_fields(surface: K8sAuthoringSurface) -> str:
    """Render version-selected advanced defaults without exposing native strings."""
    if not surface.advanced_container_fields:
        return ""
    labels = (
        """  customizedLabels:
    - label: app.kubernetes.io/managed-by
      value: dolphinscheduler
"""
        if not surface.empty_customized_labels_supported
        else "  customizedLabels: []\n"
    )
    outputs = "  outputs: []\n" if surface.output_transport_supported else ""
    return (
        "  command: []\n"
        "  args: []\n"
        "  imagePullPolicy: IfNotPresent\n"
        f"{labels}"
        "  nodeSelectors: []\n"
        f"{outputs}"
    ).rstrip()


def _is_explicit_k8s_native_opaque_mode(
    task_params: Mapping[str, YamlValue],
) -> bool:
    """Recognize the exact compact namespace JSON that marks legacy native wire."""
    if "connectionMode" in task_params or "cluster" in task_params:
        return False
    namespace = task_params.get("namespace")
    image = task_params.get("image")
    if (
        not isinstance(namespace, str)
        or not namespace.strip()
        or not isinstance(image, str)
        or not image.strip()
    ):
        return False
    try:
        decoded = json.loads(namespace)
    except (json.JSONDecodeError, RecursionError):
        return False
    if not isinstance(decoded, dict):
        return False
    name = decoded.get("name")
    cluster = decoded.get("cluster")
    if (
        set(decoded) != {"name", "cluster"}
        or not isinstance(name, str)
        or not name.strip()
        or not isinstance(cluster, str)
        or not cluster.strip()
    ):
        return False
    expected = json.dumps(
        {"name": name, "cluster": cluster},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return namespace == expected


def _k8s_runtime_exclusion_opaque_profile(
    profile_version: str,
    *,
    surface: K8sAuthoringSurface,
) -> TaskTypeAuthoringProfile:
    """Materialize the exact watcher hole with native-only opaque selection."""
    reason = surface.exclusion_reason
    if (
        profile_version not in {"3.1.0", "3.1.1", "3.1.2", "3.1.3"}
        or surface.available
        or reason != "watcher-completes-on-running-with-unset-exit-status"
        or surface.wire_epoch is not None
        or surface.connection_mode is not None
    ):
        message = f"K8S {profile_version} is not the reviewed runtime hole"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=_K8S_RUNTIME_EXCLUSION_OPAQUE_FACET,
        family="k8s-runtime-exclusion-opaque-v1",
        review="k8s-watcher-runtime-exclusion-with-explicit-native-opaque-mode",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when=f"exact {profile_version} K8S native opaque authoring",
                condition_paths=("task_params.namespace",),
                active_paths=("task_params",),
                compile_policy=(("task_params", "preserve native object unchanged"),),
                description=(
                    "Only an exact native compact namespace JSON object plus a "
                    "nonblank image may be created or edited opaquely. Canonical "
                    "connectionMode/namespace/cluster intent remains fail-closed."
                ),
            ),
        ),
        templates=(
            TaskAuthoringTemplate(
                name="minimal",
                summary=(
                    f"Author an explicit native K8S {profile_version} payload under "
                    "the reviewed "
                    "watcher runtime exclusion."
                ),
                payload_modes=("task_params",),
                yaml=task_template_with_runtime_controls(
                    f"""# Exact {profile_version} native opaque K8S scaffold
# Typed container-job authoring is disabled because the watcher can finish on
# RUNNING with an unset exit status. Use this raw native escape hatch only when
# you accept that runtime defect.
name: run-k8s-native-opaque
type: K8S
description: Author an exact native K8S {profile_version} payload at your own risk
task_params:
  namespace: '{{"name":"analytics","cluster":"production"}}'
  image: busybox:1.36
  minCpuCores: 0.0
  minMemorySpace: 0.0
  localParams: []
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
                ),
            ),
        ),
        opaque_authoring_selector=_is_explicit_k8s_native_opaque_mode,
        restrict_opaque_authoring_to_selector=True,
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=False,
        typed_edit=False,
        opaque_create=True,
        opaque_edit=True,
        opaque_preserve=True,
        constraint=(
            "K8S literal container-job typed create/edit is disabled because exact "
            f"{profile_version} has {reason}; only explicitly discriminated native "
            "opaque create/edit and unchanged opaque preservation are allowed."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="K8S",
        category="Cloud",
        kind="generic",
        default_facet=_K8S_RUNTIME_EXCLUSION_OPAQUE_FACET,
        facets={_K8S_RUNTIME_EXCLUSION_OPAQUE_FACET: membership},
    )


def _k8s_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).k8s
    if not surface.available:
        if surface.exclusion_reason is not None:
            return _k8s_runtime_exclusion_opaque_profile(
                profile_version,
                surface=surface,
            )
        message = f"K8S typed authoring is unavailable for {profile_version}"
        raise ValueError(message)
    params_model = k8s_literal_container_job_params_model(
        advanced_container_fields=surface.advanced_container_fields,
        empty_customized_labels_supported=(surface.empty_customized_labels_supported),
        output_transport_supported=surface.output_transport_supported,
    )
    contract = TaskAuthoringFacetContract(
        facet_id=K8S_LITERAL_CONTAINER_JOB_FACET,
        family="k8s-literal-container-job-v1",
        review="k8s-literal-container-job-exact-subset",
        params_model=params_model,
        fields=_k8s_fields(surface),
        state_rules=(),
        templates=_k8s_templates(surface),
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
            "K8S/literal_container_job has no raw opaque create/edit selector; "
            "preserve richer native state unchanged."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="K8S",
        category="Cloud",
        kind="typed",
        default_facet=K8S_LITERAL_CONTAINER_JOB_FACET,
        facets={K8S_LITERAL_CONTAINER_JOB_FACET: membership},
    )


def validate_semantics(
    task_params: YamlObject,
    *,
    version: str,
    surface: K8sAuthoringSurface,
) -> None:
    """Bind one stable container job to the selected connection epoch."""
    mode = task_params.get("connectionMode")
    if mode != surface.connection_mode:
        message = (
            f"K8S connectionMode {mode!r} is unsupported for "
            f"DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "K8S",
                "field": "tasks[].task_params.connectionMode",
                "value": mode,
                "supported": surface.connection_mode,
                "reason": "upstream_capability_absent",
            },
            suggestion=(
                "Use the connectionMode shown by `dsctl task-type schema K8S` "
                "for the selected version."
            ),
        )
    advanced_fields = frozenset(
        {
            "command",
            "args",
            "pullSecret",
            "imagePullPolicy",
            "customizedLabels",
            "nodeSelectors",
        }
    )
    unsupported_advanced = sorted(advanced_fields.intersection(task_params))
    if not surface.advanced_container_fields and unsupported_advanced:
        fields = ", ".join(unsupported_advanced)
        message = f"K8S container options are unavailable on {version}: {fields}"
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "K8S",
                "fields": unsupported_advanced,
                "reason": "upstream_capability_absent",
            },
        )
    if not surface.output_transport_supported and "outputs" in task_params:
        message = (
            f"K8S task output transport is unavailable for DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "selected_version": version,
                "task_type": "K8S",
                "field": "tasks[].task_params.outputs",
                "reason": "upstream_capability_absent",
            },
        )
