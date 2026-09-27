from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

K8sConnectionMode = Literal["NAMESPACE", "DATASOURCE"]
K8sWireEpoch = Literal[
    "namespace-entrypoint",
    "namespace-container-options",
    "datasource-container-options",
]
K8sOutputProtocol = Literal[
    "none",
    "legacy-dsval",
    "set-value",
    "transport-hole",
]
K8sCustomizedLabelScope = Literal["none", "job-only", "job-and-pod"]


@dataclass(frozen=True, slots=True)
class K8sAuthoringSurface:
    """Reviewed Kubernetes container wire, output, and recovery semantics."""

    available: bool
    exclusion_reason: str | None
    connection_mode: K8sConnectionMode | None
    wire_epoch: K8sWireEpoch | None
    advanced_container_fields: bool
    empty_customized_labels_supported: bool
    customized_label_scope: K8sCustomizedLabelScope | None
    output_protocol: K8sOutputProtocol
    output_transport_supported: bool
    empty_output_value_supported: bool
    output_value_equals_preserved: bool
    output_declarations_injected_as_environment: bool
    namespace_context_propagated: bool
    task_params_logged: bool
    resolved_kubeconfig_logged: bool
    all_prepared_values_injected: bool
    durable_application_id: bool
    failover_supported: bool
    cancel_requires_in_memory_job: bool
    retry_may_duplicate: bool


_K8S_ABSENT = K8sAuthoringSurface(
    available=False,
    exclusion_reason="upstream-absent",
    connection_mode=None,
    wire_epoch=None,
    advanced_container_fields=False,
    empty_customized_labels_supported=False,
    customized_label_scope=None,
    output_protocol="none",
    output_transport_supported=False,
    empty_output_value_supported=False,
    output_value_equals_preserved=False,
    output_declarations_injected_as_environment=False,
    namespace_context_propagated=False,
    task_params_logged=False,
    resolved_kubeconfig_logged=False,
    all_prepared_values_injected=False,
    durable_application_id=False,
    failover_supported=False,
    cancel_requires_in_memory_job=False,
    retry_may_duplicate=False,
)
_K8S_BROKEN_WATCHER = replace(
    _K8S_ABSENT,
    exclusion_reason="watcher-completes-on-running-with-unset-exit-status",
    all_prepared_values_injected=True,
)
_K8S_NAMESPACE_ENTRYPOINT = K8sAuthoringSurface(
    available=True,
    exclusion_reason=None,
    connection_mode="NAMESPACE",
    wire_epoch="namespace-entrypoint",
    advanced_container_fields=False,
    empty_customized_labels_supported=True,
    customized_label_scope="none",
    output_protocol="none",
    output_transport_supported=False,
    empty_output_value_supported=False,
    output_value_equals_preserved=False,
    output_declarations_injected_as_environment=False,
    namespace_context_propagated=True,
    task_params_logged=False,
    resolved_kubeconfig_logged=False,
    all_prepared_values_injected=True,
    durable_application_id=False,
    failover_supported=False,
    cancel_requires_in_memory_job=True,
    retry_may_duplicate=True,
)
_K8S_NAMESPACE_ADVANCED_LEGACY_OUTPUT = replace(
    _K8S_NAMESPACE_ENTRYPOINT,
    wire_epoch="namespace-container-options",
    advanced_container_fields=True,
    empty_customized_labels_supported=False,
    customized_label_scope="job-only",
    output_protocol="legacy-dsval",
    output_transport_supported=True,
    task_params_logged=True,
)
_K8S_NAMESPACE_ADVANCED_SET_VALUE = replace(
    _K8S_NAMESPACE_ADVANCED_LEGACY_OUTPUT,
    empty_customized_labels_supported=True,
    customized_label_scope="job-and-pod",
    output_protocol="set-value",
    output_value_equals_preserved=True,
)
_K8S_NAMESPACE_ADVANCED_SET_VALUE_EMPTY_OUTPUT = replace(
    _K8S_NAMESPACE_ADVANCED_SET_VALUE,
    empty_output_value_supported=True,
)
_K8S_DATASOURCE_TRANSPORT_HOLE = K8sAuthoringSurface(
    available=True,
    exclusion_reason=None,
    connection_mode="DATASOURCE",
    wire_epoch="datasource-container-options",
    advanced_container_fields=True,
    empty_customized_labels_supported=True,
    customized_label_scope="job-and-pod",
    output_protocol="transport-hole",
    output_transport_supported=False,
    empty_output_value_supported=False,
    output_value_equals_preserved=True,
    output_declarations_injected_as_environment=False,
    namespace_context_propagated=False,
    task_params_logged=True,
    resolved_kubeconfig_logged=True,
    all_prepared_values_injected=True,
    durable_application_id=False,
    failover_supported=False,
    cancel_requires_in_memory_job=True,
    retry_may_duplicate=True,
)
_K8S_DATASOURCE_SET_VALUE = replace(
    _K8S_DATASOURCE_TRANSPORT_HOLE,
    output_protocol="set-value",
    output_transport_supported=True,
    empty_output_value_supported=True,
    output_declarations_injected_as_environment=True,
    namespace_context_propagated=True,
)


def _k8s_surface(version: str) -> K8sAuthoringSurface:
    if version in {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
    }:
        return _K8S_ABSENT
    if version in {"3.1.0", "3.1.1", "3.1.2", "3.1.3"}:
        return _K8S_BROKEN_WATCHER
    if version in {"3.1.4", "3.1.5", "3.1.6", "3.1.7", "3.1.8", "3.1.9"}:
        return _K8S_NAMESPACE_ENTRYPOINT
    if version == "3.2.0":
        return _K8S_NAMESPACE_ADVANCED_LEGACY_OUTPUT
    if version == "3.2.1":
        return _K8S_NAMESPACE_ADVANCED_SET_VALUE
    if version == "3.2.2":
        return _K8S_NAMESPACE_ADVANCED_SET_VALUE_EMPTY_OUTPUT
    if version in {"3.3.1", "3.3.2", "3.4.0", "3.4.1"}:
        return _K8S_DATASOURCE_TRANSPORT_HOLE
    if version in {"3.4.2", "3.4.3"}:
        return _K8S_DATASOURCE_SET_VALUE
    message = f"No exact K8S authoring surface for DolphinScheduler {version}"
    raise ValueError(message)
