from __future__ import annotations

import ast
import json
import re
import runpy
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, assert_never

from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS
from ds_codegen.compiled_literals import read_compiled_literals
from ds_codegen.runtime_contract import runtime_operation_bindings
from ds_codegen.version_profiles import (
    compile_version_profile_data,
    load_version_profile_ledger,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence, Set
    from pathlib import Path


JsonObject = dict[str, Any]
ActionClassification = Literal["local", "diagnostic", "remote"]
SemanticSeamKind = Literal[
    "definition_reads",
    "direct_port",
    "bound_domain",
    "task_definitions",
]

DEFAULT_BASELINE_VERSION = "3.4.1"

_GENERATED_OPERATION = re.compile(
    r"^\s*DS operation:\s*(?P<source>[^|]+?)\s*\|\s*"
    r"(?P<method>[A-Z]+)\s+/(?P<path>\S+)\s*$",
    re.MULTILINE,
)
_GENERATED_OPERATION_ID = re.compile(
    r"^\s*DS operation ID:\s*(?P<source>\S+)\s*$",
    re.MULTILINE,
)
LOCAL_ACTIONS = frozenset(
    {
        "version",
        "context",
        "schema",
        "capabilities",
        "context.create",
        "context.update",
        "context.list",
        "context.get",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
        "enum.names",
        "enum.list",
        "lint.workflow",
        "lint.workflow-patch",
        "lint.workflow-instance-patch",
        "task-type.get",
        "task-type.schema",
        "template.workflow",
        "template.workflow-patch",
        "template.workflow-instance-patch",
        "template.params",
        "template.environment",
        "template.cluster",
        "template.datasource",
        "template.task",
    }
)

DIAGNOSTIC_ACTIONS = frozenset(
    {
        "doctor",
        "monitor.health",
        "monitor.server",
        "monitor.database",
    }
)


@dataclass(frozen=True)
class StableActionDependencySources:
    """Source and compiled ownership inputs for exact reviewed seam analysis."""

    source_root: Path
    commands_root: Path
    protocols_root: Path
    generated_client_source: Path
    generated_operations_root: Path
    generated_profile_manifest: Path | None = None
    compiled_wire_artifact_root: Path | None = None
    baseline_version: str = DEFAULT_BASELINE_VERSION

    @classmethod
    def from_repository(
        cls, project_root: Path, *, baseline_version: str = DEFAULT_BASELINE_VERSION
    ) -> StableActionDependencySources:
        """Resolve one exact baseline and prove its generated manifest identity."""
        _validate_baseline_version(baseline_version)
        source_root = project_root / "src"
        dsctl_root = source_root / "dsctl"
        slug = baseline_version.replace(".", "_")
        generated = dsctl_root / "generated" / "versions" / f"ds_{slug}"
        _require_manifest_version(generated / "_manifest.py", baseline_version)
        return cls(
            baseline_version=baseline_version,
            source_root=source_root,
            commands_root=dsctl_root / "commands",
            protocols_root=dsctl_root / "upstream" / "protocols",
            generated_client_source=generated / "client.py",
            generated_operations_root=generated / "api" / "operations",
            generated_profile_manifest=generated / "_manifest.py",
            compiled_wire_artifact_root=(dsctl_root / "generated" / "wire_programs"),
        )


@dataclass(frozen=True)
class _ImportRef:
    module: str
    name: str | None


@dataclass(frozen=True)
class _FunctionRef:
    module: str
    qualname: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    class_name: str | None

    @property
    def key(self) -> str:
        return f"{self.module}.{self.qualname}"


@dataclass(frozen=True)
class _ModuleInfo:
    name: str
    path: Path
    tree: ast.Module
    imports: Mapping[str, _ImportRef]
    functions: Mapping[str, _FunctionRef]
    classes: Mapping[str, ast.ClassDef]


@dataclass(frozen=True)
class _Symbolic:
    kind: str
    name: str | None = None


@dataclass(frozen=True)
class _AnalysisState:
    function_key: str
    bindings: tuple[tuple[str, _Symbolic], ...] = ()


@dataclass(frozen=True)
class _PortCall:
    group: str
    method: str


@dataclass(frozen=True)
class _GeneratedOperation:
    client_operation: str
    source_operation: str
    http_method: str
    path: str


@dataclass(frozen=True)
class _ReviewedSemanticSeam:
    """One explicit deep-module seam allowed to use reviewed wire closure."""

    kind: SemanticSeamKind
    group: str
    method: str
    protocol: str
    imported_module: str | None = None
    imported_name: str | None = None
    direct_transport_calls: tuple[str, ...] = ()
    required_service_methods: tuple[str, ...] = ()
    required_port_calls: tuple[_PortCall, ...] = ()


@dataclass(frozen=True)
class _ReviewedSemanticDependency:
    """Ledger-owned semantic operation plus its reviewed baseline wire closure."""

    semantic_operation: str
    source_operations: tuple[str, ...]
    seam: _ReviewedSemanticSeam
    baseline_version: str = DEFAULT_BASELINE_VERSION
    unavailable: _ReviewedUnavailableDecision | None = None


@dataclass(frozen=True)
class _ReviewedUnavailableDecision:
    """An explicit ledger decision, never inferred from a missing runtime binding."""

    availability: str
    reason: str
    constraint: str
    evidence_sources: tuple[str, ...]


@dataclass
class _Reachability:
    ports: set[_PortCall]
    direct_transport_calls: set[str]
    diagnostics: list[JsonObject]


def _reviewed_bound_domain_seams(
    semantic_operations: tuple[str, ...],
    *,
    group: str,
    domain_type: str,
    imported_module: str,
    imported_name: str,
    direct_transport_calls: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, _ReviewedSemanticSeam]:
    """Declare the exact operations reviewed behind one generated domain seam."""
    reviewed_transport = direct_transport_calls or {}
    unexpected = set(reviewed_transport) - set(semantic_operations)
    if unexpected:
        message = (
            "reviewed direct transport is declared for unknown semantic "
            f"operations: {', '.join(sorted(unexpected))}"
        )
        raise ValueError(message)
    return {
        semantic_operation: _ReviewedSemanticSeam(
            kind="bound_domain",
            group=group,
            method="bind",
            protocol=f"BoundDomain[{domain_type}]",
            imported_module=imported_module,
            imported_name=imported_name,
            direct_transport_calls=reviewed_transport.get(
                semantic_operation,
                (),
            ),
        )
        for semantic_operation in semantic_operations
    }


def _reviewed_task_definition_seam(
    *,
    service_method: str,
    required_service_methods: tuple[str, ...],
    required_port_calls: tuple[tuple[str, str], ...],
) -> _ReviewedSemanticSeam:
    """Declare one exact service-to-TaskDefinitions semantic boundary."""
    return _ReviewedSemanticSeam(
        kind="task_definitions",
        group="task-definitions",
        method=service_method,
        protocol="TaskDefinitions",
        required_service_methods=required_service_methods,
        required_port_calls=tuple(
            _PortCall(group, method) for group, method in required_port_calls
        ),
    )


_REVIEWED_SEMANTIC_SEAMS: dict[str, _ReviewedSemanticSeam] = {
    "identity.current": _ReviewedSemanticSeam(
        kind="direct_port",
        group="users",
        method="current",
        protocol="CurrentUserOperations",
        direct_transport_calls=("DolphinSchedulerClient.healthcheck",),
    ),
    "project.page": _ReviewedSemanticSeam(
        kind="definition_reads",
        group="definitions",
        method="list_projects",
        protocol="DefinitionReads",
    ),
    "project.get": _ReviewedSemanticSeam(
        kind="definition_reads",
        group="definitions",
        method="get_project",
        protocol="DefinitionReads",
    ),
    **_reviewed_bound_domain_seams(
        ("project.create", "project.delete", "project.update"),
        group="project",
        domain_type="ProjectDomain",
        imported_module="dsctl.upstream.projects",
        imported_name="PROJECT_DOMAIN",
    ),
    "workflow.page": _ReviewedSemanticSeam(
        kind="definition_reads",
        group="definitions",
        method="list_workflows",
        protocol="DefinitionReads",
    ),
    "workflow.get": _ReviewedSemanticSeam(
        kind="definition_reads",
        group="definitions",
        method="get_workflow",
        protocol="DefinitionReads",
    ),
    **_reviewed_bound_domain_seams(
        (
            "access-token.create",
            "access-token.delete",
            "access-token.generate",
            "access-token.get",
            "access-token.list",
            "access-token.update",
        ),
        group="access-token",
        domain_type="AccessTokenDomain",
        imported_module="dsctl.upstream.access_tokens",
        imported_name="ACCESS_TOKEN_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "alert-group.create",
            "alert-group.delete",
            "alert-group.get",
            "alert-group.page",
            "alert-group.update",
        ),
        group="alert-group",
        domain_type="AlertGroupDomain",
        imported_module="dsctl.upstream.alert_groups",
        imported_name="ALERT_GROUP_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "alert-plugin.create",
            "alert-plugin.definition.list",
            "alert-plugin.delete",
            "alert-plugin.get",
            "alert-plugin.page",
            "alert-plugin.schema",
            "alert-plugin.test",
            "alert-plugin.update",
        ),
        group="alert-plugin",
        domain_type="AlertPluginDomain",
        imported_module="dsctl.upstream.alert_plugins",
        imported_name="ALERT_PLUGIN_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "audit.list",
            "audit.model-types",
            "audit.operation-types",
        ),
        group="audit",
        domain_type="AuditDomain",
        imported_module="dsctl.upstream.observability",
        imported_name="AUDIT_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "cluster.create",
            "cluster.delete",
            "cluster.get",
            "cluster.page",
            "cluster.update",
        ),
        group="cluster",
        domain_type="ClusterDomain",
        imported_module="dsctl.upstream.clusters",
        imported_name="CLUSTER_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "datasource.create",
            "datasource.delete",
            "datasource.get",
            "datasource.page",
            "datasource.saved-test",
            "datasource.update",
        ),
        group="datasource",
        domain_type="DataSourceDomain",
        imported_module="dsctl.upstream.datasources",
        imported_name="DATASOURCE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "environment.create",
            "environment.delete",
            "environment.get",
            "environment.page",
            "environment.update",
        ),
        group="environment",
        domain_type="EnvironmentDomain",
        imported_module="dsctl.upstream.environments",
        imported_name="ENVIRONMENT_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "namespace.available",
            "namespace.create",
            "namespace.delete",
            "namespace.get",
            "namespace.page",
        ),
        group="namespace",
        domain_type="NamespaceDomain",
        imported_module="dsctl.upstream.namespaces",
        imported_name="NAMESPACE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "queue.create",
            "queue.delete",
            "queue.get",
            "queue.page",
            "queue.update",
        ),
        group="queue",
        domain_type="QueueDomain",
        imported_module="dsctl.upstream.queues",
        imported_name="QUEUE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "resource.create",
            "resource.delete",
            "resource.download",
            "resource.mkdir",
            "resource.page",
            "resource.upload",
            "resource.view",
        ),
        group="resource",
        domain_type="ResourceDomain",
        imported_module="dsctl.upstream.resources",
        imported_name="RESOURCE_DOMAIN",
        direct_transport_calls={
            "resource.download": ("DolphinSchedulerClient.get_binary",),
            "resource.upload": ("DolphinSchedulerClient.request_result",),
            "resource.view": ("DolphinSchedulerClient.get_binary",),
        },
    ),
    **_reviewed_bound_domain_seams(
        (
            "project-parameter.create",
            "project-parameter.delete",
            "project-parameter.get",
            "project-parameter.page",
            "project-parameter.update",
        ),
        group="project-parameter",
        domain_type="ProjectParameterDomain",
        imported_module="dsctl.upstream.project_parameters",
        imported_name="PROJECT_PARAMETER_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "project-preference.disable",
            "project-preference.enable",
            "project-preference.get",
            "project-preference.update",
        ),
        group="project-preference",
        domain_type="ProjectPreferenceDomain",
        imported_module="dsctl.upstream.project_preferences",
        imported_name="PROJECT_PREFERENCE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "project-worker-group.clear",
            "project-worker-group.page",
            "project-worker-group.set",
        ),
        group="project-worker-group",
        domain_type="ProjectWorkerGroupDomain",
        imported_module="dsctl.upstream.project_worker_groups",
        imported_name="PROJECT_WORKER_GROUP_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "schedule.create",
            "schedule.delete",
            "schedule.explain",
            "schedule.get",
            "schedule.offline",
            "schedule.online",
            "schedule.page",
            "schedule.preview",
            "schedule.update",
        ),
        group="schedule",
        domain_type="ScheduleDomain",
        imported_module="dsctl.upstream.schedules",
        imported_name="SCHEDULE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "task-instance.force-success",
            "task-instance.get",
            "task-instance.list",
            "task-instance.log",
            "task-instance.savepoint",
            "task-instance.stop",
            "task-instance.sub-workflow",
            "task-instance.watch",
            "workflow-instance.digest",
            "workflow-instance.edit",
            "workflow-instance.execute-task",
            "workflow-instance.export",
            "workflow-instance.get",
            "workflow-instance.list",
            "workflow-instance.parent",
            "workflow-instance.recover-failed",
            "workflow-instance.rerun",
            "workflow-instance.stop",
            "workflow-instance.watch",
        ),
        group="runtime-instance",
        domain_type="RuntimeInstanceDomain",
        imported_module="dsctl.upstream.runtime_instances",
        imported_name="RUNTIME_INSTANCE_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "task-group.close",
            "task-group.create",
            "task-group.get",
            "task-group.page",
            "task-group.queue.force-start",
            "task-group.queue.page",
            "task-group.queue.set-priority",
            "task-group.start",
            "task-group.update",
        ),
        group="task-group",
        domain_type="TaskGroupDomain",
        imported_module="dsctl.upstream.task_groups",
        imported_name="TASK_GROUP_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        ("task-type.list",),
        group="task-type",
        domain_type="TaskTypeDomain",
        imported_module="dsctl.upstream.task_type_inventory",
        imported_name="TASK_TYPE_DOMAIN",
    ),
    "task.get": _reviewed_task_definition_seam(
        service_method="get",
        required_service_methods=("get",),
        required_port_calls=(
            ("definitions", "resolve_workflow"),
            ("task_definitions", "describe"),
            ("task_definitions", "get"),
        ),
    ),
    "task.list": _reviewed_task_definition_seam(
        service_method="list",
        required_service_methods=("list",),
        required_port_calls=(
            ("definitions", "resolve_workflow"),
            ("task_definitions", "describe"),
        ),
    ),
    "task.update": _reviewed_task_definition_seam(
        service_method="update",
        required_service_methods=("apply", "prepare_update"),
        required_port_calls=(
            ("definitions", "resolve_workflow"),
            ("definitions", "workflow_refs"),
            ("task_definitions", "apply_update"),
            ("task_definitions", "describe"),
            ("task_definitions", "get"),
            ("task_definitions", "prepare_update"),
        ),
    ),
    **_reviewed_bound_domain_seams(
        (
            "tenant.create",
            "tenant.delete",
            "tenant.get",
            "tenant.page",
            "tenant.update",
        ),
        group="tenant",
        domain_type="TenantDomain",
        imported_module="dsctl.upstream.tenants",
        imported_name="TENANT_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "user.create",
            "user.delete",
            "user.get",
            "user.grant.datasource",
            "user.grant.namespace",
            "user.grant.project",
            "user.list",
            "user.revoke.datasource",
            "user.revoke.namespace",
            "user.revoke.project",
            "user.update",
        ),
        group="user",
        domain_type="UserDomain",
        imported_module="dsctl.upstream.users",
        imported_name="USER_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "worker-group.create",
            "worker-group.delete",
            "worker-group.get",
            "worker-group.page",
            "worker-group.update",
        ),
        group="worker-group",
        domain_type="WorkerGroupDomain",
        imported_module="dsctl.upstream.worker_groups",
        imported_name="WORKER_GROUP_DOMAIN",
    ),
    **_reviewed_bound_domain_seams(
        (
            "workflow.backfill",
            "workflow.create",
            "workflow.delete",
            "workflow.describe",
            "workflow.digest",
            "workflow.edit",
            "workflow.export",
            "workflow.lineage.dependent-tasks",
            "workflow.lineage.get",
            "workflow.lineage.list",
            "workflow.offline",
            "workflow.online",
            "workflow.run",
            "workflow.run-task",
        ),
        group="workflow",
        domain_type="WorkflowDomain",
        imported_module="dsctl.upstream.workflows",
        imported_name="WORKFLOW_DOMAIN",
    ),
}


def analyze_repository_stable_action_dependencies(
    project_root: Path, *, baseline_version: str = DEFAULT_BASELINE_VERSION
) -> JsonObject:
    """Analyze every stable action against one explicitly selected exact baseline."""
    sources = StableActionDependencySources.from_repository(
        project_root, baseline_version=baseline_version
    )
    return analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=_load_stable_actions(
            sources.source_root / "dsctl" / "cli_surface.py"
        ),
        local_actions=LOCAL_ACTIONS,
        diagnostic_actions=DIAGNOSTIC_ACTIONS,
        reviewed_semantic_dependencies=(
            _repository_reviewed_semantic_dependencies(
                project_root, baseline_version=baseline_version
            )
        ),
    )


def _load_stable_actions(cli_surface_source: Path) -> frozenset[str]:
    namespace = runpy.run_path(str(cli_surface_source))
    factory = namespace.get("stable_leaf_actions")
    if not callable(factory):
        message = f"cli surface has no stable_leaf_actions(): {cli_surface_source}"
        raise TypeError(message)
    actions = factory()
    if not isinstance(actions, frozenset) or not all(
        isinstance(action, str) for action in actions
    ):
        message = f"cli surface returned invalid stable actions: {cli_surface_source}"
        raise TypeError(message)
    return actions


def _repository_reviewed_semantic_dependencies(
    project_root: Path, *, baseline_version: str = DEFAULT_BASELINE_VERSION
) -> dict[str, _ReviewedSemanticDependency]:
    """Join ledger decisions to reviewed exact wire closures or explicit absence."""
    _validate_baseline_version(baseline_version)
    ledger = load_version_profile_ledger(
        project_root / "tools" / "ds_codegen" / "version_profile_decisions.json"
    )
    raw_contracts = ledger.get("operation_contracts")
    if not isinstance(raw_contracts, dict):
        message = "ledger.operation_contracts must be an object"
        raise TypeError(message)
    baseline_bindings = runtime_operation_bindings(baseline_version)
    compiled = compile_version_profile_data(
        stable_actions=_load_stable_actions(
            project_root / "src" / "dsctl" / "cli_surface.py"
        ),
        ledger=ledger,
    )
    profile = compiled["profiles"][baseline_version]
    dependencies: dict[str, _ReviewedSemanticDependency] = {}
    for semantic_operation, seam in sorted(_REVIEWED_SEMANTIC_SEAMS.items()):
        raw_contract = raw_contracts.get(semantic_operation)
        if not isinstance(raw_contract, dict):
            message = (
                "reviewed semantic dependency has no ledger contract: "
                f"{semantic_operation}"
            )
            raise TypeError(message)
        stable_action = raw_contract.get("stable_action")
        if not isinstance(stable_action, str) or not stable_action:
            message = (
                "reviewed semantic dependency has invalid stable_action: "
                f"{semantic_operation}"
            )
            raise TypeError(message)
        binding = baseline_bindings.get(semantic_operation)
        unavailable = _reviewed_unavailable_decision(
            profile, semantic_operation=semantic_operation, stable_action=stable_action
        )
        if binding is None and unavailable is None:
            message = (
                f"DS {baseline_version} has no reviewed binding for "
                f"{semantic_operation}"
            )
            raise ValueError(message)
        if binding is not None and unavailable is not None:
            message = (
                f"DS {baseline_version} unavailable {semantic_operation} "
                "retains a binding"
            )
            raise ValueError(message)
        if stable_action in dependencies:
            message = (
                f"stable action {stable_action!r} owns multiple reviewed "
                "semantic dependencies"
            )
            raise ValueError(message)
        dependencies[stable_action] = _ReviewedSemanticDependency(
            semantic_operation=semantic_operation,
            source_operations=binding.source_operations if binding is not None else (),
            seam=seam,
            baseline_version=baseline_version,
            unavailable=unavailable,
        )
    return dependencies


def _validate_baseline_version(version: str) -> None:
    if version not in REVIEWED_DS_VERSIONS:
        message = f"Unknown exact dependency baseline {version!r}"
        raise ValueError(message)


def _require_manifest_version(path: Path, version: str) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for name in ("DS_VERSION", "SOURCE_TAG"):
        if _literal_assignment(tree, name, source=path) != version:
            message = f"Selected baseline {version} differs from manifest {name}"
            raise ValueError(message)


def _reviewed_unavailable_decision(
    profile: Mapping[str, Any],
    *,
    semantic_operation: str,
    stable_action: str,
) -> _ReviewedUnavailableDecision | None:
    """Retain unavailable evidence only after the complete ledger compiles."""
    capability = profile["actions"][stable_action]
    decision = profile["build_decisions"][semantic_operation]
    if capability["availability"] == "supported":
        return None
    reason = capability.get("reason")
    if reason == "upstream_runtime_limited":
        if (
            capability["availability"] != "limited"
            or decision["build_status"] != "blocked"
            or decision["status_reason"] != reason
            or not decision["source_operations"]
            or decision["stable_action"] != stable_action
        ):
            message = (
                f"Invalid wire-present runtime limitation for {semantic_operation}"
            )
            raise ValueError(message)
        return None
    if (
        capability["availability"] not in {"unsupported", "limited"}
        or reason not in {"upstream_capability_absent", "upstream_capability_limited"}
        or decision["build_status"] != "blocked"
        or decision["status_reason"] != reason
        or decision["source_operations"]
        or decision["stable_action"] != stable_action
    ):
        message = f"No reviewed unavailable decision for {semantic_operation}"
        raise ValueError(message)
    return _ReviewedUnavailableDecision(
        availability=capability["availability"],
        reason=reason,
        constraint=capability["constraint"],
        evidence_sources=tuple(capability["evidence_sources"]),
    )


def analyze_stable_action_dependencies(
    *,
    sources: StableActionDependencySources,
    stable_actions: Set[str],
    local_actions: Set[str],
    diagnostic_actions: Set[str],
    reviewed_semantic_dependencies: Mapping[str, _ReviewedSemanticDependency]
    | None = None,
) -> JsonObject:
    """Build a fail-closed action-to-generated-operation evidence report."""
    _validate_baseline_version(sources.baseline_version)
    overlap = sorted(local_actions & diagnostic_actions)
    if overlap:
        message = f"actions have conflicting classifications: {', '.join(overlap)}"
        raise ValueError(message)
    unexpected = sorted((local_actions | diagnostic_actions) - stable_actions)
    if unexpected:
        message = f"classified actions are not stable: {', '.join(unexpected)}"
        raise ValueError(message)
    semantic_dependencies = dict(reviewed_semantic_dependencies or {})
    unexpected_semantic_actions = sorted(set(semantic_dependencies) - stable_actions)
    if unexpected_semantic_actions:
        message = (
            "reviewed semantic dependencies are not stable actions: "
            f"{', '.join(unexpected_semantic_actions)}"
        )
        raise ValueError(message)

    for dependency in semantic_dependencies.values():
        if dependency.baseline_version != sources.baseline_version:
            message = "reviewed semantic dependency differs from selected baseline"
            raise ValueError(message)
        if dependency.unavailable is not None and dependency.source_operations:
            message = "reviewed unavailable dependency cannot retain wire operations"
            raise ValueError(message)

    index = _build_source_index(sources.source_root)
    protocol_groups, protocol_methods = _protocol_inventory(
        sources.protocols_root,
    )
    class_ports = _class_port_inventory(index, protocol_groups)
    callers, caller_diagnostics = _discover_command_callers(
        sources.commands_root,
        stable_actions=stable_actions,
    )
    generated_operations_by_source = _generated_package_operations_by_source(sources)
    generated_operations_by_source = _merge_generated_operation_indexes(
        generated_operations_by_source,
        _compiled_wire_operations_by_source(
            sources.compiled_wire_artifact_root,
            ds_version=sources.baseline_version,
        ),
    )
    actions: list[JsonObject] = []
    for action in sorted(stable_actions):
        classification = _classification(
            action,
            local_actions=local_actions,
            diagnostic_actions=diagnostic_actions,
        )
        service_entries = sorted(
            {
                _canonical_function_key(entry, index=index) or entry
                for entry in callers.get(action, set())
            }
        )
        diagnostics = list(caller_diagnostics.get(action, ()))
        if not service_entries:
            diagnostics.append(
                _diagnostic(
                    "command-caller-missing",
                    (
                        f"stable action {action!r} has no statically reachable "
                        "service entry"
                    ),
                )
            )

        reachable = _reachable_dependencies(
            service_entries,
            index=index,
            protocol_groups=protocol_groups,
            class_ports=class_ports,
        )
        diagnostics.extend(reachable.diagnostics)
        upstream_calls: list[JsonObject] = []
        wire_dependencies: list[JsonObject] = []
        generated_operations: set[_GeneratedOperation] = set()
        service_direct_transport_calls = set(reachable.direct_transport_calls)
        direct_transport_calls = set(service_direct_transport_calls)
        semantic_dependency = semantic_dependencies.get(action)
        semantic_evidence: JsonObject | None = None
        semantic_seam_confirmed = False
        if semantic_dependency is not None:
            seam_diagnostics = _reviewed_semantic_seam_diagnostics(
                semantic_dependency,
                service_entries=service_entries,
                reachable=reachable,
                index=index,
            )
            diagnostics.extend(seam_diagnostics)
            if not seam_diagnostics:
                semantic_seam_confirmed = True
                operations, operation_diagnostics = (
                    _reviewed_semantic_generated_operations(
                        semantic_dependency,
                        generated_operations_by_source=generated_operations_by_source,
                    )
                )
                diagnostics.extend(operation_diagnostics)
                generated_operations.update(operations)
                if semantic_dependency.unavailable is None:
                    direct_transport_calls.update(
                        semantic_dependency.seam.direct_transport_calls
                    )
                rendered_operations = _render_generated_operations(operations)
                semantic_evidence = _render_reviewed_semantic_dependency(
                    semantic_dependency
                )
                upstream_calls.append(
                    {
                        "group": semantic_dependency.seam.group,
                        "method": semantic_dependency.seam.method,
                        "protocol": semantic_dependency.seam.protocol,
                        "semantic_operation": (semantic_dependency.semantic_operation),
                        "evidence": (
                            "reviewed-unavailable-decision"
                            if semantic_dependency.unavailable is not None
                            else "reviewed-semantic-closure"
                        ),
                    }
                )
                wire_dependencies.append(
                    {
                        "group": semantic_dependency.seam.group,
                        "method": semantic_dependency.seam.method,
                        "protocol": semantic_dependency.seam.protocol,
                        "semantic_operation": (semantic_dependency.semantic_operation),
                        "operation_relationship": (
                            "reviewed-unavailable"
                            if semantic_dependency.unavailable is not None
                            else "single"
                            if len(semantic_dependency.source_operations) == 1
                            and not semantic_dependency.seam.direct_transport_calls
                            else "reviewed-composition"
                        ),
                        "generated_operations": rendered_operations,
                        "direct_transport_calls": (
                            []
                            if semantic_dependency.unavailable is not None
                            else list(semantic_dependency.seam.direct_transport_calls)
                        ),
                    }
                )

        if not semantic_seam_confirmed:
            for port in sorted(
                reachable.ports,
                key=lambda item: (item.group, item.method),
            ):
                protocol = _protocol_for_call(
                    port,
                    protocol_groups=protocol_groups,
                    protocol_methods=protocol_methods,
                )
                upstream_calls.append(
                    {
                        "group": port.group,
                        "method": port.method,
                        "protocol": protocol,
                    }
                )
                if protocol is None:
                    diagnostics.append(
                        _diagnostic(
                            "upstream-protocol-method-missing",
                            (
                                f"{port.group}.{port.method} is not declared by a "
                                "reachable upstream protocol"
                            ),
                        )
                    )
                else:
                    diagnostics.append(
                        _diagnostic(
                            "reviewed-semantic-dependency-missing",
                            (
                                f"{port.group}.{port.method} is not backed by a "
                                "confirmed reviewed semantic dependency"
                            ),
                        )
                    )
                wire_dependencies.append(
                    {
                        "group": port.group,
                        "method": port.method,
                        "protocol": protocol,
                        "operation_relationship": "unresolved",
                        "generated_operations": [],
                        "direct_transport_calls": [],
                    }
                )

        if (
            classification == "remote"
            and not reachable.ports
            and not reachable.direct_transport_calls
            and not semantic_seam_confirmed
        ):
            diagnostics.append(
                _diagnostic(
                    "upstream-call-missing",
                    f"remote action {action!r} has no statically reachable DS call",
                )
            )
        if classification == "local" and (reachable.ports or direct_transport_calls):
            diagnostics.append(
                _diagnostic(
                    "local-action-reaches-remote",
                    f"local action {action!r} reaches a DolphinScheduler call",
                )
            )
        action_report: JsonObject = {
            "action": action,
            "classification": classification,
            "service_entries": service_entries,
            "upstream_calls": upstream_calls,
            "dependency_relationship": _dependency_relationship(
                wire_dependencies,
                service_direct_transport_count=len(service_direct_transport_calls),
            ),
            "wire_dependencies": wire_dependencies,
            "generated_operations": _render_generated_operations(generated_operations),
            "service_direct_transport_calls": sorted(service_direct_transport_calls),
            "direct_transport_calls": sorted(direct_transport_calls),
            "complete": not diagnostics,
            "diagnostics": _dedupe_diagnostics(diagnostics),
        }
        if semantic_evidence is not None:
            action_report["reviewed_semantic_dependency"] = semantic_evidence
        actions.append(action_report)

    incomplete = sum(not bool(item["complete"]) for item in actions)
    classifications = {
        label: sum(item["classification"] == label for item in actions)
        for label in ("local", "diagnostic", "remote")
    }
    return {
        "schema_version": 1,
        "kind": "dolphinscheduler-stable-action-dependencies",
        "claim": (
            "static-seam-plus-reviewed-semantic-closure-evidence"
            if semantic_dependencies
            else "static-reachability-evidence-only"
        ),
        "baseline": {
            "ds_version": sources.baseline_version,
            "generated_package": "ds_" + sources.baseline_version.replace(".", "_"),
            **(
                {"compiled_wire_artifact": (sources.compiled_wire_artifact_root.name)}
                if sources.compiled_wire_artifact_root is not None
                else {}
            ),
        },
        "complete": incomplete == 0,
        "summary": {
            "action_count": len(actions),
            "complete_action_count": len(actions) - incomplete,
            "incomplete_action_count": incomplete,
            "classifications": classifications,
        },
        "actions": actions,
    }


def render_stable_action_dependency_report(report: Mapping[str, object]) -> str:
    """Serialize one dependency report deterministically."""
    return json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True) + "\n"


def _reviewed_semantic_seam_diagnostics(
    dependency: _ReviewedSemanticDependency,
    *,
    service_entries: Sequence[str],
    reachable: _Reachability,
    index: Mapping[str, _ModuleInfo],
) -> list[JsonObject]:
    """Prove the service reaches the declared deep seam before trusting closure."""
    seam = dependency.seam
    if reachable.diagnostics:
        return [
            _diagnostic(
                "reviewed-semantic-seam-unproven",
                (
                    f"{dependency.semantic_operation} cannot use its reviewed "
                    "wire closure while service reachability is incomplete"
                ),
            )
        ]
    if seam.kind == "definition_reads":
        return _reviewed_direct_port_diagnostics(
            dependency,
            reachable=reachable,
        )
    if seam.kind == "direct_port":
        return _reviewed_direct_port_diagnostics(
            dependency,
            reachable=reachable,
        )
    if seam.kind == "bound_domain":
        if reachable.ports or reachable.direct_transport_calls:
            return [
                _diagnostic(
                    "reviewed-semantic-seam-mismatch",
                    (
                        f"{dependency.semantic_operation} reaches calls outside "
                        f"its reviewed {seam.protocol} seam"
                    ),
                )
            ]
        if service_entries and all(
            _service_entry_uses_bound_domain_seam(
                entry,
                seam=seam,
                index=index,
            )
            for entry in service_entries
        ):
            return []
        return [
            _diagnostic(
                "reviewed-semantic-seam-mismatch",
                (
                    f"{dependency.semantic_operation} is not connected through "
                    f"the reviewed {seam.protocol} service seam"
                ),
            )
        ]
    if seam.kind == "task_definitions":
        expected_ports = set(seam.required_port_calls)
        if reachable.ports != expected_ports or reachable.direct_transport_calls:
            actual = (
                ", ".join(
                    f"{item.group}.{item.method}"
                    for item in sorted(
                        reachable.ports,
                        key=lambda item: (item.group, item.method),
                    )
                )
                or "none"
            )
            expected_text = ", ".join(
                f"{item.group}.{item.method}"
                for item in sorted(
                    expected_ports,
                    key=lambda item: (item.group, item.method),
                )
            )
            return [
                _diagnostic(
                    "reviewed-semantic-seam-mismatch",
                    (
                        f"{dependency.semantic_operation} must reach the exact "
                        f"reviewed {seam.protocol} internal ports {expected_text}; "
                        f"found {actual}"
                    ),
                )
            ]
        if service_entries and all(
            _service_entry_uses_task_definition_seam(
                entry,
                seam=seam,
                index=index,
            )
            for entry in service_entries
        ):
            return []
        return [
            _diagnostic(
                "reviewed-semantic-seam-mismatch",
                (
                    f"{dependency.semantic_operation} is not connected through "
                    f"the reviewed {seam.protocol} service seam"
                ),
            )
        ]
    assert_never(seam.kind)


def _reviewed_direct_port_diagnostics(
    dependency: _ReviewedSemanticDependency,
    *,
    reachable: _Reachability,
) -> list[JsonObject]:
    seam = dependency.seam
    expected = {_PortCall(seam.group, seam.method)}
    if reachable.ports == expected and reachable.direct_transport_calls == set(
        seam.direct_transport_calls
    ):
        return []
    actual = (
        ", ".join(
            f"{item.group}.{item.method}"
            for item in sorted(
                reachable.ports,
                key=lambda item: (item.group, item.method),
            )
        )
        or "none"
    )
    return [
        _diagnostic(
            "reviewed-semantic-seam-mismatch",
            (
                f"{dependency.semantic_operation} must reach only "
                f"{seam.group}.{seam.method}; found {actual}"
            ),
        )
    ]


def _service_entry_uses_bound_domain_seam(
    entry: str,
    *,
    seam: _ReviewedSemanticSeam,
    index: Mapping[str, _ModuleInfo],
    visited: frozenset[str] = frozenset(),
) -> bool:
    if entry in visited:
        return False
    module_name, separator, symbol = entry.rpartition(".")
    if not separator:
        return False
    module = index.get(module_name)
    if module is None:
        return False
    function = module.functions.get(symbol)
    if function is None:
        return False
    next_visited = visited | {entry}
    service_targets: set[str] = set()
    for node in _walk_scope(function.node):
        if not isinstance(node, ast.Call):
            continue
        target = _function_reference(node.func, module=module, index=index)
        if target is None:
            continue
        if target.key not in {
            "dsctl.services.runtime.run_with_bound_domain_service_runtime",
            "dsctl.services.runtime.run_with_bound_domain_selection",
        }:
            if target.module.startswith("dsctl.services"):
                service_targets.add(target.key)
            continue
        domain = _call_argument(node, position=1, keyword="domain")
        callback = _call_argument(node, position=2, keyword="operation")
        if domain is None or callback is None:
            continue
        if not _is_imported_symbol(
            domain,
            module=module,
            imported_module=seam.imported_module,
            imported_name=seam.imported_name,
        ):
            continue
        callback_ref = _function_reference(callback, module=module, index=index)
        if callback_ref is not None and _callback_uses_bound_domain(
            callback_ref,
            index=index,
        ):
            return True
    return any(
        _service_entry_uses_bound_domain_seam(
            target,
            seam=seam,
            index=index,
            visited=next_visited,
        )
        for target in sorted(service_targets)
    )


def _call_argument(
    call: ast.Call,
    *,
    position: int,
    keyword: str,
) -> ast.AST | None:
    if len(call.args) > position:
        return call.args[position]
    return next(
        (item.value for item in call.keywords if item.arg == keyword),
        None,
    )


def _is_imported_symbol(
    node: ast.AST,
    *,
    module: _ModuleInfo,
    imported_module: str | None,
    imported_name: str | None,
) -> bool:
    if imported_module is None or imported_name is None:
        return False
    if isinstance(node, ast.Name):
        imported = module.imports.get(node.id)
        return imported == _ImportRef(imported_module, imported_name)
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        imported = module.imports.get(node.value.id)
        return (
            imported == _ImportRef(imported_module, None) and node.attr == imported_name
        )
    return False


def _callback_uses_bound_domain(
    function: _FunctionRef,
    *,
    index: Mapping[str, _ModuleInfo],
    visited: frozenset[str] = frozenset(),
) -> bool:
    if function.key in visited:
        return False
    arguments = (
        *function.node.args.posonlyargs,
        *function.node.args.args,
    )
    if not arguments:
        return False
    runtime_name = arguments[0].arg
    if any(
        isinstance(node, ast.Attribute)
        and node.attr == "domain"
        and isinstance(node.value, ast.Name)
        and node.value.id == runtime_name
        for node in _walk_scope(function.node)
    ):
        return True
    module = index.get(function.module)
    if module is None:
        return False
    next_visited = visited | {function.key}
    for node in _walk_scope(function.node):
        if not isinstance(node, ast.Call):
            continue
        target = _function_reference(node.func, module=module, index=index)
        if target is None or not target.module.startswith("dsctl.services"):
            continue
        forwarded_runtime = _call_argument(
            node,
            position=0,
            keyword="runtime",
        )
        if not (
            isinstance(forwarded_runtime, ast.Name)
            and forwarded_runtime.id == runtime_name
        ):
            continue
        if _callback_uses_bound_domain(
            target,
            index=index,
            visited=next_visited,
        ):
            return True
    return False


def _service_entry_uses_task_definition_seam(
    entry: str,
    *,
    seam: _ReviewedSemanticSeam,
    index: Mapping[str, _ModuleInfo],
    visited: frozenset[str] = frozenset(),
) -> bool:
    """Prove one service entry opens the narrow task runtime and exact methods."""
    if entry in visited:
        return False
    module_name, separator, symbol = entry.rpartition(".")
    if not separator:
        return False
    module = index.get(module_name)
    if module is None:
        return False
    function = module.functions.get(symbol)
    if function is None:
        return False
    next_visited = visited | {entry}
    service_targets: set[str] = set()
    for node in _walk_scope(function.node):
        if not isinstance(node, ast.Call):
            continue
        target = _function_reference(node.func, module=module, index=index)
        if target is None:
            continue
        if (
            target.key
            != "dsctl.services.runtime.run_with_task_definition_service_runtime"
        ):
            if target.module.startswith("dsctl.services"):
                service_targets.add(target.key)
            continue
        callback = _call_argument(node, position=1, keyword="operation")
        if callback is None:
            continue
        callback_ref = _function_reference(callback, module=module, index=index)
        if callback_ref is None:
            continue
        actual_methods = _callback_task_definition_methods(
            callback_ref,
            index=index,
        )
        if actual_methods == set(seam.required_service_methods):
            return True
    return any(
        _service_entry_uses_task_definition_seam(
            target,
            seam=seam,
            index=index,
            visited=next_visited,
        )
        for target in sorted(service_targets)
    )


def _callback_task_definition_methods(
    function: _FunctionRef,
    *,
    index: Mapping[str, _ModuleInfo],
    visited: frozenset[str] = frozenset(),
) -> set[str]:
    """Collect TaskDefinitions methods reached from one runtime callback."""
    if function.key in visited:
        return set()
    arguments = (
        *function.node.args.posonlyargs,
        *function.node.args.args,
    )
    if not arguments:
        return set()
    runtime_name = arguments[0].arg
    module = index.get(function.module)
    if module is None:
        return set()
    methods: set[str] = set()
    next_visited = visited | {function.key}
    for node in _walk_scope(function.node):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            chain = _attribute_chain(node.func)
            if chain[:2] == (runtime_name, "definitions") and len(chain) == 3:
                methods.add(chain[-1])
                continue
        target = _function_reference(node.func, module=module, index=index)
        if target is None or not target.module.startswith("dsctl.services"):
            continue
        forwarded_runtime = _call_argument(
            node,
            position=0,
            keyword="runtime",
        )
        if not (
            isinstance(forwarded_runtime, ast.Name)
            and forwarded_runtime.id == runtime_name
        ):
            continue
        methods.update(
            _callback_task_definition_methods(
                target,
                index=index,
                visited=next_visited,
            )
        )
    return methods


def _reviewed_semantic_generated_operations(
    dependency: _ReviewedSemanticDependency,
    *,
    generated_operations_by_source: Mapping[str, tuple[_GeneratedOperation, ...]],
) -> tuple[set[_GeneratedOperation], list[JsonObject]]:
    operations: set[_GeneratedOperation] = set()
    diagnostics: list[JsonObject] = []
    for source_operation in dependency.source_operations:
        candidates = generated_operations_by_source.get(source_operation, ())
        if len(candidates) == 1:
            operations.add(candidates[0])
            continue
        if not candidates:
            diagnostics.append(
                _diagnostic(
                    "reviewed-semantic-operation-missing",
                    (
                        f"{dependency.semantic_operation} requires generated "
                        f"{dependency.baseline_version} operation {source_operation}"
                    ),
                )
            )
            continue
        diagnostics.append(
            _diagnostic(
                "reviewed-semantic-operation-ambiguous",
                (
                    f"{dependency.semantic_operation} resolves "
                    f"{source_operation} to multiple generated operations: "
                    + ", ".join(item.client_operation for item in candidates)
                ),
            )
        )
    return operations, diagnostics


def _render_generated_operations(
    operations: Iterable[_GeneratedOperation],
) -> list[JsonObject]:
    return [
        {
            "client_operation": item.client_operation,
            "source_operation": item.source_operation,
            "http_method": item.http_method,
            "path": item.path,
        }
        for item in sorted(
            operations,
            key=lambda value: (value.client_operation, value.source_operation),
        )
    ]


def _render_reviewed_semantic_dependency(
    dependency: _ReviewedSemanticDependency,
) -> JsonObject:
    seam: JsonObject = {
        "kind": dependency.seam.kind,
        "group": dependency.seam.group,
        "method": dependency.seam.method,
        "protocol": dependency.seam.protocol,
    }
    if dependency.seam.imported_module is not None:
        seam["imported_module"] = dependency.seam.imported_module
    if dependency.seam.imported_name is not None:
        seam["imported_name"] = dependency.seam.imported_name
    if dependency.seam.direct_transport_calls:
        seam["direct_transport_calls"] = list(dependency.seam.direct_transport_calls)
    if dependency.seam.required_service_methods:
        seam["required_service_methods"] = list(
            dependency.seam.required_service_methods
        )
    if dependency.seam.required_port_calls:
        seam["required_port_calls"] = [
            f"{item.group}.{item.method}"
            for item in dependency.seam.required_port_calls
        ]
    return {
        "semantic_operation": dependency.semantic_operation,
        "baseline_version": dependency.baseline_version,
        "source": (
            "operation-contract-ledger-and-reviewed-unavailable-decision"
            if dependency.unavailable is not None
            else "operation-contract-ledger-and-reviewed-runtime-binding"
        ),
        **(
            {
                "unavailable": {
                    "availability": dependency.unavailable.availability,
                    "reason": dependency.unavailable.reason,
                    "constraint": dependency.unavailable.constraint,
                    "evidence_sources": list(dependency.unavailable.evidence_sources),
                }
            }
            if dependency.unavailable is not None
            else {}
        ),
        "source_operations": list(dependency.source_operations),
        "service_seam": seam,
    }


def _dependency_relationship(
    wire_dependencies: Sequence[JsonObject],
    *,
    service_direct_transport_count: int,
) -> str:
    if not wire_dependencies:
        return "direct-transport" if service_direct_transport_count else "none"
    if service_direct_transport_count:
        return "alternatives-or-composition-unresolved"
    relationships = {str(item["operation_relationship"]) for item in wire_dependencies}
    if len(wire_dependencies) == 1 and relationships <= {
        "single",
        "direct-transport",
        "non-wire",
        "reviewed-composition",
        "reviewed-unavailable",
    }:
        return next(iter(relationships))
    return "alternatives-or-composition-unresolved"


def _classification(
    action: str,
    *,
    local_actions: Set[str],
    diagnostic_actions: Set[str],
) -> ActionClassification:
    if action in local_actions:
        return "local"
    if action in diagnostic_actions:
        return "diagnostic"
    return "remote"


def _build_source_index(source_root: Path) -> dict[str, _ModuleInfo]:
    modules: dict[str, _ModuleInfo] = {}
    for path in sorted(source_root.rglob("*.py")):
        if "generated" in path.relative_to(source_root).parts:
            continue
        module = _module_name(path, source_root=source_root)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = _imports(tree)
        functions: dict[str, _FunctionRef] = {}
        classes: dict[str, ast.ClassDef] = {}
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                functions[node.name] = _FunctionRef(
                    module=module,
                    qualname=node.name,
                    node=node,
                    class_name=None,
                )
            elif isinstance(node, ast.ClassDef):
                classes[node.name] = node
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        qualname = f"{node.name}.{child.name}"
                        functions[qualname] = _FunctionRef(
                            module=module,
                            qualname=qualname,
                            node=child,
                            class_name=node.name,
                        )
        modules[module] = _ModuleInfo(
            name=module,
            path=path,
            tree=tree,
            imports=imports,
            functions=functions,
            classes=classes,
        )
    return modules


def _module_name(path: Path, *, source_root: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imports(tree: ast.Module) -> dict[str, _ImportRef]:
    imports: dict[str, _ImportRef] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            for imported in node.names:
                imports[imported.asname or imported.name] = _ImportRef(
                    module=node.module,
                    name=imported.name,
                )
        elif isinstance(node, ast.Import):
            for imported in node.names:
                local = imported.asname or imported.name.split(".", 1)[0]
                imports[local] = _ImportRef(module=imported.name, name=None)
    return imports


def _protocol_inventory(
    protocols_root: Path,
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    session_path = protocols_root / "session.py"
    session_tree = ast.parse(
        session_path.read_text(encoding="utf-8"),
        filename=str(session_path),
    )
    protocol_groups: dict[str, set[str]] = {}
    for class_node in session_tree.body:
        if not isinstance(class_node, ast.ClassDef):
            continue
        for method in class_node.body:
            if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not any(
                _decorator_name(item) == "property" for item in method.decorator_list
            ):
                continue
            for type_name in _annotation_names(method.returns):
                protocol_groups.setdefault(type_name, set()).add(method.name)

    classes: dict[str, ast.ClassDef] = {}
    for path in sorted(protocols_root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                classes[node.name] = node
    protocol_methods: dict[str, set[str]] = {}

    def collect(name: str, pending: set[str]) -> set[str]:
        cached = protocol_methods.get(name)
        if cached is not None:
            return cached
        if name in pending:
            return set()
        node = classes.get(name)
        if node is None:
            return set()
        methods = {
            child.name
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            and _decorator_name(child) != "property"
        }
        for base in node.bases:
            for base_name in _annotation_names(base):
                methods.update(collect(base_name, {*pending, name}))
        protocol_methods[name] = methods
        return methods

    for class_name in classes:
        collect(class_name, set())
    return protocol_groups, protocol_methods


def _decorator_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return None
    return None


def _annotation_names(annotation: ast.AST | None) -> set[str]:
    if annotation is None:
        return set()
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        annotation_text = annotation.value
        try:
            annotation = ast.parse(annotation_text, mode="eval").body
        except SyntaxError:
            return {annotation_text.rsplit(".", 1)[-1]}
    return {node.id for node in ast.walk(annotation) if isinstance(node, ast.Name)} | {
        node.attr for node in ast.walk(annotation) if isinstance(node, ast.Attribute)
    }


def _class_port_inventory(
    index: Mapping[str, _ModuleInfo],
    protocol_groups: Mapping[str, set[str]],
) -> dict[tuple[str, str], str]:
    class_ports: dict[tuple[str, str], str] = {}
    for module in index.values():
        for class_name, class_node in module.classes.items():
            class_key = f"{module.name}.{class_name}"
            for child in class_node.body:
                if not isinstance(child, ast.AnnAssign):
                    continue
                if not isinstance(child.target, ast.Name):
                    continue
                groups = {
                    group
                    for type_name in _annotation_names(child.annotation)
                    for group in protocol_groups.get(type_name, set())
                }
                if len(groups) == 1:
                    class_ports[(class_key, child.target.id)] = groups.pop()
    task_definitions = index.get("dsctl.upstream.task_definitions")
    runtime = index.get("dsctl.services.runtime")
    if task_definitions is not None and runtime is not None:
        for node in ast.walk(runtime.tree):
            if (
                not isinstance(node, ast.Call)
                or _call_name(node.func) != "TaskDefinitions"
            ):
                continue
            for keyword in node.keywords:
                chain = _attribute_chain(keyword.value)
                if keyword.arg is None or len(chain) < 2:
                    continue
                if chain[-2:] == ("upstream", "task_definitions"):
                    class_ports[
                        (
                            "dsctl.upstream.task_definitions.TaskDefinitions",
                            keyword.arg,
                        )
                    ] = "task_definitions"
    return class_ports


def _discover_command_callers(
    commands_root: Path,
    *,
    stable_actions: Set[str],
) -> tuple[dict[str, set[str]], dict[str, list[JsonObject]]]:
    callers: dict[str, set[str]] = {}
    diagnostics: dict[str, list[JsonObject]] = {}
    for path in sorted(commands_root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = _imports(tree)
        module_functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for function in module_functions.values():
            nested = {
                node.name: node
                for node in function.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            for node in _walk_scope(function):
                if not isinstance(node, ast.Call):
                    continue
                if _call_name(node.func) not in {"emit_result", "emit_raw_result"}:
                    continue
                if len(node.args) < 2:
                    continue
                action = _command_action(node.args[0], tree)
                if action is None:
                    continue
                if action not in stable_actions:
                    continue
                entries = _callback_service_entries(
                    node.args[1],
                    imports=imports,
                    local_functions={**module_functions, **nested},
                )
                callers.setdefault(action, set()).update(entries)
    for action in stable_actions:
        if action not in callers or callers[action]:
            continue
        diagnostics.setdefault(action, []).append(
            _diagnostic(
                "service-entry-unresolved",
                f"stable action {action!r} has no resolved service callback",
            )
        )
    return callers, diagnostics


def _command_action(expression: ast.AST, tree: ast.Module) -> str | None:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if (
        isinstance(expression, ast.Attribute)
        and expression.attr == "action"
        and isinstance(expression.value, ast.Name)
    ):
        for node in tree.body:
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            if not isinstance(node.targets[0], ast.Name):
                continue
            if node.targets[0].id != expression.value.id:
                continue
            if not isinstance(node.value, ast.Call) or not node.value.args:
                continue
            candidate = node.value.args[0]
            if isinstance(candidate, ast.Constant) and isinstance(candidate.value, str):
                return candidate.value
    return None


def _callback_service_entries(
    callback: ast.AST,
    *,
    imports: Mapping[str, _ImportRef],
    local_functions: Mapping[str, ast.FunctionDef | ast.AsyncFunctionDef],
) -> set[str]:
    entries: set[str] = set()
    pending = [callback]
    visited_functions: set[str] = set()
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Name):
            imported = imports.get(node.id)
            if imported is not None and imported.module.startswith("dsctl.services"):
                if imported.name is not None:
                    entries.add(f"{imported.module}.{imported.name}")
                continue
            local = local_functions.get(node.id)
            if local is not None and node.id not in visited_functions:
                visited_functions.add(node.id)
                pending.append(local)
                continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            function = child.func
            if isinstance(function, ast.Name):
                imported = imports.get(function.id)
                if (
                    imported is not None
                    and imported.module.startswith("dsctl.services")
                    and imported.name is not None
                ):
                    entries.add(f"{imported.module}.{imported.name}")
                    continue
                local = local_functions.get(function.id)
                if local is not None and function.id not in visited_functions:
                    visited_functions.add(function.id)
                    pending.append(local)
    return entries


def _canonical_function_key(
    key: str,
    *,
    index: Mapping[str, _ModuleInfo],
    visited: frozenset[str] = frozenset(),
) -> str | None:
    if key in visited:
        return None
    module_name, _, symbol = key.rpartition(".")
    module = index.get(module_name)
    if module is None:
        return None
    function = module.functions.get(symbol)
    if function is not None:
        return function.key
    imported = module.imports.get(symbol)
    if imported is None or imported.name is None:
        return None
    return _canonical_function_key(
        f"{imported.module}.{imported.name}",
        index=index,
        visited=visited | {key},
    )


def _reachable_dependencies(
    service_entries: Sequence[str],
    *,
    index: Mapping[str, _ModuleInfo],
    protocol_groups: Mapping[str, set[str]],
    class_ports: Mapping[tuple[str, str], str],
) -> _Reachability:
    result = _Reachability(ports=set(), direct_transport_calls=set(), diagnostics=[])
    function_index = {
        function.key: function
        for module in index.values()
        for function in module.functions.values()
    }
    pending: list[_AnalysisState] = []
    for entry in service_entries:
        if entry not in function_index:
            result.diagnostics.append(
                _diagnostic(
                    "service-entry-missing",
                    f"service entry {entry!r} does not exist in the source index",
                )
            )
            continue
        pending.append(_AnalysisState(entry))
    visited: set[_AnalysisState] = set()
    while pending:
        state = pending.pop()
        if state in visited:
            continue
        visited.add(state)
        function = function_index.get(state.function_key)
        if function is None:
            continue
        module = index[function.module]
        env = _function_environment(
            function,
            bindings=dict(state.bindings),
            protocol_groups=protocol_groups,
            class_ports=class_ports,
        )
        nodes = _walk_scope(function.node)
        _update_environment_from_bindings(
            nodes,
            env=env,
            module=module,
            index=index,
            class_ports=class_ports,
        )
        for node in nodes:
            if not isinstance(node, ast.Call):
                continue
            receiver = (
                _resolve_symbolic(
                    node.func.value,
                    env=env,
                    module=module,
                    index=index,
                    class_ports=class_ports,
                )
                if isinstance(node.func, ast.Attribute)
                else None
            )
            method_name = (
                node.func.attr if isinstance(node.func, ast.Attribute) else None
            )
            if receiver is not None and method_name is not None:
                if receiver.kind == "port" and receiver.name is not None:
                    result.ports.add(_PortCall(receiver.name, method_name))
                    continue
                if receiver.kind == "transport":
                    result.direct_transport_calls.add(
                        f"DolphinSchedulerClient.{method_name}"
                    )
                    continue
            target = _call_target(
                node,
                receiver=receiver,
                module=module,
                index=index,
            )
            if target is not None:
                pending.append(
                    _state_for_call(
                        node,
                        target=target,
                        receiver=receiver,
                        env=env,
                        module=module,
                        index=index,
                        class_ports=class_ports,
                    )
                )
            for argument in (*node.args, *(item.value for item in node.keywords)):
                reference = _function_reference(
                    argument,
                    module=module,
                    index=index,
                )
                if reference is not None:
                    pending.append(_AnalysisState(reference.key))
    return result


def _function_environment(
    function: _FunctionRef,
    *,
    bindings: Mapping[str, _Symbolic],
    protocol_groups: Mapping[str, set[str]],
    class_ports: Mapping[tuple[str, str], str],
) -> dict[str, _Symbolic]:
    env = dict(bindings)
    arguments = (
        *function.node.args.posonlyargs,
        *function.node.args.args,
        *function.node.args.kwonlyargs,
    )
    for argument in arguments:
        if argument.arg in env:
            continue
        names = _annotation_names(argument.annotation)
        runtime = next(
            (name for name in names if name.endswith("ServiceRuntime")),
            None,
        )
        if runtime is not None:
            env[argument.arg] = _Symbolic("runtime", runtime)
            continue
        groups = {
            group
            for type_name in names
            for group in protocol_groups.get(type_name, set())
        }
        if len(groups) == 1:
            env[argument.arg] = _Symbolic("port", groups.pop())
    if function.class_name is not None:
        class_key = f"{function.module}.{function.class_name}"
        env.setdefault("self", _Symbolic("object", class_key))
        for (candidate_class, attribute), group in class_ports.items():
            if candidate_class == class_key:
                env[f"self.{attribute}"] = _Symbolic("port", group)
    return env


def _update_environment_from_bindings(
    nodes: Iterable[ast.AST],
    *,
    env: dict[str, _Symbolic],
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
    class_ports: Mapping[tuple[str, str], str],
) -> None:
    for node in sorted(nodes, key=lambda item: getattr(item, "lineno", -1)):
        target: ast.Name | None = None
        value: ast.AST | None = None
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            target = node.targets[0]
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target = node.target
            value = node.value
        if target is not None and value is not None:
            symbolic = _resolve_symbolic(
                value,
                env=env,
                module=module,
                index=index,
                class_ports=class_ports,
            )
            if symbolic is not None:
                env[target.id] = symbolic
        if isinstance(node, (ast.With, ast.AsyncWith)):
            for item in node.items:
                if not isinstance(item.optional_vars, ast.Name):
                    continue
                if (
                    isinstance(item.context_expr, ast.Call)
                    and _call_name(item.context_expr.func) == "DolphinSchedulerClient"
                ):
                    env[item.optional_vars.id] = _Symbolic("transport")


def _resolve_symbolic(
    node: ast.AST,
    *,
    env: Mapping[str, _Symbolic],
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
    class_ports: Mapping[tuple[str, str], str],
) -> _Symbolic | None:
    if isinstance(node, ast.Name):
        symbolic = env.get(node.id)
        if symbolic is not None:
            return symbolic
        class_ref = _class_reference(node, module=module, index=index)
        return _Symbolic("class", class_ref) if class_ref is not None else None
    if isinstance(node, ast.Call):
        if _call_name(node.func) == "cast" and len(node.args) >= 2:
            return _resolve_symbolic(
                node.args[1],
                env=env,
                module=module,
                index=index,
                class_ports=class_ports,
            )
        receiver = (
            _resolve_symbolic(
                node.func.value,
                env=env,
                module=module,
                index=index,
                class_ports=class_ports,
            )
            if isinstance(node.func, ast.Attribute)
            else None
        )
        if (
            receiver is not None
            and receiver.kind == "identity-adapter"
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "bind_identity"
        ):
            return _Symbolic("port", "users")
        class_ref = _class_reference(node.func, module=module, index=index)
        if class_ref is not None:
            return _Symbolic("object", class_ref)
        if _call_name(node.func) == "get_identity_adapter":
            return _Symbolic("identity-adapter")
        return None
    if not isinstance(node, ast.Attribute):
        return None
    direct = env.get(".".join(_attribute_chain(node)))
    if direct is not None:
        return direct
    base = _resolve_symbolic(
        node.value,
        env=env,
        module=module,
        index=index,
        class_ports=class_ports,
    )
    if base is None:
        return None
    if base.kind == "runtime":
        if node.attr == "upstream":
            return _Symbolic("upstream")
        if node.attr == "http_client":
            return _Symbolic("transport")
        if base.name == "ProjectServiceRuntime" and node.attr == "projects":
            return _Symbolic("port", "projects")
        if base.name == "TaskDefinitionServiceRuntime" and node.attr == "definitions":
            return _Symbolic(
                "object",
                "dsctl.upstream.task_definitions.TaskDefinitions",
            )
    if base.kind == "upstream":
        return _Symbolic("port", node.attr)
    if base.kind == "object" and base.name is not None:
        group = class_ports.get((base.name, node.attr))
        if group is not None:
            return _Symbolic("port", group)
    return None


def _call_target(
    call: ast.Call,
    *,
    receiver: _Symbolic | None,
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
) -> _FunctionRef | None:
    if isinstance(call.func, ast.Name):
        return _function_reference(call.func, module=module, index=index)
    if not isinstance(call.func, ast.Attribute):
        return None
    if receiver is not None and receiver.kind == "object" and receiver.name is not None:
        return _class_method(receiver.name, call.func.attr, index=index)
    return _function_reference(call.func, module=module, index=index)


def _state_for_call(
    call: ast.Call,
    *,
    target: _FunctionRef,
    receiver: _Symbolic | None,
    env: Mapping[str, _Symbolic],
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
    class_ports: Mapping[tuple[str, str], str],
) -> _AnalysisState:
    arguments = (
        *target.node.args.posonlyargs,
        *target.node.args.args,
        *target.node.args.kwonlyargs,
    )
    bindings: dict[str, _Symbolic] = {}
    positional = list(call.args)
    if target.class_name is not None and arguments and arguments[0].arg == "self":
        if receiver is not None:
            bindings["self"] = receiver
        arguments = arguments[1:]
    for argument, value in zip(arguments, positional, strict=False):
        symbolic = _resolve_symbolic(
            value,
            env=env,
            module=module,
            index=index,
            class_ports=class_ports,
        )
        bindings[argument.arg] = symbolic or _Symbolic("unknown")
    by_name = {argument.arg: argument for argument in arguments}
    for keyword in call.keywords:
        if keyword.arg not in by_name:
            continue
        symbolic = _resolve_symbolic(
            keyword.value,
            env=env,
            module=module,
            index=index,
            class_ports=class_ports,
        )
        bindings[keyword.arg] = symbolic or _Symbolic("unknown")
    return _AnalysisState(target.key, tuple(sorted(bindings.items())))


def _function_reference(
    node: ast.AST,
    *,
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
) -> _FunctionRef | None:
    if isinstance(node, ast.Name):
        local = module.functions.get(node.id)
        if local is not None:
            return local
        imported = module.imports.get(node.id)
        if imported is None or imported.name is None:
            return None
        imported_module = index.get(imported.module)
        return (
            None
            if imported_module is None
            else imported_module.functions.get(imported.name)
        )
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        imported = module.imports.get(node.value.id)
        if imported is None or imported.name is not None:
            return None
        imported_module = index.get(imported.module)
        return (
            None
            if imported_module is None
            else imported_module.functions.get(node.attr)
        )
    return None


def _class_reference(
    node: ast.AST,
    *,
    module: _ModuleInfo,
    index: Mapping[str, _ModuleInfo],
) -> str | None:
    if isinstance(node, ast.Name):
        if node.id in module.classes:
            return f"{module.name}.{node.id}"
        imported = module.imports.get(node.id)
        if imported is None or imported.name is None:
            return None
        imported_module = index.get(imported.module)
        if imported_module is not None and imported.name in imported_module.classes:
            return f"{imported.module}.{imported.name}"
    return None


def _class_method(
    class_key: str,
    method_name: str,
    *,
    index: Mapping[str, _ModuleInfo],
) -> _FunctionRef | None:
    module_name, separator, class_name = class_key.rpartition(".")
    if not separator:
        return None
    module = index.get(module_name)
    if module is None:
        return None
    return module.functions.get(f"{class_name}.{method_name}")


def _protocol_for_call(
    port: _PortCall,
    *,
    protocol_groups: Mapping[str, set[str]],
    protocol_methods: Mapping[str, set[str]],
) -> str | None:
    if port == _PortCall("users", "current"):
        return "CurrentUserOperations"
    candidates = sorted(
        protocol
        for protocol, groups in protocol_groups.items()
        if port.group in groups and port.method in protocol_methods.get(protocol, set())
    )
    if candidates:
        return candidates[0]
    if port.group == "task_definitions" and port.method in {
        "get",
        "prepare_update",
        "apply_update",
    }:
        return "TaskDefinitionWire"
    return None


def _generated_client_bindings(
    generated_client_source: Path,
) -> dict[str, tuple[str, str]]:
    tree = ast.parse(
        generated_client_source.read_text(encoding="utf-8"),
        filename=str(generated_client_source),
    )
    imports = _imports(tree)
    client_classes = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name.startswith("DS")
        and node.name.endswith("Client")
    ]
    if len(client_classes) != 1:
        message = "generated client source must contain one exact client class"
        raise ValueError(message)
    initializers = [
        node
        for node in client_classes[0].body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    ]
    if len(initializers) != 1:
        message = "generated exact client must contain one constructor"
        raise ValueError(message)
    direct_assignments = {
        id(node)
        for node in initializers[0].body
        if _generated_client_assignment(node) is not None
    }
    misplaced = [
        node
        for node in ast.walk(tree)
        if _generated_client_assignment(node) is not None
        and id(node) not in direct_assignments
    ]
    if misplaced:
        message = (
            "generated client binding appears outside the exact client constructor"
        )
        raise ValueError(message)
    bindings: dict[str, tuple[str, str]] = {}
    for node in initializers[0].body:
        assignment = _generated_client_assignment(node)
        if assignment is None:
            continue
        attribute_name, operation_class = assignment
        imported = imports.get(operation_class or "")
        if operation_class is None or imported is None:
            continue
        if attribute_name in bindings:
            message = f"generated exact client repeats binding {attribute_name!r}"
            raise ValueError(message)
        bindings[attribute_name] = (operation_class, imported.module)
    return bindings


def _generated_package_operations_by_source(
    sources: StableActionDependencySources,
) -> dict[str, tuple[_GeneratedOperation, ...]]:
    """Index a generated package unless its exact profile proves an empty slice."""
    if (
        sources.generated_profile_manifest is not None
        and _runtime_slice_operation_count(sources.generated_profile_manifest) == 0
    ):
        return {}
    generated_bindings = _generated_client_bindings(sources.generated_client_source)
    return _generated_operations_by_source(
        generated_bindings,
        generated_operations_root=sources.generated_operations_root,
    )


def _runtime_slice_operation_count(profile_manifest: Path) -> int | None:
    tree = ast.parse(
        profile_manifest.read_text(encoding="utf-8"),
        filename=str(profile_manifest),
    )
    selection = _literal_assignment(tree, "SELECTION", source=profile_manifest)
    if selection != "runtime-slice":
        return None
    operation_count = _literal_assignment(
        tree,
        "OPERATION_COUNT",
        source=profile_manifest,
    )
    if (
        not isinstance(operation_count, int)
        or isinstance(operation_count, bool)
        or operation_count < 0
    ):
        message = "generated runtime profile operation count must be non-negative"
        raise TypeError(message)
    return operation_count


def _generated_client_assignment(
    node: ast.AST,
) -> tuple[str, str | None] | None:
    if (
        not isinstance(node, ast.Assign)
        or len(node.targets) != 1
        or not isinstance(node.targets[0], ast.Attribute)
        or not isinstance(node.targets[0].value, ast.Name)
        or node.targets[0].value.id != "self"
        or not isinstance(node.value, ast.Call)
    ):
        return None
    return node.targets[0].attr, _call_name(node.value.func)


def _generated_operations_by_source(
    generated_bindings: Mapping[str, tuple[str, str]],
    *,
    generated_operations_root: Path,
) -> dict[str, tuple[_GeneratedOperation, ...]]:
    """Index invocable generated methods by exact upstream source operation id."""
    operations_by_declared_source: dict[str, set[_GeneratedOperation]] = {}
    for client_group, (operations_class, module_name) in sorted(
        generated_bindings.items()
    ):
        operation_source = (
            generated_operations_root / f"{module_name.rsplit('.', 1)[-1]}.py"
        )
        if not operation_source.is_file():
            continue
        class_node = _generated_operations_class(
            operation_source,
            operations_class=operations_class,
        )
        if class_node is None:
            continue
        for method in class_node.body:
            if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            documentation = ast.get_docstring(method, clean=False) or ""
            match = _GENERATED_OPERATION.search(documentation)
            if match is None:
                continue
            operation = _GeneratedOperation(
                client_operation=f"{client_group}.{method.name}",
                source_operation=_generated_source_operation(documentation, match),
                http_method=match.group("method"),
                path=match.group("path").strip("/"),
            )
            operations_by_declared_source.setdefault(
                operation.source_operation,
                set(),
            ).add(operation)
    return {
        source_operation: tuple(
            sorted(
                operations,
                key=lambda item: (
                    item.client_operation,
                    item.http_method,
                    item.path,
                ),
            )
        )
        for source_operation, operations in sorted(
            operations_by_declared_source.items()
        )
    }


def _compiled_wire_operations_by_source(
    artifact_root: Path | None,
    *,
    ds_version: str,
) -> dict[str, tuple[_GeneratedOperation, ...]]:
    """Index every manifest-owned data-only module without reading adapters."""
    if artifact_root is None:
        return {}
    manifest_source = artifact_root / "_manifest.py"
    if not manifest_source.is_file():
        message = f"compiled wire artifact manifest is missing: {manifest_source}"
        raise ValueError(message)
    manifest_tree = ast.parse(
        manifest_source.read_text(encoding="utf-8"),
        filename=str(manifest_source),
    )
    modules = _literal_module_inventory(
        manifest_tree,
        "MODULES",
        source=manifest_source,
    )
    domain_modules = _literal_module_inventory(
        manifest_tree,
        "DOMAIN_MODULES",
        source=manifest_source,
    )
    support_modules = _literal_module_inventory(
        manifest_tree,
        "SUPPORT_MODULES",
        source=manifest_source,
        allow_empty=True,
    )
    if tuple(sorted(("__init__.py", *domain_modules, *support_modules))) != modules:
        message = "compiled wire domain/support module inventory is inconsistent"
        raise ValueError(message)

    indexes = []
    for raw_module in domain_modules:
        path = PurePosixPath(raw_module)
        module_name = ".".join(path.with_suffix("").parts)
        indexes.append(
            _compiled_wire_module_operations_by_source(
                artifact_root.joinpath(*path.parts),
                module_name=module_name,
                ds_version=ds_version,
            )
        )
    return _merge_generated_operation_indexes(*indexes)


def _compiled_wire_module_operations_by_source(
    artifact_source: Path,
    *,
    module_name: str,
    ds_version: str,
) -> dict[str, tuple[_GeneratedOperation, ...]]:
    """Index one manifest-owned compiled module from literal profile data."""
    if not artifact_source.is_file():
        message = f"compiled wire artifact module is missing: {artifact_source}"
        raise ValueError(message)
    tree = ast.parse(
        artifact_source.read_text(encoding="utf-8"),
        filename=str(artifact_source),
    )
    profiles = _literal_dict_assignment(tree, "PROFILES", source=artifact_source)
    codecs = _literal_dict_assignment(tree, "CODECS", source=artifact_source)
    profile = profiles.get(ds_version)
    if isinstance(profile, dict) and profile.get("status") == "upstream_absent":
        source = profile.get("source")
        if (
            profile.get("programs") != {}
            or profile.get("recipe_id") is not None
            or not isinstance(source, dict)
            or source.get("tag") != ds_version
        ):
            message = f"compiled wire DS {ds_version} absence profile is inconsistent"
            raise ValueError(message)
        return {}
    if not isinstance(profile, dict) or profile.get("status") != "supported":
        message = f"compiled wire artifact has no supported DS {ds_version} profile"
        raise ValueError(message)
    programs = profile.get("programs")
    if not isinstance(programs, dict) or not programs:
        message = f"compiled wire DS {ds_version} profile has no programs"
        raise ValueError(message)

    indexed: dict[str, set[_GeneratedOperation]] = {}
    for primitive, raw_program in sorted(programs.items()):
        if not isinstance(primitive, str) or not isinstance(raw_program, dict):
            message = "compiled wire program inventory is invalid"
            raise TypeError(message)
        source_operation = raw_program.get("source_operation")
        codec_name = raw_program.get("codec")
        if not isinstance(source_operation, str) or not isinstance(codec_name, str):
            message = f"compiled wire {primitive} program identity is invalid"
            raise TypeError(message)
        codec = codecs.get(codec_name)
        if not isinstance(codec, dict):
            message = f"compiled wire {primitive} program identity is invalid"
            raise TypeError(message)
        http_method = codec.get("method")
        path = codec.get("path")
        if not isinstance(http_method, str) or not isinstance(path, str):
            message = f"compiled wire {codec_name} transport is invalid"
            raise TypeError(message)
        indexed.setdefault(source_operation, set()).add(
            _GeneratedOperation(
                client_operation=f"compiled-wire.{module_name}.{primitive}",
                source_operation=source_operation,
                http_method=http_method,
                path=path.strip("/"),
            )
        )
    return {
        source_operation: tuple(
            sorted(operations, key=lambda item: item.client_operation)
        )
        for source_operation, operations in sorted(indexed.items())
    }


def _literal_assignment(
    tree: ast.Module,
    name: str,
    *,
    source: Path,
) -> object:
    matches = [
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == name
    ]
    if len(matches) != 1:
        message = f"compiled wire artifact must assign {name} once: {source}"
        raise ValueError(message)
    return ast.literal_eval(matches[0])


def _literal_dict_assignment(
    tree: ast.Module,
    name: str,
    *,
    source: Path,
) -> dict[str, object]:
    value = (
        read_compiled_literals(source, {name})[name]
        if name == "CODECS"
        else _literal_assignment(tree, name, source=source)
    )
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        message = f"compiled wire artifact {name} must be a string-keyed object"
        raise ValueError(message)
    return value


def _literal_module_inventory(
    tree: ast.Module,
    name: str,
    *,
    source: Path,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    value = _literal_assignment(tree, name, source=source)
    if not isinstance(value, tuple) or (not value and not allow_empty):
        message = f"compiled wire artifact {name} must be a tuple"
        raise ValueError(message)
    modules: list[str] = []
    for raw_module in value:
        if not isinstance(raw_module, str):
            message = "compiled wire artifact module path must be text"
            raise TypeError(message)
        path = PurePosixPath(raw_module)
        if (
            path.is_absolute()
            or path.as_posix() != raw_module
            or ".." in path.parts
            or path.suffix != ".py"
            or raw_module == "_manifest.py"
        ):
            message = f"compiled wire artifact module path is unsafe: {raw_module}"
            raise ValueError(message)
        modules.append(raw_module)
    if modules != sorted(set(modules)):
        message = f"compiled wire artifact {name} must be sorted and unique"
        raise ValueError(message)
    return tuple(modules)


def _merge_generated_operation_indexes(
    *indexes: Mapping[str, tuple[_GeneratedOperation, ...]],
) -> dict[str, tuple[_GeneratedOperation, ...]]:
    merged: dict[str, set[_GeneratedOperation]] = {}
    for index in indexes:
        for source_operation, operations in index.items():
            merged.setdefault(source_operation, set()).update(operations)
    return {
        source_operation: tuple(
            sorted(
                operations,
                key=lambda item: (
                    item.client_operation,
                    item.http_method,
                    item.path,
                ),
            )
        )
        for source_operation, operations in sorted(merged.items())
    }


def _generated_operations_class(
    operation_source: Path,
    *,
    operations_class: str,
) -> ast.ClassDef | None:
    """Read the exact controller class; wire families are private method seams."""
    tree = ast.parse(
        operation_source.read_text(encoding="utf-8"),
        filename=str(operation_source),
    )
    return next(
        (
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == operations_class
        ),
        None,
    )


def _generated_source_operation(
    documentation: str,
    operation_match: re.Match[str],
) -> str:
    """Prefer the exact generated operation id over its Java method name."""
    operation_id_match = _GENERATED_OPERATION_ID.search(documentation)
    if operation_id_match is not None:
        return operation_id_match.group("source")
    return operation_match.group("source").strip()


def _attribute_chain(node: ast.AST) -> tuple[str, ...]:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return tuple(reversed(parts))


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _call_name(node.value)
    return None


def _walk_scope(
    function: ast.FunctionDef | ast.AsyncFunctionDef,
) -> list[ast.AST]:
    nodes: list[ast.AST] = []
    pending: list[ast.AST] = list(reversed(function.body))
    while pending:
        node = pending.pop()
        nodes.append(node)
        children = list(ast.iter_child_nodes(node))
        pending.extend(
            reversed(
                [
                    child
                    for child in children
                    if not isinstance(
                        child,
                        (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
                    )
                ]
            )
        )
    return nodes


def _diagnostic(code: str, message: str) -> JsonObject:
    return {"code": code, "message": message}


def _dedupe_diagnostics(diagnostics: Iterable[JsonObject]) -> list[JsonObject]:
    by_key = {
        (str(item.get("code")), str(item.get("message"))): item for item in diagnostics
    }
    return [by_key[key] for key in sorted(by_key)]


__all__ = [
    "DIAGNOSTIC_ACTIONS",
    "LOCAL_ACTIONS",
    "StableActionDependencySources",
    "analyze_repository_stable_action_dependencies",
    "analyze_stable_action_dependencies",
    "render_stable_action_dependency_report",
]
