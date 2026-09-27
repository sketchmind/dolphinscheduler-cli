from __future__ import annotations

import importlib
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from dsctl.cli_surface import stable_leaf_actions


def _module(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


@pytest.fixture(scope="module")
def real_stable_action_report_payload() -> str:
    """Return one immutable snapshot of the full-repository dependency report."""
    analyzer = _module("ds_codegen.stable_action_dependencies")
    project_root = Path(__file__).resolve().parents[2]
    report = analyzer.analyze_repository_stable_action_dependencies(project_root)
    return json.dumps(report, sort_keys=True)


def _decode_report(payload: str) -> dict[str, Any]:
    report = json.loads(payload)
    assert isinstance(report, dict)
    return cast("dict[str, Any]", report)


def _action(report: dict[str, object], action: str) -> dict[str, Any]:
    actions = report["actions"]
    assert isinstance(actions, list)
    matches = [item for item in actions if item["action"] == action]
    assert len(matches) == 1
    return cast("dict[str, Any]", matches[0])


def _generated_names(action: dict[str, Any]) -> set[str]:
    operations = action["generated_operations"]
    assert isinstance(operations, list)
    return {item["client_operation"] for item in operations}


def _port_names(action: dict[str, Any]) -> set[str]:
    calls = action["upstream_calls"]
    assert isinstance(calls, list)
    return {f"{item['group']}.{item['method']}" for item in calls}


def test_real_stable_action_report_covers_surface_and_keeps_exact_evidence(
    real_stable_action_report_payload: str,
) -> None:
    report = _decode_report(real_stable_action_report_payload)

    assert report["schema_version"] == 1
    assert report["kind"] == "dolphinscheduler-stable-action-dependencies"
    assert report["claim"] == ("static-seam-plus-reviewed-semantic-closure-evidence")
    assert report["baseline"] == {
        "ds_version": "3.4.1",
        "generated_package": "ds_3_4_1",
        "compiled_wire_artifact": "wire_programs",
    }
    actions = report["actions"]
    assert isinstance(actions, list)
    assert {item["action"] for item in actions} == stable_leaf_actions()
    assert report["summary"]["action_count"] == len(stable_leaf_actions())
    assert report["summary"]["complete_action_count"] == len(stable_leaf_actions())
    assert report["summary"]["incomplete_action_count"] == 0
    assert report["complete"] is True

    version = _action(report, "version")
    assert version["classification"] == "local"
    assert version["upstream_calls"] == []
    assert version["generated_operations"] == []

    health = _action(report, "monitor.health")
    assert health["classification"] == "diagnostic"
    assert health["generated_operations"] == []
    assert health["direct_transport_calls"] == ["DolphinSchedulerClient.healthcheck"]

    project_get = _action(report, "project.get")
    assert project_get["classification"] == "remote"
    assert project_get["service_entries"] == [
        "dsctl.services.project.get_project_result"
    ]
    assert _port_names(project_get) == {"definitions.get_project"}
    assert project_get["dependency_relationship"] == "reviewed-composition"
    assert project_get["reviewed_semantic_dependency"] == {
        "semantic_operation": "project.get",
        "baseline_version": "3.4.1",
        "source": "operation-contract-ledger-and-reviewed-runtime-binding",
        "source_operations": [
            "ProjectController.queryProjectListPaging",
            "ProjectController.queryProjectByCode",
        ],
        "service_seam": {
            "kind": "definition_reads",
            "group": "definitions",
            "method": "get_project",
            "protocol": "DefinitionReads",
        },
    }
    assert {
        "compiled-wire.project.get",
        "compiled-wire.project.page",
    }.issubset(_generated_names(project_get))
    assert {
        item["source_operation"] for item in project_get["generated_operations"]
    } >= {
        "ProjectController.queryProjectByCode",
        "ProjectController.queryProjectListPaging",
    }

    task_update = _action(report, "task.update")
    assert _port_names(task_update) == {"task-definitions.update"}
    task_seam = task_update["reviewed_semantic_dependency"]["service_seam"]
    assert task_seam == {
        "kind": "task_definitions",
        "group": "task-definitions",
        "method": "update",
        "protocol": "TaskDefinitions",
        "required_service_methods": ["apply", "prepare_update"],
        "required_port_calls": [
            "definitions.resolve_workflow",
            "definitions.workflow_refs",
            "task_definitions.apply_update",
            "task_definitions.describe",
            "task_definitions.get",
            "task_definitions.prepare_update",
        ],
    }
    assert {
        "compiled-wire.workflow_runtime.task_get",
        "compiled-wire.workflow_runtime.task_update",
    }.issubset(_generated_names(task_update))

    resource_view = _action(report, "resource.view")
    assert resource_view["direct_transport_calls"] == [
        "DolphinSchedulerClient.get_binary"
    ]
    assert {
        item["source_operation"] for item in resource_view["generated_operations"]
    } == {
        "ResourcesController.downloadResource",
        "ResourcesController.viewResource",
    }
    assert resource_view["complete"] is True

    cluster_get = _action(report, "cluster.get")
    assert _generated_names(cluster_get) == {
        "compiled-wire.cluster.get",
        "compiled-wire.cluster.page",
    }
    assert all(action["diagnostics"] == [] for action in actions)


@pytest.mark.parametrize(
    "action_name",
    ["workflow.create", "workflow.edit", "workflow-instance.edit"],
)
def test_workflow_authoring_dependencies_include_datasource_name_and_id_reads(
    real_stable_action_report_payload: str,
    action_name: str,
) -> None:
    report = _decode_report(real_stable_action_report_payload)
    action = _action(report, action_name)
    source_operations = {
        item["source_operation"] for item in action["generated_operations"]
    }

    assert {
        "DataSourceController.queryDataSourceListPaging",
        "DataSourceController.queryDataSource",
    }.issubset(source_operations)
    assert action["complete"] is True


def test_real_report_does_not_read_retired_adapter_or_empty_client_shell(
    monkeypatch: Any,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    project_root = Path(__file__).resolve().parents[2]
    retired_sources = {
        (project_root / "src/dsctl/upstream/adapters/ds_3_4_1.py").resolve(),
        (project_root / "src/dsctl/generated/versions/ds_3_4_1/client.py").resolve(),
    }
    original_read_text = Path.read_text

    def guarded_read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        if path.resolve() in retired_sources:
            message = "stable-action analysis read a retired runtime facade"
            raise AssertionError(message)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)

    report = analyzer.analyze_repository_stable_action_dependencies(project_root)

    assert report["complete"] is True


def test_runtime_profile_skips_only_a_proven_empty_generated_facade(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    manifest = tmp_path / "runtime-profile.py"
    missing_client = tmp_path / "retired-client.py"
    manifest.write_text(
        'SELECTION = "runtime-slice"\nOPERATION_COUNT = 0\n',
        encoding="utf-8",
    )
    runtime_sources = replace(
        sources,
        generated_client_source=missing_client,
        generated_profile_manifest=manifest,
    )

    assert analyzer._generated_package_operations_by_source(runtime_sources) == {}

    manifest.write_text(
        'SELECTION = "runtime-slice"\nOPERATION_COUNT = 1\n',
        encoding="utf-8",
    )
    with pytest.raises(FileNotFoundError):
        analyzer._generated_package_operations_by_source(runtime_sources)


def test_runtime_slice_single_overloads_keep_exact_wire_evidence(
    real_stable_action_report_payload: str,
) -> None:
    report = _decode_report(real_stable_action_report_payload)

    expected_sources = {
        "alert-plugin.get": (
            "AlertPluginInstanceController.getAlertPluginInstance"
            "__get_alert_plugin_instances_list"
        ),
        "task-instance.log": "LoggerController.queryLog__get_log_detail",
    }
    for action_name, expected_source in expected_sources.items():
        action = _action(report, action_name)
        assert action["complete"] is True
        assert action["diagnostics"] == []
        assert expected_source in {
            item["source_operation"] for item in action["generated_operations"]
        }


@pytest.mark.parametrize("drift", ["missing", "ambiguous"])
def test_task_dependency_requires_unique_reviewed_compiled_ownership(
    drift: str,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    project_root = Path(__file__).resolve().parents[2]
    sources = analyzer.StableActionDependencySources.from_repository(project_root)
    indexed = analyzer._compiled_wire_operations_by_source(
        sources.compiled_wire_artifact_root, ds_version="3.4.1"
    )
    dependency = analyzer._repository_reviewed_semantic_dependencies(project_root)[
        "task.update"
    ]
    operations, diagnostics = analyzer._reviewed_semantic_generated_operations(
        dependency, generated_operations_by_source=indexed
    )
    assert diagnostics == []
    assert {
        "compiled-wire.workflow_runtime.task_get",
        "compiled-wire.workflow_runtime.task_update",
    }.issubset({operation.client_operation for operation in operations})

    source = "TaskDefinitionController.updateTaskWithUpstream"
    if drift == "missing":
        del indexed[source]
    else:
        operation = indexed[source][0]
        indexed[source] = (
            operation,
            replace(operation, client_operation="compiled-wire.candidate.task_update"),
        )
    _operations, diagnostics = analyzer._reviewed_semantic_generated_operations(
        dependency, generated_operations_by_source=indexed
    )
    assert [item["code"] for item in diagnostics] == [
        f"reviewed-semantic-operation-{drift}"
    ]


def test_real_reviewed_semantic_dependencies_require_the_expected_deep_seams(
    real_stable_action_report_payload: str,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    report = _decode_report(real_stable_action_report_payload)

    definition_reads = {
        "project.list": ("project.page", "definitions", "list_projects"),
        "project.get": ("project.get", "definitions", "get_project"),
        "workflow.list": ("workflow.page", "definitions", "list_workflows"),
        "workflow.get": ("workflow.get", "definitions", "get_workflow"),
    }
    for action_name, (semantic_operation, group, method) in definition_reads.items():
        action = _action(report, action_name)
        dependency = action["reviewed_semantic_dependency"]
        assert dependency["semantic_operation"] == semantic_operation
        assert action["complete"] is True
        assert _port_names(action) == {f"{group}.{method}"}
        assert action["wire_dependencies"][0]["semantic_operation"] == (
            semantic_operation
        )

    task_definitions = {
        "task.list": (
            "task.list",
            "list",
            ["list"],
            [
                "definitions.resolve_workflow",
                "task_definitions.describe",
            ],
        ),
        "task.get": (
            "task.get",
            "get",
            ["get"],
            [
                "definitions.resolve_workflow",
                "task_definitions.describe",
                "task_definitions.get",
            ],
        ),
        "task.update": (
            "task.update",
            "update",
            ["apply", "prepare_update"],
            [
                "definitions.resolve_workflow",
                "definitions.workflow_refs",
                "task_definitions.apply_update",
                "task_definitions.describe",
                "task_definitions.get",
                "task_definitions.prepare_update",
            ],
        ),
    }
    for action_name, (
        semantic_operation,
        service_method,
        required_service_methods,
        required_port_calls,
    ) in task_definitions.items():
        action = _action(report, action_name)
        dependency = action["reviewed_semantic_dependency"]
        assert dependency["semantic_operation"] == semantic_operation
        assert dependency["service_seam"] == {
            "kind": "task_definitions",
            "group": "task-definitions",
            "method": service_method,
            "protocol": "TaskDefinitions",
            "required_service_methods": required_service_methods,
            "required_port_calls": required_port_calls,
        }
        assert action["complete"] is True
        assert _port_names(action) == {f"task-definitions.{service_method}"}
        assert {
            item["source_operation"] for item in action["generated_operations"]
        } == set(dependency["source_operations"])

    bound_domains = {
        "project": (
            "ProjectDomain",
            "dsctl.upstream.projects",
            "PROJECT_DOMAIN",
            {
                "project.create": "project.create",
                "project.delete": "project.delete",
                "project.update": "project.update",
            },
        ),
        "access-token": (
            "AccessTokenDomain",
            "dsctl.upstream.access_tokens",
            "ACCESS_TOKEN_DOMAIN",
            {
                "access-token.create": "access-token.create",
                "access-token.delete": "access-token.delete",
                "access-token.generate": "access-token.generate",
                "access-token.get": "access-token.get",
                "access-token.list": "access-token.list",
                "access-token.update": "access-token.update",
            },
        ),
        "alert-group": (
            "AlertGroupDomain",
            "dsctl.upstream.alert_groups",
            "ALERT_GROUP_DOMAIN",
            {
                "alert-group.create": "alert-group.create",
                "alert-group.delete": "alert-group.delete",
                "alert-group.get": "alert-group.get",
                "alert-group.list": "alert-group.page",
                "alert-group.update": "alert-group.update",
            },
        ),
        "alert-plugin": (
            "AlertPluginDomain",
            "dsctl.upstream.alert_plugins",
            "ALERT_PLUGIN_DOMAIN",
            {
                "alert-plugin.create": "alert-plugin.create",
                "alert-plugin.definition.list": "alert-plugin.definition.list",
                "alert-plugin.delete": "alert-plugin.delete",
                "alert-plugin.get": "alert-plugin.get",
                "alert-plugin.list": "alert-plugin.page",
                "alert-plugin.schema": "alert-plugin.schema",
                "alert-plugin.test": "alert-plugin.test",
                "alert-plugin.update": "alert-plugin.update",
            },
        ),
        "audit": (
            "AuditDomain",
            "dsctl.upstream.observability",
            "AUDIT_DOMAIN",
            {
                "audit.list": "audit.list",
                "audit.model-types": "audit.model-types",
                "audit.operation-types": "audit.operation-types",
            },
        ),
        "cluster": (
            "ClusterDomain",
            "dsctl.upstream.clusters",
            "CLUSTER_DOMAIN",
            {
                "cluster.create": "cluster.create",
                "cluster.delete": "cluster.delete",
                "cluster.get": "cluster.get",
                "cluster.list": "cluster.page",
                "cluster.update": "cluster.update",
            },
        ),
        "datasource": (
            "DataSourceDomain",
            "dsctl.upstream.datasources",
            "DATASOURCE_DOMAIN",
            {
                "datasource.create": "datasource.create",
                "datasource.delete": "datasource.delete",
                "datasource.get": "datasource.get",
                "datasource.list": "datasource.page",
                "datasource.test": "datasource.saved-test",
                "datasource.update": "datasource.update",
            },
        ),
        "environment": (
            "EnvironmentDomain",
            "dsctl.upstream.environments",
            "ENVIRONMENT_DOMAIN",
            {
                "environment.create": "environment.create",
                "environment.delete": "environment.delete",
                "environment.get": "environment.get",
                "environment.list": "environment.page",
                "environment.update": "environment.update",
            },
        ),
        "namespace": (
            "NamespaceDomain",
            "dsctl.upstream.namespaces",
            "NAMESPACE_DOMAIN",
            {
                "namespace.available": "namespace.available",
                "namespace.create": "namespace.create",
                "namespace.delete": "namespace.delete",
                "namespace.get": "namespace.get",
                "namespace.list": "namespace.page",
            },
        ),
        "queue": (
            "QueueDomain",
            "dsctl.upstream.queues",
            "QUEUE_DOMAIN",
            {
                "queue.create": "queue.create",
                "queue.delete": "queue.delete",
                "queue.get": "queue.get",
                "queue.list": "queue.page",
                "queue.update": "queue.update",
            },
        ),
        "resource": (
            "ResourceDomain",
            "dsctl.upstream.resources",
            "RESOURCE_DOMAIN",
            {
                "resource.create": "resource.create",
                "resource.delete": "resource.delete",
                "resource.download": "resource.download",
                "resource.list": "resource.page",
                "resource.mkdir": "resource.mkdir",
                "resource.upload": "resource.upload",
                "resource.view": "resource.view",
            },
        ),
        "project-parameter": (
            "ProjectParameterDomain",
            "dsctl.upstream.project_parameters",
            "PROJECT_PARAMETER_DOMAIN",
            {
                "project-parameter.create": "project-parameter.create",
                "project-parameter.delete": "project-parameter.delete",
                "project-parameter.get": "project-parameter.get",
                "project-parameter.list": "project-parameter.page",
                "project-parameter.update": "project-parameter.update",
            },
        ),
        "project-preference": (
            "ProjectPreferenceDomain",
            "dsctl.upstream.project_preferences",
            "PROJECT_PREFERENCE_DOMAIN",
            {
                "project-preference.disable": "project-preference.disable",
                "project-preference.enable": "project-preference.enable",
                "project-preference.get": "project-preference.get",
                "project-preference.update": "project-preference.update",
            },
        ),
        "project-worker-group": (
            "ProjectWorkerGroupDomain",
            "dsctl.upstream.project_worker_groups",
            "PROJECT_WORKER_GROUP_DOMAIN",
            {
                "project-worker-group.clear": "project-worker-group.clear",
                "project-worker-group.list": "project-worker-group.page",
                "project-worker-group.set": "project-worker-group.set",
            },
        ),
        "schedule": (
            "ScheduleDomain",
            "dsctl.upstream.schedules",
            "SCHEDULE_DOMAIN",
            {
                "schedule.create": "schedule.create",
                "schedule.delete": "schedule.delete",
                "schedule.explain": "schedule.explain",
                "schedule.get": "schedule.get",
                "schedule.offline": "schedule.offline",
                "schedule.online": "schedule.online",
                "schedule.list": "schedule.page",
                "schedule.preview": "schedule.preview",
                "schedule.update": "schedule.update",
            },
        ),
        "runtime-instance": (
            "RuntimeInstanceDomain",
            "dsctl.upstream.runtime_instances",
            "RUNTIME_INSTANCE_DOMAIN",
            {
                "task-instance.force-success": "task-instance.force-success",
                "task-instance.get": "task-instance.get",
                "task-instance.list": "task-instance.list",
                "task-instance.log": "task-instance.log",
                "task-instance.savepoint": "task-instance.savepoint",
                "task-instance.stop": "task-instance.stop",
                "task-instance.sub-workflow": "task-instance.sub-workflow",
                "task-instance.watch": "task-instance.watch",
                "workflow-instance.digest": "workflow-instance.digest",
                "workflow-instance.edit": "workflow-instance.edit",
                "workflow-instance.execute-task": "workflow-instance.execute-task",
                "workflow-instance.export": "workflow-instance.export",
                "workflow-instance.get": "workflow-instance.get",
                "workflow-instance.list": "workflow-instance.list",
                "workflow-instance.parent": "workflow-instance.parent",
                "workflow-instance.recover-failed": (
                    "workflow-instance.recover-failed"
                ),
                "workflow-instance.rerun": "workflow-instance.rerun",
                "workflow-instance.stop": "workflow-instance.stop",
                "workflow-instance.watch": "workflow-instance.watch",
            },
        ),
        "task-group": (
            "TaskGroupDomain",
            "dsctl.upstream.task_groups",
            "TASK_GROUP_DOMAIN",
            {
                "task-group.close": "task-group.close",
                "task-group.create": "task-group.create",
                "task-group.get": "task-group.get",
                "task-group.list": "task-group.page",
                "task-group.queue.force-start": "task-group.queue.force-start",
                "task-group.queue.list": "task-group.queue.page",
                "task-group.queue.set-priority": ("task-group.queue.set-priority"),
                "task-group.start": "task-group.start",
                "task-group.update": "task-group.update",
            },
        ),
        "task-type": (
            "TaskTypeDomain",
            "dsctl.upstream.task_type_inventory",
            "TASK_TYPE_DOMAIN",
            {"task-type.list": "task-type.list"},
        ),
        "tenant": (
            "TenantDomain",
            "dsctl.upstream.tenants",
            "TENANT_DOMAIN",
            {
                "tenant.create": "tenant.create",
                "tenant.delete": "tenant.delete",
                "tenant.get": "tenant.get",
                "tenant.list": "tenant.page",
                "tenant.update": "tenant.update",
            },
        ),
        "user": (
            "UserDomain",
            "dsctl.upstream.users",
            "USER_DOMAIN",
            {
                "user.create": "user.create",
                "user.delete": "user.delete",
                "user.get": "user.get",
                "user.grant.datasource": "user.grant.datasource",
                "user.grant.namespace": "user.grant.namespace",
                "user.grant.project": "user.grant.project",
                "user.list": "user.list",
                "user.revoke.datasource": "user.revoke.datasource",
                "user.revoke.namespace": "user.revoke.namespace",
                "user.revoke.project": "user.revoke.project",
                "user.update": "user.update",
            },
        ),
        "worker-group": (
            "WorkerGroupDomain",
            "dsctl.upstream.worker_groups",
            "WORKER_GROUP_DOMAIN",
            {
                "worker-group.create": "worker-group.create",
                "worker-group.delete": "worker-group.delete",
                "worker-group.get": "worker-group.get",
                "worker-group.list": "worker-group.page",
                "worker-group.update": "worker-group.update",
            },
        ),
        "workflow": (
            "WorkflowDomain",
            "dsctl.upstream.workflows",
            "WORKFLOW_DOMAIN",
            {
                "workflow.backfill": "workflow.backfill",
                "workflow.create": "workflow.create",
                "workflow.delete": "workflow.delete",
                "workflow.describe": "workflow.describe",
                "workflow.digest": "workflow.digest",
                "workflow.edit": "workflow.edit",
                "workflow.export": "workflow.export",
                "workflow.lineage.dependent-tasks": (
                    "workflow.lineage.dependent-tasks"
                ),
                "workflow.lineage.get": "workflow.lineage.get",
                "workflow.lineage.list": "workflow.lineage.list",
                "workflow.offline": "workflow.offline",
                "workflow.online": "workflow.online",
                "workflow.run": "workflow.run",
                "workflow.run-task": "workflow.run-task",
            },
        ),
    }
    direct_transport_by_operation = {
        "resource.download": ["DolphinSchedulerClient.get_binary"],
        "resource.upload": ["DolphinSchedulerClient.request_result"],
        "resource.view": ["DolphinSchedulerClient.get_binary"],
    }
    expected_bound_actions: set[str] = set()
    for group, (
        domain_type,
        imported_module,
        imported_name,
        operations,
    ) in bound_domains.items():
        expected_bound_actions.update(operations)
        for action_name, semantic_operation in operations.items():
            action = _action(report, action_name)
            dependency = action["reviewed_semantic_dependency"]
            direct_transport_calls = direct_transport_by_operation.get(
                semantic_operation,
                [],
            )
            assert dependency["semantic_operation"] == semantic_operation
            expected_seam: dict[str, object] = {
                "kind": "bound_domain",
                "group": group,
                "method": "bind",
                "protocol": f"BoundDomain[{domain_type}]",
                "imported_module": imported_module,
                "imported_name": imported_name,
            }
            if direct_transport_calls:
                expected_seam["direct_transport_calls"] = direct_transport_calls
            assert dependency["service_seam"] == expected_seam
            assert action["complete"] is True
            assert _port_names(action) == {f"{group}.bind"}
            assert {
                item["source_operation"] for item in action["generated_operations"]
            } == set(dependency["source_operations"])
            assert action["wire_dependencies"] == [
                {
                    "group": group,
                    "method": "bind",
                    "protocol": f"BoundDomain[{domain_type}]",
                    "semantic_operation": semantic_operation,
                    "operation_relationship": (
                        "single"
                        if len(dependency["source_operations"]) == 1
                        and not direct_transport_calls
                        else "reviewed-composition"
                    ),
                    "generated_operations": action["generated_operations"],
                    "direct_transport_calls": direct_transport_calls,
                }
            ]

    actions = report["actions"]
    assert isinstance(actions, list)
    actual_bound_actions = {
        action["action"]
        for action in actions
        if action.get("reviewed_semantic_dependency", {})
        .get("service_seam", {})
        .get("kind")
        == "bound_domain"
    }
    assert actual_bound_actions == expected_bound_actions
    actual_reviewed_actions = {
        action["action"]
        for action in actions
        if action.get("reviewed_semantic_dependency") is not None
    }
    assert actual_reviewed_actions == (
        stable_leaf_actions() - analyzer.LOCAL_ACTIONS - analyzer.DIAGNOSTIC_ACTIONS
    ) | {"doctor"}

    doctor = _action(report, "doctor")
    assert doctor["reviewed_semantic_dependency"] == {
        "semantic_operation": "identity.current",
        "baseline_version": "3.4.1",
        "source": "operation-contract-ledger-and-reviewed-runtime-binding",
        "source_operations": ["UsersController.getUserInfo"],
        "service_seam": {
            "kind": "direct_port",
            "group": "users",
            "method": "current",
            "protocol": "CurrentUserOperations",
            "direct_transport_calls": ["DolphinSchedulerClient.healthcheck"],
        },
    }


def test_real_resource_seam_accounts_for_json_multipart_and_binary_wires(
    real_stable_action_report_payload: str,
) -> None:
    report = _decode_report(real_stable_action_report_payload)
    expected = {
        "resource.create": ("resource.create", []),
        "resource.delete": ("resource.delete", []),
        "resource.download": (
            "resource.download",
            ["DolphinSchedulerClient.get_binary"],
        ),
        "resource.list": ("resource.page", []),
        "resource.mkdir": ("resource.mkdir", []),
        "resource.upload": (
            "resource.upload",
            ["DolphinSchedulerClient.request_result"],
        ),
        "resource.view": (
            "resource.view",
            ["DolphinSchedulerClient.get_binary"],
        ),
    }

    for action_name, (semantic_operation, direct_transport_calls) in expected.items():
        action = _action(report, action_name)
        dependency = action["reviewed_semantic_dependency"]
        seam = dependency["service_seam"]

        assert action["complete"] is True
        assert dependency["semantic_operation"] == semantic_operation
        assert seam["kind"] == "bound_domain"
        assert seam["imported_module"] == "dsctl.upstream.resources"
        assert seam["imported_name"] == "RESOURCE_DOMAIN"
        assert action["direct_transport_calls"] == direct_transport_calls
        assert action["wire_dependencies"][0]["direct_transport_calls"] == (
            direct_transport_calls
        )
        assert {
            item["source_operation"] for item in action["generated_operations"]
        } == set(dependency["source_operations"])


def test_reviewed_semantic_dependency_fails_closed_when_service_seam_drifts(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    dependency = analyzer._ReviewedSemanticDependency(
        semantic_operation="widget.read",
        source_operations=("WidgetController.queryWidget",),
        seam=analyzer._ReviewedSemanticSeam(
            kind="definition_reads",
            group="widgets",
            method="list",
            protocol="WidgetReads",
        ),
    )

    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )

    action = _action(report, "widget.get")
    assert action["complete"] is False
    assert action.get("reviewed_semantic_dependency") is None
    assert any(
        item["code"] == "reviewed-semantic-seam-mismatch"
        for item in action["diagnostics"]
    )


@pytest.mark.parametrize(
    "runner_name",
    ["run_with_bound_domain_service_runtime", "run_with_bound_domain_selection"],
)
def test_bound_domain_dependency_proves_transitive_helper_and_exact_domain(
    tmp_path: Path,
    runner_name: str,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    service_source = sources.source_root / "dsctl" / "services" / "widget.py"
    service_text = """
from dsctl.services.runtime import (
    BoundDomainServiceRuntime,
    run_with_bound_domain_service_runtime,
)
from dsctl.upstream.widgets import WIDGET_DOMAIN

def get_widget_result():
    return _run_widget()

def _run_widget():
    return run_with_bound_domain_service_runtime(None, WIDGET_DOMAIN, _get_widget)

def _get_widget(runtime: BoundDomainServiceRuntime):
    return runtime.domain.widgets.get(code=1)
"""
    service_text = service_text.replace(
        "run_with_bound_domain_service_runtime", runner_name
    )
    service_source.write_text(service_text, encoding="utf-8")
    runtime_source = sources.source_root / "dsctl" / "services" / "runtime.py"
    runtime_source.write_text(
        """
class BoundDomainServiceRuntime:
    domain: object

def run_with_bound_domain_service_runtime(env_file, domain, operation):
    return operation
""".replace("run_with_bound_domain_service_runtime", runner_name),
        encoding="utf-8",
    )
    dependency = analyzer._ReviewedSemanticDependency(
        semantic_operation="widget.read",
        source_operations=("WidgetController.queryWidget",),
        seam=analyzer._ReviewedSemanticSeam(
            kind="bound_domain",
            group="widget",
            method="bind",
            protocol="BoundDomain[WidgetDomain]",
            imported_module="dsctl.upstream.widgets",
            imported_name="WIDGET_DOMAIN",
        ),
    )

    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )

    action = _action(report, "widget.get")
    assert action["complete"] is True
    assert _port_names(action) == {"widget.bind"}

    stale_dependency = analyzer._ReviewedSemanticDependency(
        semantic_operation="widget.read",
        source_operations=("WidgetController.queryWidget__get_widgets_code",),
        seam=dependency.seam,
    )
    stale_report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": stale_dependency},
    )

    stale_action = _action(stale_report, "widget.get")
    assert stale_action["complete"] is False
    assert any(
        item["code"] == "reviewed-semantic-operation-missing"
        for item in stale_action["diagnostics"]
    )

    service_source.write_text(
        service_text.replace("WIDGET_DOMAIN", "OTHER_DOMAIN"),
        encoding="utf-8",
    )
    drifted_report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )

    drifted = _action(drifted_report, "widget.get")
    assert drifted["complete"] is False
    assert any(
        item["code"] == "reviewed-semantic-seam-mismatch"
        for item in drifted["diagnostics"]
    )


def test_task_definition_dependency_proves_runner_callback_and_method(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    service_source = sources.source_root / "dsctl" / "services" / "widget.py"
    service_source.write_text(
        """
from dsctl.services.runtime import run_with_task_definition_service_runtime

def get_widget_result():
    return run_with_task_definition_service_runtime(None, _get_widget)

def _get_widget(runtime):
    return runtime.definitions.get("widget")
""",
        encoding="utf-8",
    )
    runtime_source = sources.source_root / "dsctl" / "services" / "runtime.py"
    runtime_source.write_text(
        """
def run_with_task_definition_service_runtime(env_file, operation):
    return operation
""",
        encoding="utf-8",
    )
    dependency = analyzer._ReviewedSemanticDependency(
        semantic_operation="widget.read",
        source_operations=("WidgetController.queryWidget",),
        seam=analyzer._ReviewedSemanticSeam(
            kind="task_definitions",
            group="task-definitions",
            method="get",
            protocol="TaskDefinitions",
            required_service_methods=("get",),
        ),
    )

    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )

    action = _action(report, "widget.get")
    assert action["complete"] is True
    assert _port_names(action) == {"task-definitions.get"}

    service_source.write_text(
        service_source.read_text(encoding="utf-8").replace(
            "runtime.definitions.get",
            "runtime.definitions.list",
        ),
        encoding="utf-8",
    )
    drifted_report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )

    drifted = _action(drifted_report, "widget.get")
    assert drifted["complete"] is False
    assert any(
        item["code"] == "reviewed-semantic-seam-mismatch"
        for item in drifted["diagnostics"]
    )


def test_report_fails_closed_when_a_generated_operation_loses_metadata(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=False)

    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": _widget_dependency(analyzer)},
    )

    assert report["complete"] is False
    action = _action(report, "widget.get")
    assert action["classification"] == "remote"
    assert action["service_entries"] == ["dsctl.services.widget.get_widget_result"]
    assert _port_names(action) == {"widgets.get"}
    assert action["generated_operations"] == []
    assert any(
        item["code"] == "reviewed-semantic-operation-missing"
        for item in action["diagnostics"]
    )


def test_generated_operations_class_reads_the_exact_controller_directly(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    operation_source = tmp_path / "widget.py"
    operation_source.write_text(
        "from dsctl.generated.wire_runtime.api.operations._kernels "
        "import get_bare_to_json as _get_widget_exchange\n\n"
        "class WidgetOperations:\n"
        "    def get_widget(self):\n"
        '        """DS operation: WidgetController.get | GET /widgets"""\n'
        "        return _get_widget_exchange(self)\n",
        encoding="utf-8",
    )

    operation_class = analyzer._generated_operations_class(
        operation_source,
        operations_class="WidgetOperations",
    )

    assert operation_class is not None
    assert operation_class.name == "WidgetOperations"


def test_generated_operations_class_does_not_follow_a_kernel_import(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    operation_source = tmp_path / "widget.py"
    operation_source.write_text(
        "from dsctl.generated.wire_runtime.api.operations._kernels "
        "import get_bare_to_json as WidgetOperations\n",
        encoding="utf-8",
    )

    assert (
        analyzer._generated_operations_class(
            operation_source,
            operations_class="WidgetOperations",
        )
        is None
    )


def test_generated_client_binding_rejects_assignment_outside_constructor(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    client_source = tmp_path / "client.py"
    client_source.write_text(
        """
from .api.operations.widget import WidgetOperations

class DS341Client:
    def __init__(self):
        pass

    def unused(self):
        self.widget = WidgetOperations()
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="outside the exact client constructor"):
        analyzer._generated_client_bindings(client_source)


def test_report_rejects_unclassified_stable_actions(tmp_path: Path) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)

    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get", "widget.orphan"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
    )

    assert report["complete"] is False
    orphan = _action(report, "widget.orphan")
    assert orphan["classification"] == "remote"
    assert orphan["service_entries"] == []
    assert orphan["upstream_calls"] == []
    assert any(
        item["code"] == "command-caller-missing" for item in orphan["diagnostics"]
    )


def test_cli_writes_reproducible_json_report(tmp_path: Path, capsys: Any) -> None:
    cli = _module("analyze_ds_stable_action_dependencies")
    project_root = Path(__file__).resolve().parents[2]
    output = tmp_path / "reports" / "stable-actions.json"

    first_status = cli.main(
        [
            "--repo-root",
            str(project_root),
            "--output",
            str(output),
        ]
    )
    first = output.read_text(encoding="utf-8")
    second_status = cli.main(
        [
            "--repo-root",
            str(project_root),
            "--output",
            str(output),
        ]
    )

    assert first_status == 0
    assert second_status == 0
    assert output.read_text(encoding="utf-8") == first
    assert json.loads(first)["summary"]["action_count"] == len(stable_leaf_actions())
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_cli_defaults_to_json_stdout(capsys: Any) -> None:
    cli = _module("analyze_ds_stable_action_dependencies")
    project_root = Path(__file__).resolve().parents[2]

    status = cli.main(["--repo-root", str(project_root)])

    assert status == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["kind"] == (
        "dolphinscheduler-stable-action-dependencies"
    )
    assert captured.err == ""


def _minimal_sources(tmp_path: Path, *, with_metadata: bool) -> Any:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    source_root = tmp_path / "src"
    commands = source_root / "dsctl" / "commands"
    services = source_root / "dsctl" / "services"
    protocols = source_root / "dsctl" / "upstream" / "protocols"
    generated = source_root / "dsctl" / "generated" / "versions" / "ds_3_4_1"
    operations = generated / "api" / "operations"
    for path in (commands, services, protocols, operations):
        path.mkdir(parents=True, exist_ok=True)
    (commands / "widget.py").write_text(
        """
from dsctl.services.widget import get_widget_result

def get_command():
    emit_result("widget.get", lambda: get_widget_result())
""",
        encoding="utf-8",
    )
    (services / "widget.py").write_text(
        """
from dsctl.services.runtime import WidgetServiceRuntime, run_with_widget_service_runtime

def get_widget_result():
    return run_with_widget_service_runtime(None, _get_widget_result)

def _get_widget_result(runtime: WidgetServiceRuntime):
    return runtime.upstream.widgets.get(code=1)
""",
        encoding="utf-8",
    )
    (services / "runtime.py").write_text(
        """
class WidgetServiceRuntime:
    upstream: WidgetSession

def run_with_widget_service_runtime(env_file, operation):
    return operation
""",
        encoding="utf-8",
    )
    (protocols / "session.py").write_text(
        """
class WidgetSession:
    @property
    def widgets(self) -> WidgetOperations: ...
""",
        encoding="utf-8",
    )
    (protocols / "widget.py").write_text(
        """
class WidgetOperations:
    def get(self, *, code: int): ...
""",
        encoding="utf-8",
    )
    (generated / "client.py").write_text(
        """
from .api.operations.widget import WidgetOperations

class DS341Client:
    def __init__(self):
        self.widget = WidgetOperations()
""",
        encoding="utf-8",
    )
    metadata = (
        "DS operation: WidgetController.queryWidget | GET /widgets/{code}"
        if with_metadata
        else "No generated source metadata."
    )
    (operations / "widget.py").write_text(
        f'''\
class WidgetOperations:
    def query_widget(self, code):
        """
        {metadata}
        """
''',
        encoding="utf-8",
    )
    return analyzer.StableActionDependencySources(
        source_root=source_root,
        commands_root=commands,
        protocols_root=protocols,
        generated_client_source=generated / "client.py",
        generated_operations_root=operations,
    )


def _widget_dependency(analyzer: Any) -> Any:
    return analyzer._ReviewedSemanticDependency(
        semantic_operation="widget.read",
        source_operations=("WidgetController.queryWidget",),
        seam=analyzer._ReviewedSemanticSeam(
            kind="direct_port",
            group="widgets",
            method="get",
            protocol="WidgetOperations",
        ),
    )


def test_unreviewed_port_cannot_use_generated_ownership_as_authorization(
    tmp_path: Path,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    report = analyzer.analyze_stable_action_dependencies(
        sources=_minimal_sources(tmp_path, with_metadata=True),
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
    )
    action = _action(report, "widget.get")
    assert action["complete"] is False
    assert action["generated_operations"] == []
    assert action["wire_dependencies"][0]["operation_relationship"] == "unresolved"
    assert any(
        item["code"] == "reviewed-semantic-dependency-missing"
        for item in action["diagnostics"]
    )


@pytest.mark.parametrize(
    ("bypass", "transport"),
    [
        ('runtime.http_client.get("unreviewed")', "DolphinSchedulerClient.get"),
        (
            'runtime.http_client.request_result("POST", "unreviewed")',
            "DolphinSchedulerClient.request_result",
        ),
        ("runtime.upstream.widgets.delete(code=1)", None),
    ],
)
def test_reviewed_port_rejects_extra_transport_or_service_calls(
    tmp_path: Path, bypass: str, transport: str | None
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    service = sources.source_root / "dsctl" / "services" / "widget.py"
    service.write_text(
        service.read_text().replace(
            "    return runtime.upstream.widgets.get(code=1)",
            f"    {bypass}\n    return runtime.upstream.widgets.get(code=1)",
        )
    )
    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": _widget_dependency(analyzer)},
    )
    action = _action(report, "widget.get")
    assert action["complete"] is False
    assert action["generated_operations"] == []
    assert any(
        item["code"] == "reviewed-semantic-seam-mismatch"
        for item in action["diagnostics"]
    )
    assert action["service_direct_transport_calls"] == (
        [transport] if transport else []
    )


@pytest.mark.parametrize("version", ["1.3.9", "3.4.2"])
def test_selected_baseline_keeps_all_actions_and_exact_source_closures(
    version: str,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    root = Path(__file__).resolve().parents[2]
    report = analyzer.analyze_repository_stable_action_dependencies(
        root, baseline_version=version
    )
    assert report["complete"] is True
    assert report["summary"]["complete_action_count"] == 181
    assert report["baseline"]["ds_version"] == version
    assert report["baseline"]["generated_package"] == "ds_" + version.replace(".", "_")
    project = _action(report, "project.get")["reviewed_semantic_dependency"]
    assert project["baseline_version"] == version
    assert project["source_operations"] == [
        "ProjectController.queryProjectListPaging",
        (
            "ProjectController.queryProjectById"
            if version == "1.3.9"
            else "ProjectController.queryProjectByCode"
        ),
    ]
    cluster = _action(report, "cluster.list")
    if version == "1.3.9":
        absence = cluster["reviewed_semantic_dependency"]["unavailable"]
        assert absence["availability"] == "unsupported"
        assert absence["reason"] == "upstream_capability_absent"
        assert "introduced in 3.1.0" in absence["constraint"]
        assert any("ClusterController" in item for item in absence["evidence_sources"])
        assert cluster["generated_operations"] == []
        assert cluster["dependency_relationship"] == "reviewed-unavailable"
        assert _port_names(cluster) == {"cluster.bind"}
    else:
        assert "unavailable" not in cluster["reviewed_semantic_dependency"]
        assert cluster["generated_operations"]


def test_absence_decision_does_not_mask_a_disconnected_service(tmp_path: Path) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    sources = _minimal_sources(tmp_path, with_metadata=True)
    service = sources.source_root / "dsctl" / "services" / "widget.py"
    service.write_text("def get_widget_result():\n    return {}\n", encoding="utf-8")
    dependency = replace(
        _widget_dependency(analyzer),
        source_operations=(),
        unavailable=analyzer._ReviewedUnavailableDecision(
            availability="unsupported",
            reason="upstream_capability_absent",
            constraint="The reviewed exact source has no widget controller.",
            evidence_sources=("apache/dolphinscheduler@3.4.1:controller-tree",),
        ),
    )
    report = analyzer.analyze_stable_action_dependencies(
        sources=sources,
        stable_actions=frozenset({"widget.get"}),
        local_actions=frozenset(),
        diagnostic_actions=frozenset(),
        reviewed_semantic_dependencies={"widget.get": dependency},
    )
    action = _action(report, "widget.get")
    assert action["complete"] is False
    assert "reviewed_semantic_dependency" not in action
    assert "reviewed-semantic-seam-mismatch" in {
        diagnostic["code"] for diagnostic in action["diagnostics"]
    }


def test_missing_selected_binding_cannot_infer_upstream_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    bindings = dict(analyzer.runtime_operation_bindings("3.4.2"))
    del bindings["project.get"]
    monkeypatch.setattr(
        analyzer, "runtime_operation_bindings", lambda version: bindings
    )
    with pytest.raises(
        ValueError, match=r"DS 3\.4\.2 has no reviewed binding for project.get"
    ):
        analyzer._repository_reviewed_semantic_dependencies(
            Path(__file__).resolve().parents[2], baseline_version="3.4.2"
        )


@pytest.mark.parametrize("drift", [None, "missing", "wrong-tag", "residue", "unknown"])
def test_compiled_absence_requires_an_explicit_empty_exact_profile(
    tmp_path: Path, drift: str | None
) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    profile: dict[str, object] = {
        "status": "upstream_absent",
        "source": {"tag": "1.3.9"},
        "recipe_id": None,
        "programs": {},
    }
    if drift == "wrong-tag":
        profile["source"] = {"tag": "3.4.1"}
    elif drift == "residue":
        profile["programs"] = {"get": {}}
    elif drift == "unknown":
        profile["status"] = "not_reviewed"
    profiles = {} if drift == "missing" else {"1.3.9": profile}
    module = tmp_path / "widget.py"
    module.write_text(f"PROFILES = {profiles!r}\nCODECS = {{}}\n", encoding="utf-8")
    if drift is None:
        assert (
            analyzer._compiled_wire_module_operations_by_source(
                module, module_name="widget", ds_version="1.3.9"
            )
            == {}
        )
    else:
        with pytest.raises(ValueError, match="compiled wire"):
            analyzer._compiled_wire_module_operations_by_source(
                module, module_name="widget", ds_version="1.3.9"
            )


def test_selected_baseline_validates_manifest_identity(tmp_path: Path) -> None:
    analyzer = _module("ds_codegen.stable_action_dependencies")
    manifest = tmp_path / "src/dsctl/generated/versions/ds_3_4_2/_manifest.py"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        "DS_VERSION = '3.4.1'\nSOURCE_TAG = '3.4.1'\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="differs from manifest DS_VERSION"):
        analyzer.StableActionDependencySources.from_repository(
            tmp_path, baseline_version="3.4.2"
        )
    with pytest.raises(ValueError, match="Unknown exact dependency baseline"):
        analyzer.StableActionDependencySources.from_repository(
            tmp_path, baseline_version="3.4"
        )
