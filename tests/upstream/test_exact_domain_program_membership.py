"""Installed program manifests and runtime expectations agree for every profile."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from dsctl.upstream.compiled_domain import CompiledDomainPrograms

_VERSIONS = (
    "1.3.9",
    *(f"2.0.{patch}" for patch in range(10)),
    *(f"3.0.{patch}" for patch in range(7)),
    *(f"3.1.{patch}" for patch in range(10)),
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_DOMAINS = (
    ("clusters", "_CLUSTER_PROGRAMS"),
    ("environments", "_ENVIRONMENT_PROGRAMS"),
    ("worker_groups", "_WORKER_GROUP_PROGRAMS"),
    ("alert_groups", "_ALERT_GROUP_PROGRAMS"),
    ("tenants", "_TENANT_PROGRAMS"),
    ("queues", "_QUEUE_PROGRAMS"),
    ("alert_plugins", "_ALERT_PLUGIN_PROGRAMS"),
    ("access_tokens", "_ACCESS_TOKEN_PROGRAMS"),
    ("namespaces", "_NAMESPACE_PROGRAMS"),
    ("task_groups", "_TASK_GROUP_PROGRAMS"),
    ("users", "_USER_PROGRAMS"),
    ("observability", "_AUDIT_PROGRAMS"),
    ("task_type_inventory", "_TASK_TYPE_PROGRAMS"),
    ("observability", "_MONITOR_PROGRAMS"),
    ("_compiled_project", "PROJECT_PROGRAMS"),
    ("datasources", "_DATASOURCE_PROGRAMS"),
    ("_compiled_resource", "RESOURCE_PROGRAMS"),
    ("project_parameters", "_PROJECT_PARAMETER_PROGRAMS"),
    ("project_preferences", "_PROJECT_PREFERENCE_PROGRAMS"),
    ("project_worker_groups", "_PROJECT_WORKER_GROUP_PROGRAMS"),
    ("_compiled_workflow_runtime", "WORKFLOW_PROGRAMS"),
)


@pytest.mark.parametrize("version", _VERSIONS)
@pytest.mark.parametrize(("module_name", "attribute"), _DOMAINS)
def test_exact_domain_program_inventory_matches_runtime_expectations(
    module_name: str,
    attribute: str,
    version: str,
) -> None:
    module = importlib.import_module(f"dsctl.upstream.{module_name}")
    domain = cast("CompiledDomainPrograms[str]", getattr(module, attribute))
    profile = domain.profile(version)
    assert profile.ds_version == version
    if profile.status == "upstream_absent":
        assert not profile.programs
        return
    assert set(profile.programs) == {
        name
        for name, policy in domain.expectations.items()
        if version not in policy.absent_versions
    }
