import json
import subprocess
import sys
from pathlib import Path
from typing import Literal

import pytest

from dsctl.cli_surface import stable_leaf_actions
from dsctl.errors import ConfigError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS, VERSION_PROFILES
from dsctl.upstream import (
    SUPPORTED_VERSIONS,
    VersionSupport,
    get_action_capability,
    get_identity_adapter,
    get_read_adapter,
    get_task_definition_adapter,
    get_version_support,
    is_action_preflight_exempt,
    preflight_action,
    supported_version_metadata,
)
from dsctl.upstream.capability_catalog import Availability, Verification
from dsctl.upstream.code_native_reads import CodeNativeReadAdapter
from dsctl.upstream.id_native_reads import IdNativeReadAdapter
from dsctl.upstream.identity import IdentityAdapter
from dsctl.upstream.task_definition_wire import TaskDefinitionAdapter

EXPECTED_VERSION_METADATA = tuple(
    {
        key: VERSION_PROFILES[version][key]
        for key in (
            "server_version",
            "contract_version",
            "family",
            "support_level",
            "tested",
        )
    }
    for version in TARGET_DS_VERSIONS
)

EXPECTED_PREFLIGHT_EXEMPT_ACTIONS = frozenset(
    {
        "capabilities",
        "context",
        "doctor",
        "schema",
        "template.datasource",
        "context.list",
        "context.get",
        "context.create",
        "context.update",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
        "version",
    }
)


def test_version_registry_loads_without_the_retired_broad_adapter() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    import_blocker = f"""
import sys
sys.path.insert(0, {str(source_root)!r})
class _BlockBroadAdapter:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "dsctl.upstream.adapters.ds_3_4_1":
            raise AssertionError(f"forbidden broad adapter import: {{fullname}}")
        return None
sys.meta_path.insert(0, _BlockBroadAdapter())
"""
    probe = """
import json
import dsctl.upstream
from dsctl.upstream import get_version_support
print(json.dumps({
    "metadata": get_version_support("3.4.1").as_dict(),
    "module_file": dsctl.upstream.__file__,
}, sort_keys=True))
"""

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", import_blocker + probe],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert Path(result["module_file"]).resolve().is_relative_to(source_root.resolve())
    assert result["metadata"] == {
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "server_version": "3.4.1",
        "support_level": "full",
        "tested": True,
    }


def test_registry_metadata_and_identity_do_not_import_exact_packages() -> None:
    source_root = Path(__file__).resolve().parents[2] / "src"
    probe = f"""
import json
import sys
sys.path.insert(0, {str(source_root)!r})

import dsctl.upstream
from dsctl.upstream import (
    get_action_capability,
    get_identity_adapter,
    get_version_support,
    supported_version_metadata,
)

def exact_modules():
    return sorted(
        name for name in sys.modules
        if name.startswith("dsctl.generated.versions.")
    )

support = get_version_support("3.4.1")
metadata = support.as_dict()
supported_version_metadata()
get_action_capability("3.4.1", "project.list")
before = exact_modules()
identity = get_identity_adapter("3.4.1")
after = exact_modules()
print(json.dumps({{
    "before": before,
    "after": after,
    "metadata": metadata,
    "cached": identity is get_identity_adapter("3.4.1"),
    "property_cached": identity is support.identity_adapter,
}}, sort_keys=True))
"""

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", probe],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["before"] == []
    assert result["after"] == []
    assert result["metadata"] == {
        "contract_version": "3.4.1",
        "family": "workflow-3.3-plus",
        "server_version": "3.4.1",
        "support_level": "full",
        "tested": True,
    }
    assert result["cached"] is True
    assert result["property_cached"] is True


def test_version_support_metadata_describes_default_profile() -> None:
    support = get_version_support("ds_3_4_1")

    assert support.server_version == "3.4.1"
    assert support.contract_version == "3.4.1"
    assert support.family == "workflow-3.3-plus"
    assert support.support_level == "full"
    assert support.tested is True
    assert SUPPORTED_VERSIONS == TARGET_DS_VERSIONS
    assert supported_version_metadata() == EXPECTED_VERSION_METADATA


def test_compatible_server_versions_use_exact_narrow_adapters() -> None:
    support = get_version_support("3.3.2")

    assert support.server_version == "3.3.2"
    assert support.contract_version == "3.3.2"
    assert support.family == "workflow-3.3-plus"
    assert support.support_level == "experimental"
    assert support.tested is False
    assert isinstance(support.read_adapter, CodeNativeReadAdapter)
    assert support.read_adapter.ds_version == "3.3.2"


def test_stable_profile_uses_generated_deep_module_adapters() -> None:
    support = get_version_support("3.4.1")

    assert isinstance(support.read_adapter, CodeNativeReadAdapter)
    assert support.read_adapter.ds_version == "3.4.1"
    assert isinstance(support.identity_adapter, IdentityAdapter)
    assert isinstance(
        support.task_definition_adapter,
        TaskDefinitionAdapter,
    )
    assert isinstance(get_read_adapter("3.4.1"), CodeNativeReadAdapter)
    assert isinstance(get_identity_adapter("3.4.1"), IdentityAdapter)
    assert isinstance(
        get_task_definition_adapter("3.4.1"),
        TaskDefinitionAdapter,
    )


@pytest.mark.parametrize("version", ["3.3.2", "3.4.0"])
def test_code_native_profiles_materialize_reviewed_local_authoring(
    version: str,
) -> None:
    catalog = get_version_support(version).catalog

    for action in (
        "enum.names",
        "lint.workflow",
        "task-type.get",
        "task-type.schema",
        "template.task",
        "template.workflow",
    ):
        capability = catalog.entries[action]
        assert capability.availability is Availability.SUPPORTED
        assert capability.verification is Verification.STATIC

    workflow_create = catalog.entries["workflow.create"]
    assert workflow_create.availability is Availability.SUPPORTED
    assert workflow_create.verification is Verification.CONTRACT_TESTED


def test_342_profile_uses_exact_narrow_adapters_and_reviewed_actions() -> None:
    support = get_version_support("3.4.2")

    assert support.catalog.server_version == "3.4.2"
    assert isinstance(support.read_adapter, CodeNativeReadAdapter)
    assert isinstance(support.identity_adapter, IdentityAdapter)
    assert isinstance(
        support.task_definition_adapter,
        TaskDefinitionAdapter,
    )
    assert isinstance(get_read_adapter("3.4.2"), CodeNativeReadAdapter)
    assert isinstance(get_identity_adapter("3.4.2"), IdentityAdapter)
    assert isinstance(
        get_task_definition_adapter("3.4.2"),
        TaskDefinitionAdapter,
    )

    for action in (
        "doctor",
        "project.create",
        "project.delete",
        "project.get",
        "project.list",
        "project.update",
        "schedule.list",
        "workflow.describe",
        "workflow.digest",
        "workflow.export",
        "workflow.get",
        "workflow.list",
        "task.get",
        "task.list",
        "task.update",
    ):
        capability = support.catalog.preflight(action)
        assert capability.availability is Availability.SUPPORTED
        assert capability.verification is Verification.LIVE_SMOKE

    for action in (
        "enum.names",
        "task-type.get",
        "template.workflow",
        "workflow.create",
    ):
        assert support.catalog.entries[action].availability is Availability.SUPPORTED


def test_322_profile_uses_exact_read_adapter_and_generated_local_catalog() -> None:
    support = get_version_support("3.2.2")

    assert isinstance(support.identity_adapter, IdentityAdapter)
    assert isinstance(support.read_adapter, CodeNativeReadAdapter)
    assert isinstance(get_read_adapter("3.2.2"), CodeNativeReadAdapter)
    assert isinstance(get_identity_adapter("3.2.2"), IdentityAdapter)
    for action in (
        "capabilities",
        "schema",
        "version",
        "context",
        "context.list",
        "context.get",
        "context.create",
        "context.update",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
        "doctor",
        "project.list",
        "project.get",
        "workflow.list",
        "workflow.get",
    ):
        assert support.catalog.entries[action].availability is Availability.SUPPORTED

    for action in ("project.list", "project.get", "workflow.list", "workflow.get"):
        assert support.catalog.entries[action].verification is Verification.LIVE_SMOKE

    for action in (
        "project.create",
        "project.delete",
        "project.update",
        "schedule.list",
    ):
        assert support.catalog.entries[action].availability is Availability.SUPPORTED
        assert (
            support.catalog.entries[action].verification is Verification.CONTRACT_TESTED
        )

    assert support.catalog.entries["doctor"].verification is Verification.STATIC

    for action in (
        "enum.names",
        "lint.workflow",
        "task-type.get",
        "template.workflow",
        "workflow.create",
    ):
        assert support.catalog.entries[action].availability is Availability.SUPPORTED


def test_registry_exposes_one_shared_capability_lookup_and_preflight() -> None:
    capability = get_action_capability("3.4.2", "project.list")

    assert capability is preflight_action("3.4.2", "project.list")
    assert capability.verification is Verification.LIVE_SMOKE


def test_registry_catalogs_are_materialized_from_exact_generated_profiles() -> None:
    for version in TARGET_DS_VERSIONS:
        support = get_version_support(version)
        expected_actions = VERSION_PROFILES[version]["actions"]

        assert support.server_version == VERSION_PROFILES[version]["server_version"]
        assert support.contract_version == VERSION_PROFILES[version]["contract_version"]
        assert support.family == VERSION_PROFILES[version]["family"]
        assert support.support_level == VERSION_PROFILES[version]["support_level"]
        assert support.tested is VERSION_PROFILES[version]["tested"]
        for action, capability in support.catalog.entries.items():
            expected = expected_actions[action]
            assert capability.availability.value == expected["availability"]
            assert capability.verification.value == expected["verification"]
            assert capability.constraint == expected.get("constraint")


def test_every_profile_selects_its_exact_compiled_identity_adapter() -> None:
    for version in TARGET_DS_VERSIONS:
        adapter = get_identity_adapter(version)

        assert isinstance(adapter, IdentityAdapter)
        assert adapter.ds_version == version


def test_every_profile_can_lazily_construct_its_exact_read_adapter() -> None:
    for version in TARGET_DS_VERSIONS:
        adapter = get_read_adapter(version)

        assert adapter.ds_version == version


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS[1:])
def test_code_identity_profiles_select_the_exact_lazy_task_adapter(
    version: str,
) -> None:
    adapter = get_task_definition_adapter(version)

    assert isinstance(adapter, TaskDefinitionAdapter)
    assert adapter.ds_version == version


def test_139_profile_uses_exact_legacy_definition_read_adapter() -> None:
    version = "1.3.9"
    support = get_version_support(version)

    assert support.server_version == version
    assert isinstance(support.read_adapter, IdNativeReadAdapter)
    assert isinstance(get_read_adapter(version), IdNativeReadAdapter)
    for action in ("project.list", "project.get", "workflow.list", "workflow.get"):
        assert support.catalog.entries[action].availability is Availability.SUPPORTED
        assert support.catalog.entries[action].verification is Verification.LIVE_SMOKE


def test_only_local_and_diagnostic_actions_bypass_version_preflight() -> None:
    actual = frozenset(
        action for action in stable_leaf_actions() if is_action_preflight_exempt(action)
    )

    assert actual == EXPECTED_PREFLIGHT_EXEMPT_ACTIONS


def test_stable_profile_evidence_distinguishes_local_and_remote_actions() -> None:
    catalog = get_version_support("3.4.1").catalog

    assert catalog.entries["schema"].verification is Verification.STATIC
    assert catalog.entries["task-type.get"].verification is Verification.STATIC
    assert (
        catalog.entries["task-type.list"].verification is Verification.CONTRACT_TESTED
    )
    assert (
        catalog.entries["workflow.create"].verification is Verification.CONTRACT_TESTED
    )


@pytest.mark.parametrize("support_level", ["full", "legacy_core"])
def test_stable_support_levels_require_live_test_evidence(
    support_level: Literal["full", "legacy_core"],
) -> None:
    with pytest.raises(ValueError, match="requires tested=True"):
        VersionSupport(
            server_version="3.4.0",
            contract_version="3.4.1",
            family="workflow-3.3-plus",
            support_level=support_level,
            tested=False,
            catalog=get_version_support("3.4.0").catalog,
        )


def test_get_version_support_rejects_unknown_versions() -> None:
    with pytest.raises(ConfigError) as exc_info:
        get_version_support("3.5.0")

    assert exc_info.value.details == {
        "version": "3.5.0",
        "supported_versions": ", ".join(TARGET_DS_VERSIONS),
    }
