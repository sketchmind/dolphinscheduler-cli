from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pytest

from dsctl.services import capabilities, schema
from dsctl.services.version_resolution import CompatibilityResolution
from dsctl.upstream import get_version_support

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.output import CommandResult
    from dsctl.support.json_types import JsonValue


def _object(value: JsonValue) -> Mapping[str, JsonValue]:
    assert isinstance(value, Mapping)
    return value


def _capability(result: CommandResult) -> Mapping[str, JsonValue]:
    return _object(_object(result.data)["capability"])


def _group_counts(result: CommandResult) -> dict[str, int]:
    assert isinstance(result.data, list)
    counts: dict[str, int] = {}
    for value in result.data:
        group = _object(value)
        name, count = group["name"], group["available_action_count"]
        assert isinstance(name, str)
        assert isinstance(count, int)
        counts[name] = count
    return counts


def _group_count(result: CommandResult) -> JsonValue:
    return _object(_object(result.data)["group"])["available_action_count"]


@pytest.fixture
def candidate(monkeypatch: pytest.MonkeyPatch) -> CompatibilityResolution:
    observed = CompatibilityResolution(
        candidate_versions=("2.0.4", "2.0.5"),
        source="cache",
        checked_at=1000.0,
        evidence="swagger2_contract",
        compatible_operations=("ProjectController.queryProjectListPaging",),
    )
    for service in (schema, capabilities):
        monkeypatch.setattr(service, "resolve_target", lambda *_a, **_k: observed)
    return observed


@pytest.mark.parametrize(
    "action",
    [
        "schema",
        "capabilities",
        "context",
        "context.create",
        "context.update",
        "context.list",
        "context.get",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
        "version",
        "enum.names",
    ],
)
def test_candidate_local_actions_do_not_claim_to_require_an_exact_version(
    candidate: CompatibilityResolution, action: str
) -> None:
    capability = _capability(capabilities.get_capabilities_result(action=action))
    assert capability["availability"] == "available_local"
    assert capability["requires_exact_version"] is False
    assert _capability(schema.get_schema_result(command_action=action)) == capability


def test_candidate_enum_and_diagnostic_availability_keep_their_conditions(
    candidate: CompatibilityResolution,
) -> None:
    enum = _capability(capabilities.get_capabilities_result(action="enum.list"))
    assert enum["availability"] == "conditional_local"
    assert enum["requires_exact_version"] is False
    assert isinstance(enum["constraint"], str)
    assert "identical complete contract" in enum["constraint"]
    doctor = _capability(capabilities.get_capabilities_result(action="doctor"))
    assert doctor["availability"] == "available_diagnostic"
    assert doctor["authenticated_read_available"] is False
    assert doctor["requires_exact_version"] is False
    counts = _group_counts(schema.get_schema_result(list_groups=True))
    assert counts["enum"] == 2
    assert counts["context"] == 6
    assert counts["config"] == 3
    assert _group_count(schema.get_schema_result(group="enum")) == 2


def test_cold_discovery_distinguishes_local_commands_from_cache_dependent_queries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DS_API_URL", "https://ds.example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-token")
    monkeypatch.setenv("DS_VERSION", "auto")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    for action in (
        "schema",
        "capabilities",
        "context",
        "context.create",
        "context.update",
        "context.list",
        "context.get",
        "context.delete",
        "config.get",
        "config.set",
        "config.unset",
    ):
        data = _capability(capabilities.get_capabilities_result(action=action))
        assert data["availability"] == "available_local"
        assert data["requires_exact_version"] is False
    for action in ("version", "enum.names", "enum.list"):
        data = _capability(capabilities.get_capabilities_result(action=action))
        assert data["availability"] == "requires_discovery"
        assert data["requires_exact_version"] is False
    doctor = _capability(capabilities.get_capabilities_result(action="doctor"))
    assert doctor["availability"] == "available_diagnostic"
    assert doctor["requires_exact_version"] is False
    counts = _group_counts(schema.get_schema_result(list_groups=True))
    assert counts["context"] == 6
    assert counts["config"] == 3
    assert counts["enum"] == 0
    assert _group_count(schema.get_schema_result(group="context")) == 6
    assert _group_count(schema.get_schema_result(group="config")) == 3


def test_exact_capability_views_keep_their_existing_catalog_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.1")
    support = get_version_support("3.4.1")
    for action in ("schema", "doctor", "enum.list", "project.list"):
        result = _object(capabilities.get_capabilities_result(action=action).data)
        assert _object(result["ds"])["selected_version"] == "3.4.1"
        assert result["capability"] == support.catalog.action_metadata(action)
        exact_schema = _object(schema.get_schema_result(command_action=action).data)
        assert _object(exact_schema["ds"])["selected_version"] == "3.4.1"
        assert exact_schema["capability"] == support.catalog.action_metadata(action)
