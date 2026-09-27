from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from dsctl.cli_surface import stable_leaf_actions
from dsctl.errors import ConfigError
from dsctl.services import capabilities, enums, schema, version_resolution
from dsctl.services.version_resolution import CompatibilityResolution
from dsctl.upstream import get_enum_spec

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from dsctl.output import CommandResult
    from dsctl.upstream.enums import EnumSpec


@pytest.fixture
def candidate(monkeypatch: pytest.MonkeyPatch) -> CompatibilityResolution:
    observed = CompatibilityResolution(
        candidate_versions=("2.0.4", "2.0.5"),
        source="cache",
        checked_at=1000.0,
        evidence="swagger2_contract",
        compatible_operations=("ProjectController.queryProjectListPaging",),
    )
    for service in (schema, capabilities, enums):
        monkeypatch.setattr(service, "resolve_target", lambda *_a, **_k: observed)
    return observed


def test_candidate_schema_reports_catalog_without_selecting_authoring_models(
    candidate: CompatibilityResolution,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail(
            "Candidate discovery must not select a task catalog or exact profile"
        )

    monkeypatch.setattr(schema, "get_task_authoring_catalog", forbidden)
    monkeypatch.setattr(schema, "get_version_support", forbidden)
    result = schema.get_schema_result(command_action="workflow.create")
    data = result.data
    assert isinstance(data, dict)
    assert data["ds"]["selected_version"] is None
    assert data["ds"]["candidate_versions"] == list(candidate.candidate_versions)
    assert data["capability"]["availability"] == "requires_exact_version"
    command = data["command"]
    assert command["invocation"].startswith("dsctl workflow create")
    assert command["contract_scope"] == "installed_cli_invocation"
    assert "payload" not in command
    assert "payload_schema" not in command
    assert any(option["flag"] == "--file" for option in command["options"])

    admitted = schema.get_schema_result(command_action="project.list").data
    assert isinstance(admitted, dict)
    assert admitted["ds"]["contract_version"] is None
    assert admitted["capability"]["availability"] == "read_compatible"
    assert admitted["command"]["version_specific_constraints"] == "unknown"


def test_candidate_schema_root_and_groups_cover_installed_actions(
    candidate: CompatibilityResolution,
) -> None:
    data = schema.get_schema_result().data
    assert isinstance(data, dict)
    actions = {row["action"] for row in data["root_actions"]}
    actions.update(action for group in data["groups"] for action in group["actions"])
    assert actions == stable_leaf_actions()
    assert data["action_count"] == len(actions)
    group = schema.get_schema_result(group="project").data
    assert isinstance(group, dict)
    by_action = {row["action"]: row for row in group["actions"]}
    assert by_action["project.list"]["availability"] == "read_compatible"
    assert by_action["project.get"]["availability"] == "requires_exact_version"
    assert by_action["project.create"]["requires_exact_version"]
    assert group["group"]["available_action_count"] == 1
    full = schema.get_schema_result(group="template", full=True)
    assert isinstance(full.resolved["schema"], dict)
    assert isinstance(full.data, dict)
    assert full.resolved["schema"]["scope"] == "group"
    assert full.data["ds"]["selected_version"] is None
    assert all(
        "payload" not in command for command in full.data["commands"][0]["commands"]
    )
    assert schema.get_schema_result(list_commands=True).data
    assert schema.get_schema_result(list_groups=True).data


def test_candidate_capabilities_never_promote_candidate_to_selected_version(
    candidate: CompatibilityResolution,
) -> None:
    summary = capabilities.get_capabilities_result().data
    assert isinstance(summary, dict)
    assert summary["ds"]["selected_version"] is None
    assert summary["ds"]["tested"] is False
    assert summary["read_compatible_actions"] == ["project.list"]
    assert summary["authoring"] == {"requires_exact_version": True}
    action = capabilities.get_capabilities_result(action="project.list").data
    assert isinstance(action, dict)
    assert action["capability"]["availability"] == "read_compatible"
    denied = capabilities.get_capabilities_result(action="workflow.create").data
    assert isinstance(denied, dict)
    assert denied["capability"]["requires_exact_version"]
    authoring = capabilities.get_capabilities_result(section="authoring").data
    assert isinstance(authoring, dict)
    assert authoring["authoring"]["exact_version_semantics"] == "unknown"
    assert "task_template_types" not in authoring["authoring"]
    full = capabilities.get_capabilities_result(full=True).data
    assert isinstance(full, dict)
    assert {row["action"] for row in full["action_catalog"]} == stable_leaf_actions()


def test_candidate_enum_discovery_uses_identical_complete_members(
    candidate: CompatibilityResolution,
) -> None:
    names = enums.list_enum_names_result()
    assert isinstance(names.data, list)
    assert isinstance(names.resolved["enum"], dict)
    assert "priority" in {row["name"] for row in names.data}
    assert names.resolved["enum"]["ds_version"] is None
    assert names.resolved["enum"]["contract_scope"] == "candidate_generated_contracts"
    assert names.resolved["enum"]["server_membership"] == "unverified"
    result = enums.list_enum_result("Priority")
    assert isinstance(result.data, dict)
    assert isinstance(result.resolved["enum"], dict)
    assert result.data["ds_version"] is None
    assert result.data["candidate_versions"] == list(candidate.candidate_versions)
    assert result.data["server_membership"] == "unverified"
    assert result.resolved["enum"]["note"] == result.data["note"]
    assert result.data["members"][0] == {
        "name": "HIGHEST",
        "value": "HIGHEST",
        "attributes": {"code": 0, "descp": "highest"},
    }


def test_candidate_enum_evolution_requires_actual_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = CompatibilityResolution(
        candidate_versions=("1.3.9", "3.4.1"),
        source="cache",
        checked_at=1000.0,
        evidence="contract_candidates",
    )
    monkeypatch.setattr(enums, "resolve_target", lambda *_a, **_k: observed)
    names = enums.list_enum_names_result().data
    assert isinstance(names, list)
    assert "db-type" not in {row["name"] for row in names}
    with pytest.raises(ConfigError) as caught:
        enums.list_enum_result("db-type")
    assert caught.value.details["reason"] == "exact_version_required"
    assert caught.value.details["ds_version"] is None


def test_enum_attribute_difference_is_not_hidden_by_matching_names_and_values(
    candidate: CompatibilityResolution,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = get_enum_spec

    def changed(version: str, name: str) -> EnumSpec | None:
        spec = original(version, name)
        if version == "2.0.5" and spec is not None and spec.name == "priority":
            member = replace(
                spec.members[0], attributes={"code": 99, "descp": "highest"}
            )
            return replace(spec, members=(member, *spec.members[1:]))
        return spec

    monkeypatch.setattr(enums, "get_enum_spec", changed)
    with pytest.raises(ConfigError, match="no identical contract"):
        enums.list_enum_result("priority")


@pytest.mark.parametrize(
    "command", [schema.get_schema_result, capabilities.get_capabilities_result]
)
def test_cold_cache_catalog_discovery_is_local_and_version_unresolved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command: Callable[[], CommandResult],
) -> None:
    monkeypatch.setenv("DS_API_URL", "https://ds.example.test/dolphinscheduler")
    monkeypatch.setenv("DS_API_TOKEN", "test-token")
    monkeypatch.setenv("DS_VERSION", "auto")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))

    def forbidden(*_a: object, **_k: object) -> None:
        pytest.fail("Local discovery must not probe HTTP")

    monkeypatch.setattr(version_resolution, "discover_target", forbidden)
    result = command()
    assert isinstance(result.data, dict)
    assert result.data["ds"]["ds_version"] is None
    assert result.data["ds"]["identification"] == "unresolved"


@pytest.mark.parametrize(
    "command", [schema.get_schema_result, capabilities.get_capabilities_result]
)
def test_local_catalog_does_not_mask_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch,
    command: Callable[[], CommandResult],
) -> None:
    monkeypatch.setenv("DS_VERSION", "3.4.99")
    with pytest.raises(ConfigError, match="Unsupported DS version"):
        command()


@pytest.mark.parametrize(
    "operations", [(), ("DataSourceController.queryDataSourceList",)]
)
def test_singleton_candidate_enum_members_are_not_server_membership_evidence(
    monkeypatch: pytest.MonkeyPatch, operations: tuple[str, ...]
) -> None:
    observed = CompatibilityResolution(
        candidate_versions=("1.3.9",),
        source="contract",
        checked_at=1000.0,
        evidence="Complete public routes constrain a structural candidate.",
        compatible_operations=operations,
    )
    monkeypatch.setattr(enums, "resolve_target", lambda *_a, **_k: observed)
    result = enums.list_enum_result("db-type")
    assert isinstance(result.data, dict)
    assert isinstance(result.resolved["enum"], dict)
    # H2 belongs to the generated 1.3.9 enum even when public docs omit it.
    # An observed operation list cannot establish global server enum membership.
    assert "H2" in {member["name"] for member in result.data["members"]}
    assert result.data["ds_version"] is None
    assert result.data["contract_scope"] == "candidate_generated_contracts"
    assert result.data["server_membership"] == "unverified"
    assert result.data["note"] == (
        "Values describe identical generated candidate contracts; "
        "observed server values may differ."
    )
