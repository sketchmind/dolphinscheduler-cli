from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import pytest
from tests.support import make_profile

from dsctl.services import runtime as runtime_service
from dsctl.services.version_resolution import RuntimeSelection
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.legacy_task_definitions import LegacyTaskDefinitions

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch

    from dsctl.config import ClusterProfile
    from dsctl.services._whole_workflow_task_update import (
        CodeNativeWholeWorkflowTaskUpdate,
    )
    from dsctl.upstream.task_definitions import TaskDefinitions


@dataclass(frozen=True)
class FakeRuntimeAdapter:
    seen_versions: list[str]

    def bind_read(self, profile: object, *, http_client: object) -> object:
        return {"profile": profile, "http_client": http_client, "scope": "read"}

    def bind_task_definitions(
        self,
        profile: object,
        *,
        http_client: object,
    ) -> object:
        return _FakeTaskDefinitionSession(
            definitions={"scope": "task-definitions"},
            task_definitions=_FakeTaskDefinitionWire(),
        )


@dataclass(frozen=True)
class _FakeTaskDefinitionWire:
    ds_version: str = "3.4.2"
    recipe_fingerprint: str = "sha256:test-recipe"


@dataclass(frozen=True)
class _FakeTaskDefinitionSession:
    definitions: object
    task_definitions: _FakeTaskDefinitionWire


class FakeRuntimeHttpClient:
    def __init__(self, profile: object) -> None:
        self.profile = profile

    def __enter__(self) -> FakeRuntimeHttpClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def healthcheck(self) -> dict[str, object]:
        return {"status": "UP"}


@dataclass(frozen=True)
class _FakeBoundDomainAdapter:
    seen_versions: list[str]

    def bind(self, profile: object, *, http_client: object) -> object:
        version = cast("ClusterProfile", profile).ds_version
        self.seen_versions.append(version)
        return {
            "profile": profile,
            "http_client": http_client,
            "scope": "tenant",
        }


def test_open_read_service_runtime_uses_exact_read_adapter(
    monkeypatch: MonkeyPatch,
) -> None:
    seen_versions: list[str] = []

    def fake_get_read_adapter(version: str) -> FakeRuntimeAdapter:
        seen_versions.append(version)
        return FakeRuntimeAdapter(seen_versions)

    monkeypatch.setattr(
        runtime_service,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(make_profile(ds_version="3.2.2")),
    )
    monkeypatch.setattr(
        runtime_service,
        "get_read_adapter",
        fake_get_read_adapter,
        raising=False,
    )
    monkeypatch.setattr(
        runtime_service, "DolphinSchedulerClient", FakeRuntimeHttpClient
    )

    with runtime_service.open_read_service_runtime() as runtime:
        assert runtime.profile.ds_version == "3.2.2"
        assert cast("dict[str, object]", runtime.upstream)["scope"] == "read"

    assert seen_versions == ["3.2.2"]


def test_open_bound_domain_runtime_binds_only_the_requested_exact_domain(
    monkeypatch: MonkeyPatch,
) -> None:
    seen_versions: list[str] = []
    domain = BoundDomain[object](
        name="tenant",
        adapter_for_version=lambda _version: _FakeBoundDomainAdapter(seen_versions),
    )

    monkeypatch.setattr(
        runtime_service,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(
            make_profile(ds_version="3.2.1"), project="selected-project"
        ),
    )
    monkeypatch.setattr(
        runtime_service,
        "DolphinSchedulerClient",
        FakeRuntimeHttpClient,
    )

    with runtime_service.open_bound_domain_service_runtime(domain) as runtime:
        assert runtime.profile.ds_version == "3.2.1"
        assert runtime.context.project == "selected-project"
        assert cast("dict[str, object]", runtime.domain)["scope"] == "tenant"

    assert seen_versions == ["3.2.1"]


def test_bound_domain_profile_reuses_the_authoring_target_without_resolving_again(
    monkeypatch: MonkeyPatch,
) -> None:
    profile = make_profile(ds_version="3.4.2")
    seen_versions: list[str] = []
    domain = BoundDomain[object](
        name="tenant",
        adapter_for_version=lambda _version: _FakeBoundDomainAdapter(seen_versions),
    )

    def unexpected_resolution(env_file: str | None = None) -> ClusterProfile:
        del env_file
        message = "A resolved authoring target must not be probed again"
        raise AssertionError(message)

    monkeypatch.setattr(
        runtime_service, "resolve_runtime_selection", unexpected_resolution
    )
    monkeypatch.setattr(
        runtime_service, "DolphinSchedulerClient", FakeRuntimeHttpClient
    )

    result = runtime_service.run_with_bound_domain_selection(
        RuntimeSelection(profile, project="authoring-project"),
        domain,
        lambda runtime: runtime,
    )

    assert result.profile is profile
    assert result.context.project == "authoring-project"
    assert cast("dict[str, object]", result.domain)["profile"] is profile
    assert seen_versions == ["3.4.2"]


def test_open_task_definition_runtime_composes_the_exact_deep_module(
    monkeypatch: MonkeyPatch,
) -> None:
    seen_versions: list[str] = []

    def fake_get_task_definition_adapter(version: str) -> FakeRuntimeAdapter:
        seen_versions.append(version)
        return FakeRuntimeAdapter(seen_versions)

    monkeypatch.setattr(
        runtime_service,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(
            make_profile(ds_version="3.4.2"), project="etl-prod"
        ),
    )
    monkeypatch.setattr(
        runtime_service,
        "get_task_definition_adapter",
        fake_get_task_definition_adapter,
        raising=False,
    )
    monkeypatch.setattr(
        runtime_service,
        "DolphinSchedulerClient",
        FakeRuntimeHttpClient,
    )

    with runtime_service.open_task_definition_service_runtime() as runtime:
        definitions = cast("TaskDefinitions", runtime.definitions)
        assert runtime.profile.ds_version == "3.4.2"
        assert runtime.context.project == "etl-prod"
        assert definitions.profile_version == "3.4.2"
        assert definitions.wire.recipe_fingerprint == "sha256:test-recipe"

    assert seen_versions == ["3.4.2"]


@pytest.mark.parametrize("ds_version", ["2.0.0", "2.0.1", "2.0.2", "2.0.3"])
def test_open_early_ds20_task_runtime_composes_the_whole_workflow_update_recipe(
    monkeypatch: MonkeyPatch,
    ds_version: str,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(
            make_profile(ds_version=ds_version), project="etl-prod"
        ),
    )
    monkeypatch.setattr(
        runtime_service,
        "DolphinSchedulerClient",
        FakeRuntimeHttpClient,
    )

    with runtime_service.open_task_definition_service_runtime() as runtime:
        definitions = cast("TaskDefinitions", runtime.definitions)
        assert definitions.profile_version == ds_version
        whole_workflow_update = cast(
            "CodeNativeWholeWorkflowTaskUpdate | None",
            definitions.whole_workflow_update,
        )
        assert whole_workflow_update is not None
        assert whole_workflow_update.profile_version == ds_version
        assert whole_workflow_update.operations.ds_version == ds_version


def test_open_task_definition_runtime_selects_legacy_embedded_task_domain(
    monkeypatch: MonkeyPatch,
) -> None:
    class FakeLegacyOperations:
        ds_version = "1.3.9"

    class FakeWorkflowDomain:
        workflows = FakeLegacyOperations()

    class FakeWorkflowBoundDomain:
        def bind(self, profile: object, *, http_client: object) -> object:
            del profile, http_client
            return FakeWorkflowDomain()

    monkeypatch.setattr(
        runtime_service,
        "resolve_runtime_selection",
        lambda env_file=None: RuntimeSelection(
            make_profile(ds_version="1.3.9"), project="etl-prod"
        ),
    )
    monkeypatch.setattr(
        runtime_service,
        "DolphinSchedulerClient",
        FakeRuntimeHttpClient,
    )
    monkeypatch.setattr(
        runtime_service,
        "WORKFLOW_DOMAIN",
        FakeWorkflowBoundDomain(),
    )
    monkeypatch.setattr(
        runtime_service,
        "get_task_definition_adapter",
        lambda _version: (_ for _ in ()).throw(
            AssertionError("legacy runtime must not bind modern task-definition wire")
        ),
    )

    with runtime_service.open_task_definition_service_runtime() as runtime:
        assert runtime.profile.ds_version == "1.3.9"
        assert isinstance(runtime.definitions, LegacyTaskDefinitions)
        assert runtime.definitions.profile_version == "1.3.9"
