from __future__ import annotations

import importlib
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
from pydantic import BaseModel

from dsctl.client import DolphinSchedulerClient
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream import compiled_domain
from dsctl.upstream.clusters import _CLUSTER_PROGRAMS
from dsctl.upstream.wire import WireContractError, load_compiled_wire_profiles
from tests.support import make_profile

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CACHE_REUSE_PROBE = f"""
import sys
from unittest.mock import patch

sys.path[:0] = [{str(_REPO_ROOT / "src")!r}, {str(_REPO_ROOT)!r}]

from dsctl.upstream import compiled_domain, wire
from dsctl.upstream.access_tokens import AccessTokenAdapter
from dsctl.upstream.alert_groups import AlertGroupAdapter
from dsctl.upstream.alert_plugins import AlertPluginAdapter
from dsctl.upstream.clusters import ClusterAdapter
from dsctl.upstream.environments import EnvironmentAdapter
from dsctl.upstream.identity import IdentityAdapter
from dsctl.upstream.namespaces import NamespaceAdapter
from dsctl.upstream.observability import (
    AuditAdapter,
    MonitorAdapter,
)
from dsctl.upstream.queues import QueueAdapter
from dsctl.upstream.project_parameters import ProjectParameterAdapter
from dsctl.upstream.project_preferences import (
    ProjectPreferenceAdapter,
)
from dsctl.upstream.project_worker_groups import (
    ProjectWorkerGroupAdapter,
)
from dsctl.upstream.task_groups import TaskGroupAdapter
from dsctl.upstream.task_type_inventory import TaskTypeAdapter
from dsctl.upstream.tenants import TenantAdapter
from dsctl.upstream.users import UserAdapter
from dsctl.upstream.worker_groups import WorkerGroupAdapter

assert wire.installed_compiled_wire_installation.cache_info().currsize == 0
with (
    patch.object(
        wire,
        "_validate_installed_artifact",
        wraps=wire._validate_installed_artifact,
    ) as artifact_checks,
    patch.object(
        compiled_domain,
        "load_compiled_wire_profiles",
        wraps=compiled_domain.load_compiled_wire_profiles,
    ) as domain_loads,
):
    for adapter in (
        ClusterAdapter,
        EnvironmentAdapter,
        WorkerGroupAdapter,
        AlertGroupAdapter,
        TenantAdapter,
        QueueAdapter,
        AlertPluginAdapter,
        AccessTokenAdapter,
        NamespaceAdapter,
        TaskGroupAdapter,
        UserAdapter,
        IdentityAdapter,
        AuditAdapter,
        TaskTypeAdapter,
        MonitorAdapter,
        ProjectParameterAdapter,
        ProjectPreferenceAdapter,
        ProjectWorkerGroupAdapter,
    ):
        for version in ("3.4.1", "3.4.2", "3.4.1"):
            assert adapter.for_version(version).ds_version == version

    assert [call.args[0] for call in artifact_checks.call_args_list] == [
        "dsctl.generated.wire_programs",
        "dsctl.generated.wire_runtime",
    ]
    assert [call.args[0] for call in domain_loads.call_args_list] == [
        "cluster", "environment", "worker_group", "alert_group", "tenant", "queue",
        "alert_plugin", "access_token", "namespace", "task_group", "user",
        "audit", "task_type", "monitor",
        "project_parameter", "project_preference", "project_worker_group",
    ]
    assert not any(name.startswith("dsctl.generated.versions.") for name in sys.modules)
"""


def test_public_adapters_reuse_one_installation_check_and_one_load_per_domain() -> None:
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [sys.executable, "-I", "-c", _CACHE_REUSE_PROBE],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize(
    ("module_name", "programs_name", "adapter_name"),
    [
        (
            "project_parameters",
            "_PROJECT_PARAMETER_PROGRAMS",
            "ProjectParameterAdapter",
        ),
        (
            "project_preferences",
            "_PROJECT_PREFERENCE_PROGRAMS",
            "ProjectPreferenceAdapter",
        ),
        (
            "project_worker_groups",
            "_PROJECT_WORKER_GROUP_PROGRAMS",
            "ProjectWorkerGroupAdapter",
        ),
    ],
)
def test_project_configuration_adapters_reject_unknown_recipes(
    module_name: str,
    programs_name: str,
    adapter_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module(f"dsctl.upstream.{module_name}")
    programs = getattr(module, programs_name)
    profile = replace(programs.profile("3.4.1"), recipe_id="unreviewed_recipe")
    monkeypatch.setattr(module, programs_name, Mock(profile=Mock(return_value=profile)))

    with pytest.raises(WireContractError, match=r"recipe.*unsupported"):
        getattr(module, adapter_name).for_version("3.4.1")


def test_domain_reuses_all_exact_profiles_but_fresh_load_is_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domain = replace(_CLUSTER_PROGRAMS)
    record_load = Mock(wraps=load_compiled_wire_profiles)
    monkeypatch.setattr(compiled_domain, "load_compiled_wire_profiles", record_load)

    cached = domain.profile("3.4.1")
    assert domain.profile("3.4.1") is cached
    assert domain.profile("3.4.2").ds_version == "3.4.2"
    assert tuple(
        domain.profile(version).ds_version for version in TARGET_DS_VERSIONS
    ) == (TARGET_DS_VERSIONS)
    assert record_load.call_count == 1

    fresh = domain.fresh_profile("3.4.1")
    assert fresh is not cached
    assert fresh.source_contract_digest == cached.source_contract_digest
    assert fresh.recipe_id == cached.recipe_id
    assert fresh.program("get").fingerprint == cached.program("get").fingerprint
    assert fresh.program("get") is not cached.program("get")
    assert record_load.call_count == 2
    assert domain.profile("3.4.1") is cached


def test_first_domain_load_checks_other_exact_versions_and_does_not_cache_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    domain = replace(_CLUSTER_PROGRAMS)
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profiles["3.4.2"]["profile_digest"] = "sha256:" + "0" * 64

    with monkeypatch.context() as patch:
        patch.setattr(module, "PROFILES", profiles)
        with pytest.raises(WireContractError, match=r"profile 3\.4\.2 digest"):
            domain.profile("3.4.1")
        with pytest.raises(WireContractError, match=r"profile 3\.4\.2 digest"):
            domain.fresh_profile("3.4.1")

    assert domain.profile("3.4.1").ds_version == "3.4.1"


def test_cached_compiled_profiles_do_not_share_clients_or_credentials() -> None:
    compiled = _CLUSTER_PROGRAMS.profile("3.4.1")
    first_profile = make_profile(api_token="first-token")
    second_profile = make_profile(api_token="second-token")
    first_requests: list[httpx.Request] = []
    second_requests: list[httpx.Request] = []

    def first_handler(request: httpx.Request) -> httpx.Response:
        first_requests.append(request)
        return httpx.Response(
            200, json={"code": 0, "msg": "success", "data": {"id": 1, "code": 7}}
        )

    def second_handler(request: httpx.Request) -> httpx.Response:
        second_requests.append(request)
        return httpx.Response(
            200, json={"code": 0, "msg": "success", "data": {"id": 2, "code": 8}}
        )

    with (
        DolphinSchedulerClient(
            first_profile,
            transport=httpx.MockTransport(first_handler),
        ) as first_client,
        DolphinSchedulerClient(
            second_profile,
            transport=httpx.MockTransport(second_handler),
        ) as second_client,
    ):
        first = _CLUSTER_PROGRAMS.bind(
            compiled, first_profile, http_client=first_client
        )
        second = _CLUSTER_PROGRAMS.bind(
            compiled, second_profile, http_client=second_client
        )
        assert first is not second
        first_payload = first.call("get", {"clusterCode": 7})
        assert isinstance(first_payload, BaseModel)
        assert first_payload.model_dump()["code"] == 7
        assert len(first_requests) == 1
        assert second_requests == []
        second_payload = second.call("get", {"clusterCode": 8})
        assert isinstance(second_payload, BaseModel)
        assert second_payload.model_dump()["code"] == 8

    assert len(first_requests) == len(second_requests) == 1
    assert first_requests[0].headers["token"] == "first-token"
    assert second_requests[0].headers["token"] == "second-token"
    assert first_requests[0].url.params["clusterCode"] == "7"
    assert second_requests[0].url.params["clusterCode"] == "8"
