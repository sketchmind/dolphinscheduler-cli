from __future__ import annotations

import hashlib
import importlib
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.upstream.clusters import (
    _CLUSTER_PROGRAMS,
    ClusterAdapter,
)
from dsctl.upstream.wire import WireContractError, WireExecutor
from tests.support import make_profile

_ZERO_DIGEST = "sha256:" + "0" * 64
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_ROOT = _REPO_ROOT / "src"
_NO_EXACT_IMPORT_PROBE = f"""
import importlib
import sys

sys.path[:0] = [{str(_SOURCE_ROOT)!r}, {str(_REPO_ROOT)!r}]

import httpx

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from tests.support import make_profile


def exact_modules():
    return sorted(
        name
        for name in sys.modules
        if name.startswith("dsctl.generated.versions.")
    )


def reject_io(request):
    raise AssertionError(
        f"binding unexpectedly sent {{request.method}} {{request.url}}"
    )


module_name, domain_name, adapter_name, absent_csv = sys.argv[1:]
absent_versions = set(filter(None, absent_csv.split(",")))
assert exact_modules() == []
module = importlib.import_module(module_name)
assert exact_modules() == [], (module_name, "module import", exact_modules())
domain = getattr(module, domain_name)
adapter_class = getattr(module, adapter_name)

for version in TARGET_DS_VERSIONS:
    assert exact_modules() == [], (module_name, version, "before", exact_modules())
    profile = make_profile(ds_version=version)
    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(reject_io),
    )
    with client:
        if version in absent_versions:
            try:
                domain.bind(profile, http_client=client)
            except UnsupportedFeatureError:
                assert exact_modules() == [], (
                    module_name,
                    version,
                    "after absent bind",
                    exact_modules(),
                )
            else:
                raise AssertionError((module_name, version, "expected absent"))
        else:
            adapter = adapter_class.for_version(version)
            assert adapter.ds_version == version
            assert exact_modules() == [], (
                module_name,
                version,
                "after for_version",
                exact_modules(),
            )
            adapter.bind(profile, http_client=client)
            assert exact_modules() == [], (
                module_name,
                version,
                "after bind",
                exact_modules(),
            )
    assert exact_modules() == [], (module_name, version, "after", exact_modules())
"""


def test_compiled_domain_rejects_an_unreviewed_version_decision() -> None:
    with pytest.raises(WireContractError, match="capability decision"):
        ClusterAdapter.for_version("9.9.9")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
            1,
            "artifact schema is unsupported",
        ),
        (
            "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
            2,
            "artifact schema is unsupported",
        ),
        (
            "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
            3,
            "artifact schema is unsupported",
        ),
        (
            "COMPILED_WIRE_ARTIFACT_SCHEMA_VERSION",
            4,
            "artifact schema is unsupported",
        ),
        ("RENDERER_ABI", 1, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 2, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 3, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 4, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 5, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 6, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 7, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 8, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 9, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 10, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 11, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 12, "renderer ABI is unsupported"),
        ("RENDERER_ABI", 13, "renderer ABI is unsupported"),
        ("CONTENT_DIGEST", _ZERO_DIGEST, "content digest does not match"),
        (
            "MODULES",
            ("__init__.py",),
            "domain/support module inventory is inconsistent",
        ),
        (
            "WIRE_RUNTIME_CONTENT_DIGEST",
            _ZERO_DIGEST,
            "targets a different wire runtime",
        ),
    ],
)
def test_compiled_wire_manifest_tampering_fails_closed(
    field: str,
    value: object,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = importlib.import_module("dsctl.generated.wire_programs._manifest")
    monkeypatch.setattr(manifest, field, value)

    with pytest.raises(WireContractError, match=message):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize("digest_version", [1, 2, 3, 4])
def test_compiled_wire_manifest_rejects_an_old_content_digest_domain(
    digest_version: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = importlib.import_module("dsctl.generated.wire_programs._manifest")
    assert manifest.__file__ is not None
    package_root = Path(manifest.__file__).parent
    digest = hashlib.sha256()
    digest.update(f"dsctl-compiled-wire-artifact-v{digest_version}\0".encode())
    for module_name in manifest.MODULES:
        digest.update(module_name.encode("utf-8"))
        digest.update(b"\0")
        digest.update((package_root / module_name).read_bytes())
        digest.update(b"\0")
    monkeypatch.setattr(manifest, "CONTENT_DIGEST", f"sha256:{digest.hexdigest()}")

    with pytest.raises(WireContractError, match="content digest does not match"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_profile_digest_mismatch_is_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profiles["3.4.1"]["profile_digest"] = _ZERO_DIGEST
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match=r"profile 3\.4\.1 digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_fresh_profile_revalidates_without_changing_cached_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached_profile = _CLUSTER_PROGRAMS.profile("3.4.1")
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profiles["3.4.1"]["profile_digest"] = _ZERO_DIGEST
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match=r"profile 3\.4\.1 digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")

    assert _CLUSTER_PROGRAMS.profile("3.4.1") is cached_profile


def test_compiled_recipe_selection_is_bound_to_the_profile_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profiles["3.4.1"]["recipe_id"] = "process_definitions_void_mutations"
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match=r"profile 3\.4\.1 digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize(
    ("version", "recipe_id"),
    [
        ("3.4.1", None),
        ("3.4.1", ""),
        ("3.4.1", "not-a-recipe"),
        ("1.3.9", "unexpected"),
    ],
)
def test_compiled_profile_rejects_inconsistent_recipe_identity(
    version: str,
    recipe_id: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profiles[version]["recipe_id"] = recipe_id
    profiles[version]["profile_digest"] = _profile_digest(profiles[version])
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match="recipe"):
        _CLUSTER_PROGRAMS.fresh_profile(version)


@pytest.mark.parametrize(
    ("domain", "adapter_module"),
    [
        ("cluster", "clusters"),
        ("environment", "environments"),
        ("worker_group", "worker_groups"),
        ("alert_group", "alert_groups"),
        ("tenant", "tenants"),
        ("queue", "queues"),
        ("alert_plugin", "alert_plugins"),
        ("access_token", "access_tokens"),
        ("namespace", "namespaces"),
        ("task_group", "task_groups"),
        ("user", "users"),
        ("audit", "observability"),
        ("task_type", "task_type_inventory"),
        ("monitor", "observability"),
    ],
)
def test_compiled_adapters_reject_an_unknown_recipe(
    domain: str,
    adapter_module: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module(f"dsctl.generated.wire_programs.{domain}")
    profiles = deepcopy(module.PROFILES)
    profiles["3.4.1"]["recipe_id"] = "unreviewed_recipe"
    profiles["3.4.1"]["profile_digest"] = _profile_digest(profiles["3.4.1"])
    monkeypatch.setattr(module, "PROFILES", profiles)
    adapter = importlib.import_module(f"dsctl.upstream.{adapter_module}")
    programs = getattr(adapter, f"_{domain.upper()}_PROGRAMS")
    fresh_profile = programs.fresh_profile("3.4.1")

    recipe_function = {
        "audit": "_audit_recipe_for_profile",
        "monitor": "_monitor_recipe_for_profile",
        "task_type": "_category_available",
    }.get(domain, "_recipe_for_profile")
    with pytest.raises(WireContractError, match=r"recipe.*unsupported"):
        getattr(adapter, recipe_function)(fresh_profile)


@pytest.mark.parametrize(
    ("selected_version", "client_version"),
    [("3.4.2", "3.4.1"), ("3.4.1", "3.4.2")],
)
def test_compiled_domain_bind_rejects_version_mismatch_before_io(
    selected_version: str,
    client_version: str,
) -> None:
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(500)

    selected_profile = make_profile(ds_version=selected_version)
    http_client = DolphinSchedulerClient(
        make_profile(ds_version=client_version),
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(WireContractError, match="selected client profile"):
        ClusterAdapter.for_version("3.4.1").bind(
            selected_profile,
            http_client=http_client,
        )

    assert requests_seen == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [("program_digest", _ZERO_DIGEST), ("source_operation", "OtherController.get")],
)
def test_compiled_wire_program_digest_mismatch_is_zero_io(
    field: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profile = profiles["3.4.1"]
    profile["programs"]["get"][field] = value
    profile["profile_digest"] = _profile_digest(profile)
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match="get program digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("path", "cluster/query-by-code-v2"),
        ("method", "POST"),
        ("channel", "form"),
        ("capture", {"tampered": True}),
        ("response_projection", "status_data"),
        ("response_projection", "single_data"),
    ],
)
def test_compiled_wire_codec_record_tampering_is_zero_io(
    field: str,
    value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    codecs = deepcopy(module.CODECS)
    codecs["get_workflow"][field] = value
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError, match="get program digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize("projection", [None, ["direct"], "single_data_list"])
def test_compiled_wire_codec_projection_is_validated_before_loading(
    projection: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    codecs = deepcopy(module.CODECS)
    codecs["get_workflow"]["response_projection"] = projection
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError, match="response projection is unsupported"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_source_digest_is_bound_to_exact_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profile = profiles["3.4.1"]
    profile["source"]["contract_digest"] = _ZERO_DIGEST
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match=r"profile 3\.4\.1 digest"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_source_only_profile_change_cannot_reuse_prepared_runtime_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = _CLUSTER_PROGRAMS.fresh_profile("3.4.1")
    original_program = original.program("get")
    prepared = original_program.prepare({"clusterCode": 7})
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    original_program_records = deepcopy(module.PROFILES["3.4.1"]["programs"])
    profiles = deepcopy(module.PROFILES)
    profile = profiles["3.4.1"]
    profile["source"]["contract_digest"] = _ZERO_DIGEST
    profile["profile_digest"] = _profile_digest(profile)
    monkeypatch.setattr(module, "PROFILES", profiles)

    changed = _CLUSTER_PROGRAMS.fresh_profile("3.4.1")
    changed_program = changed.program("get")
    assert profile["programs"] == original_program_records
    assert (
        changed_program.request_codec_fingerprint
        == original_program.request_codec_fingerprint
    )
    assert changed_program.fingerprint != original_program.fingerprint
    assert changed.source_contract_digest == _ZERO_DIGEST
    requests: list[httpx.Request] = []

    def reject_io(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        message = "source mismatch must fail before HTTP"
        raise AssertionError(message)

    with DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(reject_io)
    ) as client:
        with pytest.raises(WireContractError, match="source contract does not match"):
            WireExecutor(
                client, source_contract_digest=changed.source_contract_digest
            ).execute(original_program, prepared)
        with pytest.raises(
            WireContractError, match="does not match the selected program"
        ):
            WireExecutor(
                client, source_contract_digest=changed.source_contract_digest
            ).execute(changed_program, prepared)
    assert requests == []


def test_compiled_wire_request_schema_registry_mismatch_is_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    schemas = dict(module.REQUEST_SCHEMAS)
    schemas["code"] = schemas["create"]
    monkeypatch.setattr(module, "REQUEST_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="get_process request schema is invalid",
    ):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_response_schema_registry_mismatch_is_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    schemas = dict(module.RESPONSE_SCHEMAS)
    schemas["get_workflow"] = schemas["page_workflow"]
    monkeypatch.setattr(module, "RESPONSE_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="get_workflow response schema is invalid",
    ):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_schema_entries_bind_generated_source_and_strictness() -> None:
    cluster_module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    loose_schema = cluster_module.RESPONSE_SCHEMAS["page_process"]
    strict_schema = cluster_module.RESPONSE_SCHEMAS["page_process_strict"]
    loose_module = importlib.import_module(loose_schema.annotation.__module__)
    strict_module = importlib.import_module(strict_schema.annotation.__module__)

    assert loose_schema.annotation is loose_module.PageInfoClusterDto
    assert strict_schema.annotation is strict_module.PageInfoClusterDto
    assert loose_schema.digest == loose_module.EXECUTABLE_SCHEMA_DIGEST
    assert strict_schema.digest == strict_module.EXECUTABLE_SCHEMA_DIGEST
    assert strict_schema.digest != loose_schema.digest


def test_compiled_wire_codec_schema_identity_mismatch_is_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    codecs = deepcopy(module.CODECS)
    codecs["get_workflow"]["response_schema_digest"] = _ZERO_DIGEST
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError, match="response schema is invalid"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


def test_compiled_wire_program_schema_identity_mismatch_is_zero_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.cluster")
    profiles = deepcopy(module.PROFILES)
    profile = profiles["3.4.1"]
    program = profile["programs"]["get"]
    program["response_schema_digest"] = _ZERO_DIGEST
    program["program_digest"] = _program_digest(
        program,
        module.CODECS[program["codec"]],
    )
    profile["profile_digest"] = _profile_digest(profile)
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match="response schema does not match codec"):
        _CLUSTER_PROGRAMS.fresh_profile("3.4.1")


@pytest.mark.parametrize(
    ("module_name", "domain_name", "adapter_name", "absent_versions"),
    [
        pytest.param(
            "dsctl.upstream.clusters",
            "CLUSTER_DOMAIN",
            "ClusterAdapter",
            frozenset(
                {
                    "1.3.9",
                    *(f"2.0.{patch}" for patch in range(10)),
                    *(f"3.0.{patch}" for patch in range(7)),
                }
            ),
            id="cluster",
        ),
        pytest.param(
            "dsctl.upstream.environments",
            "ENVIRONMENT_DOMAIN",
            "EnvironmentAdapter",
            frozenset({"1.3.9"}),
            id="environment",
        ),
        pytest.param(
            "dsctl.upstream.worker_groups",
            "WORKER_GROUP_DOMAIN",
            "WorkerGroupAdapter",
            frozenset(),
            id="worker-group",
        ),
        pytest.param(
            "dsctl.upstream.alert_groups",
            "ALERT_GROUP_DOMAIN",
            "AlertGroupAdapter",
            frozenset(),
            id="alert-group",
        ),
        pytest.param(
            "dsctl.upstream.namespaces",
            "NAMESPACE_DOMAIN",
            "NamespaceAdapter",
            frozenset({"1.3.9", *(f"2.0.{patch}" for patch in range(10))}),
            id="namespace",
        ),
        pytest.param(
            "dsctl.upstream.tenants",
            "TENANT_DOMAIN",
            "TenantAdapter",
            frozenset(),
            id="tenant",
        ),
        pytest.param(
            "dsctl.upstream.queues",
            "QUEUE_DOMAIN",
            "QueueAdapter",
            frozenset(),
            id="queue",
        ),
        pytest.param(
            "dsctl.upstream.alert_plugins",
            "ALERT_PLUGIN_DOMAIN",
            "AlertPluginAdapter",
            # 1.3.9 binds unavailable operations; the domain tests check them.
            frozenset(),
            id="alert-plugin",
        ),
        pytest.param(
            "dsctl.upstream.users",
            "USER_DOMAIN",
            "UserAdapter",
            frozenset(),
            id="user",
        ),
        pytest.param(
            "dsctl.upstream.observability",
            "AUDIT_DOMAIN",
            "AuditAdapter",
            # Absent versions bind unavailable operations without an exact package.
            frozenset(),
            id="audit",
        ),
        pytest.param(
            "dsctl.upstream.task_type_inventory",
            "TASK_TYPE_DOMAIN",
            "TaskTypeAdapter",
            frozenset(
                {
                    "1.3.9",
                    *(f"2.0.{patch}" for patch in range(10)),
                    *(f"3.0.{patch}" for patch in range(7)),
                }
            ),
            id="task-type",
        ),
        pytest.param(
            "dsctl.upstream.observability",
            "MONITOR_DOMAIN",
            "MonitorAdapter",
            frozenset(),
            id="monitor",
        ),
    ],
)
def test_compiled_domain_never_imports_an_exact_generated_package(
    module_name: str,
    domain_name: str,
    adapter_name: str,
    absent_versions: frozenset[str],
) -> None:
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and probe source
        [
            sys.executable,
            "-I",
            "-c",
            _NO_EXACT_IMPORT_PROBE,
            module_name,
            domain_name,
            adapter_name,
            ",".join(sorted(absent_versions)),
        ],
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


def _profile_digest(profile: dict[str, object]) -> str:
    payload = {
        "schema_version": 2,
        "status": profile["status"],
        "source": profile["source"],
        "recipe_id": profile["recipe_id"],
        "programs": profile["programs"],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _program_digest(
    program: dict[str, Any],
    codec_record: dict[str, Any],
) -> str:
    payload = {
        "schema_version": 4,
        "codec_record": codec_record,
        **{
            field: program[field]
            for field in (
                "source_operation",
                "codec",
                "codec_digest",
                "request_schema_digest",
                "response_digest",
                "response_schema_digest",
                "result_envelope",
            )
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
