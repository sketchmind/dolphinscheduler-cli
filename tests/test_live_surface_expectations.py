from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live import test_admin_surfaces as admin_surfaces
from tests.live import test_governance_surfaces as governance_surfaces
from tests.live import test_runtime_control_surfaces as runtime_surfaces
from tests.live.support import DsctlCommandResult, LiveProfileConfig
from tests.live.task_group_support import require_task_group_project_cleanup_version
from tests.live.test_admin_surfaces import (
    _audit_rows,
    _complete_access_token_rows,
    _monitor_database_type,
    _validated_access_token_expire_time,
)
from tests.live.test_governance_surfaces import (
    _cleanup_user_project_resources,
    _safe_suffix,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def test_monitor_database_type_requires_explicit_normalized_expectation() -> None:
    assert _monitor_database_type(
        {"DS_LIVE_MONITOR_DATABASE_TYPE": " postgresql "}
    ) == ("POSTGRESQL")
    with pytest.raises(ValueError, match="DS_LIVE_MONITOR_DATABASE_TYPE"):
        _monitor_database_type({})


def test_audit_rows_accepts_empty_history_and_validates_present_rows() -> None:
    assert _audit_rows({"totalList": [], "total": 0}) == []
    row = {"modelType": "Project", "operation": "Create", "userName": "admin"}
    assert _audit_rows({"totalList": [row], "total": 1}) == [row]

    with pytest.raises(AssertionError, match="total"):
        _audit_rows({"totalList": [row], "total": 0})


@pytest.mark.parametrize(
    "ds_version",
    ["1.3.9", *(f"2.0.{patch}" for patch in range(10))],
)
def test_access_token_expiry_accepts_native_date_for_exact_legacy_versions(
    ds_version: str,
) -> None:
    observed = "2026-10-20T21:54:00.000+0000"
    assert (
        _validated_access_token_expire_time(
            observed,
            requested="2026-10-21 05:54:00",
            ds_version=ds_version,
        )
        == observed
    )


def test_access_token_expiry_requires_offset_legacy_and_exact_text_modern() -> None:
    with pytest.raises(AssertionError, match="offset"):
        _validated_access_token_expire_time(
            "2026-10-20T21:54:00",
            requested="2026-10-21 05:54:00",
            ds_version="2.0.9",
        )

    requested = "2026-10-21 05:54:00"
    assert (
        _validated_access_token_expire_time(
            requested,
            requested=requested,
            ds_version="3.0.0",
        )
        == requested
    )
    with pytest.raises(AssertionError):
        _validated_access_token_expire_time(
            "2026-10-20T21:54:00.000+0000",
            requested=requested,
            ds_version="3.0.0",
        )


def test_access_token_cleanup_is_registered_before_token_assertions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        if args[1] == "create":
            action = "access-token.create"
            data: dict[str, object] = {"id": 41, "token": ""}
        else:
            assert args == ["access-token", "delete", "41", "--force"]
            action = "access-token.delete"
            data = {"deleted": True}
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={"ok": True, "action": action, "data": data, "error": None},
        )

    monkeypatch.setattr(admin_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError, match="token string"):
        admin_surfaces.test_admin_access_token_lifecycle_round_trips(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_admin_profile=LiveProfileConfig(
                api_url="http://example.test",
                api_token="admin-token",
                ds_version="3.4.3",
            ),
        )

    assert len(calls) == 2
    assert calls[0][:-1] == (
        "access-token",
        "create",
        "--user",
        "admin",
        "--expire-time",
    )
    assert calls[1] == ("access-token", "delete", "41", "--force")


def test_complete_access_token_rows_requires_stable_full_coverage() -> None:
    row = {"id": 41, "token": "owned-token"}
    assert _complete_access_token_rows(
        {
            "totalList": [row],
            "total": 1,
            "coverage": {"scope_complete": True, "totals_changed": False},
        }
    ) == [row]

    for invalid in (
        {
            "totalList": [row],
            "total": 1,
            "coverage": {"scope_complete": False, "totals_changed": False},
        },
        {
            "totalList": [row],
            "total": 1,
            "coverage": {"scope_complete": True, "totals_changed": True},
        },
        {
            "totalList": [row],
            "total": 2,
            "coverage": {"scope_complete": True, "totals_changed": False},
        },
    ):
        with pytest.raises(AssertionError):
            _complete_access_token_rows(invalid)


def test_safe_suffix_hashes_the_complete_unique_name() -> None:
    first = _safe_suffix(
        lambda stem: f"campaign-one-{stem}-1",
        "worker-group",
    )
    second = _safe_suffix(
        lambda stem: f"campaign-two-{stem}-1",
        "worker-group",
    )

    assert first != second
    assert first == _safe_suffix(
        lambda stem: f"campaign-one-{stem}-1",
        "worker-group",
    )
    assert len(first) == 12
    assert set(first) <= set("0123456789abcdef")


def test_user_project_cleanup_continues_in_dependency_order() -> None:
    calls: list[str] = []

    def token() -> None:
        calls.append("token")
        message = "token cleanup failed"
        raise RuntimeError(message)

    def user() -> None:
        calls.append("user")

    def project() -> None:
        calls.append("project")
        message = "project cleanup failed"
        raise AssertionError(message)

    def tenant() -> None:
        calls.append("tenant")

    with pytest.raises(ExceptionGroup) as exc_info:
        _cleanup_user_project_resources(
            token=token,
            user=user,
            project=project,
            tenant=tenant,
        )

    assert calls == ["token", "user", "project", "tenant"]
    assert [type(error) for error in exc_info.value.exceptions] == [
        RuntimeError,
        AssertionError,
    ]


@pytest.mark.parametrize("failed_kind", ["access-token", "user"])
@pytest.mark.parametrize("failure", ["transport", "unconfirmed"])
def test_explicit_governance_delete_is_not_retried_after_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failed_kind: str, failure: str
) -> None:
    # Exercise the entire scenario so its finalizer observes the real registrations.
    suffix = _safe_suffix(lambda stem: "owned-scenario", "user")
    user = f"dslvu{suffix}"
    project = f"dsctl-admin-project-{suffix}"
    responses: list[tuple[str, dict[str, object], str | None]] = [
        ("tenant.create", {"id": 1}, None),
        (
            "tenant.list",
            {
                "totalList": [{"tenantCode": "bootstrap-tenant"}],
                "total": 1,
                "coverage": {
                    "scope_complete": True,
                    "totals_changed": False,
                },
            },
            None,
        ),
        ("user.create", {}, "permission_denied"),
        ("user.create", {"id": 2}, None),
        (
            "user.get",
            {"id": 2, "userName": user, "tenantId": 1, "tenantCode": None},
            None,
        ),
        ("user.list", {"totalList": [{"id": 2}]}, None),
        ("user.update", {"id": 2, "phone": "13800000000"}, None),
        ("access-token.create", {"id": 3, "token": "test-token"}, None),
        (
            "project.create",
            {
                "code": 4,
                "name": project,
                "description": "live admin grant project path",
            },
            None,
        ),
        ("project.list", {"totalList": []}, None),
        ("user.grant.project", {"granted": True, "permission": "write"}, None),
        ("project.get", {"name": project}, None),
        ("user.revoke.project", {"revoked": True}, None),
        ("project.get", {}, "not_found"),
    ]
    pending = iter(responses)
    deletes: list[str] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        action = ".".join(args[:3] if args[1] in {"grant", "revoke"} else args[:2])
        if args[1] == "delete":
            deletes.append(args[0])
            if args[0] == failed_kind and failure == "transport":
                message = "delete response lost"
                raise RuntimeError(message)
            data: dict[str, object] = {"deleted": args[0] != failed_kind}
            error = None
        else:
            expected, data, error = next(pending)
            assert action == expected
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0 if error is None else 1,
            stdout="",
            stderr="",
            payload={
                "ok": error is None,
                "action": action,
                "data": data,
                "error": {"type": error},
            },
        )

    monkeypatch.setattr(governance_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(RuntimeError if failure == "transport" else AssertionError):
        governance_surfaces.test_admin_user_lifecycle_and_project_grant_effect(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_admin_profile=LiveProfileConfig(
                api_url="http://example.test", api_token="admin-test-token"
            ),
            live_etl_env_file=tmp_path / "etl.env",
            live_bootstrap_state=type(
                "BootstrapState", (), {"tenant_code": "bootstrap-tenant"}
            )(),
            live_name_factory=lambda stem: "owned-scenario",
            tmp_path=tmp_path,
        )

    assert list(pending) == []
    assert deletes == ["access-token", "user", "project", "tenant"]


@pytest.mark.parametrize("native_identity", [{"id": 4}, {"code": 4}])
def test_governance_project_create_validates_stable_identity_without_aliasing_native_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    native_identity: dict[str, int],
) -> None:
    project = "owned-project"
    description = "owned project description"
    registered: list[bool] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        assert args == [
            "project",
            "create",
            "--name",
            project,
            "--description",
            description,
        ]
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={
                "ok": True,
                "action": "project.create",
                "data": {
                    **native_identity,
                    "name": project,
                    "description": description,
                },
                "error": None,
            },
        )

    monkeypatch.setattr(governance_surfaces, "run_dsctl", run_dsctl)
    governance_surfaces._create_project(
        tmp_path,
        tmp_path / "admin.env",
        name=project,
        description=description,
        on_created=lambda: registered.append(True),
    )

    assert registered == [True]


@pytest.mark.parametrize(
    ("visible", "error_type", "expected"),
    [
        (False, "permission_denied", "permission_denied"),
        (False, None, "not_found"),
        (True, None, "permission_denied"),
    ],
)
def test_tenant_denial_tracks_current_queue_visibility(
    *, visible: bool, error_type: str | None, expected: str
) -> None:
    result = DsctlCommandResult(
        argv=(),
        exit_code=int(error_type is not None),
        stdout="",
        stderr="",
        payload={
            "ok": error_type is None,
            "action": "queue.list",
            "error": {"type": error_type},
            "data": {
                "totalList": [{"id": 7}] if visible else [],
                "total": int(visible),
                "coverage": {"scope_complete": True, "totals_changed": False},
            },
        },
    )
    assert (
        governance_surfaces._tenant_create_denial_from_queue_visibility(
            result, queue_id=7
        )
        == expected
    )


@pytest.mark.parametrize(
    ("visible", "error_type", "expected"),
    [
        (False, "permission_denied", "permission_denied"),
        (False, None, "not_found"),
        (True, None, "permission_denied"),
    ],
)
def test_user_denial_tracks_current_tenant_visibility(
    *, visible: bool, error_type: str | None, expected: str
) -> None:
    tenant_code = "owned-tenant"
    result = DsctlCommandResult(
        argv=(),
        exit_code=int(error_type is not None),
        stdout="",
        stderr="",
        payload={
            "ok": error_type is None,
            "action": "tenant.list",
            "error": {"type": error_type},
            "data": {
                "totalList": [{"tenantCode": tenant_code}] if visible else [],
                "total": int(visible),
                "coverage": {
                    "scope_complete": True,
                    "totals_changed": False,
                },
            },
        },
    )

    assert (
        governance_surfaces._user_create_denial_from_tenant_visibility(
            result,
            tenant_code=tenant_code,
        )
        == expected
    )


def test_user_not_found_denial_must_name_the_invisible_tenant() -> None:
    tenant_code = "owned-tenant"

    def result(details: dict[str, str]) -> DsctlCommandResult:
        return DsctlCommandResult(
            argv=(),
            exit_code=1,
            stdout="",
            stderr="",
            payload={
                "ok": False,
                "action": "user.create",
                "error": {"type": "not_found", "details": details},
            },
        )

    governance_surfaces._require_user_create_denial(
        result({"resource": "tenant", "tenantCode": tenant_code}),
        expected_type="not_found",
        tenant_code=tenant_code,
    )
    with pytest.raises(AssertionError):
        governance_surfaces._require_user_create_denial(
            result({"resource": "tenant", "tenantCode": "other-tenant"}),
            expected_type="not_found",
            tenant_code=tenant_code,
        )


@pytest.mark.parametrize("error_type", ["not_found", "api_transport_error"])
def test_user_denial_rejects_unrelated_tenant_list_failures(error_type: str) -> None:
    result = DsctlCommandResult(
        argv=(),
        exit_code=1,
        stdout="",
        stderr="",
        payload={"ok": False, "action": "tenant.list", "error": {"type": error_type}},
    )
    with pytest.raises(AssertionError, match="expected 'permission_denied'"):
        governance_surfaces._user_create_denial_from_tenant_visibility(
            result,
            tenant_code="owned-tenant",
        )


@pytest.mark.parametrize("error_type", ["not_found", "api_transport_error"])
def test_tenant_denial_rejects_unrelated_queue_failures(error_type: str) -> None:
    result = DsctlCommandResult(
        argv=(),
        exit_code=1,
        stdout="",
        stderr="",
        payload={"ok": False, "action": "queue.list", "error": {"type": error_type}},
    )
    with pytest.raises(AssertionError, match="expected 'permission_denied'"):
        governance_surfaces._tenant_create_denial_from_queue_visibility(
            result, queue_id=7
        )


def test_worker_address_is_discovered_from_monitor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        assert args == ["monitor", "server", "worker"]
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={
                "ok": True,
                "action": "monitor.server",
                "data": [{"host": "worker.example", "port": 1234}],
            },
        )

    monkeypatch.setattr(governance_surfaces, "run_dsctl", run_dsctl)
    assert (
        governance_surfaces._worker_address(tmp_path, tmp_path / "admin.env")
        == "worker.example:1234"
    )


@pytest.mark.parametrize("queue_denied", [False, True])
def test_tenant_scenario_rejects_denial_that_disagrees_with_queue_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, queue_denied: bool
) -> None:
    calls: list[str] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        action = ".".join(args[:2])
        calls.append(action)
        error = None
        if action == "queue.get":
            data: dict[str, object] = {"id": 7}
        elif action == "queue.list":
            error = "permission_denied" if queue_denied else None
            data = {
                "total": 0,
                "totalList": [],
                "coverage": {"scope_complete": True, "totals_changed": False},
            }
        else:
            assert action == "tenant.create"
            assert args[args.index("--queue") + 1] == "7"
            error = "not_found" if queue_denied else "permission_denied"
            data = {}
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=int(error is not None),
            stdout="",
            stderr="",
            payload={
                "ok": error is None,
                "action": action,
                "data": data,
                "error": {"type": error},
            },
        )

    monkeypatch.setattr(governance_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError, match="etl tenant create returned error type"):
        governance_surfaces.test_admin_tenant_lifecycle_round_trips_and_etl_is_denied(
            tmp_path, tmp_path / "admin.env", tmp_path / "etl.env", lambda stem: stem
        )
    assert calls == ["queue.get", "queue.list", "tenant.create"]


def test_task_group_lifecycle_stops_legacy_profile_before_cluster_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def reject_cluster_io(*args: object, **kwargs: object) -> DsctlCommandResult:
        del args, kwargs
        pytest.fail("legacy task-group cleanup gate allowed cluster I/O")

    monkeypatch.setattr(governance_surfaces, "run_dsctl", reject_cluster_io)
    with pytest.raises(pytest.skip.Exception, match="no task-group delete REST"):
        governance_surfaces.test_etl_task_group_lifecycle_round_trips_with_project_grant(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_admin_profile=LiveProfileConfig(
                api_url="http://example.test",
                api_token="admin-token",
                ds_version="3.1.9",
            ),
            live_bootstrap_state=type("Bootstrap", (), {"user_name": "etl"})(),
            live_etl_env_file=tmp_path / "etl.env",
            live_run_prefix="legacy-task-group",
        )


def test_runtime_task_group_lifecycle_stops_legacy_profile_before_cluster_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def reject_cluster_io(*args: object, **kwargs: object) -> DsctlCommandResult:
        del args, kwargs
        pytest.fail("legacy runtime task-group gate allowed cluster I/O")

    monkeypatch.setattr(runtime_surfaces, "run_dsctl", reject_cluster_io)
    with pytest.raises(pytest.skip.Exception, match="no task-group delete REST"):
        runtime_surfaces.test_etl_task_group_queue_control_surfaces_round_trip(
            live_repo_root=tmp_path,
            live_etl_ds_version="3.1.9",
            live_etl_env_file=tmp_path / "etl.env",
            live_name_factory=lambda stem: stem,
            tmp_path=tmp_path,
        )


@pytest.mark.parametrize("ds_version", [None, "auto"])
def test_task_group_cleanup_gate_rejects_offline_default_version(
    tmp_path: Path,
    *,
    ds_version: str | None,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path,
        args: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=0,
            stdout="",
            stderr="",
            payload={
                "ok": True,
                "action": "version",
                "data": {
                    "selected_ds_version": "3.4.1",
                    "version_source": "offline_default",
                },
            },
        )

    with pytest.raises(AssertionError, match="did not prove"):
        require_task_group_project_cleanup_version(
            tmp_path,
            tmp_path / "etl.env",
            ds_version,
            execute=run_dsctl,
        )
    assert calls == [("version",)]


@pytest.mark.parametrize("residue", [False, True])
def test_task_group_failure_cleanup_proves_absence_and_rejects_residue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    residue: bool,
) -> None:
    calls: list[tuple[tuple[str, ...], Path]] = []

    def fail_after_project_creation(
        repo_root: Path,
        admin_env_file: Path,
        *,
        name: str,
        description: str,
        on_created: Callable[[], None] | None = None,
    ) -> None:
        del repo_root, admin_env_file, name, description
        assert on_created is not None
        on_created()
        message = "scenario assertion failed after project creation"
        raise RuntimeError(message)

    def run_dsctl(
        repo_root: Path,
        args: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root
        calls.append((tuple(args), env_file))
        if args[:2] == ["project", "delete"]:
            exit_code = 0
            payload: dict[str, object] = {
                "ok": True,
                "action": "project.delete",
                "data": {"deleted": True},
            }
        elif args[:2] == ["task-group", "get"]:
            exit_code = 0 if residue else 1
            payload = {
                "ok": residue,
                "action": "task-group.get",
                "data": {"name": args[2]} if residue else None,
                "error": None if residue else {"type": "not_found"},
            }
        else:
            assert args[:2] == ["task-group", "list"]
            rows = [{"name": args[args.index("--search") + 1]}] if residue else []
            exit_code = 0
            payload = {
                "ok": True,
                "action": "task-group.list",
                "data": {
                    "totalList": rows,
                    "total": len(rows),
                    "coverage": {
                        "scope_complete": True,
                        "totals_changed": False,
                    },
                },
            }
        return DsctlCommandResult(
            argv=tuple(args),
            exit_code=exit_code,
            stdout="",
            stderr="",
            payload=payload,
        )

    monkeypatch.setattr(
        governance_surfaces, "_create_project", fail_after_project_creation
    )
    monkeypatch.setattr(governance_surfaces, "run_dsctl", run_dsctl)

    expected_error = ExceptionGroup if residue else RuntimeError
    with pytest.raises(expected_error) as exc_info:
        governance_surfaces.test_etl_task_group_lifecycle_round_trips_with_project_grant(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_admin_profile=LiveProfileConfig(
                api_url="http://example.test",
                api_token="admin-token",
                ds_version="3.2.0",
            ),
            live_bootstrap_state=type("Bootstrap", (), {"user_name": "etl"})(),
            live_etl_env_file=tmp_path / "etl.env",
            live_run_prefix="modern-task-group",
        )

    assert [args[:2] for args, _ in calls] == [
        ("project", "delete"),
        ("task-group", "get"),
        ("task-group", "list"),
    ]
    assert calls[0][1] == tmp_path / "admin.env"
    assert calls[1][1] == calls[2][1] == tmp_path / "etl.env"
    if residue:
        assert isinstance(exc_info.value, ExceptionGroup)
        assert len(exc_info.value.exceptions) == 2
