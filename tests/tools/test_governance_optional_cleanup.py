from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from tests.live import test_governance_optional_surfaces as optional_surfaces
from tests.live.support import DsctlCommandResult, LiveBootstrapState

if TYPE_CHECKING:
    from pathlib import Path


def _result(
    action: str,
    *,
    data: object | None = None,
    error_type: str | None = None,
) -> DsctlCommandResult:
    payload: dict[str, object] = (
        {"ok": True, "action": action, "data": data}
        if error_type is None
        else {"ok": False, "action": action, "error": {"type": error_type}}
    )
    return DsctlCommandResult(
        argv=(),
        exit_code=0 if error_type is None else 1,
        stdout="",
        stderr="",
        payload=payload,
    )


def _bootstrap(tmp_path: Path) -> LiveBootstrapState:
    return LiveBootstrapState(
        admin_env_file=tmp_path / "admin.env",
        etl_env_file=tmp_path / "etl.env",
        tenant_code=None,
        user_name="managed-user",
        password=None,
        access_token_id=None,
        token=None,
        used_existing_etl_profile=False,
    )


def _config() -> optional_surfaces.LiveDatasourceConfig:
    return optional_surfaces.LiveDatasourceConfig(
        type="POSTGRESQL",
        host="db.example",
        port=5432,
        database="live",
        user_name="tester",
        password="secret",
    )


def test_datasource_create_assertion_failure_still_deletes_owned_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        if args == ["version"]:
            return _result("version", data={"selected_ds_version": "3.4.1"})
        if args[:2] == ["datasource", "create"]:
            authored = json.loads((tmp_path / "datasource.json").read_text())
            assert authored["type"] == "POSTGRESQL"
            return _result(
                "datasource.create",
                data={"id": 42, "name": "datasource", "type": "MYSQL"},
            )
        if args[:2] == ["datasource", "delete"]:
            return _result("datasource.delete", data={"deleted": True})
        if args[:2] == ["datasource", "get"]:
            return _result("datasource.get", error_type="not_found")
        message = f"Unexpected command {args}"
        raise AssertionError(message)

    monkeypatch.setattr(optional_surfaces, "_load_live_datasource_config", _config)
    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError, match="POSTGRESQL"):
        optional_surfaces.test_admin_datasource_lifecycle_and_user_grant_round_trip(
            tmp_path,
            tmp_path / "admin.env",
            _bootstrap(tmp_path),
            lambda stem: stem,
            tmp_path,
        )
    assert calls[-2:] == [
        ("datasource", "delete", "42", "--force"),
        ("datasource", "get", "42"),
    ]


@pytest.mark.parametrize("revoke_fails", [False, True])
def test_uncertain_grant_is_revoked_once_and_delete_is_verified(  # noqa: C901
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    revoke_fails: bool,
) -> None:
    calls: list[tuple[str, ...]] = []
    authorization = {42}
    created = False

    def run_dsctl(  # noqa: C901
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        nonlocal created
        del repo_root, env_file
        calls.append(tuple(args))
        if args == ["version"]:
            return _result("version", data={"selected_ds_version": "3.4.1"})
        if args[:2] == ["datasource", "create"]:
            created = True
            return _result(
                "datasource.create",
                data={"id": 42, "name": "datasource", "type": "POSTGRESQL"},
            )
        if args[:2] == ["datasource", "get"]:
            if not created or args[2] == "42":
                return _result("datasource.get", error_type="not_found")
            return _result(
                "datasource.get",
                data={
                    "id": 42,
                    "type": "POSTGRESQL",
                    "database": "live",
                    "name": args[2],
                    "note": "live datasource update path",
                },
            )
        if args[:2] == ["datasource", "list"]:
            return _result("datasource.list", data={"totalList": [{"id": 42}]})
        if args[:2] == ["datasource", "update"]:
            return _result(
                "datasource.update", data={"id": 42, "name": "datasource-updated"}
            )
        if args[:2] == ["datasource", "test"]:
            return _result("datasource.test", data={"connected": True})
        if args[:3] == ["user", "grant", "datasource"]:
            return _result("user.grant.datasource", error_type="conflict")
        if args[:3] == ["user", "revoke", "datasource"]:
            if revoke_fails:
                return _result("user.revoke.datasource", error_type="conflict")
            authorization.clear()
            return _result("user.revoke.datasource", data={"datasources": []})
        if args[:2] == ["datasource", "delete"]:
            return _result("datasource.delete", data={"deleted": True})
        message = f"Unexpected command {args}"
        raise AssertionError(message)

    monkeypatch.setattr(optional_surfaces, "_load_live_datasource_config", _config)
    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    if revoke_fails:
        with pytest.raises(ExceptionGroup) as caught:
            optional_surfaces.test_admin_datasource_lifecycle_and_user_grant_round_trip(
                tmp_path,
                tmp_path / "admin.env",
                _bootstrap(tmp_path),
                lambda stem: stem,
                tmp_path,
            )
        assert isinstance(caught.value.__context__, AssertionError)
        assert len(caught.value.exceptions) == 1
        assert isinstance(caught.value.exceptions[0], AssertionError)
    else:
        with pytest.raises(AssertionError, match="user grant datasource"):
            optional_surfaces.test_admin_datasource_lifecycle_and_user_grant_round_trip(
                tmp_path,
                tmp_path / "admin.env",
                _bootstrap(tmp_path),
                lambda stem: stem,
                tmp_path,
            )
        assert authorization == set()
    assert sum(call[:3] == ("user", "grant", "datasource") for call in calls) == 1
    assert sum(call[:3] == ("user", "revoke", "datasource") for call in calls) == 1
    assert calls[-2:] == [
        ("datasource", "delete", "42", "--force"),
        ("datasource", "get", "42"),
    ]


def test_namespace_cluster_is_cleaned_when_create_response_cannot_be_decoded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        if args[:2] == ["cluster", "create"]:
            return _result("cluster.create", data={"name": "namespace-cluster"})
        if args[:2] == ["namespace", "delete"]:
            return _result("namespace.delete", error_type="not_found")
        if args[:2] == ["namespace", "get"]:
            return _result("namespace.get", error_type="not_found")
        if args[:2] == ["cluster", "delete"]:
            return _result("cluster.delete", data={"deleted": True})
        if args[:2] == ["cluster", "get"]:
            return _result("cluster.get", error_type="not_found")
        message = f"Unexpected command {args}"
        raise AssertionError(message)

    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(TypeError, match="namespace probe cluster code"):
        optional_surfaces.test_admin_namespace_missing_k8s_probe(
            tmp_path,
            tmp_path / "admin.env",
            lambda stem: stem,
        )
    assert calls[-2:] == [
        ("cluster", "delete", "namespace-cluster", "--force"),
        ("cluster", "get", "namespace-cluster"),
    ]
    assert not any(call[:2] == ("namespace", "delete") for call in calls)


def test_unexpected_namespace_probe_success_cleans_namespace_before_cluster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        if args[:2] == ["cluster", "create"]:
            return _result("cluster.create", data={"code": 99})
        if args[:2] == ["namespace", "create"]:
            return _result("namespace.create", data={"id": 55})
        if args[:2] == ["namespace", "delete"]:
            return _result("namespace.delete", data={"deleted": True})
        if args[:2] == ["namespace", "get"]:
            return _result("namespace.get", error_type="not_found")
        if args[:2] == ["cluster", "delete"]:
            return _result("cluster.delete", data={"deleted": True})
        if args[:2] == ["cluster", "get"]:
            if calls.count(("cluster", "get", "99")) == 1:
                return _result(
                    "cluster.get",
                    data={
                        "config": json.dumps(
                            {
                                "k8s": "apiVersion: v1\nkind: Config\nclusters: []\n",
                                "yarn": "",
                            }
                        )
                    },
                )
            return _result("cluster.get", error_type="not_found")
        message = f"Unexpected command {args}"
        raise AssertionError(message)

    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError, match="namespace create without k8s capability"):
        optional_surfaces.test_admin_namespace_missing_k8s_probe(
            tmp_path,
            tmp_path / "admin.env",
            lambda stem: stem,
        )
    assert calls[-4:] == [
        ("namespace", "delete", "namespace", "--force"),
        ("namespace", "get", "namespace"),
        ("cluster", "delete", "99", "--force"),
        ("cluster", "get", "99"),
    ]


def test_namespace_probe_stops_before_create_when_cluster_config_drifted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        if args[:2] == ["cluster", "create"]:
            return _result("cluster.create", data={"code": 99})
        if args[:2] == ["cluster", "get"]:
            if calls.count(("cluster", "get", "99")) == 1:
                return _result("cluster.get", data={"config": "{}"})
            return _result("cluster.get", error_type="not_found")
        if args[:2] == ["namespace", "delete"]:
            return _result("namespace.delete", error_type="not_found")
        if args[:2] == ["namespace", "get"]:
            return _result("namespace.get", error_type="not_found")
        if args[:2] == ["cluster", "delete"]:
            return _result("cluster.delete", data={"deleted": True})
        message = f"Unexpected command {args}"
        raise AssertionError(message)

    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError):
        optional_surfaces.test_admin_namespace_missing_k8s_probe(
            tmp_path,
            tmp_path / "admin.env",
            lambda stem: stem,
        )
    assert not any(call[:2] == ("namespace", "create") for call in calls)
    assert not any(call[:2] == ("namespace", "delete") for call in calls)
    assert calls[-2:] == [
        ("cluster", "delete", "99", "--force"),
        ("cluster", "get", "99"),
    ]


def test_missing_namespace_checks_do_not_depend_on_probe_cluster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path, args: list[str], *, env_file: Path
    ) -> DsctlCommandResult:
        del repo_root, env_file
        calls.append(tuple(args))
        action = ".".join(args[:3]) if args[0] == "user" else ".".join(args[:2])
        return _result(action, error_type="not_found")

    monkeypatch.setattr(optional_surfaces, "run_dsctl", run_dsctl)
    optional_surfaces.test_admin_namespace_missing_resource_errors(
        tmp_path,
        tmp_path / "admin.env",
        _bootstrap(tmp_path),
        lambda stem: stem,
    )
    assert [call[:2] for call in calls] == [
        ("namespace", "get"),
        ("namespace", "delete"),
        ("user", "grant"),
        ("user", "revoke"),
    ]
