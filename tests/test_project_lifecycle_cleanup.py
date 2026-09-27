from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import pytest

from tests.live import test_project_surfaces as project_surfaces
from tests.live.project_lifecycle_cleanup import ProjectLifecycleCleanup
from tests.live.support import DsctlCommandResult

if TYPE_CHECKING:
    from pathlib import Path


IdentityKind = Literal["id", "code"]


def _result(
    argv: list[str],
    *,
    action: str,
    data: object = None,
    resolved: dict[str, object] | None = None,
    error_type: str | None = None,
) -> DsctlCommandResult:
    ok = error_type is None
    payload: dict[str, object] = {
        "ok": ok,
        "action": action,
        "data": data,
        "error": None if ok else {"type": error_type},
    }
    if resolved is not None:
        payload["resolved"] = resolved
    return DsctlCommandResult(
        argv=tuple(argv),
        exit_code=0 if ok else 1,
        stdout="",
        stderr="",
        payload=payload,
    )


def _project_payload(
    argv: list[str],
    *,
    name: str,
    description: str,
    identity_kind: IdentityKind,
    identity: int,
) -> DsctlCommandResult:
    data = {
        identity_kind: identity,
        "name": name,
        "description": description,
    }
    return _result(
        argv,
        action="project.get" if argv[1] == "get" else f"project.{argv[1]}",
        data=data,
        resolved={"project": data},
    )


def _not_found(argv: list[str]) -> DsctlCommandResult:
    return _result(argv, action="project.get", error_type="not_found")


def _project_list_page(
    argv: list[str],
    rows: list[dict[str, object]],
    *,
    total: int | None = None,
) -> DsctlCommandResult:
    search = argv[argv.index("--search") + 1]
    return _result(
        argv,
        action="project.list",
        data={
            "totalList": rows,
            "total": len(rows) if total is None else total,
            "totalPage": 0 if not rows else 1,
            "pageSize": 100,
            "currentPage": 1,
            "pageNo": 1,
        },
        resolved={"all": False, "search": search, "page_no": 1, "page_size": 100},
    )


def test_project_lifecycle_cleans_up_after_uncertain_create(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    initial_name = "owned-project"
    description = "live project create path"
    row: dict[str, object] | None = None
    calls: list[tuple[tuple[str, ...], Path]] = []

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        nonlocal row
        del repo_root
        calls.append((tuple(argv), env_file))
        if argv[:2] == ["project", "create"]:
            row = {"code": 41, "name": initial_name, "description": description}
            message = "create response lost"
            raise RuntimeError(message)
        if argv[:2] == ["project", "get"]:
            if row is None or argv[2] != row["name"]:
                return _not_found(argv)
            return _project_payload(
                argv,
                name=str(row["name"]),
                description=str(row["description"]),
                identity_kind="code",
                identity=41,
            )
        if argv[:2] == ["project", "list"]:
            return _project_list_page(argv, [] if row is None else [row])
        assert argv == ["project", "delete", "41", "--force"]
        assert env_file == tmp_path / "admin.env"
        row = None
        return _result(argv, action="project.delete", data={"deleted": True})

    monkeypatch.setattr(project_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(RuntimeError, match="create response lost"):
        project_surfaces.test_etl_project_lifecycle_round_trips_against_live_cluster(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_etl_env_file=tmp_path / "etl.env",
            live_name_factory=lambda stem: f"owned-{stem}",
        )

    assert row is None
    assert (("project", "delete", "41", "--force"), tmp_path / "admin.env") in calls


def test_project_lifecycle_cleans_up_legacy_id_after_rename_bad_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    initial_name = "owned-project"
    updated_name = "owned-project-updated"
    initial_description = "live project create path"
    updated_description = "live project update path"
    row: dict[str, object] | None = None
    deletes: list[tuple[tuple[str, ...], Path]] = []

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        nonlocal row
        del repo_root
        if argv[:2] == ["project", "create"]:
            row = {
                "id": 17,
                "name": initial_name,
                "description": initial_description,
            }
            return _project_payload(
                argv,
                name=initial_name,
                description=initial_description,
                identity_kind="id",
                identity=17,
            )
        if argv[:2] == ["project", "update"]:
            row = {
                "id": 17,
                "name": updated_name,
                "description": updated_description,
            }
            result = _project_payload(
                argv,
                name=updated_name,
                description=updated_description,
                identity_kind="id",
                identity=17,
            )
            result.payload["data"] = {
                "id": 17,
                "name": initial_name,
                "description": updated_description,
            }
            return result
        if argv[:2] == ["project", "get"]:
            if row is None or argv[2] != row["name"]:
                return _not_found(argv)
            return _project_payload(
                argv,
                name=str(row["name"]),
                description=str(row["description"]),
                identity_kind="id",
                identity=17,
            )
        if argv[:2] == ["project", "list"]:
            if "--page-no" in argv:
                return _project_list_page(argv, [] if row is None else [row])
            return _result(
                argv,
                action="project.list",
                data={"totalList": [] if row is None else [row], "total": 1},
            )
        assert argv == ["project", "delete", "17", "--force"]
        deletes.append((tuple(argv), env_file))
        row = None
        return _result(argv, action="project.delete", data={"deleted": True})

    monkeypatch.setattr(project_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError):
        project_surfaces.test_etl_project_lifecycle_round_trips_against_live_cluster(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_etl_env_file=tmp_path / "etl.env",
            live_name_factory=lambda stem: f"owned-{stem}",
        )

    assert row is None
    assert deletes == [(("project", "delete", "17", "--force"), tmp_path / "admin.env")]


@pytest.mark.parametrize("cleanup_failure", ["foreign-residue", "permission-denied"])
def test_project_lifecycle_cleanup_refuses_unproven_residue_or_visibility(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    cleanup_failure: str,
) -> None:
    initial_name = "owned-project"
    preflight_complete = False
    mutation_attempted = False
    deletes: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        nonlocal preflight_complete, mutation_attempted
        del repo_root, env_file
        if argv[:2] == ["project", "list"]:
            preflight_complete = True
            return _project_list_page(argv, [])
        if argv[:2] == ["project", "create"]:
            assert preflight_complete
            mutation_attempted = True
            message = "create response lost"
            raise RuntimeError(message)
        if argv[:2] == ["project", "delete"]:
            deletes.append(tuple(argv))
            return _result(argv, action="project.delete", data={"deleted": True})
        assert argv[:2] == ["project", "get"]
        if not mutation_attempted:
            return _not_found(argv)
        if cleanup_failure == "permission-denied":
            return _result(
                argv,
                action="project.get",
                error_type="permission_denied",
            )
        if argv[2] != initial_name:
            return _not_found(argv)
        return _project_payload(
            argv,
            name=initial_name,
            description="another owner's project",
            identity_kind="code",
            identity=99,
        )

    monkeypatch.setattr(project_surfaces, "run_dsctl", run_dsctl)
    with pytest.raises(AssertionError):
        project_surfaces.test_etl_project_lifecycle_round_trips_against_live_cluster(
            live_repo_root=tmp_path,
            live_admin_env_file=tmp_path / "admin.env",
            live_etl_env_file=tmp_path / "etl.env",
            live_name_factory=lambda stem: f"owned-{stem}",
        )

    assert deletes == []


@pytest.mark.parametrize("delete_uncertain", [False, True])
def test_expected_etl_absence_cleans_but_rejects_admin_visible_residue(
    tmp_path: Path,
    *,
    delete_uncertain: bool,
) -> None:
    initial_name = "owned-project"
    updated_name = "owned-project-updated"
    description = "live project create path"
    row: dict[str, object] | None = None
    deletes: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        nonlocal row
        del repo_root, env_file
        if argv[:2] == ["project", "get"]:
            if row is None or argv[2] != row["name"]:
                return _not_found(argv)
            return _project_payload(
                argv,
                name=initial_name,
                description=description,
                identity_kind="code",
                identity=41,
            )
        if argv[:2] == ["project", "list"]:
            matching = (
                []
                if row is None or argv[argv.index("--search") + 1] != row["name"]
                else [row]
            )
            return _project_list_page(argv, matching)
        assert argv == ["project", "delete", "41", "--force"]
        deletes.append(tuple(argv))
        row = None
        if delete_uncertain:
            message = "cleanup delete response lost"
            raise RuntimeError(message)
        return _result(argv, action="project.delete", data={"deleted": True})

    cleanup = ProjectLifecycleCleanup(
        tmp_path,
        tmp_path / "admin.env",
        initial_name=initial_name,
        updated_name=updated_name,
        owned_descriptions=(description, "live project update path"),
        execute=run_dsctl,
    )
    cleanup.prepare()
    cleanup.mark_create_attempted()
    row = {"code": 41, "name": initial_name, "description": description}

    expected_exception = RuntimeError if delete_uncertain else AssertionError
    expected_message = (
        "cleanup delete response lost" if delete_uncertain else "administrator-visible"
    )
    with pytest.raises(expected_exception, match=expected_message):
        cleanup.cleanup(expected_absent=True)

    assert deletes == [("project", "delete", "41", "--force")]


def test_project_absence_accepts_native_empty_page_without_coverage(
    tmp_path: Path,
) -> None:
    list_calls: list[tuple[str, ...]] = []

    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root, env_file
        if argv[:2] == ["project", "get"]:
            return _not_found(argv)
        list_calls.append(tuple(argv))
        return _project_list_page(argv, [])

    cleanup = ProjectLifecycleCleanup(
        tmp_path,
        tmp_path / "admin.env",
        initial_name="owned-project",
        updated_name="owned-project-updated",
        owned_descriptions=("created", "updated"),
        execute=run_dsctl,
    )

    cleanup.prepare()

    assert len(list_calls) == 2


def test_project_absence_rejects_nonzero_reported_total(
    tmp_path: Path,
) -> None:
    def run_dsctl(
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root, env_file
        if argv[:2] == ["project", "get"]:
            return _not_found(argv)
        return _project_list_page(argv, [], total=1)

    cleanup = ProjectLifecycleCleanup(
        tmp_path,
        tmp_path / "admin.env",
        initial_name="owned-project",
        updated_name="owned-project-updated",
        owned_descriptions=("created", "updated"),
        execute=run_dsctl,
    )

    with pytest.raises(AssertionError, match="empty first page"):
        cleanup.prepare()


@dataclass
class _WorkerGroupScenario:
    mode: str
    project: dict[str, object] | None = None
    binding: list[str] = field(default_factory=list)
    clear_calls: int = 0
    delete_calls: int = 0
    calls: list[tuple[tuple[str, ...], Path]] = field(default_factory=list)


def _install_project_worker_group_scenario(  # noqa: C901 - journey fake dispatch
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    mode: str,
) -> _WorkerGroupScenario:
    state = _WorkerGroupScenario(mode=mode)
    project_name = "owned-project-worker-group"
    description = "live project worker group path"

    def run_dsctl(  # noqa: C901 - explicit command-state fake
        repo_root: Path,
        argv: list[str],
        *,
        env_file: Path,
    ) -> DsctlCommandResult:
        del repo_root
        state.calls.append((tuple(argv), env_file))

        if argv[:2] == ["project", "get"]:
            selector = argv[2]
            if state.project is None or selector not in {project_name, "41"}:
                return _not_found(argv)
            return _project_payload(
                argv,
                name=project_name,
                description=description,
                identity_kind="code",
                identity=41,
            )
        if argv[:2] == ["project", "list"]:
            search = argv[argv.index("--search") + 1]
            rows = (
                [state.project]
                if state.project is not None and search == project_name
                else []
            )
            return _project_list_page(argv, rows)
        if argv[:2] == ["project", "create"]:
            state.project = {
                "code": 41,
                "name": project_name,
                "description": description,
            }
            if mode == "uncertain-create":
                message = "project create response lost"
                raise RuntimeError(message)
            return _project_payload(
                argv,
                name=project_name,
                description=description,
                identity_kind="code",
                identity=41,
            )
        if argv[:2] == ["project", "delete"]:
            assert env_file == tmp_path / "admin.env"
            state.delete_calls += 1
            state.project = None
            return _result(argv, action="project.delete", data={"deleted": True})
        if argv[:2] == ["worker-group", "list"]:
            return _result(
                argv,
                action="worker-group.list",
                data={"totalList": [{"name": "default"}]},
            )
        if argv[:2] == ["project-worker-group", "list"]:
            project_code = 99 if mode == "foreign-project-row" else 41
            return _result(
                argv,
                action="project-worker-group.list",
                data=[
                    {"projectCode": project_code, "workerGroup": worker_group}
                    for worker_group in state.binding
                ],
            )
        if argv[:2] == ["project-worker-group", "set"]:
            if env_file == tmp_path / "etl.env":
                if mode == "foreign-project-row":
                    state.binding = ["default"]
                elif mode == "foreign-worker-group":
                    state.binding = ["unexpected"]
                return _result(
                    argv,
                    action="project-worker-group.set",
                    error_type="permission_denied",
                )
            assert env_file == tmp_path / "admin.env"
            state.binding = ["default"]
            data: list[dict[str, object]] = [
                {"projectCode": 41, "workerGroup": "default"}
            ]
            if mode == "binding-assertion":
                data = []
            return _result(argv, action="project-worker-group.set", data=data)

        assert argv[:2] == ["project-worker-group", "clear"]
        assert env_file == tmp_path / "admin.env"
        state.clear_calls += 1
        if mode == "clear-failed":
            return _result(
                argv,
                action="project-worker-group.clear",
                error_type="conflict",
            )
        state.binding.clear()
        if mode == "uncertain-clear":
            message = "project worker-group clear response lost"
            raise RuntimeError(message)
        return _result(argv, action="project-worker-group.clear", data=[])

    monkeypatch.setattr(project_surfaces, "run_dsctl", run_dsctl)
    return state


def _run_project_worker_group_scenario(
    tmp_path: Path,
    *,
    ds_version: str = "3.4.3",
) -> None:
    project_surfaces.test_project_worker_group_round_trips_against_live_cluster(
        live_repo_root=tmp_path,
        live_etl_env_file=tmp_path / "etl.env",
        live_admin_env_file=tmp_path / "admin.env",
        live_etl_ds_version=ds_version,
        live_name_factory=lambda stem: f"owned-{stem}",
    )


def test_project_worker_group_322_guard_runs_before_any_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = 0

    def run_dsctl(*args: object, **kwargs: object) -> DsctlCommandResult:
        nonlocal calls
        del args, kwargs
        calls += 1
        message = "3.2.2 guard allowed cluster I/O"
        raise AssertionError(message)

    monkeypatch.setattr(project_surfaces, "run_dsctl", run_dsctl)

    with pytest.raises(pytest.skip.Exception, match=r"3\.3\.1-3\.4\.3"):
        _run_project_worker_group_scenario(tmp_path, ds_version="3.2.2")

    assert calls == 0


def test_project_worker_group_uncertain_create_is_reconciled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode="uncertain-create",
    )

    with pytest.raises(RuntimeError, match="project create response lost"):
        _run_project_worker_group_scenario(tmp_path)

    assert state.project is None
    assert state.clear_calls == 0
    assert state.delete_calls == 1


def test_project_worker_group_binding_assertion_clears_before_project_delete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode="binding-assertion",
    )

    with pytest.raises(AssertionError):
        _run_project_worker_group_scenario(tmp_path)

    assert state.binding == []
    assert state.clear_calls == 1
    assert state.delete_calls == 1


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("foreign-project-row", "different project"),
        ("foreign-worker-group", "unproven binding"),
    ],
)
def test_project_worker_group_cleanup_refuses_unproven_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    mode: str,
    message: str,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode=mode,
    )

    with pytest.raises(AssertionError, match=message):
        _run_project_worker_group_scenario(tmp_path)

    assert state.clear_calls == 0
    assert state.delete_calls == 0
    assert state.project is not None


def test_project_worker_group_clear_residue_retains_project(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode="clear-failed",
    )

    with pytest.raises(AssertionError, match="residue remained"):
        _run_project_worker_group_scenario(tmp_path)

    assert state.binding == ["default"]
    assert state.clear_calls == 1
    assert state.delete_calls == 0
    assert state.project is not None


def test_project_worker_group_uncertain_clear_proves_empty_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode="uncertain-clear",
    )

    with pytest.raises(RuntimeError, match="clear response lost"):
        _run_project_worker_group_scenario(tmp_path)

    assert state.binding == []
    assert state.clear_calls == 1
    assert state.delete_calls == 1
    assert state.project is None


def test_project_worker_group_normal_path_proves_empty_then_deletes_as_admin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state = _install_project_worker_group_scenario(
        monkeypatch,
        tmp_path,
        mode="normal",
    )

    _run_project_worker_group_scenario(tmp_path)

    assert state.binding == []
    assert state.clear_calls == 1
    assert state.delete_calls == 1
    assert state.project is None
    assert (
        ("project", "delete", "41", "--force"),
        tmp_path / "admin.env",
    ) in state.calls
