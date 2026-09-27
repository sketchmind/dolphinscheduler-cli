from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from tests.live.project_lifecycle_cleanup import (
    ProjectLifecycleCleanup,
    ProjectNativeIdentity,
)
from tests.live.support import (
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.live_developer, pytest.mark.destructive]

_PROJECT_WORKER_GROUP_CLEAR_VERSIONS = frozenset(
    {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)


def _require_int_value(value: object, *, label: str) -> int:
    if not isinstance(value, int):
        message = f"{label} must be an integer, got {type(value).__name__}"
        raise TypeError(message)
    return value


def _require_text_value(value: object, *, label: str) -> str:
    if not isinstance(value, str) or value == "":
        message = f"{label} must be a non-empty string"
        raise TypeError(message)
    return value


def _create_project(
    repo_root: Path,
    env_file: Path,
    *,
    name: str,
    description: str,
) -> int:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "project",
                "create",
                "--name",
                name,
                "--description",
                description,
            ],
            env_file=env_file,
        ),
        expected_action="project.create",
        label="project create",
    )
    data = require_mapping(payload["data"], label="project create data")
    return _require_int_value(data.get("code"), label="project code")


def _delete_project(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["project", "delete", project, "--force"],
            env_file=env_file,
        ),
        expected_action="project.delete",
        label="project delete",
    )
    data = require_mapping(payload["data"], label="project delete data")
    assert data["deleted"] is True


def _first_worker_group_name(
    repo_root: Path,
    admin_env_file: Path,
) -> str:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["worker-group", "list", "--page-size", "20"],
            env_file=admin_env_file,
        ),
        expected_action="worker-group.list",
        label="worker-group list",
    )
    data = require_mapping(payload["data"], label="worker-group list data")
    rows = require_list(data["totalList"], label="worker-group rows")
    if not rows:
        message = "Live cluster did not return any worker groups"
        raise AssertionError(message)
    first_row = require_mapping(rows[0], label="worker-group row")
    return _require_text_value(first_row.get("name"), label="worker-group name")


def _project_worker_group_names(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    expected_project_code: int,
    label: str,
) -> list[str]:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["project-worker-group", "list", "--project", project],
            env_file=env_file,
        ),
        expected_action="project-worker-group.list",
        label=label,
    )
    return _project_worker_group_names_from_rows(
        payload["data"],
        expected_project_code=expected_project_code,
        label=f"{label} data",
    )


def _project_worker_group_names_from_rows(
    value: object,
    *,
    expected_project_code: int,
    label: str,
) -> list[str]:
    rows = require_list(value, label=label)
    names: list[str] = []
    for item in rows:
        row = require_mapping(item, label=f"{label} row")
        if row.get("projectCode") != expected_project_code:
            message = f"{label} row belongs to a different project"
            raise AssertionError(message)
        names.append(
            _require_text_value(
                row.get("workerGroup"),
                label=f"{label} worker group",
            )
        )
    return names


def test_etl_task_type_list_returns_real_cluster_catalog(
    live_repo_root: Path,
    live_etl_env_file: Path,
) -> None:
    result = run_dsctl(
        live_repo_root,
        ["task-type", "list"],
        env_file=live_etl_env_file,
    )
    payload = require_ok_payload(
        result,
        expected_action="task-type.list",
        label="task-type list",
    )
    data = require_mapping(payload["data"], label="task-type data")
    task_types = require_list(data["taskTypes"], label="task-type rows")
    assert task_types
    categories = require_mapping(
        data["taskTypesByCategory"],
        label="task-type categories",
    )
    assert "Universal" in categories
    assert "Logic" in categories
    coverage = require_mapping(data["cliCoverage"], label="task-type cliCoverage")
    assert require_list(
        coverage["taskTemplateTypes"],
        label="task template types",
    )


def test_etl_project_lifecycle_round_trips_against_live_cluster(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    initial_name = live_name_factory("project")
    updated_name = live_name_factory("project-updated")
    initial_description = "live project create path"
    updated_description = "live project update path"

    cleanup = ProjectLifecycleCleanup(
        live_repo_root,
        live_admin_env_file,
        initial_name=initial_name,
        updated_name=updated_name,
        owned_descriptions=(initial_description, updated_description),
        execute=run_dsctl,
    )
    cleanup.prepare()
    etl_delete_confirmed = False
    try:
        cleanup.mark_create_attempted()
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    initial_name,
                    "--description",
                    initial_description,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_identity = cleanup.remember_identity(
            create_payload,
            label="project create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="project create data",
        )
        assert create_data[project_identity.kind] == project_identity.value

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project", "get", initial_name],
                env_file=live_etl_env_file,
            ),
            expected_action="project.get",
            label="project get",
        )
        get_data = require_mapping(get_payload["data"], label="project get data")
        assert get_data["name"] == initial_name
        assert get_data["description"] == initial_description
        assert get_data[project_identity.kind] == project_identity.value

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "list",
                    "--search",
                    initial_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.list",
            label="project list",
        )
        list_data = require_mapping(list_payload["data"], label="project list data")
        rows = require_list(list_data["totalList"], label="project list rows")
        assert any(
            require_mapping(item, label="project list row").get(project_identity.kind)
            == project_identity.value
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "update",
                    initial_name,
                    "--name",
                    updated_name,
                    "--description",
                    updated_description,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.update",
            label="project update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="project update data",
        )
        assert update_data["name"] == updated_name
        assert update_data["description"] == updated_description
        assert update_data[project_identity.kind] == project_identity.value

        get_by_identity_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project", "get", str(project_identity.value)],
                env_file=live_etl_env_file,
            ),
            expected_action="project.get",
            label="project get by native identity",
        )
        get_by_identity_data = require_mapping(
            get_by_identity_payload["data"],
            label="project get by native identity data",
        )
        assert get_by_identity_data["name"] == updated_name
        assert get_by_identity_data["description"] == updated_description
        assert get_by_identity_data[project_identity.kind] == project_identity.value

        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "delete",
                    str(project_identity.value),
                    "--force",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.delete",
            label="project delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="project delete data",
        )
        assert delete_data["deleted"] is True

        not_found_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["project", "get", str(project_identity.value)],
                env_file=live_etl_env_file,
            ),
            expected_action="project.get",
            expected_type="not_found",
            label="project get after delete",
        )
        assert not_found_error["type"] == "not_found"
        etl_delete_confirmed = True
    finally:
        cleanup.cleanup(expected_absent=etl_delete_confirmed)


def _project_parameter_lifecycle(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    *,
    updated_data_type: str,
) -> None:
    project_name = live_name_factory("project-param")
    project_description = "live project parameter path"
    parameter_name = live_name_factory("param").replace("-", "_")
    updated_parameter_name = live_name_factory("param-updated").replace("-", "_")
    cleanup = ProjectLifecycleCleanup(
        live_repo_root,
        live_etl_env_file,
        initial_name=project_name,
        updated_name=None,
        owned_descriptions=(project_description,),
        execute=run_dsctl,
    )
    cleanup.prepare()
    try:
        cleanup.mark_create_attempted()
        project_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    project_description,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project parameter owner create",
        )
        project_identity = cleanup.remember_identity(
            project_payload,
            label="project parameter owner create",
        )
        assert project_identity.kind == "code"
        project_code = project_identity.value
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "create",
                    "--project",
                    project_name,
                    "--name",
                    parameter_name,
                    "--value",
                    "jdbc:mysql://warehouse",
                    "--data-type",
                    "VARCHAR",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.create",
            label="project-parameter create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="project-parameter create data",
        )
        parameter_code = _require_int_value(
            create_data.get("code"),
            label="project parameter code",
        )
        assert create_data["projectCode"] == project_code
        assert create_data["paramName"] == parameter_name
        assert create_data["paramDataType"] == "VARCHAR"

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "list",
                    "--project",
                    project_name,
                    "--search",
                    parameter_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.list",
            label="project-parameter list",
        )
        list_data = require_mapping(
            list_payload["data"],
            label="project-parameter list data",
        )
        rows = require_list(list_data["totalList"], label="project-parameter rows")
        assert any(
            require_mapping(item, label="project-parameter row").get("code")
            == parameter_code
            for item in rows
        )

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "get",
                    parameter_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.get",
            label="project-parameter get",
        )
        get_data = require_mapping(
            get_payload["data"],
            label="project-parameter get data",
        )
        assert get_data["code"] == parameter_code
        assert get_data["paramValue"] == "jdbc:mysql://warehouse"

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "update",
                    parameter_name,
                    "--project",
                    project_name,
                    "--name",
                    updated_parameter_name,
                    "--value",
                    "8",
                    "--data-type",
                    updated_data_type,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.update",
            label="project-parameter update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="project-parameter update data",
        )
        assert update_data["code"] == parameter_code
        assert update_data["paramName"] == updated_parameter_name
        assert update_data["paramValue"] == "8"
        assert update_data["paramDataType"] == updated_data_type

        updated_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "get",
                    updated_parameter_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.get",
            label="project-parameter get after update",
        )
        updated_get_data = require_mapping(
            updated_get_payload["data"],
            label="project-parameter get after update data",
        )
        assert updated_get_data["code"] == parameter_code
        assert updated_get_data["paramName"] == updated_parameter_name
        assert updated_get_data["paramValue"] == "8"
        assert updated_get_data["paramDataType"] == updated_data_type

        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "delete",
                    updated_parameter_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.delete",
            label="project-parameter delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="project-parameter delete data",
        )
        assert delete_data["deleted"] is True

        not_found_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-parameter",
                    "get",
                    updated_parameter_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-parameter.get",
            expected_type="not_found",
            label="project-parameter get after delete",
        )
        assert not_found_error["type"] == "not_found"
    finally:
        cleanup.cleanup()


def test_etl_project_parameter_lifecycle_round_trips_against_live_cluster(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    _project_parameter_lifecycle(
        live_repo_root,
        live_etl_env_file,
        live_name_factory,
        updated_data_type="VARCHAR",
    )


@pytest.mark.optional_capability
def test_etl_typed_project_parameter_updates_native_data_type(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    _project_parameter_lifecycle(
        live_repo_root,
        live_etl_env_file,
        live_name_factory,
        updated_data_type="INT",
    )


def test_etl_project_preference_round_trips_against_live_cluster(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    project_name = live_name_factory("project-pref")
    project_code = _create_project(
        live_repo_root,
        live_etl_env_file,
        name=project_name,
        description="live project preference path",
    )
    expected_preferences = {"taskPriority": "HIGH", "workerGroup": "default"}

    try:
        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-preference",
                    "update",
                    "--project",
                    project_name,
                    "--preferences-json",
                    json.dumps(expected_preferences),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-preference.update",
            label="project-preference update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="project-preference update data",
        )
        assert update_data["projectCode"] == project_code
        assert (
            json.loads(
                _require_text_value(
                    update_data.get("preferences"),
                    label="project-preference payload",
                )
            )
            == expected_preferences
        )
        assert update_data["state"] == 1

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project-preference", "get", "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="project-preference.get",
            label="project-preference get",
        )
        get_data = require_mapping(
            get_payload["data"],
            label="project-preference get data",
        )
        assert get_data["projectCode"] == project_code
        assert (
            json.loads(
                _require_text_value(
                    get_data.get("preferences"),
                    label="project-preference fetched payload",
                )
            )
            == expected_preferences
        )

        disable_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project-preference", "disable", "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="project-preference.disable",
            label="project-preference disable",
        )
        disable_data = require_mapping(
            disable_payload["data"],
            label="project-preference disable data",
        )
        assert disable_data["state"] == 0

        enable_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["project-preference", "enable", "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="project-preference.enable",
            label="project-preference enable",
        )
        enable_data = require_mapping(
            enable_payload["data"],
            label="project-preference enable data",
        )
        assert enable_data["state"] == 1
    finally:
        _delete_project(live_repo_root, live_etl_env_file, project=project_name)


@pytest.mark.live_admin
def test_project_worker_group_round_trips_against_live_cluster(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_admin_env_file: Path,
    live_etl_ds_version: str | None,
    live_name_factory: Callable[[str], str],
) -> None:
    if live_etl_ds_version not in _PROJECT_WORKER_GROUP_CLEAR_VERSIONS:
        pytest.skip(
            "The full project worker-group lifecycle requires an exact DS "
            "3.3.1-3.4.3 profile with native empty-assignment cleanup"
        )

    project_name = live_name_factory("project-worker-group")
    project_description = "live project worker group path"
    worker_group_name: str | None = None
    binding_attempted = False
    clear_attempted = False

    def before_project_delete(
        identity: ProjectNativeIdentity,
        project: Mapping[str, object],
    ) -> None:
        nonlocal clear_attempted
        if identity.kind != "code" or project.get("code") != identity.value:
            message = "Project worker-group cleanup identity readback differs"
            raise AssertionError(message)

        remaining = _project_worker_group_names(
            live_repo_root,
            live_admin_env_file,
            project=project_name,
            expected_project_code=identity.value,
            label="project-worker-group cleanup list",
        )
        if not remaining:
            return
        if clear_attempted:
            message = "Project worker-group residue remained after a clear attempt"
            raise AssertionError(message)
        if (
            not binding_attempted
            or worker_group_name is None
            or remaining != [worker_group_name]
        ):
            message = "Project worker-group cleanup refused an unproven binding"
            raise AssertionError(message)

        clear_attempted = True
        clear_error: Exception | None = None
        try:
            require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    [
                        "project-worker-group",
                        "clear",
                        "--project",
                        project_name,
                        "--force",
                    ],
                    env_file=live_admin_env_file,
                ),
                expected_action="project-worker-group.clear",
                label="project-worker-group cleanup clear",
            )
        except Exception as exc:  # cleanup reconciles an uncertain mutation once
            clear_error = exc

        remaining = _project_worker_group_names(
            live_repo_root,
            live_admin_env_file,
            project=project_name,
            expected_project_code=identity.value,
            label="project-worker-group cleanup after clear",
        )
        if remaining:
            message = "Project worker-group cleanup left a binding; project retained"
            raise AssertionError(message) from clear_error

    cleanup = ProjectLifecycleCleanup(
        live_repo_root,
        live_admin_env_file,
        initial_name=project_name,
        updated_name=None,
        owned_descriptions=(project_description,),
        execute=run_dsctl,
        before_delete=before_project_delete,
    )
    cleanup.prepare()
    try:
        cleanup.mark_create_attempted()
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    project_description,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project-worker-group project create",
        )
        project_identity = cleanup.remember_identity(
            create_payload,
            label="project-worker-group project create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="project-worker-group project create data",
        )
        assert create_data[project_identity.kind] == project_identity.value

        worker_group_name = _first_worker_group_name(
            live_repo_root,
            live_admin_env_file,
        )

        assert (
            _project_worker_group_names(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                expected_project_code=project_identity.value,
                label="project-worker-group initial list",
            )
            == []
        )

        binding_attempted = True
        permission_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-worker-group",
                    "set",
                    "--project",
                    project_name,
                    "--worker-group",
                    worker_group_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project-worker-group.set",
            expected_type="permission_denied",
            label="project-worker-group etl set",
        )
        assert permission_error["type"] == "permission_denied"
        assert (
            _project_worker_group_names(
                live_repo_root,
                live_admin_env_file,
                project=project_name,
                expected_project_code=project_identity.value,
                label="project-worker-group list after denied set",
            )
            == []
        )

        set_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-worker-group",
                    "set",
                    "--project",
                    project_name,
                    "--worker-group",
                    worker_group_name,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="project-worker-group.set",
            label="project-worker-group admin set",
        )
        assert _project_worker_group_names_from_rows(
            set_payload["data"],
            expected_project_code=project_identity.value,
            label="project-worker-group set data",
        ) == [worker_group_name]

        assert _project_worker_group_names(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            expected_project_code=project_identity.value,
            label="project-worker-group ETL list after set",
        ) == [worker_group_name]

        clear_attempted = True
        clear_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project-worker-group",
                    "clear",
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="project-worker-group.clear",
            label="project-worker-group clear",
        )
        clear_rows = require_list(
            clear_payload["data"],
            label="project-worker-group clear data",
        )
        assert clear_rows == []
        assert (
            _project_worker_group_names(
                live_repo_root,
                live_admin_env_file,
                project=project_name,
                expected_project_code=project_identity.value,
                label="project-worker-group admin list after clear",
            )
            == []
        )
    finally:
        cleanup.cleanup()
