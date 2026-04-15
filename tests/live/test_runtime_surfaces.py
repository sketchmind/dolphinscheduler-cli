from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    require_error_payload,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]


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


def _resource_base_dir(
    repo_root: Path,
    env_file: Path,
) -> str:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["resource", "list", "--page-size", "20"],
            env_file=env_file,
        ),
        expected_action="resource.list",
        label="resource list",
    )
    resolved = require_mapping(payload["resolved"], label="resource list resolved")
    return _require_text_value(resolved.get("directory"), label="resource base dir")


def _join_resource_path(directory: str, name: str) -> str:
    if directory == "/":
        return f"/{name}"
    return f"{directory.rstrip('/')}/{name}"


@pytest.mark.live_admin
def test_admin_cluster_lifecycle_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    initial_name = live_name_factory("cluster")
    updated_name = live_name_factory("cluster-updated")
    initial_config = json.dumps(
        {
            "k8s": "apiVersion: v1\nkind: Config\nclusters: []\n",
            "yarn": "",
        }
    )
    updated_config = initial_config
    initial_description = "live cluster create path"
    updated_description = "live cluster update path"

    create_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            [
                "cluster",
                "create",
                "--name",
                initial_name,
                "--config",
                initial_config,
                "--description",
                initial_description,
            ],
            env_file=live_admin_env_file,
        ),
        expected_action="cluster.create",
        label="cluster create",
    )
    create_data = require_mapping(create_payload["data"], label="cluster create data")
    cluster_code = _require_int_value(create_data.get("code"), label="cluster code")

    try:
        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["cluster", "get", initial_name],
                env_file=live_admin_env_file,
            ),
            expected_action="cluster.get",
            label="cluster get",
        )
        get_data = require_mapping(get_payload["data"], label="cluster get data")
        assert get_data["code"] == cluster_code
        assert get_data["name"] == initial_name
        assert get_data["config"] == initial_config
        assert get_data["description"] == initial_description

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "cluster",
                    "list",
                    "--search",
                    initial_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="cluster.list",
            label="cluster list",
        )
        list_data = require_mapping(list_payload["data"], label="cluster list data")
        rows = require_list(list_data["totalList"], label="cluster list rows")
        assert any(
            require_mapping(item, label="cluster row").get("code") == cluster_code
            for item in rows
        )

        update_result = run_dsctl(
            live_repo_root,
            [
                "cluster",
                "update",
                str(cluster_code),
                "--name",
                updated_name,
                "--config",
                updated_config,
                "--description",
                updated_description,
            ],
            env_file=live_admin_env_file,
        )
        if update_result.exit_code == 0:
            update_payload = require_ok_payload(
                update_result,
                expected_action="cluster.update",
                label="cluster update",
            )
            update_data = require_mapping(
                update_payload["data"],
                label="cluster update data",
            )
            assert update_data["code"] == cluster_code
            assert update_data["name"] == updated_name
            assert update_data["config"] == updated_config
            assert update_data["description"] == updated_description

            get_by_code_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    ["cluster", "get", str(cluster_code)],
                    env_file=live_admin_env_file,
                ),
                expected_action="cluster.get",
                label="cluster get by code",
            )
            get_by_code_data = require_mapping(
                get_by_code_payload["data"],
                label="cluster get by code data",
            )
            assert get_by_code_data["name"] == updated_name
            assert get_by_code_data["config"] == updated_config
        else:
            update_error = require_error_payload(
                update_result,
                expected_action="cluster.update",
                label="cluster update",
            )
            update_source = require_mapping(
                update_error["source"],
                label="cluster update source",
            )
            assert update_source["result_code"] == 120024
    finally:
        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["cluster", "delete", str(cluster_code), "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="cluster.delete",
            label="cluster delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="cluster delete data",
        )
        assert delete_data["deleted"] is True


@pytest.mark.live_admin
def test_admin_environment_lifecycle_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    worker_group = _first_worker_group_name(live_repo_root, live_admin_env_file)
    initial_name = live_name_factory("env")
    updated_name = live_name_factory("env-updated")
    initial_config = json.dumps({"JAVA_HOME": "/usr/lib/jvm/java-17-openjdk"})
    updated_config = json.dumps({"JAVA_HOME": "/usr/lib/jvm/java-21-openjdk"})
    initial_description = "live environment create path"
    updated_description = "live environment update path"

    create_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            [
                "env",
                "create",
                "--name",
                initial_name,
                "--config",
                initial_config,
                "--description",
                initial_description,
                "--worker-group",
                worker_group,
            ],
            env_file=live_admin_env_file,
        ),
        expected_action="env.create",
        label="env create",
    )
    create_data = require_mapping(create_payload["data"], label="env create data")
    environment_code = _require_int_value(
        create_data.get("code"),
        label="environment code",
    )

    current_name = initial_name
    try:
        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["env", "get", current_name],
                env_file=live_admin_env_file,
            ),
            expected_action="env.get",
            label="env get",
        )
        get_data = require_mapping(get_payload["data"], label="env get data")
        assert get_data["code"] == environment_code
        assert get_data["name"] == initial_name
        assert get_data["config"] == initial_config
        assert get_data["description"] == initial_description
        assert worker_group in require_list(
            get_data["workerGroups"],
            label="env worker groups",
        )

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "env",
                    "list",
                    "--search",
                    initial_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="env.list",
            label="env list",
        )
        list_data = require_mapping(list_payload["data"], label="env list data")
        rows = require_list(list_data["totalList"], label="env list rows")
        assert any(
            require_mapping(item, label="env row").get("code") == environment_code
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "env",
                    "update",
                    current_name,
                    "--name",
                    updated_name,
                    "--config",
                    updated_config,
                    "--description",
                    updated_description,
                    "--worker-group",
                    worker_group,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="env.update",
            label="env update",
        )
        update_data = require_mapping(update_payload["data"], label="env update data")
        assert update_data["code"] == environment_code
        assert update_data["name"] == updated_name
        assert update_data["config"] == updated_config
        assert update_data["description"] == updated_description
        current_name = updated_name

        get_by_code_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["env", "get", str(environment_code)],
                env_file=live_admin_env_file,
            ),
            expected_action="env.get",
            label="env get by code",
        )
        get_by_code_data = require_mapping(
            get_by_code_payload["data"],
            label="env get by code data",
        )
        assert get_by_code_data["name"] == updated_name
        assert get_by_code_data["config"] == updated_config
    finally:
        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["env", "delete", current_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="env.delete",
            label="env delete",
        )
        delete_data = require_mapping(delete_payload["data"], label="env delete data")
        assert delete_data["deleted"] is True


def test_etl_resource_lifecycle_round_trips(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    base_dir = _resource_base_dir(live_repo_root, live_etl_env_file)
    created_name = f"{live_name_factory('resource')}.sql"
    uploaded_name = f"{live_name_factory('upload')}.sql"
    directory_name = live_name_factory("resource-dir")
    created_path = _join_resource_path(base_dir, created_name)
    uploaded_path = _join_resource_path(base_dir, uploaded_name)
    directory_path = _join_resource_path(base_dir, directory_name)
    created_content = "select 1;\nselect 2;\n"
    viewed_created_content = "select 1;\nselect 2;"
    uploaded_content = "select 3;\nselect 4;\n"

    upload_file = tmp_path / uploaded_name
    upload_file.write_text(uploaded_content, encoding="utf-8")

    created_remote_file = False
    uploaded_remote_file = False
    created_remote_dir = False
    try:
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "resource",
                    "create",
                    "--name",
                    created_name,
                    "--content",
                    created_content,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.create",
            label="resource create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="resource create data",
        )
        assert create_data["fullName"] == created_path
        created_remote_file = True

        view_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["resource", "view", created_path],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.view",
            label="resource view created file",
        )
        view_data = require_mapping(view_payload["data"], label="resource view data")
        assert view_data["content"] == viewed_created_content

        upload_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "resource",
                    "upload",
                    "--file",
                    str(upload_file),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.upload",
            label="resource upload",
        )
        upload_data = require_mapping(
            upload_payload["data"],
            label="resource upload data",
        )
        assert upload_data["fullName"] == uploaded_path
        uploaded_remote_file = True

        mkdir_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["resource", "mkdir", directory_name],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.mkdir",
            label="resource mkdir",
        )
        mkdir_data = require_mapping(mkdir_payload["data"], label="resource mkdir data")
        assert mkdir_data["fullName"] == directory_path
        assert mkdir_data["isDirectory"] is True
        created_remote_dir = True

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["resource", "list", "--page-size", "50"],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.list",
            label="resource list after mutations",
        )
        list_data = require_mapping(list_payload["data"], label="resource list data")
        rows = require_list(list_data["totalList"], label="resource list rows")
        rows_by_name = {
            _require_text_value(
                require_mapping(item, label="resource row").get("fileName"),
                label="resource row name",
            ): require_mapping(item, label="resource row")
            for item in rows
        }
        assert rows_by_name[created_name]["isDirectory"] is False
        assert rows_by_name[uploaded_name]["isDirectory"] is False
        assert rows_by_name[directory_name]["isDirectory"] is True

        download_result = tmp_path / "downloaded.sql"
        download_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "resource",
                    "download",
                    uploaded_path,
                    "--output",
                    str(download_result),
                    "--overwrite",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="resource.download",
            label="resource download",
        )
        download_data = require_mapping(
            download_payload["data"],
            label="resource download data",
        )
        assert download_data["fullName"] == uploaded_path
        assert download_result.read_text(encoding="utf-8") == uploaded_content
    finally:
        if created_remote_file:
            delete_created_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    ["resource", "delete", created_path, "--force"],
                    env_file=live_etl_env_file,
                ),
                expected_action="resource.delete",
                label="resource delete created file",
            )
            delete_created_data = require_mapping(
                delete_created_payload["data"],
                label="resource delete created file data",
            )
            assert delete_created_data["deleted"] is True
        if uploaded_remote_file:
            delete_uploaded_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    ["resource", "delete", uploaded_path, "--force"],
                    env_file=live_etl_env_file,
                ),
                expected_action="resource.delete",
                label="resource delete uploaded file",
            )
            delete_uploaded_data = require_mapping(
                delete_uploaded_payload["data"],
                label="resource delete uploaded file data",
            )
            assert delete_uploaded_data["deleted"] is True
        if created_remote_dir:
            delete_directory_payload = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    ["resource", "delete", f"{directory_path}/", "--force"],
                    env_file=live_etl_env_file,
                ),
                expected_action="resource.delete",
                label="resource delete directory",
            )
            delete_directory_data = require_mapping(
                delete_directory_payload["data"],
                label="resource delete directory data",
            )
            assert delete_directory_data["deleted"] is True
