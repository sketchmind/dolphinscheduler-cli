from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    LiveBootstrapState,
    require_error_payload,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.live_admin, pytest.mark.destructive]

LIVE_DATASOURCE_TYPE_ENV = "DS_LIVE_DATASOURCE_TYPE"
LIVE_DATASOURCE_HOST_ENV = "DS_LIVE_DATASOURCE_HOST"
LIVE_DATASOURCE_PORT_ENV = "DS_LIVE_DATASOURCE_PORT"
LIVE_DATASOURCE_DATABASE_ENV = "DS_LIVE_DATASOURCE_DATABASE"
LIVE_DATASOURCE_USER_ENV = "DS_LIVE_DATASOURCE_USER"
LIVE_DATASOURCE_PASSWORD_ENV = "DS_LIVE_DATASOURCE_PASSWORD"
DEFAULT_LIVE_DATASOURCE_TYPE = "MYSQL"
DEFAULT_LIVE_DATASOURCE_PORT = 3306


@dataclass(frozen=True)
class LiveDatasourceConfig:
    """External datasource settings for optional destructive live coverage."""

    type: str
    host: str
    port: int
    database: str
    user_name: str
    password: str


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _load_live_datasource_config() -> LiveDatasourceConfig:
    required_values = {
        LIVE_DATASOURCE_HOST_ENV: _optional_env_text(LIVE_DATASOURCE_HOST_ENV),
        LIVE_DATASOURCE_DATABASE_ENV: _optional_env_text(LIVE_DATASOURCE_DATABASE_ENV),
        LIVE_DATASOURCE_USER_ENV: _optional_env_text(LIVE_DATASOURCE_USER_ENV),
        LIVE_DATASOURCE_PASSWORD_ENV: _optional_env_text(LIVE_DATASOURCE_PASSWORD_ENV),
    }
    missing = [name for name, value in required_values.items() if value is None]
    if missing:
        pytest.skip(
            "Datasource lifecycle live test requires external datasource settings: "
            + ", ".join(missing)
        )

    port_text = _optional_env_text(LIVE_DATASOURCE_PORT_ENV)
    try:
        port = DEFAULT_LIVE_DATASOURCE_PORT if port_text is None else int(port_text)
    except ValueError:
        pytest.fail(f"{LIVE_DATASOURCE_PORT_ENV} must be an integer: {port_text}")

    return LiveDatasourceConfig(
        type=_optional_env_text(LIVE_DATASOURCE_TYPE_ENV)
        or DEFAULT_LIVE_DATASOURCE_TYPE,
        host=required_values[LIVE_DATASOURCE_HOST_ENV] or "",
        port=port,
        database=required_values[LIVE_DATASOURCE_DATABASE_ENV] or "",
        user_name=required_values[LIVE_DATASOURCE_USER_ENV] or "",
        password=required_values[LIVE_DATASOURCE_PASSWORD_ENV] or "",
    )


def _optional_env_text(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _datasource_payload(
    config: LiveDatasourceConfig,
    *,
    name: str,
    note: str,
    password: str,
    datasource_id: int | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": name,
        "type": config.type,
        "host": config.host,
        "port": config.port,
        "database": config.database,
        "userName": config.user_name,
        "password": password,
        "note": note,
    }
    if datasource_id is not None:
        payload["id"] = datasource_id
    return payload


def test_admin_datasource_lifecycle_and_user_grant_round_trip(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_bootstrap_state: LiveBootstrapState,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    user_name = live_bootstrap_state.user_name
    if user_name is None:
        pytest.skip("Datasource grant live test requires one managed ETL user.")

    datasource_config = _load_live_datasource_config()
    initial_name = live_name_factory("datasource")
    updated_name = live_name_factory("datasource-updated")
    create_file = _write_json(
        tmp_path / f"{initial_name}.json",
        _datasource_payload(
            datasource_config,
            name=initial_name,
            note="live datasource create path",
            password=datasource_config.password,
        ),
    )

    datasource_deleted = False
    current_name = initial_name
    datasource_id: int | None = None

    try:
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["datasource", "create", "--file", str(create_file)],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.create",
            label="datasource create",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="datasource create data",
        )
        datasource_id = require_int_value(create_data.get("id"), label="datasource id")
        assert create_data["name"] == initial_name
        assert create_data["type"] == "MYSQL"

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["datasource", "get", current_name],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.get",
            label="datasource get",
        )
        get_data = require_mapping(get_payload["data"], label="datasource get data")
        assert get_data["id"] == datasource_id
        assert get_data["database"] == datasource_config.database

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "datasource",
                    "list",
                    "--search",
                    current_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.list",
            label="datasource list",
        )
        list_data = require_mapping(list_payload["data"], label="datasource list data")
        rows = require_list(list_data["totalList"], label="datasource rows")
        assert any(
            require_mapping(item, label="datasource row").get("id") == datasource_id
            for item in rows
        )

        update_file = _write_json(
            tmp_path / f"{updated_name}.json",
            _datasource_payload(
                datasource_config,
                name=updated_name,
                note="live datasource update path",
                password="******",
                datasource_id=datasource_id,
            ),
        )
        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "datasource",
                    "update",
                    current_name,
                    "--file",
                    str(update_file),
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.update",
            label="datasource update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="datasource update data",
        )
        assert update_data["id"] == datasource_id
        assert update_data["name"] == updated_name
        current_name = updated_name

        refreshed_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["datasource", "get", current_name],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.get",
            label="datasource get after update",
        )
        refreshed_data = require_mapping(
            refreshed_payload["data"],
            label="datasource get after update data",
        )
        assert refreshed_data["name"] == updated_name
        assert refreshed_data["note"] == "live datasource update path"

        test_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["datasource", "test", current_name],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.test",
            label="datasource test",
        )
        test_data = require_mapping(test_payload["data"], label="datasource test data")
        assert test_data["connected"] is True

        grant_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "user",
                    "grant",
                    "datasource",
                    user_name,
                    "--datasource",
                    current_name,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="user.grant.datasource",
            label="user grant datasource",
        )
        grant_data = require_mapping(
            grant_payload["data"],
            label="user grant datasource data",
        )
        granted_datasources = require_list(
            grant_data["datasources"],
            label="user grant datasource list",
        )
        assert any(
            require_mapping(item, label="granted datasource").get("id") == datasource_id
            for item in granted_datasources
        )

        revoke_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "user",
                    "revoke",
                    "datasource",
                    user_name,
                    "--datasource",
                    current_name,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="user.revoke.datasource",
            label="user revoke datasource",
        )
        revoke_data = require_mapping(
            revoke_payload["data"],
            label="user revoke datasource data",
        )
        remaining_datasources = require_list(
            revoke_data["datasources"],
            label="user revoke datasource list",
        )
        assert all(
            require_mapping(item, label="remaining datasource").get("id")
            != datasource_id
            for item in remaining_datasources
        )

        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["datasource", "delete", current_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="datasource.delete",
            label="datasource delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="datasource delete data",
        )
        assert delete_data["deleted"] is True
        datasource_deleted = True
    finally:
        if current_name and not datasource_deleted:
            run_dsctl(
                live_repo_root,
                ["datasource", "delete", current_name, "--force"],
                env_file=live_admin_env_file,
            )


def test_admin_alert_plugin_and_group_lifecycle_round_trip(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    plugin_name = live_name_factory("alert-plugin")
    updated_plugin_name = live_name_factory("alert-plugin-updated")
    group_name = live_name_factory("alert-group")
    updated_group_name = live_name_factory("alert-group-updated")

    alert_group_deleted = False
    alert_plugin_deleted = False
    current_plugin_name = plugin_name
    current_group_name = group_name
    alert_plugin_id: int | None = None

    schema_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["alert-plugin", "schema", "Script"],
            env_file=live_admin_env_file,
        ),
        expected_action="alert-plugin.schema",
        label="alert-plugin schema",
    )
    schema_data = require_mapping(
        schema_payload["data"],
        label="alert-plugin schema data",
    )
    assert schema_data["pluginName"] == "Script"

    try:
        plugin_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-plugin",
                    "create",
                    "--name",
                    current_plugin_name,
                    "--plugin",
                    "Script",
                    "--params-json",
                    "[]",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.create",
            label="alert-plugin create",
        )
        plugin_create_data = require_mapping(
            plugin_create_payload["data"],
            label="alert-plugin create data",
        )
        alert_plugin_id = require_int_value(
            plugin_create_data.get("id"),
            label="alert-plugin id",
        )
        assert plugin_create_data["instanceName"] == current_plugin_name

        plugin_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["alert-plugin", "get", current_plugin_name],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.get",
            label="alert-plugin get",
        )
        plugin_get_data = require_mapping(
            plugin_get_payload["data"],
            label="alert-plugin get data",
        )
        assert plugin_get_data["id"] == alert_plugin_id

        plugin_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-plugin",
                    "list",
                    "--search",
                    current_plugin_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.list",
            label="alert-plugin list",
        )
        plugin_list_data = require_mapping(
            plugin_list_payload["data"],
            label="alert-plugin list data",
        )
        plugin_rows = require_list(
            plugin_list_data["totalList"],
            label="alert-plugin rows",
        )
        assert any(
            require_mapping(item, label="alert-plugin row").get("id") == alert_plugin_id
            for item in plugin_rows
        )

        plugin_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-plugin",
                    "update",
                    current_plugin_name,
                    "--name",
                    updated_plugin_name,
                    "--params-json",
                    "[]",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.update",
            label="alert-plugin update",
        )
        plugin_update_data = require_mapping(
            plugin_update_payload["data"],
            label="alert-plugin update data",
        )
        assert plugin_update_data["instanceName"] == updated_plugin_name
        current_plugin_name = updated_plugin_name

        group_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-group",
                    "create",
                    "--name",
                    current_group_name,
                    "--description",
                    "live alert-group create path",
                    "--instance-id",
                    str(alert_plugin_id),
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-group.create",
            label="alert-group create",
        )
        group_create_data = require_mapping(
            group_create_payload["data"],
            label="alert-group create data",
        )
        assert group_create_data["groupName"] == current_group_name

        group_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["alert-group", "get", current_group_name],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-group.get",
            label="alert-group get",
        )
        group_get_data = require_mapping(
            group_get_payload["data"],
            label="alert-group get data",
        )
        assert group_get_data["groupName"] == current_group_name

        group_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-group",
                    "list",
                    "--search",
                    current_group_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-group.list",
            label="alert-group list",
        )
        group_list_data = require_mapping(
            group_list_payload["data"],
            label="alert-group list data",
        )
        group_rows = require_list(
            group_list_data["totalList"],
            label="alert-group rows",
        )
        assert any(
            require_mapping(item, label="alert-group row").get("groupName")
            == current_group_name
            for item in group_rows
        )

        group_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "alert-group",
                    "update",
                    current_group_name,
                    "--name",
                    updated_group_name,
                    "--description",
                    "live alert-group update path",
                    "--clear-instance-ids",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-group.update",
            label="alert-group update",
        )
        group_update_data = require_mapping(
            group_update_payload["data"],
            label="alert-group update data",
        )
        assert group_update_data["groupName"] == updated_group_name
        current_group_name = updated_group_name

        plugin_test_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["alert-plugin", "test", current_plugin_name],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.test",
            expected_type="conflict",
            label="alert-plugin test",
        )
        plugin_test_source = require_mapping(
            plugin_test_error["source"],
            label="alert-plugin test source",
        )
        assert plugin_test_source["result_code"] == 110014

        group_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["alert-group", "delete", current_group_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-group.delete",
            label="alert-group delete",
        )
        group_delete_data = require_mapping(
            group_delete_payload["data"],
            label="alert-group delete data",
        )
        assert group_delete_data["deleted"] is True
        alert_group_deleted = True

        plugin_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["alert-plugin", "delete", current_plugin_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="alert-plugin.delete",
            label="alert-plugin delete",
        )
        plugin_delete_data = require_mapping(
            plugin_delete_payload["data"],
            label="alert-plugin delete data",
        )
        assert plugin_delete_data["deleted"] is True
        alert_plugin_deleted = True
    finally:
        if current_group_name and not alert_group_deleted:
            run_dsctl(
                live_repo_root,
                ["alert-group", "delete", current_group_name, "--force"],
                env_file=live_admin_env_file,
            )
        if current_plugin_name and not alert_plugin_deleted:
            run_dsctl(
                live_repo_root,
                ["alert-plugin", "delete", current_plugin_name, "--force"],
                env_file=live_admin_env_file,
            )


def test_admin_namespace_read_surfaces_return_current_cluster_catalog(
    live_repo_root: Path,
    live_admin_env_file: Path,
) -> None:
    list_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["namespace", "list", "--page-size", "20"],
            env_file=live_admin_env_file,
        ),
        expected_action="namespace.list",
        label="namespace list",
    )
    list_data = require_mapping(list_payload["data"], label="namespace list data")
    require_list(list_data["totalList"], label="namespace rows")

    available_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["namespace", "available"],
            env_file=live_admin_env_file,
        ),
        expected_action="namespace.available",
        label="namespace available",
    )
    require_list(available_payload["data"], label="namespace available data")


def test_admin_namespace_mutation_and_grant_paths_reflect_missing_k8s_capability(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_bootstrap_state: LiveBootstrapState,
    live_name_factory: Callable[[str], str],
) -> None:
    user_name = live_bootstrap_state.user_name
    if user_name is None:
        pytest.skip("Namespace grant live test requires one managed ETL user.")

    cluster_name = live_name_factory("namespace-cluster")
    namespace_name = live_name_factory("namespace")
    cluster_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            [
                "cluster",
                "create",
                "--name",
                cluster_name,
                "--config",
                json.dumps(
                    {
                        "k8s": "apiVersion: v1\nkind: Config\nclusters: []\n",
                        "yarn": "",
                    }
                ),
                "--description",
                "namespace capability probe cluster",
            ],
            env_file=live_admin_env_file,
        ),
        expected_action="cluster.create",
        label="namespace probe cluster create",
    )
    cluster_data = require_mapping(
        cluster_payload["data"],
        label="namespace probe cluster create data",
    )
    cluster_code = require_int_value(
        cluster_data.get("code"),
        label="namespace probe cluster code",
    )

    try:
        create_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                [
                    "namespace",
                    "create",
                    "--namespace",
                    namespace_name,
                    "--cluster-code",
                    str(cluster_code),
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="namespace.create",
            expected_type="user_input_error",
            label="namespace create without k8s capability",
        )
        create_source = require_mapping(
            create_error["source"],
            label="namespace create source",
        )
        assert create_source["result_code"] == 1300006

        get_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["namespace", "get", namespace_name],
                env_file=live_admin_env_file,
            ),
            expected_action="namespace.get",
            expected_type="not_found",
            label="namespace get missing namespace",
        )
        assert get_error["type"] == "not_found"

        delete_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                ["namespace", "delete", namespace_name, "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="namespace.delete",
            expected_type="not_found",
            label="namespace delete missing namespace",
        )
        assert delete_error["type"] == "not_found"

        grant_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                [
                    "user",
                    "grant",
                    "namespace",
                    user_name,
                    "--namespace",
                    namespace_name,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="user.grant.namespace",
            expected_type="not_found",
            label="user grant namespace missing namespace",
        )
        assert grant_error["type"] == "not_found"

        revoke_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                [
                    "user",
                    "revoke",
                    "namespace",
                    user_name,
                    "--namespace",
                    namespace_name,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="user.revoke.namespace",
            expected_type="not_found",
            label="user revoke namespace missing namespace",
        )
        assert revoke_error["type"] == "not_found"
    finally:
        run_dsctl(
            live_repo_root,
            ["cluster", "delete", str(cluster_code), "--force"],
            env_file=live_admin_env_file,
        )
