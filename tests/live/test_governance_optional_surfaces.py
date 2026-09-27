from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from dsctl.upstream.datasource_contracts import normalize_datasource_type
from tests.live.support import (
    DsctlCommandResult,
    LiveBootstrapState,
    cleanup_live_resources,
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


def test_admin_alert_plugin_test_send_reports_success(
    live_repo_root: Path,
    live_admin_env_file: Path,
) -> None:
    """Exercise test-send only with an explicitly enabled, configured instance."""
    if _optional_env_text("DSCTL_RUN_LIVE_ALERT_TESTS") != "1":
        pytest.skip("Alert test-send requires DSCTL_RUN_LIVE_ALERT_TESTS=1")
    instance = _optional_env_text("DS_LIVE_ALERT_TEST_INSTANCE")
    if instance is None:
        pytest.fail("Set DS_LIVE_ALERT_TEST_INSTANCE to a dedicated test instance")
    payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["alert-plugin", "test", instance],
            env_file=live_admin_env_file,
        ),
        expected_action="alert-plugin.test",
        label="configured alert-plugin test-send",
    )
    data = require_mapping(payload["data"], label="alert-plugin test-send data")
    assert data["tested"] is True


def _cleanup_owned_governance_resource(
    live_repo_root: Path,
    live_admin_env_file: Path,
    *,
    command: str,
    selector: str,
) -> None:
    result = run_dsctl(
        live_repo_root,
        [command, "delete", selector, "--force"],
        env_file=live_admin_env_file,
    )
    _require_owned_governance_resource_absent(
        live_repo_root,
        live_admin_env_file,
        command=command,
        selector=selector,
        delete_result=result,
    )


def _cleanup_owned_datasource(
    live_repo_root: Path,
    live_admin_env_file: Path,
    *,
    selector: str,
) -> None:
    result = run_dsctl(
        live_repo_root,
        ["datasource", "delete", selector, "--force"],
        env_file=live_admin_env_file,
    )
    _require_owned_governance_resource_absent(
        live_repo_root,
        live_admin_env_file,
        command="datasource",
        selector=selector,
        delete_result=result,
    )


def _require_owned_governance_resource_absent(
    live_repo_root: Path,
    live_admin_env_file: Path,
    *,
    command: str,
    selector: str,
    delete_result: DsctlCommandResult,
) -> None:
    action = f"{command}.delete"
    label = f"owned {command} cleanup"
    if delete_result.exit_code == 0:
        payload = require_ok_payload(
            delete_result,
            expected_action=action,
            label=label,
        )
        data = require_mapping(payload["data"], label=f"{label} data")
        assert data["deleted"] is True
    else:
        require_error_payload(
            delete_result,
            expected_action=action,
            expected_type="not_found",
            label=f"{label} already absent",
        )
    require_error_payload(
        run_dsctl(
            live_repo_root,
            [command, "get", selector],
            env_file=live_admin_env_file,
        ),
        expected_action=f"{command}.get",
        expected_type="not_found",
        label=f"{label} absence proof",
    )


def _revoke_owned_datasource_grant(
    live_repo_root: Path,
    live_admin_env_file: Path,
    *,
    user_name: str,
    datasource: str,
    datasource_id: int,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            [
                "user",
                "revoke",
                "datasource",
                user_name,
                "--datasource",
                datasource,
            ],
            env_file=live_admin_env_file,
        ),
        expected_action="user.revoke.datasource",
        label="owned datasource grant cleanup",
    )
    data = require_mapping(payload["data"], label="datasource revoke data")
    remaining = require_list(data["datasources"], label="remaining datasources")
    assert all(
        require_mapping(item, label="remaining datasource").get("id") != datasource_id
        for item in remaining
    )


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
    version_payload = require_ok_payload(
        run_dsctl(live_repo_root, ["version"], env_file=live_admin_env_file),
        expected_action="version",
        label="datasource selected version",
    )
    version_data = require_mapping(version_payload["data"], label="version data")
    selected_version = version_data.get("selected_ds_version")
    assert isinstance(selected_version, str), (
        "Datasource test requires an exact DS version"
    )
    expected_type = normalize_datasource_type(selected_version, datasource_config.type)
    assert expected_type is not None, (
        f"{LIVE_DATASOURCE_TYPE_ENV}={datasource_config.type!r} is not supported "
        f"by the exact {selected_version} datasource contract"
    )
    initial_name = live_name_factory("datasource")
    updated_name = live_name_factory("datasource-updated")
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["datasource", "get", initial_name],
            env_file=live_admin_env_file,
        ),
        expected_action="datasource.get",
        expected_type="not_found",
        label="owned datasource name preflight",
    )
    create_file = _write_json(
        tmp_path / f"{initial_name}.json",
        _datasource_payload(
            datasource_config,
            name=initial_name,
            note="live datasource create path",
            password=datasource_config.password,
        ),
    )

    grant_attempted = False
    revoke_attempted = False
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
        assert create_data["type"] == expected_type

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
        assert get_data["type"] == expected_type
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
        assert refreshed_data["type"] == expected_type
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

        grant_attempted = True
        grant_result = run_dsctl(
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
        )
        grant_payload = require_ok_payload(
            grant_result,
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

        revoke_attempted = True
        _revoke_owned_datasource_grant(
            live_repo_root,
            live_admin_env_file,
            user_name=user_name,
            datasource=str(datasource_id),
            datasource_id=datasource_id,
        )

    finally:
        cleanup_tasks: list[Callable[[], None]] = []
        if grant_attempted and not revoke_attempted and datasource_id is not None:
            cleanup_tasks.append(
                lambda: _revoke_owned_datasource_grant(
                    live_repo_root,
                    live_admin_env_file,
                    user_name=user_name,
                    datasource=str(datasource_id),
                    datasource_id=datasource_id,
                )
            )
        cleanup_tasks.append(
            lambda: _cleanup_owned_datasource(
                live_repo_root,
                live_admin_env_file,
                selector=str(datasource_id)
                if datasource_id is not None
                else initial_name,
            )
        )
        cleanup_live_resources(cleanup_tasks)


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
    alert_group_id: int | None = None
    plugin_cleanup_selector = plugin_name
    group_cleanup_selector = group_name

    definition_list_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["alert-plugin", "definition", "list"],
            env_file=live_admin_env_file,
        ),
        expected_action="alert-plugin.definition.list",
        label="alert-plugin definition list",
    )
    definition_list_data = require_mapping(
        definition_list_payload["data"],
        label="alert-plugin definition list data",
    )
    definition_rows = require_list(
        definition_list_data["definitions"],
        label="alert-plugin definition rows",
    )
    assert any(
        require_mapping(row, label="alert-plugin definition row").get("pluginName")
        == "Script"
        for row in definition_rows
    )

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
        plugin_cleanup_selector = str(alert_plugin_id)
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
        alert_group_id = require_int_value(
            group_create_data.get("id"),
            label="alert-group id",
        )
        group_cleanup_selector = str(alert_group_id)
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

        group_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["alert-group", "delete", str(alert_group_id), "--force"],
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
                ["alert-plugin", "delete", str(alert_plugin_id), "--force"],
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
        cleanup_tasks: list[Callable[[], None]] = []
        if current_group_name and not alert_group_deleted:
            cleanup_tasks.append(
                lambda: _cleanup_owned_governance_resource(
                    live_repo_root,
                    live_admin_env_file,
                    command="alert-group",
                    selector=group_cleanup_selector,
                )
            )
        if current_plugin_name and not alert_plugin_deleted:
            cleanup_tasks.append(
                lambda: _cleanup_owned_governance_resource(
                    live_repo_root,
                    live_admin_env_file,
                    command="alert-plugin",
                    selector=plugin_cleanup_selector,
                )
            )
        cleanup_live_resources(cleanup_tasks)


def test_admin_namespace_read_surfaces_decode_supported_shapes(
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


def test_admin_namespace_missing_k8s_probe(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_name_factory: Callable[[str], str],
) -> None:
    cluster_name = live_name_factory("namespace-cluster")
    namespace_name = live_name_factory("namespace")
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["cluster", "get", cluster_name],
            env_file=live_admin_env_file,
        ),
        expected_action="cluster.get",
        expected_type="not_found",
        label="owned cluster name preflight",
    )
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["namespace", "get", namespace_name],
            env_file=live_admin_env_file,
        ),
        expected_action="namespace.get",
        expected_type="not_found",
        label="owned namespace name preflight",
    )
    inert_config = {
        "k8s": "apiVersion: v1\nkind: Config\nclusters: []\n",
        "yarn": "",
    }
    cluster_code: int | None = None
    namespace_create_attempted = False
    namespace_probe_absent = False
    try:
        cluster_result = run_dsctl(
            live_repo_root,
            [
                "cluster",
                "create",
                "--name",
                cluster_name,
                "--config",
                json.dumps(inert_config),
                "--description",
                "namespace capability probe cluster",
            ],
            env_file=live_admin_env_file,
        )
        cluster_payload = require_ok_payload(
            cluster_result,
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
        # Upstream checks Kubernetes before inserting the DS record.
        # Read back the deliberately unresolvable kubeconfig before that call.
        cluster_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["cluster", "get", str(cluster_code)],
                env_file=live_admin_env_file,
            ),
            expected_action="cluster.get",
            label="namespace probe cluster preflight",
        )
        cluster_get_data = require_mapping(
            cluster_get_payload["data"],
            label="namespace probe cluster preflight data",
        )
        stored_config = cluster_get_data.get("config")
        assert isinstance(stored_config, str), "Cluster config must be JSON text"
        assert json.loads(stored_config) == inert_config
        namespace_create_attempted = True
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
        namespace_probe_absent = True
    finally:
        cleanup_tasks: list[Callable[[], None]] = []
        if namespace_create_attempted and not namespace_probe_absent:
            cleanup_tasks.append(
                lambda: _cleanup_owned_governance_resource(
                    live_repo_root,
                    live_admin_env_file,
                    command="namespace",
                    selector=namespace_name,
                )
            )
        cleanup_tasks.append(
            lambda: _cleanup_owned_governance_resource(
                live_repo_root,
                live_admin_env_file,
                command="cluster",
                selector=str(cluster_code)
                if cluster_code is not None
                else cluster_name,
            )
        )
        cleanup_live_resources(cleanup_tasks)


def test_admin_namespace_missing_resource_errors(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_bootstrap_state: LiveBootstrapState,
    live_name_factory: Callable[[str], str],
) -> None:
    user_name = live_bootstrap_state.user_name
    if user_name is None:
        pytest.skip("Namespace grant live test requires one managed ETL user.")
    namespace_name = live_name_factory("namespace-missing")

    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["namespace", "get", namespace_name],
            env_file=live_admin_env_file,
        ),
        expected_action="namespace.get",
        expected_type="not_found",
        label="namespace get missing namespace",
    )
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["namespace", "delete", namespace_name, "--force"],
            env_file=live_admin_env_file,
        ),
        expected_action="namespace.delete",
        expected_type="not_found",
        label="namespace delete missing namespace",
    )
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["user", "grant", "namespace", user_name, "--namespace", namespace_name],
            env_file=live_admin_env_file,
        ),
        expected_action="user.grant.namespace",
        expected_type="not_found",
        label="user grant namespace missing namespace",
    )
    require_error_payload(
        run_dsctl(
            live_repo_root,
            ["user", "revoke", "namespace", user_name, "--namespace", namespace_name],
            env_file=live_admin_env_file,
        ),
        expected_action="user.revoke.namespace",
        expected_type="not_found",
        label="user revoke namespace missing namespace",
    )
