from __future__ import annotations

import os
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    future_expire_time,
    require_list,
    require_mapping,
    require_ok_payload,
    run_dsctl,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tests.live.support import LiveProfileConfig


pytestmark = [pytest.mark.live]

_MONITOR_DATABASE_TYPE_ENV = "DS_LIVE_MONITOR_DATABASE_TYPE"
_NATIVE_DATE_ACCESS_TOKEN_VERSIONS = frozenset(
    {
        "1.3.9",
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
    }
)
_AUDIT_LIST_WITHOUT_METADATA_VERSIONS = frozenset(
    {
        *(f"3.0.{patch}" for patch in range(7)),
        *(f"3.1.{patch}" for patch in range(10)),
        "3.2.0",
        "3.2.1",
    }
)


def _monitor_database_type(environ: Mapping[str, str]) -> str:
    value = environ.get(_MONITOR_DATABASE_TYPE_ENV, "").strip().upper()
    if not value:
        message = f"Set {_MONITOR_DATABASE_TYPE_ENV} to the cluster metadata DB type."
        raise ValueError(message)
    return value


def _audit_rows(data: object) -> list[object]:
    page = require_mapping(data, label="audit list data")
    rows = require_list(page["totalList"], label="audit list rows")
    total = page.get("total")
    if not isinstance(total, int) or isinstance(total, bool) or total < len(rows):
        message = "audit list total must be a non-negative integer covering its rows"
        raise AssertionError(message)
    for item in rows:
        row = require_mapping(item, label="audit row")
        assert row["modelType"]
        assert row["operation"]
        assert row["userName"]
    return rows


def _validated_access_token_expire_time(
    observed: object,
    *,
    requested: str,
    ds_version: str,
) -> str:
    if not isinstance(observed, str) or observed == "":
        message = "access-token readback did not return an expiration string"
        raise AssertionError(message)
    if ds_version not in _NATIVE_DATE_ACCESS_TOKEN_VERSIONS:
        assert observed == requested
        return observed
    try:
        parsed = datetime.fromisoformat(observed)
    except ValueError as error:
        message = "legacy access-token readback was not a parseable native date"
        raise AssertionError(message) from error
    if parsed.utcoffset() is None:
        message = "legacy access-token native date did not include an offset"
        raise AssertionError(message)
    return observed


def _complete_access_token_rows(data: object) -> list[object]:
    page = require_mapping(data, label="access-token list data")
    rows = require_list(page["totalList"], label="access-token list rows")
    coverage = require_mapping(page["coverage"], label="access-token list coverage")
    assert coverage["scope_complete"] is True
    assert coverage["totals_changed"] is False
    total = page.get("total")
    if not isinstance(total, int) or isinstance(total, bool):
        message = "access-token list total was not an integer"
        raise TypeError(message)
    assert total == len(rows)
    return rows


@pytest.fixture
def live_monitor_database_type() -> str:
    try:
        return _monitor_database_type(os.environ)
    except ValueError as error:
        pytest.fail(str(error))


@pytest.mark.live_admin
def test_admin_monitor_surfaces_return_live_cluster_state(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_monitor_database_type: str,
) -> None:
    server_result = run_dsctl(
        live_repo_root,
        ["monitor", "server", "master"],
        env_file=live_admin_env_file,
    )
    server_payload = require_ok_payload(
        server_result,
        expected_action="monitor.server",
        label="monitor server",
    )
    server_data = require_list(server_payload["data"], label="monitor server data")
    assert server_data
    first_server = require_mapping(server_data[0], label="monitor server item")
    assert first_server["host"]
    assert first_server["port"]

    database_result = run_dsctl(
        live_repo_root,
        ["monitor", "database"],
        env_file=live_admin_env_file,
    )
    database_payload = require_ok_payload(
        database_result,
        expected_action="monitor.database",
        label="monitor database",
    )
    database_data = require_list(
        database_payload["data"],
        label="monitor database data",
    )
    assert database_data
    first_database = require_mapping(
        database_data[0],
        label="monitor database item",
    )
    assert first_database["dbType"] == live_monitor_database_type
    assert first_database["state"] in {"YES", "NO"}


@pytest.mark.live_admin
def test_admin_audit_surfaces_expose_real_metadata_and_rows(
    live_repo_root: Path,
    live_admin_env_file: Path,
) -> None:
    model_types_result = run_dsctl(
        live_repo_root,
        ["audit", "model-types"],
        env_file=live_admin_env_file,
    )
    model_types_payload = require_ok_payload(
        model_types_result,
        expected_action="audit.model-types",
        label="audit model-types",
    )
    model_types = require_list(
        model_types_payload["data"],
        label="audit model-types data",
    )
    assert any(
        require_mapping(item, label="audit model-type item").get("name") == "Project"
        for item in model_types
    )

    operation_types_result = run_dsctl(
        live_repo_root,
        ["audit", "operation-types"],
        env_file=live_admin_env_file,
    )
    operation_types_payload = require_ok_payload(
        operation_types_result,
        expected_action="audit.operation-types",
        label="audit operation-types",
    )
    operation_types = require_list(
        operation_types_payload["data"],
        label="audit operation-types data",
    )
    operation_names = {
        require_mapping(item, label="audit operation-type item")["name"]
        for item in operation_types
    }
    assert {"Create", "Delete"}.issubset(operation_names)

    list_result = run_dsctl(
        live_repo_root,
        ["audit", "list", "--page-size", "5"],
        env_file=live_admin_env_file,
    )
    list_payload = require_ok_payload(
        list_result,
        expected_action="audit.list",
        label="audit list",
    )
    _audit_rows(list_payload["data"])


@pytest.mark.live_admin
def test_admin_audit_list_without_metadata_endpoints(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
) -> None:
    if live_admin_profile.ds_version not in _AUDIT_LIST_WITHOUT_METADATA_VERSIONS:
        pytest.skip("Standalone audit.list applies to exact DS 3.0.0-3.2.1 profiles")

    list_payload = require_ok_payload(
        run_dsctl(
            live_repo_root,
            ["audit", "list", "--page-size", "5"],
            env_file=live_admin_env_file,
        ),
        expected_action="audit.list",
        label="audit list without metadata endpoints",
    )
    _audit_rows(list_payload["data"])


@pytest.mark.live_admin
@pytest.mark.destructive
def test_admin_access_token_lifecycle_round_trips(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_admin_profile: LiveProfileConfig,
) -> None:
    ds_version = live_admin_profile.ds_version
    if ds_version is None:
        message = "access-token lifecycle requires an exact DS_VERSION"
        raise AssertionError(message)
    create_expire_time = future_expire_time(days=30)
    update_expire_time = future_expire_time(days=60)
    generate_expire_time = future_expire_time(days=90)

    create_result = run_dsctl(
        live_repo_root,
        [
            "access-token",
            "create",
            "--user",
            "admin",
            "--expire-time",
            create_expire_time,
        ],
        env_file=live_admin_env_file,
    )
    create_payload = require_ok_payload(
        create_result,
        expected_action="access-token.create",
        label="access-token create",
    )
    create_data = require_mapping(
        create_payload["data"],
        label="access-token create data",
    )
    token_id = create_data.get("id")
    if not isinstance(token_id, int):
        message = "access-token create did not return an integer id"
        raise TypeError(message)

    try:
        original_token = create_data.get("token")
        if not isinstance(original_token, str) or original_token == "":
            message = "access-token create did not return a token string"
            raise AssertionError(message)
        created_native_expire_time = _validated_access_token_expire_time(
            create_data.get("expireTime"),
            requested=create_expire_time,
            ds_version=ds_version,
        )

        get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["access-token", "get", str(token_id)],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.get",
            label="access-token get",
        )
        get_data = require_mapping(get_payload["data"], label="access-token get data")
        assert get_data["id"] == token_id
        assert get_data["token"] == original_token
        assert get_data["expireTime"] == created_native_expire_time

        list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["access-token", "list", "--all"],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.list",
            label="access-token list",
        )
        rows = _complete_access_token_rows(list_payload["data"])
        assert any(
            require_mapping(item, label="access-token list row").get("id") == token_id
            for item in rows
        )

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "access-token",
                    "update",
                    str(token_id),
                    "--expire-time",
                    update_expire_time,
                    "--regenerate-token",
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.update",
            label="access-token update",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="access-token update data",
        )
        updated_token = update_data.get("token")
        if not isinstance(updated_token, str) or updated_token == "":
            message = "access-token update did not return a regenerated token string"
            raise AssertionError(message)
        assert updated_token != original_token
        updated_native_expire_time = _validated_access_token_expire_time(
            update_data.get("expireTime"),
            requested=update_expire_time,
            ds_version=ds_version,
        )
        assert updated_native_expire_time != created_native_expire_time

        refreshed_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["access-token", "get", str(token_id)],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.get",
            label="access-token get after update",
        )
        refreshed_data = require_mapping(
            refreshed_payload["data"],
            label="access-token refreshed data",
        )
        assert refreshed_data["token"] == updated_token
        assert refreshed_data["expireTime"] == updated_native_expire_time

        generate_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "access-token",
                    "generate",
                    "--user",
                    "admin",
                    "--expire-time",
                    generate_expire_time,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.generate",
            label="access-token generate",
        )
        generate_data = require_mapping(
            generate_payload["data"],
            label="access-token generate data",
        )
        generated_token = generate_data.get("token")
        if not isinstance(generated_token, str) or generated_token == "":
            message = "access-token generate did not return a token string"
            raise AssertionError(message)
        assert generate_data["expireTime"] == generate_expire_time
        assert generated_token not in {original_token, updated_token}
    finally:
        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["access-token", "delete", str(token_id), "--force"],
                env_file=live_admin_env_file,
            ),
            expected_action="access-token.delete",
            label="access-token delete",
        )
        delete_data = require_mapping(
            delete_payload["data"],
            label="access-token delete data",
        )
        assert delete_data["deleted"] is True
