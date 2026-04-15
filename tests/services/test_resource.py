from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
from tests.fakes import (
    FakeProjectAdapter,
    FakeResourceAdapter,
    FakeResourceItem,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import NotFoundError, UserInputError
from dsctl.services import resource as resource_service
from dsctl.services import runtime as runtime_service


def _install_resource_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeResourceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            resource_adapter=adapter,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def _resource_adapter() -> FakeResourceAdapter:
    return FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="demo.sql",
                file_name_value="demo.sql",
                full_name_value="/tenant/resources/demo.sql",
                is_directory_value=False,
                size=20,
            ),
            FakeResourceItem(
                alias="scripts",
                file_name_value="scripts",
                full_name_value="/tenant/resources/scripts",
                is_directory_value=True,
            ),
            FakeResourceItem(
                alias="job.py",
                file_name_value="job.py",
                full_name_value="/tenant/resources/scripts/job.py",
                is_directory_value=False,
                size=22,
            ),
        ],
        contents_by_full_name={
            "/tenant/resources/demo.sql": b"select 1;\nselect 2;\n",
            "/tenant/resources/scripts/job.py": b"print('a')\nprint('b')\n",
        },
        base_dir_value="/tenant/resources",
    )


def test_list_resources_result_uses_base_dir_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    result = resource_service.list_resources_result(page_size=10)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved == {
        "directory": "/tenant/resources",
        "search": None,
        "page_no": 1,
        "page_size": 10,
        "all": False,
    }
    assert {
        "total": data["total"],
        "totalPage": data["totalPage"],
        "pageSize": data["pageSize"],
        "currentPage": data["currentPage"],
        "pageNo": data["pageNo"],
    } == {
        "total": 2,
        "totalPage": 1,
        "pageSize": 10,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert list(items) == [
        {
            "alias": "demo.sql",
            "userName": None,
            "fileName": "demo.sql",
            "fullName": "/tenant/resources/demo.sql",
            "isDirectory": False,
            "type": "FILE",
            "size": 20,
            "createTime": None,
            "updateTime": None,
        },
        {
            "alias": "scripts",
            "userName": None,
            "fileName": "scripts",
            "fullName": "/tenant/resources/scripts",
            "isDirectory": True,
            "type": "FILE",
            "size": 0,
            "createTime": None,
            "updateTime": None,
        },
    ]


def test_view_resource_result_returns_requested_content_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    result = resource_service.view_resource_result(
        "/tenant/resources/demo.sql",
        skip_line_num=1,
        limit=1,
    )

    assert result.resolved == {
        "resource": {
            "fullName": "/tenant/resources/demo.sql",
            "name": "demo.sql",
            "directory": "/tenant/resources",
            "isDirectory": False,
        },
        "skip_line_num": 1,
        "limit": 1,
    }
    assert result.data == {"content": "select 2;"}


def test_upload_resource_result_uploads_local_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)
    local_file = tmp_path / "upload.sql"
    local_file.write_text("select 42;\n", encoding="utf-8")

    result = resource_service.upload_resource_result(file=local_file)

    assert result.resolved == {
        "resource": {
            "fullName": "/tenant/resources/upload.sql",
            "name": "upload.sql",
            "directory": "/tenant/resources",
            "isDirectory": False,
        },
        "source_file": str(local_file),
    }
    assert result.data == {
        "alias": "upload.sql",
        "userName": None,
        "fileName": "upload.sql",
        "fullName": "/tenant/resources/upload.sql",
        "isDirectory": False,
        "type": "FILE",
        "size": 11,
        "createTime": None,
        "updateTime": None,
    }


def test_create_and_mkdir_resource_results_return_created_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    created = resource_service.create_resource_result(
        name="notes.txt",
        content="hello",
    )
    directory = resource_service.mkdir_resource_result(name="archive")

    assert created.data == {
        "alias": "notes.txt",
        "userName": None,
        "fileName": "notes.txt",
        "fullName": "/tenant/resources/notes.txt",
        "isDirectory": False,
        "type": "FILE",
        "size": 5,
        "createTime": None,
        "updateTime": None,
    }
    assert directory.data == {
        "alias": "archive",
        "userName": None,
        "fileName": "archive",
        "fullName": "/tenant/resources/archive",
        "isDirectory": True,
        "type": "FILE",
        "size": 0,
        "createTime": None,
        "updateTime": None,
    }


def test_create_resource_result_rejects_empty_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    with pytest.raises(UserInputError, match="must not be empty") as exc_info:
        resource_service.create_resource_result(name="notes.txt", content="")

    assert exc_info.value.suggestion == (
        "Pass non-empty inline content, or use `resource upload` if the content "
        "already lives in a local file."
    )


def test_download_resource_result_writes_binary_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    result = resource_service.download_resource_result(
        "/tenant/resources/demo.sql",
        output=tmp_path,
        overwrite=False,
    )
    saved_path = tmp_path / "demo.sql"

    assert saved_path.read_bytes() == b"select 1;\nselect 2;\n"
    assert result.data == {
        "fullName": "/tenant/resources/demo.sql",
        "saved_to": str(saved_path.resolve()),
        "size": 20,
        "content_type": "application/octet-stream",
    }


def test_download_resource_result_rejects_existing_output_without_overwrite(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)
    output_path = tmp_path / "demo.sql"
    output_path.write_text("existing", encoding="utf-8")

    with pytest.raises(UserInputError, match="--overwrite") as exc_info:
        resource_service.download_resource_result(
            "/tenant/resources/demo.sql",
            output=output_path,
            overwrite=False,
        )

    assert exc_info.value.suggestion == (
        "Retry with --overwrite, or choose a different --output path."
    )


def test_delete_resource_result_requires_force() -> None:
    with pytest.raises(UserInputError, match="requires --force"):
        resource_service.delete_resource_result(
            "/tenant/resources/demo.sql",
            force=False,
        )


def test_delete_resource_result_returns_deleted_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    result = resource_service.delete_resource_result(
        "/tenant/resources/scripts/",
        force=True,
    )

    assert result.data == {
        "deleted": True,
        "resource": {
            "fullName": "/tenant/resources/scripts",
            "name": "scripts",
            "directory": "/tenant/resources",
            "isDirectory": True,
        },
    }


def test_view_resource_result_maps_missing_resource_to_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _resource_adapter()
    _install_resource_service_fakes(monkeypatch, adapter)

    with pytest.raises(NotFoundError, match=r"missing\.sql"):
        resource_service.view_resource_result("/tenant/resources/missing.sql")
