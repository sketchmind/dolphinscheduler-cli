"""In-memory resources collaborators with explicit test-owned state."""

from __future__ import annotations

from dataclasses import dataclass, field

from dsctl.client import BinaryResponse
from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)


@dataclass(frozen=True)
class FakeResourceItem:
    alias: str | None
    full_name_value: str | None
    is_directory_value: bool
    id_value: int | None = None
    size: int = 0
    user_name_value: str | None = None
    file_name_value: str | None = None
    type_value: FakeEnumValue | None = field(
        default_factory=lambda: FakeEnumValue("FILE")
    )
    create_time_value: str | None = None
    update_time_value: str | None = None

    @property
    def id(self) -> int | None:
        return self.id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def fileName(self) -> str | None:  # noqa: N802
        return self.file_name_value

    @property
    def fullName(self) -> str | None:  # noqa: N802
        return self.full_name_value

    @property
    def isDirectory(self) -> bool:  # noqa: N802
        return self.is_directory_value

    @property
    def type(self) -> FakeEnumValue | None:
        return self.type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value


@dataclass(frozen=True)
class FakeResourcePage(_FakePage[FakeResourceItem]):
    pass


@dataclass(frozen=True)
class FakeResourceContent:
    content: str | None


@dataclass(frozen=True)
class FakeTaskResourceFileResolution:
    resource_id: int | None
    wire_full_name: str


@dataclass
class FakeResourceAdapter:
    resources: list[FakeResourceItem]
    contents_by_full_name: dict[str, bytes] = field(default_factory=dict)
    base_dir_value: str = "/tenant/resources"

    def resolve_id(self, full_name: str) -> int:
        item = self._require_item(full_name)
        if item.isDirectory or item.id is None or item.id <= 0:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        return item.id

    def resolve_task_file(self, full_name: str) -> FakeTaskResourceFileResolution:
        return FakeTaskResourceFileResolution(
            resource_id=self.resolve_id(full_name),
            wire_full_name=full_name,
        )

    def resolve_full_name(self, resource_id: int) -> str:
        for item in self.resources:
            if item.id == resource_id and not item.isDirectory and item.fullName:
                return item.fullName
        raise ApiResultError(
            result_code=20004,
            result_message=f"resource id {resource_id} not found",
        )

    def base_dir(self) -> str:
        return self.base_dir_value

    def list(
        self,
        *,
        directory: str,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeResourcePage:
        normalized_directory = _normalize_resource_path(directory)
        filtered = [
            resource
            for resource in self.resources
            if _resource_parent(resource.fullName) == normalized_directory
        ]
        if search is not None:
            needle = search.lower()
            filtered = [
                resource
                for resource in filtered
                if resource.fileName is not None and needle in resource.fileName.lower()
            ]
        return FakeResourcePage.from_items(
            filtered,
            page_no=page_no,
            page_size=page_size,
        )

    def view(
        self,
        *,
        full_name: str,
        skip_line_num: int,
        limit: int,
    ) -> FakeResourceContent:
        item = self._require_item(full_name)
        if item.isDirectory:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        content_bytes = self.contents_by_full_name.get(
            _normalize_resource_path(full_name)
        )
        if content_bytes is None:
            raise ApiResultError(
                result_code=20012,
                result_message=f"resource file {full_name} not found",
            )
        text = content_bytes.decode("utf-8", errors="replace")
        lines = text.splitlines()
        if not lines:
            return FakeResourceContent(content=text)
        selected = lines[skip_line_num : skip_line_num + limit]
        return FakeResourceContent(content="\n".join(selected))

    def upload(
        self,
        *,
        current_dir: str,
        name: str,
        file: object,
    ) -> None:
        if not hasattr(file, "read"):
            message = "upload file must provide read()"
            raise TypeError(message)
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, name)
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20011,
                result_message=f"resource file {full_name} already exists",
            )
        file_bytes = file.read()
        if not isinstance(file_bytes, bytes):
            message = "upload file read() must return bytes"
            raise TypeError(message)
        self.resources.append(
            FakeResourceItem(
                alias=name,
                file_name_value=name,
                full_name_value=full_name,
                is_directory_value=False,
                size=len(file_bytes),
            )
        )
        self.contents_by_full_name[_normalize_resource_path(full_name)] = file_bytes

    def create_from_content(
        self,
        *,
        current_dir: str,
        file_name: str,
        suffix: str,
        content: str,
    ) -> None:
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, f"{file_name}.{suffix}")
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20011,
                result_message=f"resource file {full_name} already exists",
            )
        self.resources.append(
            FakeResourceItem(
                alias=f"{file_name}.{suffix}",
                file_name_value=f"{file_name}.{suffix}",
                full_name_value=full_name,
                is_directory_value=False,
                size=len(content.encode("utf-8")),
            )
        )
        self.contents_by_full_name[_normalize_resource_path(full_name)] = (
            content.encode("utf-8")
        )

    def create_directory(self, *, current_dir: str, name: str) -> None:
        self._require_directory(current_dir)
        full_name = _join_resource_path(current_dir, name)
        if self._find_item(full_name) is not None:
            raise ApiResultError(
                result_code=20005,
                result_message=f"resource {full_name} already exists",
            )
        self.resources.append(
            FakeResourceItem(
                alias=name,
                file_name_value=name,
                full_name_value=full_name,
                is_directory_value=True,
            )
        )

    def delete(self, *, full_name: str) -> bool:
        normalized_full_name = _normalize_resource_path(full_name)
        item = self._find_item(normalized_full_name)
        if item is None:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        prefix = f"{normalized_full_name}/"
        self.resources = [
            resource
            for resource in self.resources
            if resource.fullName != normalized_full_name
            and (resource.fullName is None or not resource.fullName.startswith(prefix))
        ]
        removable = [
            path
            for path in self.contents_by_full_name
            if path == normalized_full_name or path.startswith(prefix)
        ]
        for path in removable:
            self.contents_by_full_name.pop(path, None)
        return True

    def download(self, *, full_name: str) -> BinaryResponse:
        item = self._require_item(full_name)
        if item.isDirectory:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        normalized_full_name = _normalize_resource_path(full_name)
        content = self.contents_by_full_name.get(normalized_full_name)
        if content is None:
            raise ApiResultError(
                result_code=20012,
                result_message=f"resource file {full_name} not found",
            )
        return BinaryResponse(
            content=content,
            headers={"content-type": "application/octet-stream"},
            content_type="application/octet-stream",
        )

    def _find_item(self, full_name: str) -> FakeResourceItem | None:
        normalized_full_name = _normalize_resource_path(full_name)
        for resource in self.resources:
            if resource.fullName == normalized_full_name:
                return resource
        return None

    def _require_item(self, full_name: str) -> FakeResourceItem:
        resource = self._find_item(full_name)
        if resource is None:
            raise ApiResultError(
                result_code=20004,
                result_message=f"resource {full_name} not found",
            )
        return resource

    def _require_directory(self, full_name: str) -> None:
        normalized_full_name = _normalize_resource_path(full_name)
        if normalized_full_name == _normalize_resource_path(self.base_dir_value):
            return
        resource = self._find_item(normalized_full_name)
        if resource is None or not resource.isDirectory:
            raise ApiResultError(
                result_code=20015,
                result_message=f"parent resource {full_name} not found",
            )


def _normalize_resource_path(value: str | None) -> str:
    if value is None:
        return ""
    normalized = value.strip()
    if normalized == "/":
        return normalized
    return normalized.rstrip("/")


def _resource_parent(full_name: str | None) -> str | None:
    normalized = _normalize_resource_path(full_name)
    if not normalized or "/" not in normalized:
        return None
    parent = normalized.rsplit("/", 1)[0]
    return parent or "/"


def _join_resource_path(directory: str, name: str) -> str:
    normalized_directory = _normalize_resource_path(directory)
    if normalized_directory == "/":
        return f"/{name}"
    return f"{normalized_directory}/{name}"


def empty_resource_adapter() -> FakeResourceAdapter:
    return FakeResourceAdapter(resources=[])
